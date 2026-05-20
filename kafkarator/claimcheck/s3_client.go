package claimcheck

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	// envS3Endpoint and envSTSEndpoint are the standard AWS SDK environment
	// variables injected by the Happi operator to point clients at the
	// cluster-local Ceph RadosGW endpoint. boto3/botocore reads them
	// automatically; we do the same here.
	envS3Endpoint  = "AWS_ENDPOINT_URL_S3"
	envSTSEndpoint = "AWS_ENDPOINT_URL_STS"
)

// newS3Client creates an S3Client for use on Happi. It:
//  1. Uses the provided system and env to build the role ARN.
//  2. On first use, exchanges the Happi IDP token for a short-lived S3 token
//     and writes it to AWS_WEB_IDENTITY_TOKEN_FILE.
//  3. Assumes the role via AssumeRoleWithWebIdentity, refreshing as needed.
//
// access should be "rw" for writers or "r" for readers.
// exchanger may be nil, in which case a new one is created from env vars.
func newS3Client(bucket, access, system, env string, exchanger *tokenExchanger) (S3Client, error) {
	roleARN, err := claimCheckRoleARN(system, env, bucket, access)
	if err != nil {
		return nil, err
	}

	if exchanger == nil {
		exchanger, err = newTokenExchanger()
		if err != nil {
			return nil, err
		}
	}

	return &s3Client{
		exchanger:    exchanger,
		roleARN:      roleARN,
		awsTokenFile: exchanger.awsTokenFile,
		s3Endpoint:   os.Getenv(envS3Endpoint),
		stsEndpoint:  os.Getenv(envSTSEndpoint),
	}, nil
}

// s3Client wraps the AWS SDK S3 client and implements S3Client.
// It exchanges the Happi IDP token before each operation.
type s3Client struct {
	exchanger    *tokenExchanger
	roleARN      string
	awsTokenFile string
	s3Endpoint   string
	stsEndpoint  string

	mu     sync.Mutex
	client *awss3.Client
}

func (p *s3Client) awsClient(ctx context.Context) (*awss3.Client, error) {
	if err := p.exchanger.ensureFreshToken(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}

	// Build STS client options. AWS_ENDPOINT_URL_STS (or opts.STSEndpoint) is
	// respected here so that the AssumeRoleWithWebIdentity call is directed at
	// the cluster-local Ceph RadosGW STS endpoint rather than AWS STS.
	var stsOpts []func(*sts.Options)
	if p.stsEndpoint != "" {
		ep := p.stsEndpoint
		stsOpts = append(stsOpts, func(o *sts.Options) {
			o.BaseEndpoint = &ep
		})
	}
	stsClient := sts.New(sts.Options{}, stsOpts...)
	provider := stscreds.NewWebIdentityRoleProvider(
		stsClient,
		p.roleARN,
		stscreds.IdentityTokenFile(p.awsTokenFile),
	)

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(provider),
	)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: load AWS config: %w", err)
	}

	// Apply S3 endpoint override. AWS_ENDPOINT_URL_S3 (or opts.S3Endpoint) is
	// read at construction time and stored in p.s3Endpoint. Path-style
	// addressing is required for Ceph RadosGW and any non-AWS S3 endpoint.
	var s3Opts []func(*awss3.Options)
	if p.s3Endpoint != "" {
		ep := p.s3Endpoint
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = &ep
			o.UsePathStyle = true
		})
	}
	p.client = awss3.NewFromConfig(cfg, s3Opts...)
	return p.client, nil
}

func (p *s3Client) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	c, err := p.awsClient(ctx)
	if err != nil {
		return "", err
	}
	out, err := c.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", fmt.Errorf("claimcheck: CreateMultipartUpload: %w", err)
	}
	return aws.ToString(out.UploadId), nil
}

func (p *s3Client) UploadPart(
	ctx context.Context,
	bucket, key, uploadID string,
	partNumber int,
	body io.Reader,
) (string, error) {
	c, err := p.awsClient(ctx)
	if err != nil {
		return "", err
	}
	num := int32(partNumber) // #nosec G115 — part numbers are small positive ints
	out, err := c.UploadPart(ctx, &awss3.UploadPartInput{
		Bucket:     &bucket,
		Key:        &key,
		UploadId:   &uploadID,
		PartNumber: &num,
		Body:       body,
	})
	if err != nil {
		return "", fmt.Errorf("claimcheck: UploadPart: %w", err)
	}
	return aws.ToString(out.ETag), nil
}

func (p *s3Client) CompleteMultipartUpload(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []CompletedPart,
) error {
	c, err := p.awsClient(ctx)
	if err != nil {
		return err
	}
	awsParts := make([]types.CompletedPart, len(parts))
	for i, pt := range parts {
		n := int32(pt.PartNumber) // #nosec G115
		etag := pt.ETag
		awsParts[i] = types.CompletedPart{PartNumber: &n, ETag: &etag}
	}
	_, err = c.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: awsParts,
		},
	})
	if err != nil {
		return fmt.Errorf("claimcheck: CompleteMultipartUpload: %w", err)
	}
	return nil
}

func (p *s3Client) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	c, err := p.awsClient(ctx)
	if err != nil {
		return err
	}
	_, err = c.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadID,
	})
	// Ignore NoSuchUpload — best-effort cleanup
	if err != nil {
		var noUpload *types.NoSuchUpload
		if isError(err, noUpload) {
			return nil
		}
		return fmt.Errorf("claimcheck: AbortMultipartUpload: %w", err)
	}
	return nil
}

func (p *s3Client) PutObject(ctx context.Context, bucket, key string, body io.Reader) error {
	c, err := p.awsClient(ctx)
	if err != nil {
		return err
	}
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("claimcheck: PutObject read body: %w", err)
	}
	_, err = c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(bodyBytes),
		ContentLength: aws.Int64(int64(len(bodyBytes))),
	})
	if err != nil {
		return fmt.Errorf("claimcheck: PutObject: %w", err)
	}
	return nil
}

func (p *s3Client) GetObject(ctx context.Context, bucket, key string, byteRange *string) (io.ReadCloser, int64, error) {
	c, err := p.awsClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	input := &awss3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	if byteRange != nil {
		input.Range = byteRange
	}
	out, err := c.GetObject(ctx, input)
	if err != nil {
		return nil, 0, fmt.Errorf("claimcheck: GetObject: %w", err)
	}
	return out.Body, aws.ToInt64(out.ContentLength), nil
}

func (p *s3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	c, err := p.awsClient(ctx)
	if err != nil {
		return err
	}
	_, err = c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil && !isError(err, &types.NoSuchKey{}) {
		return fmt.Errorf("claimcheck: DeleteObject: %w", err)
	}
	return nil
}

// defaultS3WriterFactory returns a caching factory that creates a production
// S3Writer for each unique bucket on first use. Called when no WithWriterS3Client
// option is provided to NewWriter.
//
// The provided exchanger is shared across all bucket clients to avoid
// concurrent token exchanges racing on the same awsTokenFile.
func defaultS3WriterFactory(exchanger *tokenExchanger, system, env string) func(bucket string) (S3Writer, error) {
	var (
		mu    sync.Mutex
		cache = map[string]S3Writer{}
	)
	return func(bucket string) (S3Writer, error) {
		mu.Lock()
		defer mu.Unlock()
		if c, ok := cache[bucket]; ok {
			return c, nil
		}
		c, err := newS3Client(bucket, "rw", system, env, exchanger)
		if err != nil {
			return nil, err
		}
		cache[bucket] = c
		return c, nil
	}
}

// defaultS3ReaderFor creates a production S3Reader for the given bucket.
// Called when no WithProcessorS3Client option is provided to NewProcessor.
func defaultS3ReaderFor(bucket, system, env string) (S3Reader, error) {
	return newS3Client(bucket, "r", system, env, nil)
}

// isError is a helper to check for AWS SDK typed errors.
func isError[T error](err error, target T) bool {
	return errors.As(err, &target)
}
