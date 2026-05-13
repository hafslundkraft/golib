package claimcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

const (
	envHappiSystemName = "HAPPI_SYSTEM_NAME"
	envHappiEnv        = "HAPPI_ENV"
	envIDPIssuerURL    = "HAPPI_IDP_ISSUER_URL"
	envIDPTokenFile    = "HAPPI_IDP_TOKEN_FILE"        //nolint:gosec // environment variable name, not a credential
	envAWSTokenFile    = "AWS_WEB_IDENTITY_TOKEN_FILE" //nolint:gosec // environment variable name, not a credential
	// envS3Endpoint and envSTSEndpoint are the standard AWS SDK environment
	// variables injected by the Happi operator to point clients at the
	// cluster-local Ceph RadosGW endpoint. boto3/botocore reads them
	// automatically; we do the same here.
	envS3Endpoint       = "AWS_ENDPOINT_URL_S3"
	envSTSEndpoint      = "AWS_ENDPOINT_URL_STS"
	defaultIDPTokenFile = "/happi/idp-token" //nolint:gosec // well-known path on Happi pods, not a hardcoded credential
	defaultAWSTokenFile = "/tmp/ceph_token"  // #nosec G108 — well-known path on Happi pods
	defaultIDPS3Scope   = "ceph_rgw"

	tokenExpiryMargin      = 60 * time.Second
	tokenExpiryFallback    = 3600 * time.Second
	tokenRequestTimeout    = 10 * time.Second
	tokenExpiresInFallback = 3600
)

// ClaimCheckRoleARN computes the Ceph IAM role ARN for a claim-check bucket.
//
//	arn:aws:iam:::role/happi/{system}/{env}/{bucket}/{system}.{env}.{bucket}.{access}
//
// access is "rw" for writers and "r" for readers.
func claimCheckRoleARN(system, env, bucket, access string) (string, error) {
	var missing []string
	for name, val := range map[string]string{
		"system": system, "env": env, "bucket": bucket, "access": access,
	} {
		if strings.TrimSpace(val) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf(
			"claimCheckRoleARN: required argument(s) empty or whitespace: %s",
			strings.Join(missing, ", "),
		)
	}
	return fmt.Sprintf("arn:aws:iam:::role/happi/%s/%s/%s/%s.%s.%s.%s",
		system, env, bucket, system, env, bucket, access), nil
}

// s3ClientOptions configures newS3Client.
type s3ClientOptions struct {
	// IDPIssuerURL overrides HAPPI_IDP_ISSUER_URL.
	IDPIssuerURL string
	// IDPTokenFile overrides HAPPI_IDP_TOKEN_FILE (source token).
	IDPTokenFile string
	// AWSTokenFile overrides AWS_WEB_IDENTITY_TOKEN_FILE (exchanged token).
	AWSTokenFile string
	// IDPScope is the OAuth scope for the token exchange. Defaults to "ceph_rgw".
	IDPScope string
	// S3Endpoint overrides AWS_ENDPOINT_URL_S3. When set (or when the env
	// var is present), path-style addressing is enabled automatically.
	S3Endpoint string
	// STSEndpoint overrides AWS_ENDPOINT_URL_STS.
	STSEndpoint string
	// Tracer is used to instrument the IDP token exchange. Defaults to a no-op tracer.
	Tracer trace.Tracer
}

// newS3Client creates an S3Client for use on Happi. It:
//  1. Reads HAPPI_SYSTEM_NAME and HAPPI_ENV to build the role ARN.
//  2. Reads HAPPI_IDP_ISSUER_URL (or opts.IDPIssuerURL) to find the IDP.
//  3. On first use, exchanges the Happi IDP token for a short-lived S3 token
//     and writes it to AWS_WEB_IDENTITY_TOKEN_FILE (or opts.AWSTokenFile).
//  4. Assumes the role via AssumeRoleWithWebIdentity, refreshing as needed.
//
// access should be "rw" for writers or "r" for readers.
func newS3Client(bucket, access string, opts *s3ClientOptions) (S3Client, error) {
	system := os.Getenv(envHappiSystemName)
	env := os.Getenv(envHappiEnv)
	if strings.TrimSpace(system) == "" {
		return nil, fmt.Errorf("claimcheck: %s env var is not set", envHappiSystemName)
	}
	if strings.TrimSpace(env) == "" {
		return nil, fmt.Errorf("claimcheck: %s env var is not set", envHappiEnv)
	}

	roleARN, err := claimCheckRoleARN(system, env, bucket, access)
	if err != nil {
		return nil, err
	}

	idpIssuerURL := opts.IDPIssuerURL
	if idpIssuerURL == "" {
		idpIssuerURL = os.Getenv(envIDPIssuerURL)
	}
	if strings.TrimSpace(idpIssuerURL) == "" {
		return nil, fmt.Errorf("claimcheck: %s env var is not set", envIDPIssuerURL)
	}

	idpTokenFile := opts.IDPTokenFile
	if idpTokenFile == "" {
		if v := os.Getenv(envIDPTokenFile); v != "" {
			idpTokenFile = v
		} else {
			idpTokenFile = defaultIDPTokenFile
		}
	}

	awsTokenFile := opts.AWSTokenFile
	if awsTokenFile == "" {
		if v := os.Getenv(envAWSTokenFile); v != "" {
			awsTokenFile = v
		} else {
			awsTokenFile = defaultAWSTokenFile
		}
	}

	idpScope := opts.IDPScope
	if idpScope == "" {
		idpScope = defaultIDPS3Scope
	}

	exchanger := &tokenExchanger{
		idpIssuerURL: idpIssuerURL,
		idpTokenFile: idpTokenFile,
		awsTokenFile: awsTokenFile,
		idpScope:     idpScope,
		tracer:       opts.Tracer,
	}
	if exchanger.tracer == nil {
		exchanger.tracer = nooptrace.NewTracerProvider().Tracer("")
	}

	s3Endpoint := opts.S3Endpoint
	if s3Endpoint == "" {
		s3Endpoint = os.Getenv(envS3Endpoint)
	}
	stsEndpoint := opts.STSEndpoint
	if stsEndpoint == "" {
		stsEndpoint = os.Getenv(envSTSEndpoint)
	}

	return &s3Client{
		exchanger:    exchanger,
		roleARN:      roleARN,
		awsTokenFile: awsTokenFile,
		s3Endpoint:   s3Endpoint,
		stsEndpoint:  stsEndpoint,
	}, nil
}

type tokenExchanger struct {
	idpIssuerURL string
	idpTokenFile string
	awsTokenFile string
	idpScope     string
	tracer       trace.Tracer

	mu        sync.Mutex
	expiresAt time.Time
}

// ensureFreshToken proactively refreshes the AWS token file if it is about
// to expire (within tokenExpiryMargin). Thread-safe.
func (e *tokenExchanger) ensureFreshToken(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if time.Now().Before(e.expiresAt.Add(-tokenExpiryMargin)) {
		return nil // still fresh
	}

	expiresIn, err := e.exchangeToken(ctx)
	if err != nil {
		return err
	}
	e.expiresAt = time.Now().Add(expiresIn)
	return nil
}

// exchangeToken performs the IDP token exchange and writes the AWS token file.
// Returns the token lifetime.
func (e *tokenExchanger) exchangeToken(ctx context.Context) (_ time.Duration, retErr error) {
	ctx, span := e.tracer.Start(ctx, "idp token exchange",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer func() {
		if retErr != nil {
			span.RecordError(retErr)
			span.SetStatus(codes.Error, retErr.Error())
		}
		span.End()
	}()

	assertionBytes, err := os.ReadFile(e.idpTokenFile)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: read IDP token file %s: %w", e.idpTokenFile, err)
	}
	assertion := strings.TrimSpace(string(assertionBytes))

	tokenURL := strings.TrimSuffix(e.idpIssuerURL, "/") + "/protocol/openid-connect/token"
	formData := url.Values{
		"grant_type":            {"client_credentials"},
		"client_assertion_type": {"urn:ietf:params:oauth:client-assertion-type:jwt-bearer"},
		"client_assertion":      {assertion},
		"scope":                 {e.idpScope},
	}

	reqCtx, cancel := context.WithTimeout(ctx, tokenRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, tokenURL,
		strings.NewReader(formData.Encode()))
	if err != nil {
		return 0, fmt.Errorf("claimcheck: build token exchange request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: token exchange request to %s: %w", tokenURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: read token exchange response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("claimcheck: token exchange failed: HTTP %d %s from %s",
			resp.StatusCode, resp.Status, tokenURL)
	}

	var data struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, fmt.Errorf("claimcheck: parse token exchange response: %w", err)
	}
	if data.AccessToken == "" {
		return 0, fmt.Errorf("claimcheck: token exchange response missing access_token")
	}

	// Write atomically: write to a temp file then rename.
	dir := "."
	if idx := strings.LastIndex(e.awsTokenFile, "/"); idx >= 0 {
		dir = e.awsTokenFile[:idx]
		if dir == "" {
			dir = "/"
		}
	}
	tmpFile, err := os.CreateTemp(dir, ".ceph_token.*")
	if err != nil {
		return 0, fmt.Errorf("claimcheck: create temp token file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }() // clean up temp on error

	if err := tmpFile.Chmod(0o600); err != nil {
		_ = tmpFile.Close()
		return 0, fmt.Errorf("claimcheck: chmod temp token file: %w", err)
	}
	if _, err := tmpFile.WriteString(data.AccessToken); err != nil {
		_ = tmpFile.Close()
		return 0, fmt.Errorf("claimcheck: write temp token file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, fmt.Errorf("claimcheck: close temp token file: %w", err)
	}
	if err := os.Rename(tmpPath, e.awsTokenFile); err != nil {
		return 0, fmt.Errorf("claimcheck: rename token file: %w", err)
	}

	lifetime := time.Duration(data.ExpiresIn) * time.Second
	if lifetime == 0 {
		lifetime = tokenExpiryFallback
	}
	return lifetime, nil
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
func defaultS3WriterFactory() func(bucket string) (S3Writer, error) {
	var mu sync.Mutex
	cache := map[string]S3Writer{}
	return func(bucket string) (S3Writer, error) {
		mu.Lock()
		defer mu.Unlock()
		if c, ok := cache[bucket]; ok {
			return c, nil
		}
		c, err := newS3Client(bucket, "rw", &s3ClientOptions{})
		if err != nil {
			return nil, err
		}
		cache[bucket] = c
		return c, nil
	}
}

// defaultS3ReaderFor creates a production S3Reader for the given topic's
// default bucket. Called when no WithProcessorS3Client option is provided to NewProcessor.
func defaultS3ReaderFor(topic string) (S3Reader, error) {
	return newS3Client(DefaultBucketResolver(topic), "r", &s3ClientOptions{})
}

// isError is a helper to check for AWS SDK typed errors.
func isError[T error](err error, target T) bool {
	return errors.As(err, &target)
}
