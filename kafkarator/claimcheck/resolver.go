package claimcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// envelopeDeserializer decodes raw Kafka message bytes into an Envelope.
type envelopeDeserializer interface {
	DeserializeEnvelope(ctx context.Context, topic string, data []byte) (*Envelope, error)
}

// s3ReaderFactory builds an S3Reader scoped to a producing system and bucket.
// The system determines which Ceph IAM role is assumed, so it must be the
// system that owns the bucket (the producer named in the envelope), not the
// consumer's own system.
type s3ReaderFactory func(system, bucket string) (S3Reader, error)

// resolver decodes claim-check envelopes and fetches payloads from S3.
type resolver struct {
	s3Factory      s3ReaderFactory
	deserializer   envelopeDeserializer
	tracer         trace.Tracer
	bucketResolver BucketResolver
}

func newResolver(
	s3Factory s3ReaderFactory,
	deserializer envelopeDeserializer,
	tracer trace.Tracer,
	bucketResolver BucketResolver,
) *resolver {
	if bucketResolver == nil {
		bucketResolver = DefaultBucketResolver
	}
	return &resolver{
		s3Factory:      s3Factory,
		deserializer:   deserializer,
		tracer:         tracer,
		bucketResolver: bucketResolver,
	}
}

func (r *resolver) peekEnvelope(ctx context.Context, topic string, data []byte) (*Envelope, error) {
	envelope, err := r.deserializer.DeserializeEnvelope(ctx, topic, data)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: peek envelope: %w", err)
	}
	return envelope, nil
}

func (r *resolver) fetchPayload(ctx context.Context, topic string, data []byte) (*PayloadReader, error) {
	envelope, err := r.deserializer.DeserializeEnvelope(ctx, topic, data)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: fetch payload: %w", err)
	}
	return r.fetchPayloadFromEnvelope(ctx, topic, envelope)
}

func (r *resolver) fetchPayloadFromEnvelope(
	ctx context.Context,
	topic string,
	envelope *Envelope,
) (*PayloadReader, error) {
	bucket, key, err := s3URIParts(envelope.StorageURI)
	if err != nil {
		return nil, err
	}
	if expected := r.bucketResolver(topic); bucket != expected {
		return nil, fmt.Errorf(
			"claimcheck: envelope StorageURI bucket %q does not match expected bucket %q for topic %q; possible misconfiguration or tampered envelope",
			bucket,
			expected,
			topic,
		)
	}
	if expectedPrefix := topic + "/"; !strings.HasPrefix(key, expectedPrefix) {
		return nil, fmt.Errorf(
			"claimcheck: envelope StorageURI key %q does not have expected prefix %q for topic %q; possible misconfiguration or tampered envelope",
			key,
			expectedPrefix,
			topic,
		)
	}
	// The producing system is stamped on the envelope by the writer (derived from
	// the topic, never overridable). It identifies the bucket owner whose Ceph IAM
	// role must be assumed to read the payload. It must always be set; an empty
	// system means a malformed or pre-system-field envelope we cannot resolve.
	system := envelope.System
	if system == "" {
		return nil, fmt.Errorf(
			"claimcheck: envelope for topic %q has no system; cannot determine the bucket owner whose role must be assumed",
			topic,
		)
	}
	s3, err := r.s3Factory(system, bucket)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create S3 reader for system %q bucket %q: %w", system, bucket, err)
	}
	return &PayloadReader{ctx: ctx, s3: s3, bucket: bucket, key: key, size: envelope.ByteSize}, nil
}

// PayloadReader provides read access to the raw Parquet bytes of a
// claim-check payload fetched from S3. It implements io.ReaderAt and
// io.Closer. Call Size to get the total byte length needed by Parquet
// file openers.
//
// Each ReadAt call issues an independent bounded S3 range-GET, so concurrent
// calls are safe and there is no sequential read position to disturb. Close
// is a no-op because no persistent connection is held between calls.
type PayloadReader struct {
	ctx    context.Context //nolint:containedctx // stored to satisfy io.ReaderAt which has no ctx parameter
	s3     S3Reader
	bucket string
	key    string
	size   int64
}

// ReadAt implements io.ReaderAt. Each call issues an independent S3
// range-GET for the closed byte range [off, off+len(b)-1].
func (p *PayloadReader) ReadAt(b []byte, off int64) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if off >= p.size {
		return 0, io.EOF
	}
	end := off + int64(len(b)) - 1
	pastEnd := end >= p.size
	if pastEnd {
		end = p.size - 1
	}
	rangeHdr := fmt.Sprintf("bytes=%d-%d", off, end)
	body, _, err := p.s3.GetObject(p.ctx, p.bucket, p.key, &rangeHdr)
	if err != nil {
		return 0, fmt.Errorf("claimcheck: ReadAt range-GET %s: %w", rangeHdr, err)
	}
	defer body.Close() //nolint:errcheck // body is a bounded range-GET response; close error is non-actionable
	n, err := io.ReadFull(body, b[:end-off+1])
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	if err == nil && pastEnd {
		err = io.EOF
	}

	return n, err
}

// Size returns the total size of the Parquet file in bytes.
func (p *PayloadReader) Size() int64 { return p.size }

// Close is a no-op. Must be called when the caller is done reading the
// payload to satisfy io.Closer. Safe to call multiple times.
func (p *PayloadReader) Close() error {
	return nil
}
