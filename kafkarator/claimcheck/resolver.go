package claimcheck

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// envelopeDeserializer decodes raw Kafka message bytes into an Envelope.
type envelopeDeserializer interface {
	DeserializeEnvelope(ctx context.Context, topic string, data []byte) (*Envelope, error)
}

// resolver decodes claim-check envelopes and fetches payloads from S3.
type resolver struct {
	s3           S3Reader
	deserializer envelopeDeserializer
	tracer       trace.Tracer
}

func newResolver(s3 S3Reader, deserializer envelopeDeserializer, tracer trace.Tracer) *resolver {
	return &resolver{s3: s3, deserializer: deserializer, tracer: tracer}
}

func (r *resolver) peekEnvelope(ctx context.Context, topic string, data []byte) (*Envelope, error) {
	env, err := r.deserializer.DeserializeEnvelope(ctx, topic, data)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: peek envelope: %w", err)
	}
	return env, nil
}

func (r *resolver) fetchPayload(ctx context.Context, topic string, data []byte) (*PayloadReader, error) {
	env, err := r.deserializer.DeserializeEnvelope(ctx, topic, data)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: fetch payload: %w", err)
	}
	bucket, key, err := s3URIParts(env.StorageURI)
	if err != nil {
		return nil, err
	}
	spanCtx, span := r.tracer.Start(ctx, "claim_check resolve "+env.Topic,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", env.Topic),
			attribute.String("claim_check.batch_id", env.BatchID),
			attribute.Int64("claim_check.record_count", env.RecordCount),
			attribute.Int64("claim_check.byte_size", env.ByteSize),
			attribute.String("claim_check.storage_uri", env.StorageURI),
		),
	)
	return &PayloadReader{ctx: spanCtx, span: span, s3: r.s3, bucket: bucket, key: key, size: env.ByteSize}, nil
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
	ctx    context.Context //nolint:containedctx — stored to satisfy io.ReaderAt (no ctx param)
	span   trace.Span
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
		p.span.RecordError(err)
		p.span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("claimcheck: ReadAt range-GET %s: %w", rangeHdr, err)
	}
	defer body.Close() //nolint:errcheck
	n, err := io.ReadFull(body, b[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if err == nil && pastEnd {
		err = io.EOF
	}
	return n, err
}

// Size returns the total size of the Parquet file in bytes.
func (p *PayloadReader) Size() int64 { return p.size }

// Close ends the resolve span. Must be called when the caller is done reading
// the payload. Safe to call multiple times.
func (p *PayloadReader) Close() error {
	p.span.End()
	return nil
}
