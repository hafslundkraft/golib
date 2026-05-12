package claimcheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	parquet "github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

const (
	defaultRowGroupSize = 100_000
	minPartSize         = 5 * 1024 * 1024 // 5 MiB — S3 multipart minimum
)

// multipartWriter is an io.Writer that buffers bytes and flushes S3 multipart
// parts whenever the buffer reaches partSize. The full Parquet file is never
// held entirely in memory.
type multipartWriter struct {
	ctx        context.Context //nolint:containedctx — stored to satisfy io.Writer (no ctx param)
	s3         S3Writer
	bucket     string
	key        string
	partSize   int
	uploadID   string
	parts      []CompletedPart
	buf        []byte
	totalBytes int64
}

func newMultipartWriter(ctx context.Context, s3 S3Writer, bucket, key string, partSize int) (*multipartWriter, error) {
	if partSize < minPartSize {
		return nil, fmt.Errorf("claimcheck: partSize %d is below the S3 minimum of %d bytes", partSize, minPartSize)
	}
	uploadID, err := s3.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create multipart upload: %w", err)
	}
	return &multipartWriter{
		ctx:      ctx,
		s3:       s3,
		bucket:   bucket,
		key:      key,
		partSize: partSize,
		uploadID: uploadID,
	}, nil
}

// Write implements io.Writer.
func (w *multipartWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	if len(w.buf) >= w.partSize {
		if err := w.flushPart(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *multipartWriter) flushPart() error {
	if len(w.buf) == 0 {
		return nil
	}
	partNumber := len(w.parts) + 1
	etag, err := w.s3.UploadPart(w.ctx, w.bucket, w.key, w.uploadID, partNumber, bytes.NewReader(w.buf))
	if err != nil {
		return fmt.Errorf("claimcheck: upload part %d: %w", partNumber, err)
	}
	w.parts = append(w.parts, CompletedPart{PartNumber: partNumber, ETag: etag})
	w.totalBytes += int64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

// Complete flushes any remaining buffered bytes and finalises the multipart
// upload. Returns the total number of bytes uploaded.
func (w *multipartWriter) Complete(ctx context.Context) (int64, error) {
	if err := w.flushPart(); err != nil {
		return 0, err
	}
	if err := w.s3.CompleteMultipartUpload(ctx, w.bucket, w.key, w.uploadID, w.parts); err != nil {
		return 0, fmt.Errorf("claimcheck: complete multipart upload: %w", err)
	}
	return w.totalBytes, nil
}

// Abort discards all uploaded parts. Best-effort: errors are silently ignored
// so that the original error context is preserved by the caller.
func (w *multipartWriter) Abort() {
	_ = w.s3.AbortMultipartUpload(context.Background(), w.bucket, w.key, w.uploadID)
}

// stageSession manages the S3 multipart upload for one batch. It is the
// shared infrastructure layer used by both BatchWriter (map[string]any) and
// typedBatch[T] (typed structs).
type stageSession struct {
	topic   string
	batchID string
	s3Key   string
	pipe    *multipartWriter
	env     *Envelope
	span    trace.Span   // may be a no-op span
	logger  *slog.Logger // never nil
}

func newStageSession(ctx context.Context, s3 S3Writer, bucket, topic string, partSize int, tracer trace.Tracer, logger *slog.Logger) (*stageSession, context.Context, error) {
	batchID := uuid.New().String()
	key := topic + "/" + batchID + ".parquet"

	// Start the span before any S3 call so all multipart operations are children.
	spanCtx, span := tracer.Start(ctx, "claim_check stage "+topic,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", topic),
			attribute.String("claim_check.batch_id", batchID),
		),
	)

	pipe, err := newMultipartWriter(spanCtx, s3, bucket, key, partSize)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return nil, ctx, err
	}
	return &stageSession{topic: topic, batchID: batchID, s3Key: key, pipe: pipe, span: span}, spanCtx, nil
}

// complete finalises the S3 multipart upload and builds the Envelope.
// Must be called after the Parquet writer is closed.
func (s *stageSession) complete(ctx context.Context, recordCount int64) error {
	byteSize, err := s.pipe.Complete(ctx)
	if err != nil {
		s.pipe.Abort()
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
		s.span.End()
		return err
	}
	s.env = &Envelope{
		BatchID:     s.batchID,
		StorageURI:  "s3://" + s.pipe.bucket + "/" + s.s3Key,
		Topic:       s.topic,
		RecordCount: recordCount,
		ByteSize:    byteSize,
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	s.span.AddEvent("upload complete", trace.WithAttributes(
		attribute.Int64("claim_check.record_count", recordCount),
		attribute.Int64("claim_check.byte_size", byteSize),
	))
	s.span.End()
	return nil
}

func (s *stageSession) abort() {
	if s.env != nil {
		// Upload completed successfully before abort was called (e.g. Kafka write
		// failed after Commit). Best-effort delete to avoid orphaned S3 objects.
		if err := s.pipe.s3.DeleteObject(context.Background(), s.pipe.bucket, s.s3Key); err != nil {
			s.logger.Warn("claimcheck: failed to delete orphaned S3 object",
				"bucket", s.pipe.bucket,
				"key", s.s3Key,
				"err", err,
			)
		}
	} else {
		s.pipe.Abort()
	}
	s.span.SetStatus(codes.Error, "batch aborted")
	s.span.End()
}

func (s *stageSession) getEnvelope() (*Envelope, error) {
	if s.env == nil {
		return nil, fmt.Errorf("claimcheck: Envelope() called before successful Commit")
	}
	return s.env, nil
}

// BucketResolver maps a topic name to an S3 bucket name.
type BucketResolver func(topic string) string

// DefaultBucketResolver is the standard bucket naming convention: "cc-" + hex(sha256(topic))[:16].
func DefaultBucketResolver(topic string) string {
	sum := sha256.Sum256([]byte(topic))
	return "cc-" + hex.EncodeToString(sum[:])[:16]
}

// SchemaFetcher fetches the latest Avro schema string and its metadata for a
// given Schema Registry subject.
type SchemaFetcher interface {
	// GetLatestSchema returns the Avro schema JSON string, version, and schema
	// ID for the given subject.
	GetLatestSchema(ctx context.Context, subject string) (schemaStr string, version, id int, err error)
}

// stager fetches the payload Avro schema from Schema Registry, converts it to
// a Parquet schema, and opens write sessions to S3.
type stager struct {
	s3Factory      func(bucket string) (S3Writer, error)
	schemaFetcher  SchemaFetcher
	bucketResolver BucketResolver
	rowGroupSize   int
	partSize       int
	tracer         trace.Tracer
	logger         *slog.Logger
}

type stagerOption func(*stager)

func withBucketResolver(fn BucketResolver) stagerOption {
	return func(s *stager) { s.bucketResolver = fn }
}

func withRowGroupSize(n int) stagerOption {
	return func(s *stager) {
		if n > 0 {
			s.rowGroupSize = n
		}
	}
}

func withPartSize(n int) stagerOption {
	return func(s *stager) {
		if n >= minPartSize {
			s.partSize = n
		}
	}
}

func withTracer(t trace.Tracer) stagerOption {
	return func(s *stager) {
		if t != nil {
			s.tracer = t
		}
	}
}

func withLogger(l *slog.Logger) stagerOption {
	return func(s *stager) {
		if l != nil {
			s.logger = l
		}
	}
}

// newStagerWithFactory creates a stager that calls factory(bucket) to obtain
// the S3Writer for each unique bucket.
func newStagerWithFactory(factory func(bucket string) (S3Writer, error), fetcher SchemaFetcher, opts ...stagerOption) *stager {
	st := &stager{
		s3Factory:      factory,
		schemaFetcher:  fetcher,
		bucketResolver: DefaultBucketResolver,
		rowGroupSize:   defaultRowGroupSize,
		partSize:       minPartSize,
		tracer:         nooptrace.NewTracerProvider().Tracer(""),
		logger:         slog.Default(),
	}
	for _, o := range opts {
		o(st)
	}
	return st
}

// stage resolves the payload schema for topic from Schema Registry, creates a
// stageSession, and returns a GenericWriter[any] ready to receive records.
// Both concrete structs and map[string]any values can be passed to Write.
//
// The payload schema subject is "{topic}-claim-check-payload".
func (s *stager) stage(ctx context.Context, topic string) (*stageSession, *parquet.GenericWriter[any], error) {
	subject := topic + "-claim-check-payload"
	schemaStr, version, id, err := s.schemaFetcher.GetLatestSchema(ctx, subject)
	if err != nil {
		return nil, nil, fmt.Errorf("claimcheck: fetch schema %q: %w", subject, err)
	}

	parquetSchema, err := avroSchemaToParquet(schemaStr)
	if err != nil {
		return nil, nil, fmt.Errorf("claimcheck: convert avro schema to parquet: %w", err)
	}

	bucket := s.bucketResolver(topic)
	if bucket == "" {
		return nil, nil, fmt.Errorf("claimcheck: bucket resolver returned empty string for topic %q", topic)
	}

	s3, err := s.s3Factory(bucket)
	if err != nil {
		return nil, nil, fmt.Errorf("claimcheck: create S3 writer for bucket %q: %w", bucket, err)
	}

	sess, _, err := newStageSession(ctx, s3, bucket, topic, s.partSize, s.tracer, s.logger)
	if err != nil {
		return nil, nil, err
	}

	writerOpts := []parquet.WriterOption{
		parquetSchema,
		parquet.Compression(&parquet.Snappy),
		parquet.KeyValueMetadata("avro.schema", schemaStr),
		parquet.KeyValueMetadata("avro.schema.subject", subject),
		parquet.KeyValueMetadata("avro.schema.version", fmt.Sprintf("%d", version)),
		parquet.KeyValueMetadata("avro.schema.id", fmt.Sprintf("%d", id)),
	}
	pw := parquet.NewGenericWriter[any](sess.pipe, writerOpts...)
	return sess, pw, nil
}

// s3URIParts parses "s3://bucket/key" into (bucket, key).
func s3URIParts(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("claimcheck: expected s3:// URI, got %q", uri)
	}
	without := uri[len("s3://"):]
	idx := strings.IndexByte(without, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("claimcheck: s3 URI missing key: %q", uri)
	}
	bucket = without[:idx]
	key = without[idx+1:]
	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("claimcheck: invalid s3 URI %q", uri)
	}
	return bucket, key, nil
}
