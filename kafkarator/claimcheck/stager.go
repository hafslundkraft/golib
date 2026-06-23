package claimcheck

import (
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

// serializer serializes a value to Confluent Avro wire-format bytes.
// Satisfied by *kafkarator.AvroSerializer (via Connection.Serializer()).
type serializer interface {
	Serialize(ctx context.Context, topic string, value any) ([]byte, error)
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
	ser            serializer
	system         string
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

// withSystem sets the producing system name stamped onto each envelope so that
// readers can assume the producer's Ceph IAM role.
func withSystem(system string) stagerOption {
	return func(s *stager) { s.system = system }
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
func newStagerWithFactory(
	factory func(bucket string) (S3Writer, error),
	fetcher SchemaFetcher,
	ser serializer,
	opts ...stagerOption,
) *stager {
	st := &stager{
		s3Factory:      factory,
		schemaFetcher:  fetcher,
		ser:            ser,
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

// stage resolves the payload Avro schema for topic from Schema Registry, opens
// an S3 multipart upload, and returns a [Batch] ready to receive records.
// The payload schema subject is "{topic}-claim-check-payload".
func (s *stager) stage(ctx context.Context, topic string) (*Batch, error) {
	if strings.ContainsRune(topic, '/') {
		return nil, fmt.Errorf("claimcheck: topic name must not contain '/': %q", topic)
	}
	subject := topic + "-claim-check-payload"
	schemaStr, version, id, err := s.schemaFetcher.GetLatestSchema(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: fetch schema %q: %w", subject, err)
	}

	parquetSchema, err := avroSchemaToParquet(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: convert avro schema to parquet: %w", err)
	}

	bucket := s.bucketResolver(topic)
	if bucket == "" {
		return nil, fmt.Errorf("claimcheck: bucket resolver returned empty string for topic %q", topic)
	}

	s3Client, err := s.s3Factory(bucket)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create S3 writer for bucket %q: %w", bucket, err)
	}

	batchID := uuid.New().String()
	s3Key := topic + "/" + batchID + ".parquet"

	spanCtx, span := s.tracer.Start(ctx, "claim_check stage "+topic,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", topic),
			attribute.String("claim_check.batch_id", batchID),
		),
	)

	pipe, err := newMultipartWriter(spanCtx, s3Client, bucket, s3Key, s.partSize, s.logger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return nil, err
	}

	writerOpts := []parquet.WriterOption{
		parquetSchema,
		parquet.Compression(&parquet.Snappy),
		parquet.KeyValueMetadata("avro.schema", schemaStr),
		parquet.KeyValueMetadata("avro.schema.subject", subject),
		parquet.KeyValueMetadata("avro.schema.version", fmt.Sprintf("%d", version)),
		parquet.KeyValueMetadata("avro.schema.id", fmt.Sprintf("%d", id)),
	}

	return &Batch{
		topic:    topic,
		batchID:  batchID,
		system:   s.system,
		pipe:     pipe,
		span:     span,
		logger:   s.logger,
		ser:      s.ser,
		pw:       parquet.NewGenericWriter[any](pipe, writerOpts...),
		rowGroup: s.rowGroupSize,
	}, nil
}

// Batch is an open write session returned by [Writer.NewBatch]. Write records
// into it, then call Produce to finalize the S3 upload and produce the envelope
// to Kafka, or Cleanup to discard without producing.
//
// Records can be any Go value whose fields map to the Avro schema registered
// in Schema Registry — either a concrete struct with parquet field tags, or a
// map[string]any keyed by field name.
//
// Batch is not safe for concurrent use. All Write, Produce, and Cleanup calls
// must be made from the same goroutine.
//
// Producing an empty batch (zero Write calls) is an error.
type Batch struct {
	// produce is set by writer.go's NewBatch; it handles the Kafka write.
	produce func(ctx context.Context, value []byte) error
	topic   string
	batchID string
	system  string
	pipe    *multipartWriter
	span    trace.Span
	logger  *slog.Logger
	ser     serializer
	// Parquet write state
	pw          *parquet.GenericWriter[any]
	pending     []any
	rowGroup    int
	recordCount int64
	done        bool
}

// finalizeUpload flushes and closes the Parquet writer, serializes the envelope,
// and completes the S3 multipart upload. Serialization happens before
// CompleteMultipartUpload: any failure can abort the still-in-progress upload
// rather than needing to delete a completed object.
// On any error the upload is aborted and the span ended with an error status.
func (b *Batch) finalizeUpload(ctx context.Context) ([]byte, error) {
	if err := b.flushRowGroup(); err != nil {
		_ = b.pw.Close()
		b.pipe.Abort()
		b.span.RecordError(err)
		b.span.SetStatus(codes.Error, err.Error())
		b.span.End()
		return nil, err
	}
	if err := b.pw.Close(); err != nil {
		b.pipe.Abort()
		b.span.RecordError(err)
		b.span.SetStatus(codes.Error, err.Error())
		b.span.End()
		return nil, fmt.Errorf("claimcheck: close parquet writer: %w", err)
	}
	byteSize := b.pipe.Size()
	env := &Envelope{
		BatchID:     b.batchID,
		StorageURI:  "s3://" + b.pipe.bucket + "/" + b.pipe.key,
		Topic:       b.topic,
		System:      b.system,
		RecordCount: b.recordCount,
		ByteSize:    byteSize,
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	value, err := b.ser.Serialize(ctx, b.topic, env)
	if err != nil {
		b.pipe.Abort()
		b.span.RecordError(err)
		b.span.SetStatus(codes.Error, err.Error())
		b.span.End()
		return nil, fmt.Errorf("claimcheck: serialize envelope: %w", err)
	}
	if err := b.pipe.Complete(ctx); err != nil {
		b.pipe.Abort()
		b.span.RecordError(err)
		b.span.SetStatus(codes.Error, err.Error())
		b.span.End()
		return nil, err
	}
	b.span.AddEvent("upload complete", trace.WithAttributes(
		attribute.Int64("claim_check.record_count", b.recordCount),
		attribute.Int64("claim_check.byte_size", byteSize),
	))
	b.span.End()
	return value, nil
}

// Write buffers one record into the batch.
func (b *Batch) Write(record any) error {
	b.pending = append(b.pending, record)
	if len(b.pending) >= b.rowGroup {
		return b.flushRowGroup()
	}
	return nil
}

func (b *Batch) flushRowGroup() error {
	if len(b.pending) == 0 {
		return nil
	}
	if _, err := b.pw.Write(b.pending); err != nil {
		return fmt.Errorf("claimcheck: write parquet row group: %w", err)
	}
	if err := b.pw.Flush(); err != nil {
		return fmt.Errorf("claimcheck: flush parquet writer: %w", err)
	}
	b.recordCount += int64(len(b.pending))
	b.pending = b.pending[:0]
	return nil
}

// Produce finalizes the S3 upload and produces the claim-check envelope to Kafka.
// On any error the batch is permanently closed and cannot be retried; obtain a
// new batch via [Writer.NewBatch]. Calling Produce more than once returns an error.
func (b *Batch) Produce(ctx context.Context) error {
	if b.done {
		return fmt.Errorf("claimcheck: batch already closed")
	}
	b.done = true
	value, err := b.finalizeUpload(ctx)
	if err != nil {
		return err
	}
	return b.produce(ctx, value)
}

// Cleanup aborts the S3 upload and discards the batch without producing to Kafka.
// Safe to call after a successful Produce — no-op if the batch is already closed.
// Intended for use with defer to ensure resources are released on error paths.
func (b *Batch) Cleanup() {
	if b.done {
		return
	}
	b.done = true
	_ = b.pw.Close()
	b.pipe.Abort()
	b.span.SetStatus(codes.Error, "batch aborted")
	b.span.End()
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
