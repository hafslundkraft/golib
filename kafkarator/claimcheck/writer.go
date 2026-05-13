package claimcheck

import (
	"context"
	"fmt"

	parquet "github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/trace"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"
)

// serializer serializes a value to Confluent Avro wire-format bytes.
// Satisfied by *kafkarator.AvroSerializer (via Connection.Serializer()).
type serializer interface {
	Serialize(ctx context.Context, topic string, value any) ([]byte, error)
}

// kafkaWriter sends a single message to Kafka.
// Satisfied by *kafkarator.Writer (via Connection.Writer()).
type kafkaWriter interface {
	Write(ctx context.Context, message *kafkarator.Message) error
	Close(ctx context.Context) error
}

// Writer streams records to S3/Parquet and produces claim-check envelopes to
// Kafka. Obtain one via NewWriter.
type Writer struct {
	kw     kafkaWriter
	ser    serializer
	stager *stager
}

// WriterOption configures NewWriter.
type WriterOption func(*writerConfig)

type writerConfig struct {
	s3Factory  func(bucket string) (S3Writer, error)
	fetcher    SchemaFetcher
	stagerOpts []stagerOption
}

// WithWriterS3Client sets a fixed S3 writer client used for all batches. Use this
// in tests with FakeS3Client, or when managing production clients manually.
// If omitted, a production client is constructed automatically per bucket from
// HAPPI_SYSTEM_NAME, HAPPI_ENV, and HAPPI_IDP_ISSUER_URL.
func WithWriterS3Client(s3 S3Writer) WriterOption {
	return func(c *writerConfig) {
		c.s3Factory = func(_ string) (S3Writer, error) { return s3, nil }
	}
}

// WithWriterSchemaFetcher overrides the default Schema Registry-backed schema fetcher.
func WithWriterSchemaFetcher(f SchemaFetcher) WriterOption {
	return func(c *writerConfig) { c.fetcher = f }
}

// WithWriterBucketResolver overrides the default topic→bucket naming convention used by the writer.
func WithWriterBucketResolver(fn BucketResolver) WriterOption {
	return func(c *writerConfig) { c.stagerOpts = append(c.stagerOpts, withBucketResolver(fn)) }
}

// WithWriterRowGroupSize sets the number of records per Parquet row group. Default is 100 000.
func WithWriterRowGroupSize(n int) WriterOption {
	return func(c *writerConfig) { c.stagerOpts = append(c.stagerOpts, withRowGroupSize(n)) }
}

// WithWriterPartSize sets the S3 multipart part size in bytes. Must be ≥ 5 MiB. Default is 5 MiB.
func WithWriterPartSize(n int) WriterOption {
	return func(c *writerConfig) { c.stagerOpts = append(c.stagerOpts, withPartSize(n)) }
}

// WithWriterTracer sets the OpenTelemetry tracer used to instrument S3 and Kafka operations.
// Defaults to the tracer from the kafkarator.Connection's TelemetryProvider.
func WithWriterTracer(t trace.Tracer) WriterOption {
	return func(c *writerConfig) { c.stagerOpts = append(c.stagerOpts, withTracer(t)) }
}

// NewWriter creates a Writer from a kafkarator.Connection.
//
// By default the schema fetcher is backed by the connection's Schema Registry
// client. If no [WithWriterS3Client] option is provided, a production S3 client is
// constructed automatically per batch-bucket from HAPPI_SYSTEM_NAME, HAPPI_ENV,
// and HAPPI_IDP_ISSUER_URL.
//
// Options:
//   - [WithWriterS3Client] — inject a custom/test S3 writer (e.g. [FakeS3Client])
//   - [WithWriterSchemaFetcher] — override the Schema Registry-backed schema fetcher
//   - [WithWriterBucketResolver] — override the topic→bucket naming convention
//   - [WithWriterRowGroupSize] — set the number of records per Parquet row group
//   - [WithWriterPartSize] — set the S3 multipart part size in bytes
func NewWriter(conn *kafkarator.Connection, opts ...WriterOption) (*Writer, error) {
	cfg := &writerConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.s3Factory == nil {
		cfg.s3Factory = defaultS3WriterFactory()
	}
	if cfg.fetcher == nil {
		cfg.fetcher = newSRSchemaFetcher(conn.SchemaRegistryClient())
	}
	kw, err := conn.Writer()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create kafka writer: %w", err)
	}
	// Inject the connection's tracer and logger as defaults; callers can override with WithWriterTracer.
	cfg.stagerOpts = append([]stagerOption{withTracer(conn.Tracer()), withLogger(conn.Logger())}, cfg.stagerOpts...)
	stager := newStagerWithFactory(cfg.s3Factory, cfg.fetcher, cfg.stagerOpts...)
	return &Writer{kw: kw, ser: conn.Serializer(), stager: stager}, nil
}

// Close closes the underlying Kafka writer.
func (w *Writer) Close(ctx context.Context) error {
	if err := w.kw.Close(ctx); err != nil {
		return fmt.Errorf("claimcheck: close kafka writer: %w", err)
	}
	return nil
}

// BatchOption configures a single Batch call.
type BatchOption func(*batchConfig)

type batchConfig struct {
	key     []byte
	headers map[string][]byte
}

// WithBatchKey sets the Kafka message key on the envelope message.
func WithBatchKey(key []byte) BatchOption {
	return func(c *batchConfig) { c.key = key }
}

// WithBatchHeaders sets additional Kafka headers on the envelope message.
func WithBatchHeaders(headers map[string][]byte) BatchOption {
	return func(c *batchConfig) { c.headers = headers }
}

// Batch is an open write session returned by [Writer.NewBatch]. Write records
// into it, then call Produce to finalize the S3 upload and produce the envelope
// to Kafka, or Cleanup to discard without producing.
//
// Records can be any Go value whose fields map to the Avro schema registered
// in Schema Registry — either a concrete struct with parquet field tags, or a
// map[string]any keyed by field name.
type Batch struct {
	sess        *stageSession
	pw          *parquet.GenericWriter[any]
	pending     []any
	rowGroup    int
	recordCount int64
	writer      *Writer
	topic       string
	cfg         batchConfig
	done        bool
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

// Produce finalizes the Parquet upload and produces the envelope to Kafka.
// On any error the batch is permanently closed and cannot be retried; obtain a
// new batch via [Writer.NewBatch]. Calling Produce more than once returns an error.
func (b *Batch) Produce(ctx context.Context) error {
	if b.done {
		return fmt.Errorf("claimcheck: batch already closed")
	}
	b.done = true
	if err := b.flushRowGroup(); err != nil {
		b.sess.abort()
		return err
	}
	if err := b.pw.Close(); err != nil {
		b.sess.abort()
		return fmt.Errorf("claimcheck: close parquet writer: %w", err)
	}
	if err := b.sess.complete(ctx, b.recordCount); err != nil {
		return err
	}

	env, err := b.sess.getEnvelope()
	if err != nil {
		b.sess.abort()
		return err
	}

	value, err := b.writer.ser.Serialize(ctx, b.topic, env)
	if err != nil {
		b.sess.abort()
		return fmt.Errorf("claimcheck: serialize envelope: %w", err)
	}

	if err := b.writer.kw.Write(ctx, &kafkarator.Message{
		Topic:   b.topic,
		Key:     b.cfg.key,
		Headers: b.cfg.headers,
		Value:   value,
	}); err != nil {
		b.sess.abort()
		return fmt.Errorf("claimcheck: write kafka message: %w", err)
	}
	return nil
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
	b.sess.abort()
}

// NewBatch opens a write session for topic. The Parquet schema is derived from
// the Avro schema registered in Schema Registry under
// "{topic}-claim-check-payload".
//
// Records passed to [Batch.Write] can be any value whose fields map to that
// schema — a concrete struct with parquet field tags, or a map[string]any.
//
//	batch, err := w.NewBatch(ctx, topic)
//	if err != nil { ... }
//	defer batch.Cleanup()
//	for _, r := range readings {
//	    if err := batch.Write(r); err != nil { return err }
//	}
//	return batch.Produce(ctx)
//
// Options:
//   - [WithBatchKey] — set the Kafka message key on the envelope
//   - [WithBatchHeaders] — set additional Kafka headers on the envelope
func (w *Writer) NewBatch(ctx context.Context, topic string, opts ...BatchOption) (*Batch, error) {
	cfg := &batchConfig{}
	for _, o := range opts {
		o(cfg)
	}
	sess, pw, err := w.stager.stage(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: stage batch: %w", err)
	}
	return &Batch{
		sess:     sess,
		pw:       pw,
		rowGroup: w.stager.rowGroupSize,
		writer:   w,
		topic:    topic,
		cfg:      *cfg,
	}, nil
}

type srSchemaFetcher struct {
	client kafkarator.SchemaRegistryClient
}

func newSRSchemaFetcher(client kafkarator.SchemaRegistryClient) SchemaFetcher {
	return &srSchemaFetcher{client: client}
}

func (f *srSchemaFetcher) GetLatestSchema(
	ctx context.Context,
	subject string,
) (schemaStr string, version, id int, err error) {
	meta, err := f.client.GetLatestSchemaMetadata(ctx, subject)
	if err != nil {
		return "", 0, 0, fmt.Errorf("claimcheck: schema registry GetLatestSchemaMetadata(%q): %w", subject, err)
	}
	return meta.Schema, meta.Version, meta.ID, nil
}
