package claimcheck

import (
	"context"
	"fmt"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"
)

// kafkaWriter sends a single message to Kafka.
// Satisfied by *kafkarator.Writer (via Connection.Writer()).
type kafkaWriter interface {
	Write(ctx context.Context, message *kafkarator.Message, opts ...kafkarator.WriteOption) error
	Close(ctx context.Context) error
}

// Writer streams records to S3/Parquet and produces claim-check envelopes to
// Kafka. Obtain one via NewWriter.
type Writer struct {
	kw     kafkaWriter
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

// BatchOption configures a single [Batch] call.
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
	connCfg := conn.Config()
	if cfg.s3Factory == nil {
		exchanger, err := newTokenExchanger()
		if err != nil {
			return nil, fmt.Errorf("claimcheck: init token exchanger: %w", err)
		}
		cfg.s3Factory = defaultS3WriterFactory(exchanger, connCfg.SystemName, connCfg.Env)
	}
	if cfg.fetcher == nil {
		cfg.fetcher = newSRSchemaFetcher(conn.SchemaRegistryClient())
	}
	kw, err := conn.Writer()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create kafka writer: %w", err)
	}

	cfg.stagerOpts = append(
		[]stagerOption{withTracer(conn.Tracer()), withLogger(conn.Logger()), withSystem(connCfg.SystemName)},
		cfg.stagerOpts...,
	)
	stager := newStagerWithFactory(cfg.s3Factory, cfg.fetcher, conn.Serializer(), cfg.stagerOpts...)
	return &Writer{kw: kw, stager: stager}, nil
}

// Close closes the underlying Kafka writer.
func (w *Writer) Close(ctx context.Context) error {
	if err := w.kw.Close(ctx); err != nil {
		return fmt.Errorf("claimcheck: close kafka writer: %w", err)
	}
	return nil
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
	batch, err := w.stager.stage(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: stage batch: %w", err)
	}
	// produce ships the envelope synchronously: kafkarator.Writer.Write is async,
	// so we register a one-shot delivery channel and block until the broker has
	// acked (or failed) the message. The S3 object must not be considered
	// produced until the envelope is durably on the topic.
	batch.produce = func(ctx context.Context, value []byte) error {
		msg := &kafkarator.Message{
			Topic:   topic,
			Key:     cfg.key,
			Headers: cfg.headers,
			Value:   value,
		}

		deliveryChan := make(chan kafkarator.DeliveryReport, 1)
		defer close(deliveryChan)

		if err := w.kw.Write(ctx, msg, kafkarator.WithDeliveryChannel(deliveryChan)); err != nil {
			return fmt.Errorf("claimcheck: write kafka message: %w", err)
		}

		ev := <-deliveryChan
		if err := ev.Err; err != nil {
			return fmt.Errorf("claimcheck: message delivery failed: %w", err)
		}

		return nil
	}
	return batch, nil
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
