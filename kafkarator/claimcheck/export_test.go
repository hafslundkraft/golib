package claimcheck

import (
	"bytes"
	"context"

	parquet "github.com/parquet-go/parquet-go"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

// AvroSchemaToParquet re-exported for tests only.
func AvroSchemaToParquet(avroSchemaStr string) (*parquet.Schema, error) {
	return avroSchemaToParquet(avroSchemaStr)
}

// ClaimCheckRoleARN re-exported for tests only.
func ClaimCheckRoleARN(system, env, bucket, access string) (string, error) {
	return claimCheckRoleARN(system, env, bucket, access)
}

// RoleARNForClient builds a production S3 client via the same path used by the
// writer/processor and returns the role ARN it computed for the given bucket.
// The token exchanger is constructed from env vars but no network call is made
// at construction time. For e2e tests only.
func RoleARNForClient(bucket, access, system, env string) (string, error) {
	c, err := newS3Client(bucket, access, system, env, nil)
	if err != nil {
		return "", err
	}
	return c.(*s3Client).roleARN, nil
}

// EnvelopeDeserializer is re-exported for use in tests only.
type EnvelopeDeserializer = envelopeDeserializer

// S3ReaderFactory mirrors the per-(system,bucket) reader factory used on the
// read path. For use in tests only.
type S3ReaderFactory = func(system, bucket string) (S3Reader, error)

// ResolveForTest decodes the envelope and resolves a PayloadReader through the
// full resolver path, using the given reader factory and default system name.
// It returns the (system, bucket) the factory was asked to build a client for,
// so tests can assert the issuer's system flows into the IAM role construction.
// For use in tests only.
func ResolveForTest(
	factory S3ReaderFactory,
	defaultSystem string,
	de EnvelopeDeserializer,
	topic string,
	value []byte,
) error {
	r := newResolver(factory, defaultSystem, de, nooptrace.NewTracerProvider().Tracer(""), DefaultBucketResolver, DefaultSystemResolver, nil)
	_, err := r.fetchPayload(context.Background(), topic, value)
	return err
}

// NewMessage constructs a Message with an embedded resolver. For use in tests
// that exercise handler logic without a full [Processor] setup.
func NewMessage(
	topic string,
	key, value []byte,
	headers map[string][]byte,
	s3 S3Reader,
	de EnvelopeDeserializer,
) *Message {
	return &Message{
		Topic:   topic,
		Key:     key,
		value:   value,
		Headers: headers,
		resolver: newResolver(
			func(_, _ string) (S3Reader, error) { return s3, nil },
			"",
			de,
			nooptrace.NewTracerProvider().Tracer(""),
			DefaultBucketResolver,
			DefaultSystemResolver,
			nil,
		),
	}
}

// WithWriterSystemForTest stamps the producing system name onto each envelope.
// For use in tests only.
func WithWriterSystemForTest(system string) WriterOption {
	return func(c *writerConfig) { c.stagerOpts = append(c.stagerOpts, withSystem(system)) }
}

// WithWriterS3FactoryForTest injects a (system, bucket) writer factory so tests
// can assert which system the write client is built for. For use in tests only.
func WithWriterS3FactoryForTest(fn func(system, bucket string) (S3Writer, error)) WriterOption {
	return func(c *writerConfig) { c.s3Factory = fn }
}

// NewTestWriter creates a Writer for use in tests, bypassing kafkarator.Connection.
// Inject a test S3 store and schema via [WithWriterS3Client] and [WithWriterSchemaFetcher].
// Additional write options ([WithWriterBucketResolver], [WithWriterRowGroupSize], [WithWriterPartSize]) are also accepted.
func NewTestWriter(kw kafkaWriter, ser serializer, opts ...WriterOption) *Writer {
	cfg := &writerConfig{}
	for _, o := range opts {
		o(cfg)
	}
	stager := newStagerWithFactory(cfg.s3Factory, cfg.fetcher, ser, cfg.stagerOpts...)
	return &Writer{kw: kw, stager: stager}
}

// NewBytesReaderAt wraps a byte slice in a *bytes.Reader which implements
// both io.ReaderAt and io.ReadSeeker. Useful for opening a Parquet file
// from an in-memory buffer in tests.
func NewBytesReaderAt(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
