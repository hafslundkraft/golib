package claimcheck

import (
	"bytes"

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

// EnvelopeDeserializer is re-exported for use in tests only.
type EnvelopeDeserializer = envelopeDeserializer

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
		Topic:    topic,
		Key:      key,
		Value:    value,
		Headers:  headers,
		resolver: newResolver(s3, de, nooptrace.NewTracerProvider().Tracer("")),
	}
}

// NewTestWriter creates a Writer for use in tests, bypassing kafkarator.Connection.
// Inject a test S3 store and schema via [WithWriterS3Client] and [WithWriterSchemaFetcher].
// Additional write options ([WithWriterBucketResolver], [WithWriterRowGroupSize], [WithWriterPartSize]) are also accepted.
func NewTestWriter(kw kafkaWriter, ser serializer, opts ...WriterOption) *Writer {
	cfg := &writerConfig{}
	for _, o := range opts {
		o(cfg)
	}
	stager := newStagerWithFactory(cfg.s3Factory, cfg.fetcher, cfg.stagerOpts...)
	return &Writer{kw: kw, ser: ser, stager: stager}
}

// NewBytesReaderAt wraps a byte slice in a *bytes.Reader which implements
// both io.ReaderAt and io.ReadSeeker. Useful for opening a Parquet file
// from an in-memory buffer in tests.
func NewBytesReaderAt(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
