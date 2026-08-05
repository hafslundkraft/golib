package claimcheck_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// fakeSchemaFetcher implements SchemaFetcher for tests.
type fakeSchemaFetcher struct {
	schemaStr string
	version   int
	id        int
	subject   string // last subject requested
}

func (f *fakeSchemaFetcher) GetLatestSchema(
	_ context.Context,
	subject string,
) (schemaStr string, version, id int, err error) {
	f.subject = subject
	return f.schemaStr, f.version, f.id, nil
}

// simpleSchema returns a minimal Avro record schema with the given int fields.
func simpleSchema(fields ...string) string {
	avroFields := make([]string, len(fields))
	for i, name := range fields {
		avroFields[i] = fmt.Sprintf(`{"name":%q,"type":"int"}`, name)
	}
	return fmt.Sprintf(`{"type":"record","name":"R","fields":[%s]}`, strings.Join(avroFields, ","))
}

// fakeEnvelopeDeserializer implements EnvelopeDeserializer for tests.
type fakeEnvelopeDeserializer struct {
	envelope *claimcheck.Envelope
}

func (f *fakeEnvelopeDeserializer) DeserializeEnvelope(
	_ context.Context,
	_ string,
	_ []byte,
) (*claimcheck.Envelope, error) {
	return f.envelope, nil
}

// captureKW records the last message written to Kafka.
type captureKW struct {
	last *kafkarator.Message
}

func (c *captureKW) Write(_ context.Context, msg *kafkarator.Message, opts ...kafkarator.WriteOption) error {
	o := &kafkarator.WriteOptions{}
	for _, opt := range opts {
		opt(o)
	}

	c.last = msg

	if o.DeliveryChannel != nil {
		o.DeliveryChannel <- kafkarator.DeliveryReport{}
	}

	return nil
}

func (c *captureKW) Close(_ context.Context) error { return nil }

// jsonSerializer marshals any value as JSON — stand-in for AvroSerializer.
type jsonSerializer struct{}

func (j *jsonSerializer) Serialize(_ context.Context, _ string, value any) ([]byte, error) {
	return json.Marshal(value) //nolint:wrapcheck // test helper, wrapping adds no value
}

// unmarshalEnvelope decodes a JSON-serialized envelope captured from a test KafkaWriter.
func unmarshalEnvelope(t *testing.T, data []byte) *claimcheck.Envelope {
	t.Helper()
	var envelope claimcheck.Envelope
	require.NoError(t, json.Unmarshal(data, &envelope))
	return &envelope
}

// abortSpyS3 wraps FakeS3Client and counts the multipart calls that decide
// whether an upload was cleaned up or left dangling.
type abortSpyS3 struct {
	*claimcheck.FakeS3Client
	aborts    int
	completes int
}

func (s *abortSpyS3) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	s.aborts++
	return s.FakeS3Client.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

func (s *abortSpyS3) CompleteMultipartUpload(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []claimcheck.CompletedPart,
) error {
	s.completes++
	return s.FakeS3Client.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

// bucketAndKey splits an s3://bucket/key URI into its two components.
func bucketAndKey(t *testing.T, storageURI string) (bucket, key string) {
	t.Helper()
	trimmed := strings.TrimPrefix(storageURI, "s3://")
	idx := strings.IndexByte(trimmed, '/')
	require.Positive(t, idx, "invalid storage URI: %s", storageURI)
	return trimmed[:idx], trimmed[idx+1:]
}
