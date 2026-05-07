package claimcheck_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
	"github.com/stretchr/testify/require"
)

// fakeSchemaFetcher implements SchemaFetcher for tests.
type fakeSchemaFetcher struct {
	schemaStr string
	version   int
	id        int
	subject   string // last subject requested
}

func (f *fakeSchemaFetcher) GetLatestSchema(_ context.Context, subject string) (string, int, int, error) {
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
	env *claimcheck.Envelope
}

func (f *fakeEnvelopeDeserializer) DeserializeEnvelope(_ context.Context, _ string, _ []byte) (*claimcheck.Envelope, error) {
	return f.env, nil
}

// captureKW records the last message written to Kafka.
type captureKW struct {
	last *kafkarator.Message
}

func (c *captureKW) Write(_ context.Context, msg *kafkarator.Message) error {
	c.last = msg
	return nil
}

func (c *captureKW) Close(_ context.Context) error { return nil }

// jsonSerializer marshals any value as JSON — stand-in for AvroSerializer.
type jsonSerializer struct{}

func (j *jsonSerializer) Serialize(_ context.Context, _ string, value any) ([]byte, error) {
	return json.Marshal(value)
}

// unmarshalEnvelope decodes a JSON-serialized envelope captured from a test KafkaWriter.
func unmarshalEnvelope(t *testing.T, data []byte) *claimcheck.Envelope {
	t.Helper()
	var env claimcheck.Envelope
	require.NoError(t, json.Unmarshal(data, &env))
	return &env
}

// bucketAndKey splits an s3://bucket/key URI into its two components.
func bucketAndKey(t *testing.T, storageURI string) (string, string) {
	t.Helper()
	trimmed := strings.TrimPrefix(storageURI, "s3://")
	idx := strings.IndexByte(trimmed, '/')
	require.Positive(t, idx, "invalid storage URI: %s", storageURI)
	return trimmed[:idx], trimmed[idx+1:]
}
