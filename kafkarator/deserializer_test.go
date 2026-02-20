package kafkarator

import (
	"context"
	"errors"
	"testing"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hamba/avro/v2"
)

func TestAvroDeserializer_Success(t *testing.T) {
	ctx := context.Background()
	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	// Build valid avro payload
	schema, _ := avro.Parse(schemaStr)
	avroBytes, _ := avro.Marshal(schema, map[string]any{"id": "hello"})

	// Confluent framing
	wire := append([]byte{0, 0, 0, 0, 7}, avroBytes...)

	mock := newMockSRClient()
	mock.byID["test-topic-value"] = map[int]sr.SchemaInfo{
		7: {Schema: schemaStr},
	}

	tel := newMockTelemetry()

	d := newAvroDeserializer(
		mock,
		tel,
	)

	out, err := d.Deserialize(ctx, "test-topic", wire)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mapped := out.(map[string]any)
	if mapped["id"] != "hello" {
		t.Fatalf("wrong decoded value: %+v", mapped)
	}

	if mock.getBySubjectAndIDCalls != 1 {
		t.Fatalf("expected registry to be called once, got %d", mock.getBySubjectAndIDCalls)
	}
}

func TestAvroDeserializer_InvalidMagicByte(t *testing.T) {
	ctx := context.Background()
	mock := newMockSRClient()

	tel := newMockTelemetry()

	d := newAvroDeserializer(
		mock,
		tel,
	)

	out, err := d.Deserialize(ctx, "test-topic", []byte{9, 9, 9})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil (non-avro), got %#v", out)
	}
}

func TestAvroDeserializer_UnknownSchemaID(t *testing.T) {
	ctx := context.Background()

	mock := newMockSRClient()
	mock.errLatest = errors.New("schema not found")

	tel := newMockTelemetry()

	d := newAvroDeserializer(
		mock,
		tel,
	)

	wire := []byte{0, 0, 0, 0, 7} // no payload but valid header

	_, err := d.Deserialize(ctx, "test-topic", wire)
	if err == nil {
		t.Fatalf("expected schema registry error")
	}
}

func TestAvroDeserializer_BadPayload(t *testing.T) {
	ctx := context.Background()

	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	mock := newMockSRClient()
	mock.byID["test-topic-value"] = map[int]sr.SchemaInfo{
		7: {Schema: schemaStr},
	}

	tel := newMockTelemetry()

	d := newAvroDeserializer(
		mock,
		tel,
	)

	// invalid (non-avro) payload
	wire := []byte{0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF}

	_, err := d.Deserialize(ctx, "test-topic", wire)
	if err == nil {
		t.Fatalf("expected decode failure")
	}
}

func TestAvroDeserializer_DoesNotKeepLocalSchemaCache(t *testing.T) {
	ctx := context.Background()

	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	mock := newMockSRClient()
	mock.byID["test-topic-value"] = map[int]sr.SchemaInfo{
		7: {Schema: schemaStr},
	}

	tel := newMockTelemetry()

	d := newAvroDeserializer(
		mock,
		tel,
	)

	// Encode avro payload twice
	schema, _ := avro.Parse(schemaStr)
	avroBytes, _ := avro.Marshal(schema, map[string]any{"id": "hello"})
	wire := append([]byte{0, 0, 0, 0, 7}, avroBytes...)

	// First call loads schema from registry
	d.Deserialize(ctx, "test-topic", wire)

	// Second call goes through schema registry client again.
	// The client implementation is responsible for caching.
	d.Deserialize(ctx, "test-topic", wire)

	if mock.getBySubjectAndIDCalls != 2 {
		t.Fatalf("expected schema registry to be called twice, got %d", mock.getBySubjectAndIDCalls)
	}
}
