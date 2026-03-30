package kafkarator

import (
	"context"
	"errors"
	"strings"
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

	var out map[string]any
	err := d.Deserialize(ctx, "test-topic", wire, &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if out["id"] != "hello" {
		t.Fatalf("wrong decoded value: %+v", out)
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

	var out map[string]any
	err := d.Deserialize(ctx, "test-topic", []byte{9, 9, 9}, &out)
	if err == nil || !strings.HasPrefix(err.Error(), "invalid Avro framing") {
		t.Fatalf("expected invalid framing error, got %v", err)
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

	var out map[string]any
	err := d.Deserialize(ctx, "test-topic", wire, &out)
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

	var out map[string]any
	err := d.Deserialize(ctx, "test-topic", wire, &out)
	if err == nil {
		t.Fatalf("expected decode failure")
	}
}

func TestAvroDeserializer_IntoStruct(t *testing.T) {
	type TestMsg struct {
		ID string `avro:"id"`
	}

	ctx := context.Background()
	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	schema, _ := avro.Parse(schemaStr)
	avroBytes, _ := avro.Marshal(schema, TestMsg{ID: "hello"})
	wire := append([]byte{0, 0, 0, 0, 7}, avroBytes...)

	mock := newMockSRClient()
	mock.byID["test-topic-value"] = map[int]sr.SchemaInfo{
		7: {Schema: schemaStr},
	}

	d := newAvroDeserializer(mock, newMockTelemetry())

	var out TestMsg
	if err := d.Deserialize(ctx, "test-topic", wire, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.ID != "hello" {
		t.Fatalf("wrong decoded value: %+v", out)
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
	var out1 map[string]any
	_ = d.Deserialize(ctx, "test-topic", wire, &out1)
	// Second call goes through schema registry client again.
	// The client implementation is responsible for caching.
	_ = d.Deserialize(ctx, "test-topic", wire, &out1)
	if mock.getBySubjectAndIDCalls != 2 {
		t.Fatalf("expected schema registry to be called twice, got %d", mock.getBySubjectAndIDCalls)
	}
}
