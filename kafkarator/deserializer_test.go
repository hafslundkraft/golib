package kafkarator

import (
	"context"
	"errors"
	"testing"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/telemetry"
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

	tel, shutdown := telemetry.New(ctx, "test", telemetry.WithLocal(true))
	defer shutdown(ctx)

	d := newAvroDeserializer(
		mock,
		Options{
			SubjectNameProvider: defaultSubjectNameProvider,
		},
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

	tel, shutdown := telemetry.New(ctx, "test", telemetry.WithLocal(true))
	defer shutdown(ctx)

	d := newAvroDeserializer(
		mock,
		Options{
			SubjectNameProvider: func(topic string) (string, error) {
				return topic + "-value", nil
			},
		},
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

	tel, shutdown := telemetry.New(ctx, "test", telemetry.WithLocal(true))
	defer shutdown(ctx)

	d := newAvroDeserializer(
		mock,
		Options{
			SubjectNameProvider: func(topic string) (string, error) {
				return topic + "-value", nil
			},
		},
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

	tel, shutdown := telemetry.New(ctx, "test", telemetry.WithLocal(true))
	defer shutdown(ctx)

	d := newAvroDeserializer(
		mock,
		Options{
			SubjectNameProvider: func(topic string) (string, error) {
				return topic + "-value", nil
			},
		},
		tel,
	)

	// invalid (non-avro) payload
	wire := []byte{0, 0, 0, 0, 1, 0xFF, 0xFF, 0xFF}

	_, err := d.Deserialize(ctx, "test-topic", wire)
	if err == nil {
		t.Fatalf("expected decode failure")
	}
}

func TestAvroDeserializer_SchemaCache(t *testing.T) {
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

	tel, shutdown := telemetry.New(ctx, "test", telemetry.WithLocal(true))
	defer shutdown(ctx)

	d := newAvroDeserializer(
		mock,
		Options{},
		tel,
	)

	// Encode avro payload twice
	schema, _ := avro.Parse(schemaStr)
	avroBytes, _ := avro.Marshal(schema, map[string]any{"id": "hello"})
	wire := append([]byte{0, 0, 0, 0, 7}, avroBytes...)

	// First call loads schema from registry
	d.Deserialize(ctx, "test-topic", wire)

	// Second call must hit cache
	d.Deserialize(ctx, "test-topic", wire)

	if mock.getBySubjectAndIDCalls != 1 {
		t.Fatalf("expected schema registry to be called once, got %d", mock.getBySubjectAndIDCalls)
	}
}
