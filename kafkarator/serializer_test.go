package kafkarator

import (
	"context"
	"testing"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/telemetry"
)

func TestEnsureSchemaLoadtype_Success(t *testing.T) {
	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	mock := newMockSRClient()
	mock.latest["test-topic-value"] = sr.SchemaMetadata{
		SchemaInfo: sr.SchemaInfo{Schema: schemaStr},
		ID:         42,
	}

	tel, shutdown := telemetry.New(context.Background(), "test", telemetry.WithLocal(true))
	defer shutdown(context.Background())

	s := newAvroSerializer(mock, "test-topic", tel)

	err := s.ensureSchemaLoaded(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !s.schemaLoaded {
		t.Fatal("expected schemaLoaded=true")
	}

	if s.schemaID != 42 {
		t.Fatalf("expected schemaID=42, got %d", s.schemaID)
	}
}

func TestSerialize_Success(t *testing.T) {
	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	mock := newMockSRClient()
	mock.latest["test-topic-value"] = sr.SchemaMetadata{
		SchemaInfo: sr.SchemaInfo{Schema: schemaStr},
		ID:         7,
	}

	tel, shutdown := telemetry.New(context.Background(), "test", telemetry.WithLocal(true))
	defer shutdown(context.Background())

	s := newAvroSerializer(mock, "test-topic", tel)

	msg := map[string]any{
		"id": "hello",
	}

	out, err := s.Serialize(context.Background(), msg)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Validate wire format: [magic][schemaID][payload]
	if out[0] != 0 {
		t.Fatalf("expected magic byte=0, got %d", out[0])
	}

	schemaID := int(out[1])<<24 | int(out[2])<<16 | int(out[3])<<8 | int(out[4])
	if schemaID != 7 {
		t.Fatalf("expected schema ID=7, got %d", schemaID)
	}

	payload := out[5:]
	if len(payload) == 0 {
		t.Fatal("expected payload not empty")
	}
}

func TestSerialize_MarshalError(t *testing.T) {
	schemaStr := `{
        "type": "record",
        "name": "TestMsg",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }`

	mock := newMockSRClient()
	mock.latest["topic-value"] = sr.SchemaMetadata{
		SchemaInfo: sr.SchemaInfo{Schema: schemaStr},
		ID:         42,
	}

	tel, shutdown := telemetry.New(context.Background(), "test", telemetry.WithLocal(true))
	defer shutdown(context.Background())

	s := newAvroSerializer(mock, "test-topic", tel)

	// Provide invalid message
	msg := map[string]any{
		"id": 123, // should be string → marshal should fail
	}

	_, err := s.Serialize(context.Background(), msg)
	if err == nil {
		t.Fatal("expected marshal error but got nil")
	}
}
