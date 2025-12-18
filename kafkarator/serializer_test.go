package kafkarator

import (
	"context"
	"testing"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/telemetry"
)

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

	serializer := newAvroSerializer(
		mock,
		Options{
			UseLatestVersion: true,
			SubjectNameProvider: func(topic string) (string, error) {
				return topic + "-value", nil
			},
		},
		tel,
	)

	msg := map[string]any{
		"id": "hello",
	}

	out, err := serializer.Serialize(context.Background(), "test-topic", msg)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Confluent wire format:
	// [magic][schemaID][payload]
	if out[0] != 0 {
		t.Fatalf("expected magic byte=0, got %d", out[0])
	}

	schemaID := int(out[1])<<24 | int(out[2])<<16 | int(out[3])<<8 | int(out[4])
	if schemaID != 7 {
		t.Fatalf("expected schema ID=7, got %d", schemaID)
	}

	if len(out[5:]) == 0 {
		t.Fatal("expected non-empty payload")
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
	mock.latest["test-topic-value"] = sr.SchemaMetadata{
		SchemaInfo: sr.SchemaInfo{Schema: schemaStr},
		ID:         42,
	}

	tel, shutdown := telemetry.New(context.Background(), "test", telemetry.WithLocal(true))
	defer shutdown(context.Background())

	serializer := newAvroSerializer(
		mock,
		Options{
			UseLatestVersion: true,
			SubjectNameProvider: func(topic string) (string, error) {
				return topic + "-value", nil
			},
		},
		tel,
	)

	msg := map[string]any{
		"id": 123,
	}

	_, err := serializer.Serialize(context.Background(), "test-topic", msg)
	if err == nil {
		t.Fatal("expected marshal error but got nil")
	}
}
