package kafkarator

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
)

// ValueSerializer serializes domain values into a byte representation
type ValueSerializer interface {
	// Serialize takes a value of any type and returns the final bytes to send to Kafka
	// For Avro: magic byte + schema ID (big-endian) + avro payload.
	Serialize(ctx context.Context, topic string, value any) ([]byte, error)
}

// AvroSerializer serializes values using Avro encoding and the
// Confluent Schema Registry wire format.
type AvroSerializer struct {
	srClient SchemaRegistryClient
	tel      TelemetryProvider
}

func newAvroSerializer(srClient SchemaRegistryClient, tel TelemetryProvider) *AvroSerializer {
	if tel == nil {
		panic("telemetry provider is nil")
	}
	if srClient == nil {
		panic("srClient provider is nil")
	}

	return &AvroSerializer{
		srClient: srClient,
		tel:      tel,
	}
}

// Serialize creates the Confluent-wire-format payload
func (s *AvroSerializer) Serialize(
	ctx context.Context,
	topic string,
	value any,
) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("cannot serialize nil value")
	}

	subject, err := defaultSubjectNameProvider(topic)
	if err != nil {
		return nil, err
	}

	meta, err := s.srClient.GetLatestSchemaMetadata(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("get latest schema metadata: %w", err)
	}

	if meta.Schema == "" {
		return nil, fmt.Errorf("empty schema for subject %s", subject)
	}

	schema, err := avro.Parse(meta.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	avroBytes, err := avro.Marshal(schema, value)
	if err != nil {
		return nil, fmt.Errorf("avro marshal failed: %w", err)
	}

	// Confluent wire format:
	// magic byte (0) + schema ID (4 bytes big-endian) + payload
	final := make([]byte, 0, len(avroBytes)+5)
	final = append(final,
		0,
		byte(meta.ID>>24),
		byte(meta.ID>>16),
		byte(meta.ID>>8),
		byte(meta.ID),
	)
	final = append(final, avroBytes...)

	return final, nil
}
