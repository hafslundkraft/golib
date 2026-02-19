package kafkarator

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
)

// ValueDeserializer deserializes bytes into domain values
type ValueDeserializer interface {
	// Deserialize takes bytes and returns the message deserialized with its given schema
	Deserialize(ctx context.Context, topic string, value []byte) (any, error)
}

// AvroDeserializer deserializes values with Avro encoding and the
// Confluent Schema Registry wire format to bytes
type AvroDeserializer struct {
	srClient SchemaRegistryClient
	tel      TelemetryProvider
	cache    *parsedSchemaCache
}

func newAvroDeserializer(
	srClient SchemaRegistryClient,
	tel TelemetryProvider,
) *AvroDeserializer {
	if srClient == nil {
		panic("srClient not provided")
	}
	if tel == nil {
		panic("telemetry provider was not given")
	}

	return &AvroDeserializer{
		srClient: srClient,
		tel:      tel,
		cache:    newParsedSchemaCache(),
	}
}

// Deserialize deserializes the bytes to a domain value according to its schema
func (d *AvroDeserializer) Deserialize(ctx context.Context, topic string, value []byte) (any, error) {
	// Not Avro Confluent framing
	if len(value) < 5 || value[0] != magicByte {
		return nil, nil
	}

	schemaID := int(value[1])<<24 | int(value[2])<<16 | int(value[3])<<8 | int(value[4])
	subject, err := defaultSubjectNameProvider(topic)
	if err != nil {
		return nil, err
	}

	info, err := d.srClient.GetBySubjectAndID(ctx, subject, schemaID)
	if err != nil {
		return nil, fmt.Errorf("schema registry lookup: %w", err)
	}

	schema, err := d.cache.GetOrParse(schemaID, subject, info.Schema)
	if err != nil {
		return nil, fmt.Errorf("get or parse schema: %w", err)
	}

	var out any
	payload := value[5:]

	if err := avro.Unmarshal(schema, payload, &out); err != nil {
		return nil, fmt.Errorf("avro decode failed: %w", err)
	}

	return out, nil
}
