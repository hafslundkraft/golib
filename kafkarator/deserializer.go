package kafkarator

import (
	"context"
	"fmt"
	"sync"

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

	mu      sync.Mutex
	schemas map[string]cachedSchema
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
		schemas:  map[string]cachedSchema{},
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

	schema, err := d.loadSchema(ctx, subject, schemaID)
	if err != nil {
		return nil, err
	}

	var out any
	payload := value[5:]

	if err := avro.Unmarshal(schema, payload, &out); err != nil {
		return nil, fmt.Errorf("avro decode failed: %w", err)
	}

	return out, nil
}

func (d *AvroDeserializer) loadSchema(ctx context.Context, subject string, schemaID int) (avro.Schema, error) {
	d.mu.Lock()
	s, ok := d.schemas[subject]
	d.mu.Unlock()

	if ok && s.schemaID == schemaID {
		return s.schema, nil
	}

	_, span := d.tel.Tracer().Start(ctx, "kafkarator.AvroDeserializer.loadSchema")
	defer span.End()

	info, err := d.srClient.GetBySubjectAndID(subject, schemaID)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("schema registry lookup: %w", err)
	}

	parsed, err := avro.Parse(info.Schema)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("schema parse failed: %w", err)
	}

	d.mu.Lock()
	d.schemas[subject] = cachedSchema{schemaID: schemaID, schema: parsed}
	d.mu.Unlock()

	return parsed, nil
}
