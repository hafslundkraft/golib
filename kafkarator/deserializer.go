package kafkarator

import (
	"context"
	"fmt"
	"sync"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/telemetry"
	"github.com/hamba/avro/v2"
	"go.opentelemetry.io/otel/attribute"
)

// ValueDeserializer deserializes bytes into domain values
type ValueDeserializer interface {
	// Deserialize takes bytes and returns the message deserialized with its given schema
	Deserialize(ctx context.Context, topic string, value []byte) (any, error)
}

// AvroDeserializer deserializes values with Avro encoding and the
// Confluent Schema Registry wire format to bytes
type AvroDeserializer struct {
	srClient sr.Client
	tel      *telemetry.Provider

	mu      sync.Mutex
	schemas map[string]cachedSchema
}

type cachedSchema struct {
	id     int
	schema avro.Schema
}

func newAvroDeserializer(srClient sr.Client, tel *telemetry.Provider) *AvroDeserializer {
	return &AvroDeserializer{
		srClient: srClient,
		tel:      tel,
		schemas:  map[string]cachedSchema{},
	}
}

// Deserialize deserializes the bytes to a domain value according to its schema
func (d *AvroDeserializer) Deserialize(ctx context.Context, topic string, value []byte) (any, error) {
	ctx, span := d.tel.Tracer().Start(ctx, "AvroDeserializer.Deserialize")
	defer span.End()

	// Not Avro Confluent framing
	if len(value) < 5 || value[0] != magicByte {
		return nil, nil
	}

	schemaID := int(value[1])<<24 | int(value[2])<<16 | int(value[3])<<8 | int(value[4])
	subject := topic + "-value"

	span.SetAttributes(
		attribute.String("schema.subject", subject),
		attribute.Int("schema.id", schemaID),
	)

	schema, err := d.loadSchema(ctx, subject, schemaID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var out any
	payload := value[5:]

	if err := avro.Unmarshal(schema, payload, &out); err != nil {
		d.tel.Logger().ErrorContext(ctx, "avro decode failed", "error", err)
		span.RecordError(err)
		return nil, fmt.Errorf("avro decode failed: %w", err)
	}

	span.SetAttributes(attribute.Int("payload_size", len(payload)))

	return out, nil
}

func (d *AvroDeserializer) loadSchema(ctx context.Context, subject string, schemaID int) (avro.Schema, error) {
	d.mu.Lock()
	s, ok := d.schemas[subject]
	d.mu.Unlock()

	if ok && s.id == schemaID {
		return s.schema, nil
	}

	ctx, span := d.tel.Tracer().Start(ctx, "AvroDeserializer.loadSchema")
	defer span.End()

	d.tel.Logger().InfoContext(ctx, "fetching schema from registry",
		"subject", subject, "schemaID", schemaID)

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
	d.schemas[subject] = cachedSchema{id: schemaID, schema: parsed}
	d.mu.Unlock()

	return parsed, nil
}
