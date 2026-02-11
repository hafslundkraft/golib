package kafkarator

import (
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"go.opentelemetry.io/otel/attribute"
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

	mu          sync.Mutex
	schemaCache map[string]cachedSchema
}

func newAvroSerializer(srClient SchemaRegistryClient, tel TelemetryProvider) *AvroSerializer {
	if tel == nil {
		panic("telemetry provider is nil")
	}
	if srClient == nil {
		panic("srClient provider is nil")
	}

	return &AvroSerializer{
		srClient:    srClient,
		tel:         tel,
		schemaCache: make(map[string]cachedSchema),
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

	cached, err := s.getOrLoadSchema(ctx, subject)
	if err != nil {
		return nil, err
	}

	avroBytes, err := avro.Marshal(cached.schema, value)
	if err != nil {
		return nil, fmt.Errorf("avro marshal failed: %w", err)
	}

	// Confluent wire format:
	// magic byte (0) + schema ID (4 bytes big-endian) + payload
	final := make([]byte, 0, len(avroBytes)+5)
	final = append(final,
		0,
		byte(cached.schemaID>>24),
		byte(cached.schemaID>>16),
		byte(cached.schemaID>>8),
		byte(cached.schemaID),
	)
	final = append(final, avroBytes...)

	return final, nil
}

func (s *AvroSerializer) getOrLoadSchema(
	ctx context.Context,
	subject string,
) (cachedSchema, error) {
	s.mu.Lock()
	cached, ok := s.schemaCache[subject]
	s.mu.Unlock()

	if ok && cached.schemaLoaded {
		return cached, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if cached, ok := s.schemaCache[subject]; ok && cached.schemaLoaded {
		return cached, nil
	}

	_, span := s.tel.Tracer().Start(ctx, "kafkarator.AvroSerializer.loadSchema")
	defer span.End()

	span.SetAttributes(attribute.String("schema.subject", subject))

	meta, err := s.srClient.GetLatestSchemaMetadata(subject)
	if err != nil {
		span.RecordError(err)
		return cachedSchema{}, fmt.Errorf("get latest schema metadata: %w", err)
	}

	if meta.Schema == "" {
		return cachedSchema{}, fmt.Errorf("empty schema for subject %s", subject)
	}

	schema, err := avro.Parse(meta.Schema)
	if err != nil {
		span.RecordError(err)
		return cachedSchema{}, fmt.Errorf("parse avro schema: %w", err)
	}

	cached = cachedSchema{
		schema:       schema,
		schemaID:     meta.ID,
		schemaLoaded: true,
	}

	s.schemaCache[subject] = cached
	span.SetAttributes(attribute.Int("schema.id", meta.ID))

	return cached, nil
}
