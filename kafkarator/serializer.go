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

// ValueSerializer serializes domain values into a byte representation
type ValueSerializer interface {
	// Serialize takes a value of any type and returns the final bytes to send to Kafka
	// For Avro: magic byte + schema ID (big-endian) + avro payload.
	Serialize(ctx context.Context, value any) ([]byte, error)
}

// AvroSerializer serializes values using Avro encoding and the
// Confluent Schema Registry wire format.
type AvroSerializer struct {
	srClient      sr.Client
	schemaSubject string
	tel           *telemetry.Provider

	mu           sync.Mutex
	schema       avro.Schema
	schemaID     int
	schemaLoaded bool
}

func newAvroSerializer(srClient sr.Client, topic string, tel *telemetry.Provider) *AvroSerializer {
	if tel == nil {
		panic("telemetry provider is nil")
	}
	if srClient == nil {
		panic("srClient provider is nil")
	}
	return &AvroSerializer{
		srClient:      srClient,
		schemaSubject: topic + "-value",
		tel:           tel,
	}
}

func (s *AvroSerializer) ensureSchemaLoaded(ctx context.Context) error {
	if s.schemaLoaded {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	spanCtx, span := s.tel.Tracer().Start(ctx, "AvroSerializer.ensureSchemaLoaded")
	defer span.End()

	span.SetAttributes(attribute.String("schema.subject", s.schemaSubject))

	meta, err := s.srClient.GetLatestSchemaMetadata(s.schemaSubject)
	if err != nil {
		s.tel.Logger().ErrorContext(spanCtx, "schema registry lookup failed",
			"subject", s.schemaSubject, "error", err)
		span.RecordError(err)
		return fmt.Errorf("load schema from registry: %w", err)
	}

	if meta.Schema == "" {
		emptyErr := fmt.Errorf("empty schema returned for subject %s", s.schemaSubject)
		s.tel.Logger().ErrorContext(spanCtx, "empty schema returned",
			"subject", s.schemaSubject)
		span.RecordError(emptyErr)
		return fmt.Errorf("returned empty: %s", s.schemaSubject)
	}

	// Parse schema using hamba/avro lib
	parsed, err := avro.Parse(meta.Schema)
	if err != nil {
		s.tel.Logger().ErrorContext(spanCtx, "avro schema parse failed",
			"subject", s.schemaSubject, "error", err)
		span.RecordError(err)
		return fmt.Errorf("parse avro schema: %w", err)
	}

	s.schema = parsed
	s.schemaID = meta.ID
	s.schemaLoaded = true
	span.SetAttributes(attribute.Int("schema.id", s.schemaID))

	return nil
}

// Serialize creates the Confluent-wire-format payload
func (s *AvroSerializer) Serialize(ctx context.Context, msg any) ([]byte, error) {
	spanCtx, span := s.tel.Tracer().Start(ctx, "AvroSerializer.Serialize")
	defer span.End()

	span.SetAttributes(
		attribute.String("schema.subject", s.schemaSubject),
	)
	if err := s.ensureSchemaLoaded(ctx); err != nil {
		span.RecordError(err)
		return nil, err
	}

	avroBytes, err := avro.Marshal(s.schema, msg)
	if err != nil {
		s.tel.Logger().ErrorContext(spanCtx, "avro marshal failed",
			"subject", s.schemaSubject, "error", err)
		span.RecordError(err)
		return nil, fmt.Errorf("avro encoding failed: %w", err)
	}

	final := make([]byte, 0, len(avroBytes)+5)

	// schema ID is 4 bytes big-endian
	final = append(final,
		0, // magic byte
		byte(s.schemaID>>24),
		byte(s.schemaID>>16),
		byte(s.schemaID>>8),
		byte(s.schemaID),
	)

	final = append(final, avroBytes...)
	span.SetAttributes(
		attribute.Int("schema.id", s.schemaID),
		attribute.Int("avro.payload_size", len(avroBytes)),
		attribute.Int("final.wire_size", len(final)),
	)
	return final, nil
}
