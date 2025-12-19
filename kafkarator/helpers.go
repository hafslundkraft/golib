package kafkarator

import (
	"context"
	"fmt"
	"maps"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func defaultSubjectNameProvider(topic string) (string, error) {
	if topic == "" {
		return "", fmt.Errorf("topic is empty")
	}
	return topic + "-value", nil
}

func convertHeaders(hdrs []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = h.Value
	}
	return m
}

// injectTraceContext extracts the trace context from the current span and injects it into the headers
func injectTraceContext(ctx context.Context, headers map[string][]byte) map[string][]byte {
	// Create a new map to avoid modifying the original
	propagatedHeaders := make(map[string][]byte, len(headers))
	maps.Copy(propagatedHeaders, headers)

	// Use a MapCarrier to inject trace context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Add trace context headers to the Kafka headers
	for k, v := range carrier {
		propagatedHeaders[k] = []byte(v)
	}

	return propagatedHeaders
}
