package kafkarator

import (
	"context"
	"fmt"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

const meterProducedMessages = "messages_produced_total"

type producer struct {
	writer     *kafka.Writer
	tel        *telemetry.Provider
	msgCounter metric.Int64Counter
}

func (p *producer) Produce(ctx context.Context, msg []byte, headers map[string][]byte) error {
	// Inject trace context into headers if telemetry is configured
	if p.tel != nil {
		headers = p.injectTraceContext(ctx, headers)
	}

	if err := p.writer.WriteMessages(ctx, kafkaMessage(msg, headers)); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	p.msgCounter.Add(ctx, 1)

	return nil
}

// injectTraceContext extracts the trace context from the current span and injects it into the headers
func (p *producer) injectTraceContext(ctx context.Context, headers map[string][]byte) map[string][]byte {
	// Create a new map to avoid modifying the original
	propagatedHeaders := make(map[string][]byte, len(headers))
	for k, v := range headers {
		propagatedHeaders[k] = v
	}

	// Use a MapCarrier to inject trace context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Add trace context headers to the Kafka headers
	for k, v := range carrier {
		propagatedHeaders[k] = []byte(v)
	}

	return propagatedHeaders
}

func kafkaMessage(b []byte, headers map[string][]byte) kafka.Message {
	headerList := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		headerList = append(headerList, kafka.Header{Key: k, Value: v})
	}

	return kafka.Message{
		Value:   b,
		Headers: headerList,
	}
}
