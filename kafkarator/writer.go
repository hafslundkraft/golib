package kafkarator

import (
	"context"
	"fmt"
	"maps"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hafslundkraft/golib/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

func newWriter(
	p *kafka.Producer,
	topic string,
	pmc metric.Int64Counter,
	tel *telemetry.Provider,
) *Writer {
	w := &Writer{
		producer:                p,
		topic:                   topic,
		tel:                     tel,
		producedMessagesCounter: pmc,
		closed:                  false,
	}

	go w.handleDeliveryReports()

	return w
}

// Writer provides an interface for writing messages to the Kafka topic, as well
// as closing it when the client is done writing.
type Writer struct {
	producedMessagesCounter metric.Int64Counter
	producer                *kafka.Producer
	topic                   string
	tel                     *telemetry.Provider
	closed                  bool
}

// Close closes the underlying infrastructure, and renders this interface unusable for writing messages.
func (w *Writer) Close(ctx context.Context) error {
	if w.closed {
		return nil // It's ok to close multiple times.
	}

	w.producer.Flush(5000)
	w.producer.Close()

	w.closed = true

	return nil
}

// Write writes the given message with headers to the topic. An important side effect is
// that if there is an OpenTelemetry tracing span associated with the context, it is extracted
// and included in the header that is sent to Kafka.
func (w *Writer) Write(ctx context.Context, key, value []byte, headers map[string][]byte) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	traceHeaders := injectTraceContext(ctx, headers)

	kafkaHeaders := make([]kafka.Header, 0, len(traceHeaders))
	for k, v := range traceHeaders {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: v})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &w.topic,
			Partition: kafka.PartitionAny,
		},
		Value:   value,
		Key:     key,
		Headers: kafkaHeaders,
	}

	// Produce messages synchronously
	deliveryChan := make(chan kafka.Event, 1)
	err := w.producer.Produce(msg, deliveryChan)
	if err != nil {
		w.tel.Logger().ErrorContext(ctx, fmt.Sprintf("failed to produce message: %v", err))
		return fmt.Errorf("failed to produce message: %w", err)
	}

	ev := <-deliveryChan

	m := ev.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		w.tel.Logger().ErrorContext(ctx,
			"delivery failed", "topic", w.topic, "error", m.TopicPartition.Error,
		)
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	w.producedMessagesCounter.Add(ctx, 1)
	return nil
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

// handleDeliveryReports logs delivery failures for observability.
//
//nolint:sloglint // context not available in kafka delivery callback
func (w *Writer) handleDeliveryReports() {
	for e := range w.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				w.tel.Logger().Error(
					"delivery failed",
					"topic", *ev.TopicPartition.Topic,
					"partition", ev.TopicPartition.Partition,
					"offset", ev.TopicPartition.Offset,
					"error", ev.TopicPartition.Error,
				)
			}
		default:
			// Ignore other producer events
		}
	}
}
