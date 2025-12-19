package kafkarator

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hafslundkraft/golib/telemetry"
	"go.opentelemetry.io/otel/metric"
)

func newWriter(
	p *kafka.Producer,
	pmc metric.Int64Counter,
	topic string,
	tel *telemetry.Provider,
) *Writer {
	w := &Writer{
		producer:                p,
		tel:                     tel,
		producedMessagesCounter: pmc,
		topic:                   topic,
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
func (w *Writer) Write(ctx context.Context, message *Message) error {
	if w.closed {
		return fmt.Errorf("writer closed")
	}

	traceHeaders := injectTraceContext(ctx, message.Headers)

	kafkaHeaders := make([]kafka.Header, 0, len(traceHeaders))
	for k, v := range traceHeaders {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: v})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &w.topic,
			Partition: kafka.PartitionAny,
		},
		Value:   message.Value,
		Key:     message.Key,
		Headers: kafkaHeaders,
	}

	// Produce messages synchronously
	deliveryChan := make(chan kafka.Event, 1)
	err := w.producer.Produce(msg, deliveryChan)
	if err != nil {
		w.tel.Logger().ErrorContext(ctx, fmt.Sprintf("producing message: %v", err))
		return fmt.Errorf("produce kafka message: %w", err)
	}

	ev := <-deliveryChan

	m := ev.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		w.tel.Logger().ErrorContext(ctx,
			"delivery failed", "topic", message.Topic, "error", m.TopicPartition.Error,
		)
		return fmt.Errorf("deliver kafka message: %w", err)
	}

	w.producedMessagesCounter.Add(ctx, 1)
	return nil
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
