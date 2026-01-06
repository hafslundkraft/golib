package kafkarator

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func newWriter(
	p *kafka.Producer,
	topic string,
	pmc metric.Int64Counter,
	tel TelemetryProvider,
) *Writer {
	w := &Writer{
		producer:                p,
		topic:                   topic,
		producedMessagesCounter: pmc,
		tel:                     tel,
		closed:                  false,
	}

	return w
}

// Writer provides an interface for writing messages to the Kafka topic, as well
// as closing it when the client is done writing.
type Writer struct {
	producedMessagesCounter metric.Int64Counter
	producer                *kafka.Producer
	topic                   string
	tel                     TelemetryProvider
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
	ctx, span := w.tel.Tracer().Start(ctx, "kafkarator.Writer.Write")
	defer span.End()

	if w.closed {
		span.RecordError(fmt.Errorf("writer is closed"))
		return fmt.Errorf("writer is closed")
	}

	span.SetAttributes(attribute.String("writer.topic", w.topic))

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
		span.RecordError(err)
		return fmt.Errorf("produce message: %w", err)
	}

	ev := <-deliveryChan

	m := ev.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		span.RecordError(err)
		return fmt.Errorf("topic partiion error for delivery: %w", err)
	}

	w.producedMessagesCounter.Add(ctx, 1)
	return nil
}
