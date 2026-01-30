package kafkarator

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

func newWriter(
	p *kafka.Producer,
	pmc messagingconv.ClientSentMessages,
	tel TelemetryProvider,
) *Writer {
	w := &Writer{
		producer:                p,
		producedMessagesCounter: pmc,
		tel:                     tel,
		closed:                  false,
	}

	return w
}

// Writer provides an interface for writing messages to the Kafka topic, as well
// as closing it when the client is done writing.
type Writer struct {
	producedMessagesCounter messagingconv.ClientSentMessages
	producer                *kafka.Producer
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
func (w *Writer) Write(ctx context.Context, message *Message) error {
	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}

	if message.Topic == "" {
		return fmt.Errorf("message topic cannot be empty")
	}

	ctx, span := startProducerSpan(ctx, w.tel.Tracer(), message.Topic)
	defer span.End()

	if w.closed {
		err := fmt.Errorf("writer is closed")
		setProducerError(span, err)
		return err
	}

	if message.Key != nil {
		span.SetAttributes(semconv.MessagingKafkaMessageKey(string(message.Key)))
	}

	traceHeaders := injectTraceContext(ctx, message.Headers)

	kafkaHeaders := make([]kafka.Header, 0, len(traceHeaders))
	for k, v := range traceHeaders {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: v})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &message.Topic,
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
		setProducerError(span, err)
		return fmt.Errorf("produce message: %w", err)
	}

	ev := <-deliveryChan

	m := ev.(*kafka.Message)
	partitionID := fmt.Sprintf("%d", m.TopicPartition.Partition)

	if m.TopicPartition.Error != nil {
		setProducerError(span, m.TopicPartition.Error)
		recordSentMessage(ctx, w.producedMessagesCounter, message.Topic, partitionID, m.TopicPartition.Error)
		return fmt.Errorf("topic partition error for delivery: %w", m.TopicPartition.Error)
	}

	setProducerSuccess(span, partitionID, int64(m.TopicPartition.Offset))
	recordSentMessage(ctx, w.producedMessagesCounter, message.Topic, partitionID, nil)
	return nil
}
