package kafkarator

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// OpenTelemetry semantic conventions for Kafka messaging.
// See: https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
const (
	// Messaging system
	MessagingSystem      = "messaging.system"
	MessagingSystemKafka = "kafka"

	// Messaging operation attributes
	MessagingOperationType = "messaging.operation.type"
	MessagingOperationName = "messaging.operation.name"

	// Operation types
	MessagingOperationTypeSend    = "send"
	MessagingOperationTypeReceive = "receive"
	MessagingOperationTypeProcess = "process"
	MessagingOperationTypeSettle  = "settle"

	// Operation names
	MessagingOperationNameSend   = "send"
	MessagingOperationNamePoll   = "poll"
	MessagingOperationNameCommit = "commit"

	// Error type
	MessagingErrorType = "error.type"
	DefaultErrorType   = "_OTHER"

	// Messaging destination attributes
	MessagingDestinationName        = "messaging.destination.name"
	MessagingDestinationPartitionID = "messaging.destination.partition.id"

	// Messaging consumer attributes
	MessagingConsumerGroupName = "messaging.consumer.group.name"

	// Messaging batch attributes
	MessagingBatchMessageCount = "messaging.batch.message_count"

	// Kafka-specific attributes
	MessagingKafkaMessageKey = "messaging.kafka.message.key"
	MessagingKafkaOffset     = "messaging.kafka.offset"
)

// getErrorType extracts a low-cardinality error type from an error.
// For Kafka errors, it returns the error code. For other errors, it returns a generic type.
func getErrorType(err error) string {
	if err == nil {
		return ""
	}

	// Check if it's a Kafka error with a code
	if ke, ok := err.(interface{ Code() int }); ok {
		return fmt.Sprintf("kafka_error_%d", ke.Code())
	}

	// For other errors, return a generic type to maintain low cardinality
	return DefaultErrorType
}

// startProducerSpan creates a span for a Kafka send operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startProducerSpan(ctx context.Context, tracer trace.Tracer, topic string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNameSend, topic)
	ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindProducer))

	span.SetAttributes(
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingDestinationName, topic),
		attribute.String(MessagingOperationName, MessagingOperationNameSend),
		attribute.String(MessagingOperationType, MessagingOperationTypeSend))

	return ctx, span
}

// startConsumerSpan creates a span for a Kafka poll operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startConsumerSpan(ctx context.Context, tracer trace.Tracer, topic, group string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNamePoll, topic)
	ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))

	span.SetAttributes(
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingDestinationName, topic),
		attribute.String(MessagingConsumerGroupName, group),
		attribute.String(MessagingOperationName, MessagingOperationNamePoll),
		attribute.String(MessagingOperationType, MessagingOperationTypeReceive))

	return ctx, span
}

// startCommitSpan creates a span for a Kafka commit operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startCommitSpan(ctx context.Context, tracer trace.Tracer, topic, group string) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNameCommit, topic)
	ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))

	span.SetAttributes(
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingDestinationName, topic),
		attribute.String(MessagingConsumerGroupName, group),
		attribute.String(MessagingOperationName, MessagingOperationNameCommit),
		attribute.String(MessagingOperationType, MessagingOperationTypeSettle),
	)

	return ctx, span
}

// recordSentMessage records a metric for a sent message with standard attributes.
func recordSentMessage(ctx context.Context, counter metric.Int64Counter, topic, partition string, err error) {
	attrs := make([]attribute.KeyValue, 0, 5)

	attrs = append(attrs,
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingOperationName, MessagingOperationNameSend),
	)

	if topic != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationName, topic))
	}

	if partition != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationPartitionID, partition))
	}

	if err != nil {
		attrs = append(attrs, attribute.String(MessagingErrorType, getErrorType(err)))
	}

	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// recordConsumedMessage records a metric for a consumed message with standard attributes.
func recordConsumedMessage(
	ctx context.Context,
	counter metric.Int64Counter,
	topic, group, partition string,
	err error,
) {
	attrs := make([]attribute.KeyValue, 0, 6)

	attrs = append(attrs,
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingOperationName, MessagingOperationNamePoll),
	)

	if topic != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationName, topic))
	}

	if group != "" {
		attrs = append(attrs, attribute.String(MessagingConsumerGroupName, group))
	}

	if partition != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationPartitionID, partition))
	}

	if err != nil {
		attrs = append(attrs, attribute.String(MessagingErrorType, getErrorType(err)))
	}

	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// recordPollFailure records a metric for a poll failure with standard attributes.
func recordPollFailure(ctx context.Context, counter metric.Int64Counter, topic, group string, err error) {
	attrs := make([]attribute.KeyValue, 0, 6)

	attrs = append(attrs,
		attribute.String(MessagingSystem, MessagingSystemKafka),
		attribute.String(MessagingDestinationName, topic),
		attribute.String(MessagingConsumerGroupName, group),
		attribute.String(MessagingOperationName, MessagingOperationNamePoll),
		attribute.String(MessagingOperationType, MessagingOperationTypeReceive),
	)

	if err != nil {
		attrs = append(attrs, attribute.String(MessagingErrorType, getErrorType(err)))
	}

	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// setProducerSuccess sets span attributes and status for a successful send.
func setProducerSuccess(span trace.Span, partition string, offset int64) {
	span.SetAttributes(
		attribute.String(MessagingDestinationPartitionID, partition),
		attribute.Int64(MessagingKafkaOffset, offset),
	)
	span.SetStatus(codes.Ok, "message sent successfully")
}

// setProducerError sets span attributes and status for a failed send.
func setProducerError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	span.SetAttributes(attribute.String(MessagingErrorType, getErrorType(err)))
}

// setConsumerSuccess sets span attributes and status for successful poll.
func setConsumerSuccess(span trace.Span, messageCount int, partition string, offset int64) {
	if messageCount > 0 {
		attrs := []attribute.KeyValue{}

		// Only set batch count for actual batches (2+ messages)
		if messageCount > 1 {
			attrs = append(attrs, attribute.Int(MessagingBatchMessageCount, messageCount))
		}

		// Only set partition/offset if provided (single partition batch)
		if partition != "" {
			attrs = append(attrs,
				attribute.String(MessagingDestinationPartitionID, partition),
				attribute.Int64(MessagingKafkaOffset, offset),
			)
		}

		if len(attrs) > 0 {
			span.SetAttributes(attrs...)
		}
		span.SetStatus(codes.Ok, "messages received")
	} else {
		span.SetStatus(codes.Ok, "no messages available")
	}
}

// setConsumerError sets span attributes and status for failed poll.
func setConsumerError(span trace.Span, err error, isTimeout bool) {
	if isTimeout {
		span.SetStatus(codes.Ok, "poll timeout")
	} else {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String(MessagingErrorType, getErrorType(err)))
	}
}
