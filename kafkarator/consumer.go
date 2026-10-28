package kafkarator

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

// consumerCore is a Kafka consumer with the OAuth refresh loop feeding it and
// its telemetry, neither subscribed nor assigned partitions. [Reader] builds
// on it.
type consumerCore struct {
	consumer            *kafka.Consumer
	readMessagesCounter messagingconv.ClientConsumedMessages
	operationDuration   messagingconv.ClientOperationDuration
	srv                 serverInfo
	tel                 TelemetryProvider
	closed              atomic.Bool
	topic               string
	// consumerGroup is recorded in telemetry. Empty when not reading as a
	// member of a group.
	consumerGroup string

	// stopAuth stops the OAuth token refresh goroutine feeding this consumer
	// and waits for it to exit. Nil when not using SASL.
	stopAuth func()
}

// newConsumerCore wraps a consumer. stopAuth may be nil; see the field on
// [consumerCore].
func newConsumerCore(
	c *kafka.Consumer,
	rmc messagingconv.ClientConsumedMessages,
	opDur messagingconv.ClientOperationDuration,
	srv serverInfo,
	tel TelemetryProvider,
	topic string,
	consumerGroup string,
	stopAuth func(),
) *consumerCore {
	return &consumerCore{
		consumer:            c,
		readMessagesCounter: rmc,
		operationDuration:   opDur,
		srv:                 srv,
		tel:                 tel,
		topic:               topic,
		consumerGroup:       consumerGroup,
		stopAuth:            stopAuth,
	}
}

// close waits for the OAuth refresh loop to exit before destroying the
// consumer; otherwise the loop could call into a freed librdkafka handle.
// Calls after the first do nothing.
func (cc *consumerCore) close() error {
	if !cc.closed.CompareAndSwap(false, true) {
		return nil // It's ok to close multiple times.
	}

	// Before the consumer is torn down, and unconditionally, so a failed close
	// does not leak the goroutine for the lifetime of the process.
	if cc.stopAuth != nil {
		cc.stopAuth()
	}

	if err := cc.consumer.Close(); err != nil {
		return fmt.Errorf("error closing reader: %w", err)
	}
	return nil
}

// recordReceiveDuration records messaging.client.operation.duration for a
// poll/receive.
func (cc *consumerCore) recordReceiveDuration(ctx context.Context, seconds float64, err error) {
	recordOperationDuration(
		ctx,
		cc.operationDuration,
		MessagingOperationNamePoll,
		messagingconv.OperationTypeReceive,
		cc.topic,
		cc.consumerGroup,
		"",
		cc.srv,
		seconds,
		err,
	)
}

// recordConsumed records one messaging.client.consumed.messages for a delivered
// message, or a failed receive when err is non-nil.
func (cc *consumerCore) recordConsumed(ctx context.Context, partition string, err error) {
	recordConsumedMessage(ctx, cc.readMessagesCounter, cc.topic, cc.consumerGroup, partition, cc.srv, err)
}

func toMessage(e *kafka.Message) Message {
	return Message{
		Topic:     *e.TopicPartition.Topic,
		Partition: int(e.TopicPartition.Partition),
		Offset:    int64(e.TopicPartition.Offset),
		Key:       e.Key,
		Value:     e.Value,
		Headers:   convertHeaders(e.Headers),
	}
}
