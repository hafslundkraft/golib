package kafkarator

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

const magicByte = 0

// CommitFunc commits offsets for messages previously read.
// It is safe to call multiple times; committing is idempotent.
type CommitFunc func(ctx context.Context) error

func newReader(
	c *kafka.Consumer,
	rmc messagingconv.ClientConsumedMessages,
	opDur messagingconv.ClientOperationDuration,
	srv serverInfo,
	tel TelemetryProvider,
	topic string,
	consumerGroup string,
) (*Reader, error) {
	r := &Reader{
		consumer:            c,
		readMessagesCounter: rmc,
		operationDuration:   opDur,
		srv:                 srv,
		tel:                 tel,
		topic:               topic,
		consumerGroup:       consumerGroup,
	}

	return r, nil
}

// Reader provides an interface for reading messages from a Kafka topic, as well
// as closing it when the client is done reading. Additionally, the act of fetching
// messages and committing them ("committing" == registering the largest offset per
// partition as the high watermark within the consumer group) is split giving the
// client total control and responsibility.
type Reader struct {
	consumer            *kafka.Consumer
	readMessagesCounter messagingconv.ClientConsumedMessages
	operationDuration   messagingconv.ClientOperationDuration
	srv                 serverInfo
	tel                 TelemetryProvider
	closed              bool
	topic               string
	consumerGroup       string
}

// Close closes releases the underlying infrastructure, and renders this instance unusable.
func (r *Reader) Close(ctx context.Context) error {
	if r.closed {
		return nil // It's ok to close multiple times.
	}

	if err := r.consumer.Close(); err != nil {
		return fmt.Errorf("error closing reader: %w", err)
	}
	r.closed = true
	return nil
}

// Read returns a slice of messages at most maxMessages long. If the duration maxWait
// is exceeded before maxMessages have been fetched from the topic, the func will
// return with as many messages in the list as were fetched before timeout.
//
// commiter can be used to commit the high watermark per partition to the consumer group. It
// is up to the client if and when commiter is invoked. Committing often can affect
// performance considerably in a high-volume scenario, so the client could for example
// employ a strategy where commiter is only invoked every N iterations.
func (rc *Reader) Read(
	ctx context.Context,
	maxMessages int,
	maxWait time.Duration,
) ([]Message, CommitFunc, error) {
	ctx, span := startPollSpan(ctx, rc.tel.Tracer(), rc.topic, rc.consumerGroup, rc.srv)
	defer span.End()

	// Record one operation.duration per receive, matching the poll span. Emitted
	// on every read — a caught-up read that waits out maxWait is expected, not an
	// error — so the metric mirrors the span duration in all cases.
	readStart := time.Now()
	var pollErr error
	defer func() {
		rc.recordReceiveDuration(ctx, time.Since(readStart).Seconds(), pollErr)
	}()

	deadline := time.Now().Add(maxWait)
	msgs := make([]Message, 0, maxMessages)

	// Track last seen offsets per partition
	latestOffsets := map[int32]kafka.Offset{}

	commit := CommitFunc(func(ctx context.Context) error {
		_, span := startCommitSpan(ctx, rc.tel.Tracer(), rc.topic, rc.consumerGroup, rc.srv)
		defer span.End()

		commitStart := time.Now()
		var commitErr error
		defer func() {
			rc.recordCommitDuration(ctx, time.Since(commitStart).Seconds(), commitErr)
		}()

		for partition, off := range latestOffsets {
			_, err := rc.consumer.CommitOffsets([]kafka.TopicPartition{
				{
					Topic:     &rc.topic,
					Partition: partition,
					Offset:    off + 1,
				},
			})
			if err != nil {
				commitErr = err
				setSpanError(span, err)
				return fmt.Errorf("commit failed: %w", err)
			}
		}

		span.SetStatus(codes.Ok, "offsets committed")
		return nil
	})

	for len(msgs) < maxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		ev := rc.consumer.Poll(int(remaining.Milliseconds()))
		if ev == nil {
			break
		}

		switch e := ev.(type) {

		case *kafka.Message:
			msgs = append(msgs, Message{
				Topic:     *e.TopicPartition.Topic,
				Partition: int(e.TopicPartition.Partition),
				Offset:    int64(e.TopicPartition.Offset),
				Key:       e.Key,
				Value:     e.Value,
				Headers:   convertHeaders(e.Headers),
			})

			latestOffsets[e.TopicPartition.Partition] = e.TopicPartition.Offset
			partitionID := fmt.Sprintf("%d", e.TopicPartition.Partition)
			rc.recordConsumed(ctx, partitionID, nil)

		case kafka.Error:
			setPollError(span, e, e.IsTimeout())
			if e.IsTimeout() {
				return msgs, commit, nil
			}
			rc.recordConsumed(ctx, "", e)
			pollErr = e
			return msgs, commit, fmt.Errorf("poll error: %w", e)

		default:
			// ignore rebalance events etc.
		}
	}

	if len(msgs) > 0 {
		// Check if all messages are from the same partition
		firstPartition := msgs[0].Partition
		samePartition := true
		for _, msg := range msgs[1:] {
			if msg.Partition != firstPartition {
				samePartition = false
				break
			}
		}

		if samePartition {
			// Single partition: include partition and offset details
			partitionID := fmt.Sprintf("%d", firstPartition)
			setPollSuccess(span, len(msgs), partitionID, msgs[0].Offset)
		} else {
			// Multiple partitions: only include batch count
			setPollSuccess(span, len(msgs), "", 0)
		}
	} else {
		setPollSuccess(span, 0, "", 0)
	}

	return msgs, commit, nil
}

// recordReceiveDuration records messaging.client.operation.duration for a
// poll/receive covering the whole Read.
func (rc *Reader) recordReceiveDuration(ctx context.Context, seconds float64, err error) {
	recordOperationDuration(
		ctx,
		rc.operationDuration,
		MessagingOperationNamePoll,
		messagingconv.OperationTypeReceive,
		rc.topic,
		rc.consumerGroup,
		"",
		rc.srv,
		seconds,
		err,
	)
}

// recordCommitDuration records messaging.client.operation.duration for an
// offset commit (settle).
func (rc *Reader) recordCommitDuration(ctx context.Context, seconds float64, err error) {
	recordOperationDuration(
		ctx,
		rc.operationDuration,
		MessagingOperationNameCommit,
		messagingconv.OperationTypeSettle,
		rc.topic,
		rc.consumerGroup,
		"",
		rc.srv,
		seconds,
		err,
	)
}

// recordConsumed records one messaging.client.consumed.messages for a delivered
// message, or a failed receive when err is non-nil.
func (rc *Reader) recordConsumed(ctx context.Context, partition string, err error) {
	recordConsumedMessage(ctx, rc.readMessagesCounter, rc.topic, rc.consumerGroup, partition, rc.srv, err)
}
