package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

const magicByte = 0

// CommitFunc commits offsets for messages previously read.
// It is safe to call multiple times; committing is idempotent.
type CommitFunc func(ctx context.Context) error

// newReader wraps an already-subscribed consumer. stopAuth may be nil; see the
// field on [Reader].
func newReader(
	c *kafka.Consumer,
	rmc messagingconv.ClientConsumedMessages,
	opDur messagingconv.ClientOperationDuration,
	srv serverInfo,
	tel TelemetryProvider,
	topic string,
	consumerGroup string,
	stopAuth func(),
) *Reader {
	return &Reader{
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
	closed              atomic.Bool
	topic               string
	consumerGroup       string

	// stopAuth stops the OAuth token refresh goroutine feeding this reader's
	// consumer and waits for it to exit. Nil when not using SASL.
	stopAuth func()

	// partitionEOF is set by [WithPartitionEOF]. While [Reader.ReadToEnd]
	// runs, atEnd holds the partitions that have reached their end, and
	// partitions is how many the topic has.
	partitionEOF bool
	atEnd        map[int32]bool
	partitions   int
}

// Close closes releases the underlying infrastructure, and renders this instance unusable.
//
// Close ignores ctx. It waits for the OAuth refresh loop to exit before
// destroying the consumer; otherwise the loop could call into a freed
// librdkafka handle.
func (r *Reader) Close(ctx context.Context) error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil // It's ok to close multiple times.
	}

	// Before the consumer is torn down, and unconditionally, so a failed close
	// does not leak the goroutine for the lifetime of the process.
	if r.stopAuth != nil {
		r.stopAuth()
	}

	if err := r.consumer.Close(); err != nil {
		return fmt.Errorf("error closing reader: %w", err)
	}
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
	msgs := make([]Message, 0, maxMessages)
	// Always stamp batch attrs (plus duration/status), even when the read fails
	// mid-batch, with the messages collected so far.
	defer func() {
		setPollSpanAttrs(span, msgs)
		rc.recordReceiveDuration(ctx, time.Since(readStart).Seconds(), pollErr)
		setSpanStatus(span, pollErr)
	}()

	deadline := time.Now().Add(maxWait)

	// Track last seen offsets per partition
	latestOffsets := map[int32]kafka.Offset{}

	commit := CommitFunc(func(ctx context.Context) error {
		commitCtx, span := startCommitSpan(ctx, rc.tel.Tracer(), rc.topic, rc.consumerGroup, rc.srv)
		defer span.End()

		commitStart := time.Now()
		var commitErr error
		defer func() {
			rc.recordCommitDuration(commitCtx, time.Since(commitStart).Seconds(), commitErr)
			setSpanStatus(span, commitErr)
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
				return fmt.Errorf("commit failed: %w", err)
			}
		}

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

		case kafka.PartitionEOF:
			// Only delivered with WithPartitionEOF, and only tracked during
			// ReadToEnd, which needs Read to return as soon as every
			// partition has been read to its end.
			if rc.atEnd != nil {
				rc.atEnd[e.Partition] = true
				if len(rc.atEnd) >= rc.partitions {
					return msgs, commit, nil
				}
			}

		case kafka.Error:
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

	return msgs, commit, nil
}

// ErrPartitionEOFNotEnabled is returned by [Reader.ReadToEnd] on a reader made
// without [WithPartitionEOF].
var ErrPartitionEOFNotEnabled = errors.New("ReadToEnd needs a reader made with WithPartitionEOF")

const (
	readToEndBatch     = 500
	readToEndPollWait  = time.Second
	topicMetadataLimit = 10 * time.Second
)

// ReadToEnd reads the topic from where the consumer group stands to the end of
// every partition, and returns what it read. Use it to load a topic's current
// state, for example a full read of a compacted topic: with no committed
// offsets and [OffsetEarliest], that is every message still on the topic.
//
// A partition counts as read to its end the first time the reader reaches its
// end, so a message written to it after that may or may not be included. An
// empty partition counts once the reader has reached it.
//
// It needs a reader made with [WithPartitionEOF], and expects to be the only
// member of its consumer group: a partition assigned to another member is never
// reached. It returns when ctx is done, with the messages read so far and an
// error. The returned CommitFunc commits everything that was read.
func (rc *Reader) ReadToEnd(ctx context.Context) ([]Message, CommitFunc, error) {
	if !rc.partitionEOF {
		return nil, nil, ErrPartitionEOFNotEnabled
	}

	md, err := rc.consumer.GetMetadata(&rc.topic, false, int(topicMetadataLimit.Milliseconds()))
	if err != nil {
		return nil, nil, fmt.Errorf("topic metadata: %w", err)
	}
	tm, ok := md.Topics[rc.topic]
	if !ok {
		return nil, nil, fmt.Errorf("topic %s not in metadata", rc.topic)
	}
	if tm.Error.Code() != kafka.ErrNoError {
		return nil, nil, fmt.Errorf("topic metadata for %s: %w", rc.topic, tm.Error)
	}
	if len(tm.Partitions) == 0 {
		return nil, nil, fmt.Errorf("topic %s has no partitions", rc.topic)
	}

	rc.partitions = len(tm.Partitions)
	rc.atEnd = map[int32]bool{}
	defer func() { rc.atEnd = nil }()

	var all []Message
	var commits []CommitFunc
	commitAll := CommitFunc(func(ctx context.Context) error {
		for _, commit := range commits {
			if err := commit(ctx); err != nil {
				return err
			}
		}
		return nil
	})

	for len(rc.atEnd) < rc.partitions {
		if err := ctx.Err(); err != nil {
			return all, commitAll, fmt.Errorf(
				"read to end: %d of %d partitions reached: %w", len(rc.atEnd), rc.partitions, err,
			)
		}
		msgs, commit, err := rc.Read(ctx, readToEndBatch, readToEndPollWait)
		all = append(all, msgs...)
		commits = append(commits, commit)
		if err != nil {
			return all, commitAll, err
		}
	}
	return all, commitAll, nil
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
