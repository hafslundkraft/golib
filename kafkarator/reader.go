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
	partitionEOF bool,
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
		partitionEOF:        partitionEOF,
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

	// partitionEOF is set by [WithPartitionEOF], which [Reader.ReadToEnd]
	// needs.
	partitionEOF bool
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
	msgs, latest, err := rc.poll(ctx, maxMessages, maxWait, nil)
	return msgs, rc.commitFunc(latest), err
}

// poll reads as [Reader.Read] describes, and returns the latest offset read
// per partition. onEOF, when not nil, is called for each end-of-partition
// event, and poll returns as soon as it returns true. A Kafka error other than
// a timeout ends the poll; the error wraps the [kafka.Error].
func (rc *Reader) poll(
	ctx context.Context,
	maxMessages int,
	maxWait time.Duration,
	onEOF func(partition int32) bool,
) ([]Message, map[int32]kafka.Offset, error) {
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
			// Only delivered with WithPartitionEOF.
			if onEOF != nil && onEOF(e.Partition) {
				return msgs, latestOffsets, nil
			}

		case kafka.Error:
			if e.IsTimeout() {
				return msgs, latestOffsets, nil
			}
			rc.recordConsumed(ctx, "", e)
			pollErr = e
			return msgs, latestOffsets, fmt.Errorf("poll error: %w", e)

		default:
			// ignore rebalance events etc.
		}
	}

	return msgs, latestOffsets, nil
}

// commitFunc commits the given latest offset per partition.
func (rc *Reader) commitFunc(latestOffsets map[int32]kafka.Offset) CommitFunc {
	return func(ctx context.Context) error {
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
	}
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
// The end of a partition is its end offset when ReadToEnd starts. A partition
// is done when it is empty, when the reader's position has reached that
// offset, or when the reader gets an end-of-partition event, which also covers
// a partition whose last offsets are not messages. A message written after the
// start may or may not be included. So may a partition added after the start.
//
// It needs a reader made with [WithPartitionEOF], and expects to be the only
// member of its consumer group: a partition assigned to another member is never
// reached. Transient errors librdkafka recovers from by itself, such as a lost
// broker connection, are skipped; any other error ends the read. It returns
// when ctx is done, with the messages read so far and an error. The returned
// CommitFunc commits everything that was read, once per partition.
func (rc *Reader) ReadToEnd(ctx context.Context) ([]Message, CommitFunc, error) {
	if !rc.partitionEOF {
		return nil, nil, ErrPartitionEOFNotEnabled
	}

	ends, err := rc.endOffsets()
	if err != nil {
		return nil, nil, err
	}
	done := map[int32]bool{}
	for partition, end := range ends {
		if end.low >= end.high {
			done[partition] = true
		}
	}

	var all []Message
	latest := map[int32]kafka.Offset{}
	// A partition added after the start is not counted: it could otherwise
	// stand in for one that has not been read to its end.
	onEOF := func(partition int32) bool {
		if _, ok := ends[partition]; ok {
			done[partition] = true
		}
		return len(done) == len(ends)
	}
	var lastTransient error

	for {
		if err := rc.markPositions(ends, done); err != nil {
			return all, rc.commitFunc(latest), err
		}
		if len(done) == len(ends) {
			return all, rc.commitFunc(latest), nil
		}
		if err := ctx.Err(); err != nil {
			err = fmt.Errorf("read to end: %d of %d partitions reached: %w", len(done), len(ends), err)
			if lastTransient != nil {
				err = fmt.Errorf("%w; last Kafka error: %w", err, lastTransient)
			}
			return all, rc.commitFunc(latest), err
		}

		wait := readToEndPollWait
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait {
			wait = time.Until(deadline)
		}
		msgs, offsets, err := rc.poll(ctx, readToEndBatch, wait, onEOF)
		all = append(all, msgs...)
		for partition, off := range offsets {
			latest[partition] = off
		}
		if isTransient(err) {
			lastTransient = err
			continue // librdkafka retries by itself
		}
		if err != nil {
			return all, rc.commitFunc(latest), err
		}
	}
}

// isTransient reports whether err is a Kafka error librdkafka recovers from by
// itself. Permanent errors, such as a failed topic authorization, are not
// marked fatal by librdkafka, so IsFatal cannot tell them apart.
func isTransient(err error) bool {
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return false
	}
	switch kerr.Code() {
	case kafka.ErrTransport, kafka.ErrAllBrokersDown, kafka.ErrResolve, kafka.ErrTimedOut,
		kafka.ErrRequestTimedOut, kafka.ErrNetworkException,
		kafka.ErrLeaderNotAvailable, kafka.ErrNotLeaderForPartition:
		return true
	default:
		return false
	}
}

type partitionEnds struct{ low, high int64 }

// endOffsets is each partition's low and high watermark, now.
func (rc *Reader) endOffsets() (map[int32]partitionEnds, error) {
	limit := int(topicMetadataLimit.Milliseconds())
	md, err := rc.consumer.GetMetadata(&rc.topic, false, limit)
	if err != nil {
		return nil, fmt.Errorf("topic metadata: %w", err)
	}
	tm, ok := md.Topics[rc.topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not in metadata", rc.topic)
	}
	if tm.Error.Code() != kafka.ErrNoError {
		return nil, fmt.Errorf("topic metadata for %s: %w", rc.topic, tm.Error)
	}
	if len(tm.Partitions) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", rc.topic)
	}
	ends := make(map[int32]partitionEnds, len(tm.Partitions))
	for _, p := range tm.Partitions {
		low, high, err := rc.consumer.QueryWatermarkOffsets(rc.topic, p.ID, limit)
		if err != nil {
			return nil, fmt.Errorf("end offset of %s partition %d: %w", rc.topic, p.ID, err)
		}
		ends[p.ID] = partitionEnds{low: low, high: high}
	}
	return ends, nil
}

// markPositions marks done each partition this reader is assigned whose
// position has reached its end offset. That covers a reader already at the end,
// which gets no end-of-partition event.
func (rc *Reader) markPositions(ends map[int32]partitionEnds, done map[int32]bool) error {
	var pending []kafka.TopicPartition
	for partition := range ends {
		if !done[partition] {
			pending = append(pending, kafka.TopicPartition{Topic: &rc.topic, Partition: partition})
		}
	}
	if len(pending) == 0 {
		return nil
	}
	positions, err := rc.consumer.Position(pending)
	if err != nil {
		return fmt.Errorf("consumer position: %w", err)
	}
	for _, p := range positions {
		// Unassigned or not yet fetched partitions have a negative position.
		if p.Offset >= 0 && int64(p.Offset) >= ends[p.Partition].high {
			done[p.Partition] = true
		}
	}
	return nil
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
