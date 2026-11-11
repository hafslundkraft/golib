package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

const magicByte = 0

// ErrReaderClosed is returned by reads and commits after Close.
var ErrReaderClosed = errors.New("kafkarator: reader closed")

// CommitFunc commits offsets for messages previously read.
// It is safe to call multiple times; committing is idempotent.
// By default it commits the offsets of every message in the batch. With
// [WithCommitHandled], it commits, on each partition, only up to the offset of
// the first message not handled.
// Commit batches in read order: a commit covers everything before it on the
// same partitions.
type CommitFunc func(ctx context.Context, opts ...CommitOption) error

// CommitOption configures a call to a [CommitFunc].
type CommitOption func(*commitOptions)

type commitOptions struct {
	handled    []Message
	hasHandled bool
}

// WithCommitHandled commits offsets only for the handled messages msgs, instead
// of the whole batch. On each partition, it commits up to the offset of the
// first message not in msgs, and later reads return messages again from there,
// including any handled messages after it. Partitions with no handled messages
// get no offset committed.
// Empty msgs means none were handled. msgs is copied, so changing it afterwards
// has no effect.
func WithCommitHandled(msgs []Message) CommitOption {
	msgs = slices.Clone(msgs)
	return func(o *commitOptions) {
		o.handled = msgs
		o.hasHandled = true
	}
}

// newReader wraps a consumer that has subscribed to the topic.
func newReader(core *consumerCore) *Reader {
	return &Reader{
		consumerCore: core,
		positions:    map[int32]*partitionPosition{},
	}
}

// Reader provides an interface for reading messages from a Kafka topic, as well
// as closing it when the client is done reading. Additionally, the act of fetching
// messages and committing them ("committing" == recording in the consumer group,
// per partition, that the group resumes after the messages read) is split giving the
// client total control and responsibility.
type Reader struct {
	*consumerCore

	// mu guards the fields below; commit funcs may run on other goroutines.
	mu sync.Mutex
	// deferredErr is returned by the next Read, so that the messages read
	// before it are not dropped by the caller.
	deferredErr error
	// gen counts rewinds, so a commit can tell if its batch is stale.
	gen       uint64
	positions map[int32]*partitionPosition
}

type partitionPosition struct {
	next        kafka.Offset
	seekPending bool
	// rewoundAt is the gen of the partition's latest rewind.
	rewoundAt uint64
}

type partitionOffset struct {
	partition int32
	offset    kafka.Offset
}

// Close closes releases the underlying infrastructure, and renders this instance unusable.
//
// Close ignores ctx. It waits for the OAuth refresh loop to exit before
// destroying the consumer; otherwise the loop could call into a freed
// librdkafka handle.
func (r *Reader) Close(ctx context.Context) error {
	return r.close()
}

// Read returns a slice of messages at most maxMessages long. If the duration maxWait
// is exceeded before maxMessages have been fetched from the topic, the func will
// return with as many messages in the list as were fetched before timeout.
//
// commiter can be used to commit the offsets read so far to the consumer group. It
// is up to the client if and when commiter is invoked. Committing often can affect
// performance considerably in a high-volume scenario, so the client could for example
// employ a strategy where commiter is only invoked every N iterations.
//
// Messages left out of [WithCommitHandled] are returned again. If reading fails after
// messages were read, Read returns them and the next Read returns the error.
func (rc *Reader) Read(
	ctx context.Context,
	maxMessages int,
	maxWait time.Duration,
) ([]Message, CommitFunc, error) {
	// Close frees the consumer.
	if rc.closed.Load() {
		return nil, nil, ErrReaderClosed
	}
	ctx, span := startPollSpan(ctx, rc.tel.Tracer(), rc.topic, rc.consumerGroup, rc.srv)
	defer span.End()

	// Record one operation.duration per receive, matching the poll span. Emitted
	// on every read — a caught-up read that waits out maxWait is expected, not an
	// error — so the metric mirrors the span duration in all cases.
	readStart := time.Now()
	var pollErr error
	var msgs []Message
	// Always stamp batch attrs (plus duration/status), even when the read fails
	// mid-batch, with the messages collected so far.
	defer func() {
		setPollSpanAttrs(span, msgs)
		rc.recordReceiveDuration(ctx, time.Since(readStart).Seconds(), pollErr)
		setSpanStatus(span, pollErr)
	}()

	deadline := time.Now().Add(maxWait)

	rc.mu.Lock()
	failure := rc.deferredErr
	rc.deferredErr = nil
	// Keep polling if a seek fails: rebalances are only served by Poll.
	seekErr := rc.seekPendingLocked(ctx)
	pendingSeeks := map[int32]bool{}
	for p, pos := range rc.positions {
		pendingSeeks[p] = pos.seekPending
	}
	gen := rc.gen
	rc.mu.Unlock()

	if failure != nil {
		rc.recordConsumed(ctx, "", failure)
		pollErr = failure
		return msgs, rc.commitFunc(nil, gen), failure
	}

	msgs, failure = rc.poll(ctx, maxMessages, deadline, pendingSeeks)
	if failure == nil && seekErr != nil {
		failure = fmt.Errorf("rewind: %w", seekErr)
	}
	if failure != nil && len(msgs) == 0 {
		rc.recordConsumed(ctx, "", failure)
		pollErr = failure
		return msgs, rc.commitFunc(nil, gen), failure
	}

	rc.mu.Lock()
	rc.deferredErr = failure
	msgs = rc.recordReadLocked(msgs, gen)
	rc.mu.Unlock()

	return msgs, rc.commitFunc(msgs, gen), nil
}

// recordReadLocked advances partition positions past msgs, polled at gen, and
// returns the msgs to deliver. rc.mu must be held.
func (rc *Reader) recordReadLocked(msgs []Message, gen uint64) []Message {
	// A commit rewound these partitions while polling; their messages come
	// again after the seek, and keeping them would move pos.next past it.
	msgs = slices.DeleteFunc(msgs, func(m Message) bool {
		pos := rc.positions[m.Partition]
		return pos != nil && pos.rewoundAt > gen
	})
	for _, m := range msgs {
		p := m.Partition
		next := kafka.Offset(m.Offset) + 1
		if pos, ok := rc.positions[p]; ok {
			pos.next = next
		} else {
			rc.positions[p] = &partitionPosition{next: next}
		}
	}
	return msgs
}

// poll reads up to maxMessages before the deadline. Messages on partitions in
// skip are dropped; they are delivered again once their seek succeeds.
func (rc *Reader) poll(
	ctx context.Context,
	maxMessages int,
	deadline time.Time,
	skip map[int32]bool,
) ([]Message, error) {
	msgs := make([]Message, 0, maxMessages)
	for len(msgs) < maxMessages && time.Now().Before(deadline) {
		ev := rc.consumer.Poll(int(time.Until(deadline).Milliseconds()))
		switch e := ev.(type) {
		case nil: // no event before the timeout
			return msgs, nil
		case kafka.Error:
			if e.IsTimeout() {
				return msgs, nil
			}
			return msgs, fmt.Errorf("poll error: %w", e)
		case *kafka.Message:
			p := e.TopicPartition.Partition
			if !skip[p] {
				msgs = append(msgs, toMessage(e))
				rc.recordConsumed(ctx, fmt.Sprintf("%d", p), nil)
			}
		}
		// Other events, such as rebalances, are ignored.
	}
	return msgs, nil
}

// readBatch records what a Read returned, for committing it later.
type readBatch struct {
	// gen is the reader's gen when the batch was read.
	gen   uint64
	msgs  map[partitionOffset]bool
	first map[int32]kafka.Offset
	last  map[int32]kafka.Offset
	// rewoundAt is the gen of this batch's own latest rewind per partition,
	// so a retried commit is not taken as stale. Guarded by Reader.mu.
	rewoundAt map[int32]uint64
}

// commitFunc returns the commit func for msgs, read at gen.
func (rc *Reader) commitFunc(msgs []Message, gen uint64) CommitFunc {
	// Copied, so callers changing msgs don't change the commit.
	b := &readBatch{
		gen:       gen,
		msgs:      make(map[partitionOffset]bool, len(msgs)),
		first:     map[int32]kafka.Offset{},
		last:      map[int32]kafka.Offset{},
		rewoundAt: map[int32]uint64{},
	}
	for _, m := range msgs {
		p, o := m.Partition, kafka.Offset(m.Offset)
		b.msgs[partitionOffset{p, o}] = true
		if _, ok := b.first[p]; !ok {
			b.first[p] = o
		}
		b.last[p] = o
	}

	return func(ctx context.Context, opts ...CommitOption) error {
		return rc.commit(ctx, b, opts...)
	}
}

// rewindOffsets returns, per partition, the first message of b not in handled.
// Every handled message must be part of b.
func (b *readBatch) rewindOffsets(topic string, handled []Message) (map[int32]kafka.Offset, error) {
	done := make(map[partitionOffset]bool, len(handled))
	for _, m := range handled {
		k := partitionOffset{m.Partition, kafka.Offset(m.Offset)}
		if m.Topic != topic || !b.msgs[k] {
			return nil, fmt.Errorf(
				"handled message %s[%d] at offset %d is not part of this batch",
				m.Topic,
				m.Partition,
				m.Offset,
			)
		}
		done[k] = true
	}
	rewindTo := map[int32]kafka.Offset{}
	for k := range b.msgs {
		if cur, ok := rewindTo[k.partition]; !done[k] && (!ok || k.offset < cur) {
			rewindTo[k.partition] = k.offset
		}
	}
	return rewindTo, nil
}

// commit commits b, rewinding partitions with messages left out of [WithCommitHandled].
func (rc *Reader) commit(ctx context.Context, b *readBatch, opts ...CommitOption) error {
	if rc.closed.Load() {
		return ErrReaderClosed
	}
	var o commitOptions
	for _, opt := range opts {
		opt(&o)
	}

	rewindTo := map[int32]kafka.Offset{}
	if o.hasHandled {
		var err error
		if rewindTo, err = b.rewindOffsets(rc.topic, o.handled); err != nil {
			return err
		}
	}

	commitCtx, span := startCommitSpan(ctx, rc.tel.Tracer(), rc.topic, rc.consumerGroup, rc.srv)
	defer span.End()

	commitStart := time.Now()
	var commitErr error
	defer func() {
		rc.recordCommitDuration(commitCtx, time.Since(commitStart).Seconds(), commitErr)
		setSpanStatus(span, commitErr)
	}()

	rc.mu.Lock()
	var offsets []kafka.TopicPartition
	for p, l := range b.last {
		// Committing would mark messages that are read again as handled.
		if pos := rc.positions[p]; pos != nil && pos.rewoundAt > b.gen && pos.rewoundAt != b.rewoundAt[p] {
			rc.tel.Logger().WarnContext(ctx, "Skipped commit of rewound partition", "topic", rc.topic, "partition", p)
			continue
		}
		if r, ok := rewindTo[p]; !ok {
			offsets = append(offsets, kafka.TopicPartition{Topic: &rc.topic, Partition: p, Offset: l + 1})
		} else if r != b.first[p] {
			offsets = append(offsets, kafka.TopicPartition{Topic: &rc.topic, Partition: p, Offset: r})
		}
	}
	// After the stale check, which must ignore this rewind, and before the
	// commit, so a failed commit keeps it.
	for _, p := range rc.rewindLocked(rewindTo) {
		b.rewoundAt[p] = rc.gen
	}
	rc.mu.Unlock()

	if len(offsets) == 0 {
		return nil
	}
	if _, err := rc.consumer.CommitOffsets(offsets); err != nil {
		commitErr = err
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}

// rewindLocked moves partitions back for the next Read to seek, and returns
// the partitions it moved. rc.mu must be held.
func (rc *Reader) rewindLocked(offsets map[int32]kafka.Offset) []int32 {
	var moved []int32
	for p, o := range offsets {
		pos := rc.positions[p]
		if o >= pos.next {
			continue
		}
		if moved == nil {
			rc.gen++
		}
		moved = append(moved, p)
		pos.next = o
		pos.seekPending = true
		pos.rewoundAt = rc.gen
	}
	return moved
}

// seekPendingLocked seeks rewound partitions. Revoked ones are skipped: commits
// stop at the rewind offset, so the next owner reads them again. rc.mu must be
// held.
func (rc *Reader) seekPendingLocked(ctx context.Context) error {
	var errs []error
	for p, pos := range rc.positions {
		if !pos.seekPending {
			continue
		}
		res, err := rc.consumer.SeekPartitions([]kafka.TopicPartition{
			{Topic: &rc.topic, Partition: p, Offset: pos.next},
		})
		if err == nil && len(res) > 0 {
			err = res[0].Error
		}
		switch {
		case err == nil:
			rc.tel.Logger().
				InfoContext(ctx, "Rewound partition", "topic", rc.topic, "partition", p, "offset", int64(pos.next))
		case rc.isAssigned(p):
			rc.tel.Logger().WarnContext(ctx, "Rewind of partition failed",
				"topic", rc.topic, "partition", p, "offset", int64(pos.next), "error", err)
			errs = append(errs, fmt.Errorf("seek partition %d to offset %d: %w", p, pos.next, err))
			continue
		default:
			rc.tel.Logger().
				InfoContext(ctx, "Skipped rewind of revoked partition", "topic", rc.topic, "partition", p, "error", err)
		}
		pos.seekPending = false
	}
	return errors.Join(errs...)
}

func (rc *Reader) isAssigned(partition int32) bool {
	assignment, err := rc.consumer.Assignment()
	if err != nil {
		return true // Retry the seek rather than risk skipping messages.
	}
	for _, tp := range assignment {
		if tp.Topic != nil && *tp.Topic == rc.topic && tp.Partition == partition {
			return true
		}
	}
	return false
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
