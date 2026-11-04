package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
)

const (
	readToEndPollWait    = 100 * time.Millisecond
	readToEndRequestWait = time.Second
)

// readToEnd reads cc's topic for [Connection.ReadToEnd], which documents what
// it yields. cc must have no partitions assigned, and the caller closes it.
// readToEnd returns nil when every partition is read to its end or yield
// returns false, and otherwise the error that ended it.
func readToEnd(ctx context.Context, cc *consumerCore, yield func(Message, error) bool) (err error) {
	ctx, span := startPollSpan(ctx, cc.tel.Tracer(), cc.topic, "", cc.srv)
	defer span.End()

	start := time.Now()
	count := 0
	defer func() {
		span.SetAttributes(semconv.MessagingBatchMessageCount(count))
		cc.recordReceiveDuration(ctx, time.Since(start).Seconds(), err)
		setSpanStatus(span, err)
	}()

	ends, err := highWatermarks(ctx, cc)
	if err != nil {
		return err
	}
	if len(ends) == 0 {
		return nil
	}

	assignment := make([]kafka.TopicPartition, 0, len(ends))
	for partition := range ends {
		assignment = append(assignment, kafka.TopicPartition{
			Topic:     &cc.topic,
			Partition: partition,
			Offset:    kafka.OffsetBeginning,
		})
	}
	if err := cc.consumer.Assign(assignment); err != nil {
		return fmt.Errorf("assign: %w", err)
	}

	total := len(ends)
	var lastTransient error
	for len(ends) > 0 {
		if err := ctx.Err(); err != nil {
			err = fmt.Errorf("read to end: %d of %d partitions not read to their end: %w", len(ends), total, err)
			if lastTransient != nil {
				err = fmt.Errorf("%w; last Kafka error: %w", err, lastTransient)
			}
			return err
		}

		switch e := cc.consumer.Poll(int(readToEndPollWait.Milliseconds())).(type) {
		case *kafka.Message:
			partition := e.TopicPartition.Partition
			offset := int64(e.TopicPartition.Offset)
			end, ok := ends[partition]
			if !ok || offset >= end {
				continue // written after iteration started
			}
			if offset >= end-1 {
				delete(ends, partition)
			}
			count++
			cc.recordConsumed(ctx, fmt.Sprintf("%d", partition), nil)
			if !yield(toMessage(e), nil) {
				return nil
			}

		case kafka.PartitionEOF:
			// Matters only when a partition's last offsets are not messages, such
			// as transaction markers, or when an open transaction holds the read
			// back short of the high watermark. Otherwise, the partition was done
			// at its last message.
			delete(ends, e.Partition)

		case kafka.Error:
			if e.IsTimeout() {
				continue
			}
			cc.recordConsumed(ctx, "", e)
			if isTransient(e) {
				lastTransient = e
				span.RecordError(e)
				continue // librdkafka retries by itself
			}
			return fmt.Errorf("poll error: %w", e)

		default:
			// nil on timeout; ignore other events
		}
	}

	return nil
}

// highWatermarks returns the current high watermark of each partition of cc's
// topic that has messages. Errors Kafka recovers from are retried until ctx
// ends.
func highWatermarks(ctx context.Context, cc *consumerCore) (map[int32]int64, error) {
	var tm kafka.TopicMetadata
	err := retryTransient(ctx, func(timeoutMs int) error {
		md, err := cc.consumer.GetMetadata(&cc.topic, false, timeoutMs)
		if err != nil {
			return err //nolint:wrapcheck // wrapped by the caller of retryTransient
		}
		var ok bool
		if tm, ok = md.Topics[cc.topic]; !ok {
			return errors.New("topic not in the response")
		}
		if tm.Error.Code() != kafka.ErrNoError {
			return tm.Error
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("topic metadata for %s: %w", cc.topic, err)
	}

	ends := make(map[int32]int64, len(tm.Partitions))
	for _, p := range tm.Partitions {
		var low, high int64
		err := retryTransient(ctx, func(timeoutMs int) (err error) {
			low, high, err = cc.consumer.QueryWatermarkOffsets(cc.topic, p.ID, timeoutMs)
			return err //nolint:wrapcheck // wrapped by the caller of retryTransient
		})
		if err != nil {
			return nil, fmt.Errorf("watermarks of %s partition %d: %w", cc.topic, p.ID, err)
		}
		if low < high {
			ends[p.ID] = high
		}
	}
	return ends, nil
}

// retryTransient calls f until it returns nil or an error that is not
// transient, or ctx ends. f passes timeoutMs to its Kafka call, which takes a
// timeout rather than ctx.
func retryTransient(ctx context.Context, f func(timeoutMs int) error) error {
	var lastTransient error
	for {
		if err := ctx.Err(); err != nil {
			if lastTransient != nil {
				err = fmt.Errorf("%w; last Kafka error: %w", err, lastTransient)
			}
			return err
		}

		try := readToEndRequestWait
		if deadline, ok := ctx.Deadline(); ok {
			try = min(try, time.Until(deadline))
		}
		// Tries start at least one wait apart, so an error that comes back at
		// once does not spin.
		nextTry := time.After(try)
		err := f(max(int(try.Milliseconds()), 1))
		if err == nil || !isTransient(err) {
			return err
		}
		lastTransient = err

		select {
		case <-ctx.Done():
		case <-nextTry:
		}
	}
}

// isTransient reports whether err is a Kafka error librdkafka recovers from by
// itself. Permanent errors, such as a failed topic authorization, are not
// marked fatal by librdkafka, so IsFatal cannot tell them apart. IsRetriable
// cannot either: it is never set on the errors that Poll, GetMetadata and
// QueryWatermarkOffsets return.
func isTransient(err error) bool {
	var kerr kafka.Error
	if !errors.As(err, &kerr) {
		return false
	}
	switch kerr.Code() {
	// Connection and request failures.
	case kafka.ErrTransport, kafka.ErrAllBrokersDown, kafka.ErrResolve, kafka.ErrTimedOut,
		kafka.ErrRequestTimedOut, kafka.ErrNetworkException, kafka.ErrBrokerNotAvailable,
		// A partition's leader moving or being elected.
		kafka.ErrLeaderNotAvailable, kafka.ErrNotLeaderForPartition, kafka.ErrReplicaNotAvailable,
		kafka.ErrUnknownLeaderEpoch, kafka.ErrFencedLeaderEpoch, kafka.ErrOffsetNotAvailable,
		kafka.ErrKafkaStorageError:
		return true
	default:
		return false
	}
}
