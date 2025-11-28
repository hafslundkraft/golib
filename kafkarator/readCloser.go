package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/metric"
)

func newReadCloser(r *kafka.Reader, rmc metric.Int64Counter, logger *slog.Logger) ReadCloser {
	return &readCloser{
		reader:              r,
		readMessagesCounter: rmc,
		logger:              logger,
		closed:              false,
	}
}

type readCloser struct {
	reader              *kafka.Reader
	readMessagesCounter metric.Int64Counter
	lagGauge            metric.Int64Gauge
	logger              *slog.Logger
	closed              bool
}

func (rc *readCloser) Close(ctx context.Context) error {
	if rc.closed {
		return nil // It's ok to close multiple times.
	}

	if err := rc.reader.Close(); err != nil {
		rc.logger.ErrorContext(ctx, "error closing reader", err)
		return fmt.Errorf("error closing reader: %w", err)
	}
	rc.closed = true
	return nil
}

func (rc *readCloser) Read(ctx context.Context, maxMessages int, maxWait time.Duration) ([]Message, error) {
	messages := make([]Message, 0, maxMessages)
	deadline := time.Now().Add(maxWait)

	for len(messages) < maxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		readCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := rc.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
				// Timeout reached, return what we have
				break
			}
			if ctx.Err() != nil {
				// Parent context canceled
				return messages, fmt.Errorf("parent context canceled: %w", ctx.Err())
			}
			return messages, fmt.Errorf("error fetching message: %w", err)
		}

		messages = append(messages, message(&msg))

		rc.readMessagesCounter.Add(ctx, 1)
	}

	return messages, nil
}

func (rc *readCloser) Commit(ctx context.Context, messages []Message) error {
	if rc.closed {
		return errors.New("the reader is closed")
	}

	partitionMap := map[int]int64{}
	for _, m := range messages {
		if _, ok := partitionMap[m.Partition]; !ok {
			partitionMap[m.Partition] = m.Offset
			continue
		}
		if m.Offset > partitionMap[m.Partition] {
			partitionMap[m.Partition] = m.Offset
		}
	}

	topic := rc.reader.Config().Topic
	for p, o := range partitionMap {
		km := kafka.Message{
			Topic:     topic,
			Partition: p,
			Offset:    o,
		}
		if err := rc.reader.CommitMessages(ctx, km); err != nil {
			rc.logger.ErrorContext(ctx, "error committing message", err)
			return fmt.Errorf("error committing message: %w", err)
		}
	}

	return nil
}
