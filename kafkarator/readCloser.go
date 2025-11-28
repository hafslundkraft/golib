package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/metric"
)

func newReadCloser(r *kafka.Reader, rmc metric.Int64Counter, lagGauge metric.Int64Gauge, logger *slog.Logger) ReadCloser {
	return &readCloser{
		reader:              r,
		readMessagesCounter: rmc,
		lagGauge:            lagGauge,
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
		rc.logger.ErrorContext(ctx, fmt.Sprintf("error closing reader %v", err))
		return fmt.Errorf("error closing reader: %w", err)
	}
	rc.closed = true
	return nil
}

func (rc *readCloser) Read(
	ctx context.Context,
	maxMessages int,
	maxWait time.Duration,
) (messages []Message, commiter func(ctx context.Context) error, err error) {
	messages = make([]Message, 0, maxMessages)
	deadline := time.Now().Add(maxWait)

	partitionMap := map[int]int64{}
	commiter = func(ctx context.Context) error {
		topic := rc.reader.Config().Topic
		for p, o := range partitionMap {
			msg := kafka.Message{
				Topic:     topic,
				Partition: p,
				Offset:    o,
			}
			if err := rc.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("error committing messages: %w", err)
			}
		}
		return nil
	}

	lagMap := map[int]int64{}

	for len(messages) < maxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		readCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := rc.reader.FetchMessage(readCtx)
		cancel()

		var hwm int64
		var ok bool
		if hwm, ok = partitionMap[msg.Partition]; !ok {
			hwm = -1
		}
		if msg.Offset > hwm {
			partitionMap[msg.Partition] = msg.Offset
			lag := msg.HighWaterMark - msg.Offset - 1
			lagMap[msg.Partition] = lag
		}

		if err != nil {
			if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
				// Timeout reached, return what we have
				break
			}
			if ctx.Err() != nil {
				// Parent context canceled
				return messages, commiter, fmt.Errorf("parent context canceled: %w", ctx.Err())
			}
			return messages, commiter, fmt.Errorf("error fetching message: %w", err)
		}

		messages = append(messages, message(&msg))

		rc.readMessagesCounter.Add(ctx, 1)
		for p, l := range lagMap {
			rc.lagGauge.Record(
				ctx,
				l,
				metric.WithAttributes(
					attribute.KeyValue{Key: "partition", Value: attribute.StringValue(fmt.Sprint(p))},
				),
			)
		}
	}

	return messages, commiter, nil
}
