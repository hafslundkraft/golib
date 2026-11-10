package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// produceValues writes values to a new topic, all on one partition.
func produceValues(t *testing.T, conn *Connection, values ...string) string {
	t.Helper()
	ctx := context.Background()
	topic := fmt.Sprintf("kafkarator-it-%s", generateID())

	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	for _, v := range values {
		require.NoError(t, writer.Write(ctx, &Message{Topic: topic, Key: []byte("k"), Value: []byte(v)}))
	}
	require.NoError(t, writer.Flush(ctx))
	time.Sleep(testTopicCreateDelay)
	return topic
}

// processUntil calls ProcessNext until handled has n values, and returns them.
func processUntil(t *testing.T, processor *Processor, handled *[]string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * testTimeout)
	for len(*handled) < n && time.Now().Before(deadline) {
		_, _ = processor.ProcessNext(context.Background())
	}
	return *handled
}

func TestProcessorHandlerErrorRedeliversFailedMessage(t *testing.T) {
	ctx := context.Background()
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	topic := produceValues(t, conn, "v1", "v2", "v3")

	var handled []string
	failed := false
	handler := func(_ context.Context, msg *Message) error {
		handled = append(handled, string(msg.Value))
		if string(msg.Value) == "v2" && !failed {
			failed = true
			return errors.New("boom")
		}
		return nil
	}
	processor, err := conn.Processor(topic, handler, WithProcessorMaxMessages(3), WithProcessorReadTimeout(2*time.Second))
	require.NoError(t, err)

	require.Equal(t, []string{"v1", "v2", "v2", "v3"}, processUntil(t, processor, &handled, 4))
	require.NoError(t, processor.Close(ctx))

	// Everything was handled, so everything is committed.
	processor2, err := conn.Processor(topic, handler, WithProcessorReadTimeout(5*time.Second))
	require.NoError(t, err)
	defer processor2.Close(ctx)
	n, err := processor2.ProcessNext(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestProcessorHandlerErrorCommitsHandledMessages(t *testing.T) {
	ctx := context.Background()
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	topic := produceValues(t, conn, "v1", "v2")

	var handled []string
	handler := func(_ context.Context, msg *Message) error {
		handled = append(handled, string(msg.Value))
		if string(msg.Value) == "v2" {
			return errors.New("boom")
		}
		return nil
	}
	processor, err := conn.Processor(topic, handler, WithProcessorMaxMessages(2), WithProcessorReadTimeout(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, []string{"v1", "v2"}, processUntil(t, processor, &handled, 2))
	require.NoError(t, processor.Close(ctx))

	// v1 was committed, v2 was not.
	handled = nil
	processor2, err := conn.Processor(topic, handler, WithProcessorMaxMessages(1), WithProcessorReadTimeout(2*time.Second))
	require.NoError(t, err)
	defer processor2.Close(ctx)
	require.Equal(t, []string{"v2"}, processUntil(t, processor2, &handled, 1))
}

func TestReaderUnhandledCommitStopsCommitOfLaterBatch(t *testing.T) {
	ctx := context.Background()
	conn, err := NewConnection(&config, newMockTelemetry())
	require.NoError(t, err)
	topic := produceValues(t, conn, "v1", "v2")

	reader, err := conn.Reader(topic)
	require.NoError(t, err)
	a, commitA, err := retryRead(ctx, reader, 1, testTimeout)
	require.NoError(t, err)
	require.Len(t, a, 1)
	b, commitB, err := retryRead(ctx, reader, 1, testTimeout)
	require.NoError(t, err)
	require.Len(t, b, 1)

	require.Error(t, commitA(ctx, WithCommitHandled(b)), "b is not part of a's batch")
	require.NoError(t, commitA(ctx, WithCommitHandled(nil)))
	// b was read before a was sent back, so committing it must not mark a as handled.
	require.NoError(t, commitB(ctx))

	again, _, err := retryRead(ctx, reader, 2, testTimeout)
	require.NoError(t, err)
	require.Equal(t, "v1", string(again[0].Value))
	require.NoError(t, reader.Close(ctx))

	reader2, err := conn.Reader(topic)
	require.NoError(t, err)
	defer reader2.Close(ctx)
	first, _, err := retryRead(ctx, reader2, 1, testTimeout)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, "v1", string(first[0].Value), "nothing should have been committed")
}

func TestReaderReturnsErrReaderClosedAfterClose(t *testing.T) {
	ctx := context.Background()
	reader := newReader(&consumerCore{})
	commit := reader.commitFunc(nil, 0)
	reader.closed.Store(true)

	_, _, err := reader.Read(ctx, 1, time.Second)
	require.ErrorIs(t, err, ErrReaderClosed)
	require.ErrorIs(t, commit(ctx), ErrReaderClosed)
}
