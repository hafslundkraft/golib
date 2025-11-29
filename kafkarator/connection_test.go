package kafkarator

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/stretchr/testify/require"
)

// Test_connection_ChannelConsumer is a test for the basics. We test that the channel
// reader works as expected. Also, we test that the writer writes as expected. Finally,
// we check that OpenTelemetry gauges and counters are set as expected.
func Test_connection_ChannelConsumer(t *testing.T) {
	ctx := context.Background()

	var telemetryBuf bytes.Buffer
	telemetryProvider, telClose := telemetry.New(
		ctx,
		"kafka-test",
		telemetry.WithLocal(true),
		telemetry.WithLocalWriter(&telemetryBuf))

	ctx, span := telemetryProvider.Tracer().Start(ctx, "test_connection")
	defer span.End()

	// Create a topic using kafka-go
	topicName := "test-topic"
	adminCloser := createTopic(t, topicName)
	defer adminCloser()

	// Initialize connection
	conn, err := New(config, telemetryProvider)
	require.NoError(t, err, "failed to create connection")

	// Test the connection
	err = conn.Test(ctx)
	require.NoError(t, err, "failed to test connection")

	// Now you can test Consumer
	readChan, err := conn.ChannelReader(ctx, topicName, "test-group")
	require.NoError(t, err, "failed to create reader")
	require.NotNil(t, t, readChan)

	wCloser, err := conn.Writer(topicName)
	require.NoError(t, err)
	defer func() {
		if err := wCloser.Close(ctx); err != nil {
			require.NoError(t, err, "failed to close writer")
		}
	}()

	msg := []byte("hello world")
	headers := map[string][]byte{
		"Content-Type": []byte("application/json"),
	}

	errChan := make(chan error)
	go func() {
		if err := wCloser.Write(ctx, msg, headers); err != nil {
			errChan <- err
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)

	var receivedMessage *Message
	var receivedError error
	go func() {
		defer wg.Done()

		select {
		case err := <-errChan:
			receivedError = err
			return
		case m := <-readChan:
			receivedMessage = &m
		}
	}()

	wg.Wait()

	require.NoError(t, receivedError)
	require.NotNil(t, receivedMessage)
	require.Equal(t, "hello world", string(receivedMessage.Value))
	require.Equal(t, topicName, receivedMessage.Topic)

	// Check that original headers are present
	require.Equal(t, headers["Content-Type"], receivedMessage.Headers["Content-Type"])

	// Check that trace context headers were injected
	traceparent, hasTraceparent := receivedMessage.Headers["traceparent"]
	require.True(t, hasTraceparent, "traceparent header should be present")
	require.NotEmpty(t, traceparent, "traceparent header should not be empty")

	t.Logf("Trace context headers: traceparent=%s", string(traceparent))

	telClose(ctx)
	// wait 50ms, and then close telemetry in order to flush all output to buf
	time.Sleep(50 * time.Millisecond)

	telemetryOutput := telemetryBuf.String()
	require.Contains(t, telemetryOutput, "name=kafka_message_lag value=0 attributes: partition=0")
	require.Contains(t, telemetryOutput, "name=messages_produced_total value=1")
}

// Test_connection_committer is a test for Reader. Specifically, we test that the
// logic around committing and not commtting to a consumer group works as expected.
func Test_connection_committer(t *testing.T) {
	ctx := context.Background()

	var telemetryBuf bytes.Buffer
	telemetryProvider, telClose := telemetry.New(
		ctx,
		"kafka-test",
		telemetry.WithLocal(true),
		telemetry.WithLocalWriter(&telemetryBuf))
	defer telClose(ctx)

	// Create a topic using kafka-go
	topicName := "reader-consumer"
	adminCloser := createTopic(t, topicName)
	defer adminCloser()

	// Initialize connection
	conn, err := New(config, telemetryProvider)
	require.NoError(t, err, "failed to create connection")

	// Test the connection
	err = conn.Test(ctx)
	require.NoError(t, err, "failed to test connection")

	msg1 := []byte("message 1")
	headers1 := map[string][]byte{
		"nr": []byte("1"),
	}

	msg2 := []byte("message 2")
	headers2 := map[string][]byte{
		"nr": []byte("2"),
	}

	writer, err := conn.Writer(topicName)
	require.NoError(t, err)
	err = writer.Write(ctx, msg1, headers1)
	require.NoError(t, err)
	err = writer.Write(ctx, msg2, headers2)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	group1 := "consumer-group-1"
	group2 := "consumer-group-2"

	longTime := 1 * time.Hour

	// read message for group1 without committing, we should get the same message
	// for this consumer group a second time
	reader, err := conn.Reader(topicName, group1)
	require.NoError(t, err)
	messages, _, err := reader.Read(ctx, 1, longTime)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "1", string(messages[0].Headers["nr"]))
	require.NoError(t, reader.Close(ctx))

	// read message for group2, this time we commit. We should
	// not get the same message again
	reader, err = conn.Reader(topicName, group2)
	require.NoError(t, err)
	messages, committer, err := reader.Read(ctx, 1, longTime)
	require.NoError(t, err)
	require.NoError(t, committer(ctx))
	require.Len(t, messages, 1)
	require.Equal(t, "1", string(messages[0].Headers["nr"]))
	require.NoError(t, reader.Close(ctx))

	// read group1 a second time, we should get the first message again
	// since we didn't commit the first time
	reader, err = conn.Reader(topicName, group1)
	require.NoError(t, err)
	messages, _, err = reader.Read(ctx, 1, longTime)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "1", string(messages[0].Headers["nr"]))
	require.NoError(t, reader.Close(ctx))

	// read group2 a second time, we should get a new message since
	// we committed the first message
	reader, err = conn.Reader(topicName, group2)
	require.NoError(t, err)
	messages, _, err = reader.Read(ctx, 1, longTime)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "2", string(messages[0].Headers["nr"]))
	require.NoError(t, reader.Close(ctx))
}
