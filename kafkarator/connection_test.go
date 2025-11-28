package kafkarator

import (
	"bytes"
	"context"
	"github.com/hafslundkraft/golib/telemetry"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_connection_Consumer(t *testing.T) {
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

func Test_connection_Reader(t *testing.T) {}
