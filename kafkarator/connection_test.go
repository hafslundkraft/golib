package kafkarator

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hafslundkraft/golib/telemetry"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

func Test_connection_Consumer(t *testing.T) {
	ctx := context.Background()

	// Set up the global text map propagator for W3C Trace Context
	// This is required for trace context propagation to work
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var buf bytes.Buffer

	telClosed := false
	tel, telClose := telemetry.New(
		ctx,
		"kafka-test",
		telemetry.WithLocal(true),
		telemetry.WithLocalWriter(&buf))
	defer func() {
		if telClosed {
			return
		}
		telClose(ctx)
	}()

	// Start Kafka container
	_, brokers, closer := startTestContainer(ctx, t)
	defer closer()

	ctx, span := tel.Tracer().Start(ctx, "test_connection")
	defer span.End()

	// Create a topic using kafka-go
	topicName := "test-topic"
	adminCloser := createTopic(t, topicName, brokers[0])
	defer adminCloser()

	// Create Config for New() - TLS is optional, so we can omit cert files for local testing
	config := Config{
		Brokers: brokers,
	}

	// Initialize connection
	conn, err := New(config, tel)
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	// Test the connection
	err = conn.Test(ctx)
	if err != nil {
		t.Fatalf("failed to test connection: %v", err)
	}

	// Now you can test Consumer
	readChan, err := conn.Reader(ctx, topicName, "test-group")
	if err != nil {
		t.Fatalf("failed to create cnsmr: %v", err)
	}

	// Verify messageChan was created successfully
	if readChan == nil {
		t.Fatal("readChan is nil")
	}

	wCloser, err := conn.Writer(topicName)
	require.NoError(t, err)
	defer func() {
		if err := wCloser.Close(ctx); err != nil {
			t.Errorf("failed to close writer: %v", err)
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
		select {
		case err := <-errChan:
			receivedError = err
			return
		case m := <-readChan:
			receivedMessage = &m
		}
		defer wg.Done()
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

	// wait 50ms, and then close telemetry in order to flush all output to buf
	time.Sleep(50 * time.Millisecond)
	err = telClose(ctx)
	require.NoError(t, err)
	telClosed = true

	telemetryOutput := buf.String()
	require.Contains(t, telemetryOutput, "name=kafka_message_lag value=0 attributes: partition=0")
	require.Contains(t, telemetryOutput, "name=messages_produced_total value=1")
}

func startTestContainer(
	ctx context.Context,
	t *testing.T,
) (kafkaContainer *testkafka.KafkaContainer, brokers []string, closer func()) {
	t.Helper()

	var err error
	kafkaContainer, err = testkafka.Run(
		ctx,
		kafkaImage,
		testkafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Fatalf("failed to start kafka container: %v", err)
	}

	brokers, err = kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %v", err)
	}

	closer = func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Errorf("failed to terminate kafka container: %v", err)
		}
	}

	return
}

func createTopic(t *testing.T, topicName, broker string) func() error {
	t.Helper()

	adminConn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("failed to dial kafka: %v", err)
	}

	err = adminConn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	return adminConn.Close
}
