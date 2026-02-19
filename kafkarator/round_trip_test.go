package kafkarator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	testTimeout          = 10 * time.Second
	testRetryDelay       = 2 * time.Second
	testMaxRetries       = 3
	testPollFrequency    = 100 * time.Millisecond
	testTopicCreateDelay = 2 * time.Second
)

// Common test helpers

// generateID creates a random hex string for unique topic/group names.
func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// deliveryStatus tracks message delivery completion.
type deliveryStatus struct {
	mu     sync.Mutex
	called bool
	err    error
}

func (d *deliveryStatus) markDelivered(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.called = true
	d.err = err
}

func (d *deliveryStatus) isDelivered() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.called
}

func (d *deliveryStatus) getError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// traceContext captures trace information from handler execution.
type traceContext struct {
	mu       sync.Mutex
	traceID  trace.TraceID
	spanID   trace.SpanID
	headers  map[string][]byte
	captured bool
}

func (tc *traceContext) capture(ctx context.Context, headers map[string][]byte) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		tc.traceID = spanCtx.TraceID()
		tc.spanID = spanCtx.SpanID()
	}
	tc.headers = headers
	tc.captured = true
}

func (tc *traceContext) isCaptured() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.captured
}

func (tc *traceContext) get() (trace.TraceID, trace.SpanID, map[string][]byte) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.traceID, tc.spanID, tc.headers
}

// waitFor waits for a condition to become true within a timeout period.
func waitFor(condition func() bool, timeout time.Duration) bool {
	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if condition() {
			return true
		}
		time.Sleep(testPollFrequency)
	}
	return false
}

// assertDelivery waits for delivery and asserts no error occurred.
func assertDelivery(t *testing.T, status *deliveryStatus, timeout time.Duration) {
	t.Helper()

	delivered := waitFor(status.isDelivered, timeout)
	require.True(t, delivered, "message was not delivered within timeout")
	require.NoError(t, status.getError(), "delivery error occurred")
}

// retryRead attempts to read messages with retries for topic availability.
func retryRead(ctx context.Context, reader *Reader, count int, timeout time.Duration) ([]Message, CommitFunc, error) {
	var msgs []Message
	var commit CommitFunc
	var err error

	for attempt := 0; attempt < testMaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(testRetryDelay)
		}

		msgs, commit, err = reader.Read(ctx, count, timeout)
		if err == nil && len(msgs) > 0 {
			return msgs, commit, nil
		}
	}

	return msgs, commit, err
}

// retryProcess attempts to process messages with retries for topic availability.
func retryProcess(ctx context.Context, processor *Processor) (int, error) {
	var processed int
	var err error

	for attempt := 0; attempt < testMaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(testRetryDelay)
		}

		processed, err = processor.ProcessNext(ctx)
		if err == nil && processed > 0 {
			return processed, nil
		}
	}

	return processed, err
}

// findSpanByName finds a span by name in the list.
func findSpanByName(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

// assertTraceContext verifies trace ID and span ID relationships.
func assertTraceContext(t *testing.T, parent, child sdktrace.ReadOnlySpan) {
	t.Helper()
	assert.Equal(t, parent.SpanContext().TraceID(), child.SpanContext().TraceID(),
		"child span should have same trace ID as parent")
	assert.Equal(t, parent.SpanContext().SpanID(), child.Parent().SpanID(),
		"child span should reference parent span ID")
}

// TestWriterReaderRoundtrip verifies basic message preservation in a write-read cycle.
func TestWriterReaderRoundtrip(t *testing.T) {
	ctx := context.Background()
	topic := fmt.Sprintf("kafkarator-it-%s", generateID())

	telemetry := newMockTelemetry()
	conn, err := New(&config, telemetry)
	require.NoError(t, err)

	// Write message
	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	payload := []byte(fmt.Sprintf("hello-%d", time.Now().UnixMilli()))
	produced := &Message{
		Topic:   topic,
		Key:     []byte("it"),
		Value:   payload,
		Headers: map[string][]byte{},
	}

	status := &deliveryStatus{}
	err = writer.Write(ctx, produced)
	require.NoError(t, err)

	// Simulate delivery confirmation
	status.markDelivered(nil)
	assertDelivery(t, status, testTimeout)

	// Read message back with retry for topic creation
	time.Sleep(testTopicCreateDelay)

	reader, err := conn.Reader(topic)
	require.NoError(t, err)
	defer reader.Close(ctx)

	msgs, commit, err := retryRead(ctx, reader, 1, testTimeout)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "expected 1 message")

	assert.Equal(t, produced.Key, msgs[0].Key)
	assert.Equal(t, produced.Value, msgs[0].Value)

	require.NoError(t, commit(ctx))
}

// TestWriterReaderRoundtripWithSerde verifies message preservation with Avro serialization.
func TestWriterReaderRoundtripWithSerde(t *testing.T) {
	ctx := context.Background()
	topic := fmt.Sprintf("kafkarator-it-%s", generateID())

	// Setup mock schema registry
	schemaStr := `{
		"type": "record",
		"name": "TestValue",
		"namespace": "kafkarator.it",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "ts", "type": "long"}
		]
	}`

	mockSchemaRegistry := newMockSRClient()
	subject := fmt.Sprintf("%s-value", topic)

	// Configure mock for serialization
	mockSchemaRegistry.latest[subject] = sr.SchemaMetadata{
		SchemaInfo: sr.SchemaInfo{
			Schema:     schemaStr,
			SchemaType: "AVRO",
		},
		ID:      123,
		Version: 1,
		Subject: subject,
	}

	// Configure mock for deserialization
	if mockSchemaRegistry.byID[subject] == nil {
		mockSchemaRegistry.byID[subject] = make(map[int]sr.SchemaInfo)
	}
	mockSchemaRegistry.byID[subject][123] = sr.SchemaInfo{
		Schema:     schemaStr,
		SchemaType: "AVRO",
	}

	telemetry := newMockTelemetry()
	serializer := newAvroSerializer(mockSchemaRegistry, telemetry)
	deserializer := newAvroDeserializer(mockSchemaRegistry, telemetry)

	conn, err := New(&config, telemetry)
	require.NoError(t, err)

	// Serialize and write
	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	valueObj := map[string]interface{}{
		"id": fmt.Sprintf("test-%d", time.Now().UnixMilli()),
		"ts": time.Now().UnixMilli(),
	}

	encodedValue, err := serializer.Serialize(ctx, topic, valueObj)
	require.NoError(t, err)

	produced := &Message{
		Topic:   topic,
		Key:     []byte("k"),
		Value:   encodedValue,
		Headers: map[string][]byte{},
	}

	status := &deliveryStatus{}
	err = writer.Write(ctx, produced)
	require.NoError(t, err)

	status.markDelivered(nil)
	assertDelivery(t, status, testTimeout)

	// Read and deserialize with retry
	time.Sleep(testTopicCreateDelay)

	reader, err := conn.Reader(topic)
	require.NoError(t, err)
	defer reader.Close(ctx)

	msgs, commit, err := retryRead(ctx, reader, 1, testTimeout)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "expected 1 message")

	decoded, err := deserializer.Deserialize(ctx, topic, msgs[0].Value)
	require.NoError(t, err)

	decodedMap, ok := decoded.(map[string]interface{})
	require.True(t, ok, "decoded value should be a map")
	assert.Equal(t, valueObj["id"], decodedMap["id"])
	assert.Equal(t, valueObj["ts"], decodedMap["ts"])

	require.NoError(t, commit(ctx))
}

// TestWriterProcessorRoundtripWithTracing verifies the Processor API functionality and
// end-to-end trace propagation including header injection and span hierarchy.
func TestWriterProcessorRoundtripWithTracing(t *testing.T) {
	ctx := context.Background()
	topic := fmt.Sprintf("kafkarator-it-%s", generateID())

	// Setup telemetry with span recording
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	defer func() { _ = tracerProvider.Shutdown(ctx) }()

	telemetry := &mockTelemetry{
		tracer: tracerProvider.Tracer("test"),
		meter:  newMockTelemetry().Meter(),
		logger: newMockTelemetry().Logger(),
	}

	conn, err := New(&config, telemetry)
	require.NoError(t, err)

	// Handler that captures message content and trace context
	var (
		receivedMsg *Message
		handlerMu   sync.Mutex
	)
	tc := &traceContext{}

	handler := func(handlerCtx context.Context, msg *Message) error {
		handlerMu.Lock()
		defer handlerMu.Unlock()
		receivedMsg = msg
		tc.capture(handlerCtx, msg.Headers)
		return nil
	}

	processor, err := conn.Processor(topic, handler, WithProcessorReadTimeout(testTimeout))
	require.NoError(t, err)
	defer processor.Close(ctx)

	writer, err := conn.Writer()
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Produce message with parent trace
	parentCtx, parentSpan := tracerProvider.Tracer("test").Start(ctx, "parent-operation")
	producerTraceID := trace.SpanContextFromContext(parentCtx).TraceID()

	payload := []byte(fmt.Sprintf("process-%d", time.Now().UnixMilli()))
	msg := &Message{
		Topic:   topic,
		Key:     []byte("it-proc"),
		Value:   payload,
		Headers: map[string][]byte{},
	}

	err = writer.Write(parentCtx, msg)
	require.NoError(t, err)
	parentSpan.End()

	time.Sleep(testTopicCreateDelay)

	// Process message with retry
	count, err := retryProcess(ctx, processor)
	require.NoError(t, err)
	require.Equal(t, 1, count, "expected 1 message processed")

	// 1. Verify message content (Processor functionality)
	handlerMu.Lock()
	require.NotNil(t, receivedMsg, "processor should have received message")
	assert.Equal(t, msg.Key, receivedMsg.Key)
	assert.Equal(t, msg.Value, receivedMsg.Value)
	handlerMu.Unlock()

	// 2. Verify trace header injection (W3C traceparent)
	require.True(t, tc.isCaptured(), "trace context should be captured")

	_, _, headers := tc.get()
	require.NotNil(t, headers, "headers should not be nil")

	traceparent, hasTraceparent := headers["traceparent"]
	require.True(t, hasTraceparent, "should have 'traceparent' header")
	assert.Contains(t, string(traceparent), producerTraceID.String(),
		"traceparent header should contain producer's trace ID")

	// 3. Verify span hierarchy
	spans := spanRecorder.Ended()
	require.GreaterOrEqual(t, len(spans), 3, "should have at least 3 spans (parent, send, process)")

	parentSpanRecorded, foundParent := findSpanByName(spans, "parent-operation")
	require.True(t, foundParent, "parent span not found")

	sendSpan, foundSend := findSpanByName(spans, fmt.Sprintf("%s %s", MessagingOperationNameSend, topic))
	require.True(t, foundSend, "send span not found")

	processSpan, foundProcess := findSpanByName(spans, fmt.Sprintf("%s %s", MessagingOperationNameProcess, topic))
	require.True(t, foundProcess, "process span not found")

	// Verify parent-child relationships
	assertTraceContext(t, parentSpanRecorded, sendSpan)

	// Verify same trace across send and process
	assert.Equal(t, sendSpan.SpanContext().TraceID(), processSpan.SpanContext().TraceID(),
		"send and process spans should share the same trace ID")
}
