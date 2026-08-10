package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestStartProcessingSpanPropagatesBaggage verifies that baggage set on the
// producer's context survives into the handler's context via the message
// headers, even though the processing span links to (rather than parents
// from) the producer's span.
func TestStartProcessingSpanPropagatesBaggage(t *testing.T) {
	ctx := context.Background()

	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	defer func() { _ = tracerProvider.Shutdown(ctx) }()
	tracer := tracerProvider.Tracer("test")

	member, err := baggage.NewMember("tenant.id", "acme-corp")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	producerCtx := baggage.ContextWithBaggage(ctx, bag)
	producerCtx, producerSpan := tracer.Start(producerCtx, "send test-topic")

	headers := injectTraceContext(producerCtx, map[string][]byte{})
	producerSpan.End()

	msg := &Message{Topic: "test-topic", Headers: headers}

	msgCtx, span := startProcessingSpan(ctx, tracer, "test-group", msg, serverInfo{})
	defer span.End()

	assert.Equal(t, "acme-corp", baggage.FromContext(msgCtx).Member("tenant.id").Value(),
		"handler context should carry the producer's baggage")
}

// TestSetPollSpanAttrs verifies the poll span is treated as a batch span:
// batch.message_count is always set (including 0 and 1), partition.id only when
// the whole batch came from one partition, and kafka.offset never (it is a
// single-message attribute that belongs on the process span).
func TestSetPollSpanAttrs(t *testing.T) {
	tests := []struct {
		name          string
		msgs          []Message
		wantCount     int64
		wantPartition string // "" means expect the attribute to be absent
	}{
		{name: "empty poll", msgs: nil, wantCount: 0, wantPartition: ""},
		{name: "single message", msgs: []Message{{Partition: 3}}, wantCount: 1, wantPartition: "3"},
		{
			name:          "batch single partition",
			msgs:          []Message{{Partition: 2}, {Partition: 2}, {Partition: 2}},
			wantCount:     3,
			wantPartition: "2",
		},
		{
			name:          "batch multiple partitions",
			msgs:          []Message{{Partition: 1}, {Partition: 2}},
			wantCount:     2,
			wantPartition: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			spanRecorder := tracetest.NewSpanRecorder()
			tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
			defer func() { _ = tracerProvider.Shutdown(ctx) }()

			_, span := tracerProvider.Tracer("test").Start(ctx, "poll test-topic")
			setPollSpanAttrs(span, tt.msgs)
			span.End()

			ended := spanRecorder.Ended()
			require.Len(t, ended, 1)
			attrs := ended[0].Attributes()

			count, ok := findAttr(attrs, "messaging.batch.message_count")
			require.True(t, ok, "batch.message_count should always be set on a poll span")
			assert.Equal(t, tt.wantCount, count.Value.AsInt64())

			_, hasOffset := findAttr(attrs, "messaging.kafka.offset")
			assert.False(t, hasOffset, "kafka.offset must not be set on a poll span")

			partition, hasPartition := findAttr(attrs, "messaging.destination.partition.id")
			if tt.wantPartition == "" {
				assert.False(t, hasPartition, "partition.id should be absent for a mixed/empty batch")
			} else {
				require.True(t, hasPartition, "partition.id should be set for a single-partition batch")
				assert.Equal(t, tt.wantPartition, partition.Value.AsString())
			}
		})
	}
}

// TestParseServerInfo verifies the broker string is reduced to a
// server.address/server.port pair, and that unparseable forms degrade to an
// address-only (or empty) result rather than producing a bogus port.
func TestParseServerInfo(t *testing.T) {
	tests := []struct {
		name        string
		broker      string
		wantAddress string
		wantPort    int
	}{
		{name: "host and port", broker: "kafka.example.com:9092", wantAddress: "kafka.example.com", wantPort: 9092},
		{
			name:        "comma-separated uses first endpoint",
			broker:      "first.example.com:9092,second.example.com:9093",
			wantAddress: "first.example.com",
			wantPort:    9092,
		},
		{
			name:        "surrounding whitespace trimmed",
			broker:      "  kafka.example.com:9092 , other:9093",
			wantAddress: "kafka.example.com",
			wantPort:    9092,
		},
		{name: "no port", broker: "kafka.example.com", wantAddress: "kafka.example.com", wantPort: 0},
		{name: "non-numeric port", broker: "kafka.example.com:kafka", wantAddress: "kafka.example.com", wantPort: 0},
		{name: "ipv6 with port", broker: "[::1]:9092", wantAddress: "::1", wantPort: 9092},
		{name: "empty", broker: "", wantAddress: "", wantPort: 0},
		{name: "whitespace only", broker: "   ", wantAddress: "", wantPort: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := parseServerInfo(tt.broker)
			assert.Equal(t, tt.wantAddress, srv.address)
			assert.Equal(t, tt.wantPort, srv.port)
		})
	}
}

// TestServerInfoSpanAttrs verifies the attributes are omitted entirely when the
// endpoint is unknown, and that port is dropped when it could not be parsed.
func TestServerInfoSpanAttrs(t *testing.T) {
	tests := []struct {
		name string
		srv  serverInfo
		want []attribute.KeyValue
	}{
		{name: "unknown endpoint", srv: serverInfo{}, want: nil},
		{
			name: "address only",
			srv:  serverInfo{address: "kafka.example.com"},
			want: []attribute.KeyValue{attribute.String("server.address", "kafka.example.com")},
		},
		{
			name: "address and port",
			srv:  serverInfo{address: "kafka.example.com", port: 9092},
			want: []attribute.KeyValue{
				attribute.String("server.address", "kafka.example.com"),
				attribute.Int("server.port", 9092),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.srv.spanAttrs())
		})
	}
}

// TestGetErrorType verifies Kafka error codes survive fmt.Errorf(%w) wrapping
// and that everything else collapses to the low-cardinality default.
func TestGetErrorType(t *testing.T) {
	kafkaErr := kafka.NewError(kafka.ErrTimedOut, "timed out", false)

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "kafka error", err: kafkaErr, want: fmt.Sprintf("kafka_error_%d", kafka.ErrTimedOut)},
		{
			name: "wrapped kafka error",
			err:  fmt.Errorf("produce failed: %w", fmt.Errorf("inner: %w", kafkaErr)),
			want: fmt.Sprintf("kafka_error_%d", kafka.ErrTimedOut),
		},
		{name: "plain error", err: errors.New("boom"), want: DefaultErrorType},
		{name: "wrapped plain error", err: fmt.Errorf("context: %w", errors.New("boom")), want: DefaultErrorType},
		{name: "context canceled", err: context.Canceled, want: DefaultErrorType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getErrorType(tt.err))
		})
	}
}

func findAttr(attrs []attribute.KeyValue, key string) (attribute.KeyValue, bool) {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a, true
		}
	}
	return attribute.KeyValue{}, false
}
