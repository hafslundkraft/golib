package kafkarator

import (
	"context"
	"testing"

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

func findAttr(attrs []attribute.KeyValue, key string) (attribute.KeyValue, bool) {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a, true
		}
	}
	return attribute.KeyValue{}, false
}
