package kafkarator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
