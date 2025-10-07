package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// singleIDGenerator is a test ID generator that always returns the same IDs. It can be useful
// for testing purposes where deterministic IDs are needed.
type singleIDGenerator struct{}

func (singleIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	traceID := trace.TraceID{
		0x8b,
		0xef,
		0x2b,
		0x28,
		0x24,
		0xb4,
		0x10,
		0x29,
		0xc1,
		0x48,
		0xc2,
		0x76,
		0x9d,
		0x14,
		0x3f,
		0xcf,
	}
	spanID := trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	return traceID, spanID
}

func (singleIDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	return trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
}
