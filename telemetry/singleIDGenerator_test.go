package telemetry

import (
	"context"
	"reflect"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func Test_singleIDGenerator_NewSpanID(t *testing.T) {
	ctx := context.Background()
	traceID := trace.TraceID{}

	generator := singleIDGenerator{}

	spanID := generator.NewSpanID(ctx, traceID)
	expectedSpanID := trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	if !reflect.DeepEqual(spanID, expectedSpanID) {
		t.Errorf("NewSpanID() = %v, want %v", spanID, expectedSpanID)
	}
}
