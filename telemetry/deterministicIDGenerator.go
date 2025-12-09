package telemetry

import (
	"context"
	"encoding/binary"
	"math/rand"

	"go.opentelemetry.io/otel/trace"
)

var defaultSeed int64 = 42

// deterministicIDGenerator generates deterministic IDs based on a seed.
// Useful for testing where reproducible IDs are needed across test runs.
type deterministicIDGenerator struct {
	rng *rand.Rand
}

func newDeterministicIDGenerator(seed int64) *deterministicIDGenerator {
	return &deterministicIDGenerator{
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (g *deterministicIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	var traceID trace.TraceID
	var spanID trace.SpanID
	binary.BigEndian.PutUint64(traceID[:8], g.rng.Uint64())
	binary.BigEndian.PutUint64(traceID[8:], g.rng.Uint64())
	binary.BigEndian.PutUint64(spanID[:], g.rng.Uint64())
	return traceID, spanID
}

func (g *deterministicIDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	var spanID trace.SpanID
	binary.BigEndian.PutUint64(spanID[:], g.rng.Uint64())
	return spanID
}
