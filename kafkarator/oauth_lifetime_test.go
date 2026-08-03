package kafkarator

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

// The OAuth refresh loop that keeps a producer/consumer authenticated is
// started by Connection.Writer/Connection.Reader but owned by the resulting
// Writer/Reader: its stop func is handed over at construction and must be
// invoked by Close. If that wiring breaks, the loop outlives the Kafka handle
// it feeds and keeps running until the process exits.
//
// internal/auth.TestStartOAuthRefreshLoop_StopWaitsForLoopToExit covers the
// other half — that stopping actually ends the goroutine.

// dummyBroker is deliberately not the container address from TestMain: these
// tests only construct and close handles, and librdkafka does not dial on
// construction, so nothing here needs a reachable broker. Port 9 (discard) so a
// real local Kafka can never be picked up by accident.
const dummyBroker = "127.0.0.1:9"

func TestWriterCloseStopsOAuthRefresh(t *testing.T) {
	tel := newMockTelemetry()

	counter, err := messagingconv.NewClientSentMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": dummyBroker})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	stopped := make(chan struct{})
	w := newWriter(p, counter, tel, func() { close(stopped) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	select {
	case <-stopped:
	default:
		t.Fatal("Writer.Close did not stop the OAuth refresh loop")
	}
}

func TestWriterCloseStopsOAuthRefreshOnlyOnce(t *testing.T) {
	tel := newMockTelemetry()

	counter, err := messagingconv.NewClientSentMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": dummyBroker})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	calls := 0
	w := newWriter(p, counter, tel, func() { calls++ })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Close is documented as idempotent. Stopping twice would be harmless on
	// its own, but a second call must not slip past the guard and tear the
	// producer down twice.
	if err := w.Close(ctx); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected stop to be called exactly once, got %d", calls)
	}
}

// A Writer built without SASL has no refresh loop and therefore a nil stop
// func. Close must tolerate that rather than panicking.
func TestWriterCloseWithoutOAuthRefresh(t *testing.T) {
	tel := newMockTelemetry()

	counter, err := messagingconv.NewClientSentMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": dummyBroker})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	w := newWriter(p, counter, tel, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer with nil stopAuth: %v", err)
	}
}

func TestReaderCloseStopsOAuthRefresh(t *testing.T) {
	tel := newMockTelemetry()

	consumed, err := messagingconv.NewClientConsumedMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create consumed counter: %v", err)
	}
	pollFailures, err := tel.Meter().Int64Counter(meterPollFailures)
	if err != nil {
		t.Fatalf("create poll failure counter: %v", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": dummyBroker,
		"group.id":          "oauth-lifetime-test",
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	stopped := make(chan struct{})
	r := newReader(
		c, consumed, pollFailures, tel,
		"oauth-lifetime-topic", "oauth-lifetime-test",
		func() { close(stopped) },
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	select {
	case <-stopped:
	default:
		t.Fatal("Reader.Close did not stop the OAuth refresh loop")
	}
}

func TestReaderCloseWithoutOAuthRefresh(t *testing.T) {
	tel := newMockTelemetry()

	consumed, err := messagingconv.NewClientConsumedMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create consumed counter: %v", err)
	}
	pollFailures, err := tel.Meter().Int64Counter(meterPollFailures)
	if err != nil {
		t.Fatalf("create poll failure counter: %v", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": dummyBroker,
		"group.id":          "oauth-lifetime-test-nil",
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	r := newReader(
		c, consumed, pollFailures, tel,
		"oauth-lifetime-topic", "oauth-lifetime-test-nil",
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx); err != nil {
		t.Fatalf("close reader with nil stopAuth: %v", err)
	}
}
