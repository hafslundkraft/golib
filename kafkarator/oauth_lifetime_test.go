package kafkarator

import (
	"bytes"
	"context"
	"runtime"
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
// internal/auth.TestStartOAuthRefreshLoop_StopReturnsAndIsIdempotent covers the
// other half — that the stop func returns and tolerates repeat calls.

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
	w := newWriter(p, counter, tel, func() {
		// The loop must be stopped while the handle is still alive; stopping it
		// afterwards is the use-after-free this wiring exists to prevent.
		if p.IsClosed() {
			t.Error("stopAuth ran after the producer was closed")
		}
		close(stopped)
	})

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
		func() {
			// See the producer equivalent: order is the point, not just the call.
			if c.IsClosed() {
				t.Error("stopAuth ran after the consumer was closed")
			}
			close(stopped)
		},
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

// The tests above inject a stub stop func, so they prove Close calls whatever it
// was given. The two below go through Connection instead, so the loop they stop
// is the real one: they cover the handover in Connection.Writer/Reader, which
// nothing else exercises.

// stubTokenProvider returns a token librdkafka accepts. Principal is empty, as
// it is on the WithTokenSource path; librdkafka does not require it locally.
type stubTokenProvider struct{}

func (stubTokenProvider) GetAccessToken(context.Context) (kafka.OAuthBearerToken, error) {
	return kafka.OAuthBearerToken{
		TokenValue: "abc123",
		Expiration: time.Now().Add(time.Hour),
		Extensions: map[string]string{},
	}, nil
}

// saslConnection builds a Connection on the SASL/OAUTHBEARER path, so Writer and
// Reader start a real refresh loop without needing Entra ID or a live broker.
func saslConnection() *Connection {
	return &Connection{
		config: Config{
			AuthMode:     AuthSASL,
			SystemName:   "oauth",
			Env:          "test",
			WorkloadName: "lifetime",
		},
		configMap: &kafka.ConfigMap{
			"bootstrap.servers": dummyBroker,
			"security.protocol": "SASL_PLAINTEXT",
			"sasl.mechanisms":   "OAUTHBEARER",
		},
		tel:           newMockTelemetry(),
		tokenProvider: stubTokenProvider{},
	}
}

// refreshLoopCount reports how many goroutines are sitting in the OAuth refresh
// loop. Reading stacks is blunt, but the loop is otherwise unobservable from
// here: its stop func is private to Writer/Reader, and the minimum refresh
// interval of one minute puts a second token fetch out of a fast test's reach.
//
// Matching the .func1 frame rather than the bare function name is deliberate:
// runtime.Stack also emits a "created by ...StartOAuthRefreshLoop" line, so the
// plain name occurs twice per goroutine.
func refreshLoopCount() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return bytes.Count(buf[:n], []byte("auth.StartOAuthRefreshLoop.func1()"))
}

func TestWriterCloseStopsRealOAuthRefreshLoop(t *testing.T) {
	before := refreshLoopCount()

	w, err := saslConnection().Writer()
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	if w.stopAuth == nil {
		t.Fatal("Connection.Writer did not hand the loop's stop func to the Writer")
	}
	if got := refreshLoopCount(); got != before+1 {
		t.Fatalf("refresh loop goroutines after Writer: got %d, want %d", got, before+1)
	}

	// Close's flush budget comes from ctx, and on the SASL path librdkafka waits
	// out the whole budget against an unreachable broker. Nothing was written, so
	// keep it short rather than idling for seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	if got := refreshLoopCount(); got != before {
		t.Errorf("refresh loop outlived Writer.Close: got %d goroutines, want %d", got, before)
	}
}

func TestReaderCloseStopsRealOAuthRefreshLoop(t *testing.T) {
	before := refreshLoopCount()

	r, err := saslConnection().Reader("oauth-lifetime-topic")
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}

	if r.stopAuth == nil {
		t.Fatal("Connection.Reader did not hand the loop's stop func to the Reader")
	}
	if got := refreshLoopCount(); got != before+1 {
		t.Fatalf("refresh loop goroutines after Reader: got %d, want %d", got, before+1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	if got := refreshLoopCount(); got != before {
		t.Errorf("refresh loop outlived Reader.Close: got %d goroutines, want %d", got, before)
	}
}
