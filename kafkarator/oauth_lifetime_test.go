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

// The OAuth refresh loop is started by Connection but owned by the Writer/Reader
// it is handed to, which must stop it in Close. Break that wiring and the loop
// outlives the Kafka handle it feeds. internal/auth covers the stop func itself.

// dummyBroker: librdkafka does not dial on construction, so nothing here needs a
// reachable broker. Port 9 (discard) so a local Kafka is never picked up.
const dummyBroker = "127.0.0.1:9"

func TestWriterCloseStopsOAuthRefresh(t *testing.T) {
	stopped := make(chan struct{})
	w := newTestWriter(t, func(p *kafka.Producer) {
		// Order is the point: stopping after teardown is the use-after-free.
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
	calls := 0
	w := newTestWriter(t, func(*kafka.Producer) { calls++ })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A second Close must not slip past the guard and close the producer twice.
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

// Without SASL there is no loop and stopAuth is nil; Close must not panic.
func TestWriterCloseWithoutOAuthRefresh(t *testing.T) {
	w := newTestWriter(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer with nil stopAuth: %v", err)
	}
}

func TestReaderCloseStopsOAuthRefresh(t *testing.T) {
	stopped := make(chan struct{})
	r := newTestReader(t, "oauth-lifetime-test", func(c *kafka.Consumer) {
		// Order is the point; see the producer equivalent.
		if c.IsClosed() {
			t.Error("stopAuth ran after the consumer was closed")
		}
		close(stopped)
	})

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

func TestReaderCloseStopsOAuthRefreshOnlyOnce(t *testing.T) {
	calls := 0
	r := newTestReader(t, "oauth-lifetime-test-once", func(*kafka.Consumer) { calls++ })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The consumer half of the guard: a second Close must not close it twice.
	if err := r.Close(ctx); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected stop to be called exactly once, got %d", calls)
	}
}

func TestReaderCloseWithoutOAuthRefresh(t *testing.T) {
	r := newTestReader(t, "oauth-lifetime-test-nil", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx); err != nil {
		t.Fatalf("close reader with nil stopAuth: %v", err)
	}
}

// The tests above inject a stub stop func. Those below go through Connection, so
// the loop is real and the handover in Writer/Reader is actually covered.

// Empty Principal matches the WithTokenSource path; librdkafka accepts it locally.
type stubTokenProvider struct{}

func (stubTokenProvider) GetAccessToken(context.Context) (kafka.OAuthBearerToken, error) {
	return kafka.OAuthBearerToken{
		TokenValue: "abc123",
		Expiration: time.Now().Add(time.Hour),
		Extensions: map[string]string{},
	}, nil
}

// saslConnection puts Writer/Reader on the SASL path without Entra ID or a broker.
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

// refreshLoopCount counts goroutines in the refresh loop. Assert the count rises
// before asserting it falls: a renamed frame reads as zero, which would let the
// leak check pass vacuously. Match .func1 — the bare name also appears in the
// "created by" line. Process-wide, so no t.Parallel in this package.
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

	// Close's flush waits out the whole ctx budget against an unreachable broker.
	// Nothing was written, so keep it short.
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

// Test is the third place a loop is started and the only one that stops it itself,
// via defers whose order matters. Watched from a second goroutine because checking
// after Test returns would pass even if no loop was ever started; the 5s
// GetMetadata timeout against the unreachable broker is the observation window.
func TestConnectionTestStopsOAuthRefreshLoop(t *testing.T) {
	before := refreshLoopCount()

	done := make(chan error, 1)
	go func() {
		done <- saslConnection().Test(context.Background())
	}()

	sawLoop := false
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if refreshLoopCount() == before+1 {
			sawLoop = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawLoop {
		t.Fatal("no refresh loop seen while Test ran: none started, or the frame was renamed")
	}

	// The error is expected; the point is that the defers run on the failure path.
	if err := <-done; err == nil {
		t.Fatal("expected Test to fail against an unreachable broker")
	}

	if got := refreshLoopCount(); got != before {
		t.Errorf("refresh loop outlived Test: got %d goroutines, want %d", got, before)
	}
}

// newTestWriter builds a Writer over a producer pointed at the dummy broker.
// stopAuth stands in for the refresh loop's stop func and is handed the producer
// so callers can assert on teardown order; a nil stopAuth is passed through as a
// nil func, matching the no-SASL case.
func newTestWriter(t *testing.T, stopAuth func(*kafka.Producer)) *Writer {
	t.Helper()
	tel := newMockTelemetry()

	counter, err := messagingconv.NewClientSentMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}

	opDur, err := messagingconv.NewClientOperationDuration(tel.Meter())
	if err != nil {
		t.Fatalf("create operation duration: %v", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": dummyBroker})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	var stop func()
	if stopAuth != nil {
		stop = func() { stopAuth(p) }
	}

	dummyServer := parseServerInfo(dummyBroker)

	return newWriter(p, counter, opDur, dummyServer, tel, stop)
}

// newTestReader is the consumer-side equivalent of [newTestWriter].
func newTestReader(t *testing.T, group string, stopAuth func(*kafka.Consumer)) *Reader {
	t.Helper()
	tel := newMockTelemetry()

	consumed, err := messagingconv.NewClientConsumedMessages(tel.Meter())
	if err != nil {
		t.Fatalf("create consumed counter: %v", err)
	}

	opDur, err := messagingconv.NewClientOperationDuration(tel.Meter())
	if err != nil {
		t.Fatalf("create operation duration: %v", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": dummyBroker,
		"group.id":          group,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	var stop func()
	if stopAuth != nil {
		stop = func() { stopAuth(c) }
	}

	dummyServer := parseServerInfo(dummyBroker)

	return newReader(c, consumed, opDur, dummyServer, tel, "oauth-lifetime-topic", group, stop)
}
