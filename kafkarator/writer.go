package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
)

const (
	defaultFlushTimeout = time.Second * 5
)

// ErrWriterClosed is reported via DeliveryChannel when the writer is closed
// before a per-message delivery report arrives from the broker. Callers can
// distinguish this from a broker-side delivery failure with errors.Is.
var ErrWriterClosed = errors.New("kafkarator: writer closed before delivery report received")

// DeliveryReport describes the outcome of an asynchronous Write. It is sent
// on the channel registered via [WithDeliveryChannel] exactly once per
// accepted Write. Err is nil on success, the broker delivery error on
// failure, or [ErrWriterClosed] if the writer shut down before the broker
// reported back.
type DeliveryReport struct {
	Err error
}

// WriteOptions holds the resolved per-call configuration assembled from
// [WriteOption] values.
type WriteOptions struct {
	// DeliveryChannel, if non-nil, receives one [DeliveryReport] per accepted
	// Write. The caller owns the channel; the writer never closes it.
	DeliveryChannel chan DeliveryReport
}

// WriteOption configures a single [Writer.Write] call.
type WriteOption func(*WriteOptions)

// WithDeliveryChannel registers ch to receive the [DeliveryReport] for this
// Write. The caller owns ch and is responsible for receiving from it. A
// buffered channel of capacity at least 1 is recommended so the writer's
// delivery goroutine never blocks on the send.
func WithDeliveryChannel(ch chan DeliveryReport) WriteOption {
	return func(opts *WriteOptions) {
		opts.DeliveryChannel = ch
	}
}

func defaultWriteOptions() *WriteOptions {
	return &WriteOptions{}
}

func newWriter(
	p *kafka.Producer,
	pmc messagingconv.ClientSentMessages,
	tel TelemetryProvider,
) *Writer {
	w := &Writer{
		producer:                p,
		producedMessagesCounter: pmc,
		tel:                     tel,
		done:                    make(chan struct{}),
	}

	return w
}

// Writer publishes messages to Kafka asynchronously. Write enqueues a message
// and returns immediately; per-message delivery results can be observed via
// [WithDeliveryChannel] or aggregated with [Writer.Flush]. Close shuts the
// writer down and releases the underlying producer.
//
// A Writer is safe for concurrent Write and Flush calls. Close must not be
// called concurrently with Write or Flush.
type Writer struct {
	producedMessagesCounter messagingconv.ClientSentMessages
	producer                *kafka.Producer
	tel                     TelemetryProvider
	closed                  atomic.Bool

	// pending tracks in-flight per-message delivery goroutines so Flush can wait
	// for them to record their results before reporting.
	pending sync.WaitGroup

	// done is closed during shutdown to unblock per-message goroutines that are
	// still waiting for delivery reports when the producer is being closed.
	done chan struct{}

	// firstDeliveryErr, deliveryErrCount and deliveryErrsMu accumulate broker
	// delivery failures observed by per-message goroutines since the previous
	// Flush. Flush drains them on return.
	firstDeliveryErr error
	deliveryErrCount int
	deliveryErrsMu   sync.Mutex
}

// Close shuts down the writer. It first runs a best-effort [Writer.Flush] to
// give in-flight messages a chance to deliver, then tears down the underlying
// producer. Any per-message goroutines still waiting for delivery reports are
// unblocked and observe [ErrWriterClosed] on their delivery channel.
//
// Close is idempotent. It must not be called concurrently with Write or Flush.
func (w *Writer) Close(ctx context.Context) error {
	// Best-effort flush of any in-flight messages before tearing down. We
	// intentionally ignore the error here because Close's contract is to
	// shut things down cleanly regardless of in-flight failures. Do this
	// before flipping closed so Flush is not rejected.
	_ = w.Flush(ctx)

	if !w.closed.CompareAndSwap(false, true) {
		return nil // It's ok to call Close multiple times.
	}

	// Signal per-message goroutines that are still blocked on their delivery
	// channel to exit so they don't leak when librdkafka is torn down without
	// sending the remaining delivery reports.
	close(w.done)

	w.producer.Close()

	return nil
}

// Flush waits for all messages enqueued so far to be delivered to the broker
// and returns once their delivery reports have been processed.
//
// The wait is bounded by ctx's deadline if one is set, otherwise by a
// default 5-second timeout. Canceling ctx also unblocks Flush.
//
// Flush returns nil only when every in-flight message was delivered
// successfully. It returns an error if:
//
//   - One or more messages failed delivery. The error wraps the first
//     observed broker error and reports the total failure count.
//   - The timeout elapsed with messages still pending in the producer queue.
//
// Delivery errors observed since the previous Flush are drained on return —
// they are reported exactly once. Reports from goroutines that hadn't
// completed by the deadline will surface in a later Flush if they finish.
//
// Calling Flush on a closed writer returns an error without waiting.
func (w *Writer) Flush(ctx context.Context) error {
	if w.closed.Load() {
		return fmt.Errorf("writer is closed")
	}

	flushTimeout := getTimeoutOrDefault(ctx, defaultFlushTimeout)
	stillPending := w.producer.Flush(int(flushTimeout.Milliseconds()))

	// producer.Flush returns when librdkafka has drained its internal queue
	// and dispatched delivery reports to each per-call deliveryChan, but the
	// goroutines that read those reports and update firstDeliveryErr may not
	// have run yet. Wait for them, bounded by the same timeout/ctx budget so
	// a stuck goroutine cannot make Flush hang past its deadline.
	waitDone := make(chan struct{})
	go func() {
		w.pending.Wait()
		close(waitDone)
	}()

	waitCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, flushTimeout)
		defer cancel()
	}

	select {
	case <-waitDone:
	case <-waitCtx.Done():
		// Deadline or cancellation hit — some goroutines may still be running.
		// Their results will simply not be reflected in this Flush call.
	}

	w.deliveryErrsMu.Lock()
	firstErr := w.firstDeliveryErr
	errorCount := w.deliveryErrCount
	w.firstDeliveryErr = nil
	w.deliveryErrCount = 0
	w.deliveryErrsMu.Unlock()

	if firstErr != nil {
		return fmt.Errorf("flush completed with %d errors, first error: %w", errorCount, firstErr)
	}

	if stillPending > 0 {
		return fmt.Errorf("flush timed out with %d messages still pending", stillPending)
	}

	return nil
}

func getTimeoutOrDefault(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return defaultTimeout
}

// Write enqueues message for asynchronous delivery to its topic and returns
// as soon as the underlying producer has accepted it. It does not block on
// the broker round-trip.
//
// The returned error covers only failures that happen before the message is
// handed off: a nil message, an empty topic, the writer being closed, or the
// producer rejecting the enqueue (typically because its internal queue is
// full). It does not reflect broker-side delivery outcomes.
//
// Delivery results can be observed in two ways:
//
//   - Pass [WithDeliveryChannel] to receive a [DeliveryReport] for each
//     accepted Write — exactly once. The report's Err field is nil on
//     success, the broker error on failure, or [ErrWriterClosed] if the
//     writer shut down before a delivery report arrived. The caller owns
//     the channel; Write never closes it.
//
//   - Call [Writer.Flush] to wait for all pending messages and receive an
//     aggregated error if any of them failed.
//
// As a side effect, if ctx carries an OpenTelemetry tracing span, the W3C
// trace context is injected into the message headers so downstream consumers
// can continue the trace.
func (w *Writer) Write(ctx context.Context, message *Message, opts ...WriteOption) error {
	writeOpts := defaultWriteOptions()
	for _, opt := range opts {
		opt(writeOpts)
	}

	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}

	if message.Topic == "" {
		return fmt.Errorf("message topic cannot be empty")
	}

	ctx, span := startProduceSpan(ctx, w.tel.Tracer(), message.Topic)

	if w.closed.Load() {
		err := fmt.Errorf("writer is closed")
		setProducerError(span, err)
		span.End()
		return err
	}

	if message.Key != nil {
		span.SetAttributes(semconv.MessagingKafkaMessageKey(string(message.Key)))
	}

	traceHeaders := injectTraceContext(ctx, message.Headers)

	kafkaHeaders := make([]kafka.Header, 0, len(traceHeaders))
	for k, v := range traceHeaders {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: v})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &message.Topic,
			Partition: kafka.PartitionAny,
		},
		Value:   message.Value,
		Key:     message.Key,
		Headers: kafkaHeaders,
	}

	deliveryChan := make(chan kafka.Event, 1)

	// Register the pending delivery before Produce so Flush cannot miss it.
	w.pending.Add(1)
	err := w.producer.Produce(msg, deliveryChan)
	if err != nil {
		w.pending.Done()
		setProducerError(span, err)
		span.End()
		return fmt.Errorf("produce message: %w", err)
	}

	go func() {
		defer w.pending.Done()
		defer close(deliveryChan)
		defer span.End()

		var ev kafka.Event
		select {
		case ev = <-deliveryChan:
		case <-w.done:
			// Writer is shutting down and the producer is being torn down
			// without delivering the remaining reports. Notify the caller so
			// a goroutine blocked on DeliveryChannel doesn't hang waiting for
			// a report that will never come, then exit cleanly.
			if writeOpts.DeliveryChannel != nil {
				select {
				case writeOpts.DeliveryChannel <- DeliveryReport{Err: ErrWriterClosed}:
				default:
				}
			}
			return
		}

		m := ev.(*kafka.Message)
		partitionID := fmt.Sprintf("%d", m.TopicPartition.Partition)
		defer recordSentMessage(ctx, w.producedMessagesCounter, message.Topic, partitionID, m.TopicPartition.Error)

		if writeOpts.DeliveryChannel != nil {
			defer func() {
				select {
				case writeOpts.DeliveryChannel <- DeliveryReport{Err: m.TopicPartition.Error}:
				case <-w.done:
					// shutdown — caller is no longer expected to drain
				}
			}()
		}

		if m.TopicPartition.Error != nil {
			setProducerError(span, m.TopicPartition.Error)

			w.deliveryErrsMu.Lock()
			if w.firstDeliveryErr == nil {
				w.firstDeliveryErr = m.TopicPartition.Error
			}
			w.deliveryErrCount++
			w.deliveryErrsMu.Unlock()

			return
		}

		setProducerSuccess(span, partitionID, int64(m.TopicPartition.Offset))
	}()

	return nil
}
