package kafkarator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/semconv/v1.38.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

// tracePropagator writes W3C trace context and baggage onto Kafka message
// headers at produce time (extraction uses the narrower TraceContext/Baggage
// propagators directly — see extractSpanContext and extractBaggage). It's a
// package-local instance rather than otel.GetTextMapPropagator(): that
// global defaults to a no-op until some caller explicitly registers one via
// otel.SetTextMapPropagator, and kafkarator shouldn't depend on the host
// application having done so.
var tracePropagator = propagation.NewCompositeTextMapPropagator( //nolint:gochecknoglobals // stateless, read-only propagator config
	propagation.TraceContext{},
	propagation.Baggage{},
)

// OpenTelemetry semantic conventions for Kafka messaging.
// See: https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
const (
	// Operation names (custom, not in semconv)
	MessagingOperationNameSend    = "send"
	MessagingOperationNamePoll    = "poll"
	MessagingOperationNameCommit  = "commit"
	MessagingOperationNameProcess = "process"

	// Error type for unknown errors
	DefaultErrorType = "_OTHER"
)

// messagingDurationBuckets is the ExplicitBucketBoundaries advisory the
// messaging semantic conventions specify for the duration histograms
// (messaging.client.operation.duration, messaging.process.duration). The
// generated messagingconv constructors omit it, so it's passed at instrument
// creation; without it the SDK defaults to buckets tuned for a 0–10000 range,
// which are useless for second-scale durations.
var messagingDurationBuckets = []float64{ //nolint:gochecknoglobals // static semconv advisory
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
}

// serverInfo is the parsed broker endpoint recorded as server.address /
// server.port on spans and metrics. The zero value (empty address) means
// "unknown" and the attributes are omitted.
type serverInfo struct {
	address string
	port    int
}

// parseServerInfo extracts server.address/server.port from a configured broker
// string. The broker may be comma-separated; the first endpoint is used (the
// client-facing endpoint is the same regardless of how many nodes back it).
func parseServerInfo(broker string) serverInfo {
	first := strings.TrimSpace(strings.SplitN(broker, ",", 2)[0])
	if first == "" {
		return serverInfo{}
	}
	host, port, err := net.SplitHostPort(first)
	if err != nil {
		// No parseable port — use the whole value as the address.
		return serverInfo{address: first}
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return serverInfo{address: host}
	}
	return serverInfo{address: host, port: p}
}

// spanAttrs returns the server.address/server.port span attributes, or nil when
// the broker endpoint is unknown.
func (s serverInfo) spanAttrs() []attribute.KeyValue {
	if s.address == "" {
		return nil
	}
	attrs := []attribute.KeyValue{semconv.ServerAddress(s.address)}
	if s.port > 0 {
		attrs = append(attrs, semconv.ServerPort(s.port))
	}
	return attrs
}

// getErrorType extracts a low-cardinality error type from an error.
// For Kafka errors, it returns the error code. For other errors, it returns a generic type.
func getErrorType(err error) string {
	if err == nil {
		return ""
	}

	// Kafka errors carry a broker/client code; surface it as a low-cardinality
	// error.type. errors.As unwraps fmt.Errorf(%w) chains.
	var ke kafka.Error
	if errors.As(err, &ke) {
		return fmt.Sprintf("kafka_error_%d", ke.Code())
	}

	// For other errors, return a generic type to maintain low cardinality
	return DefaultErrorType
}

// startProduceSpan creates a span for a Kafka send operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startProduceSpan(ctx context.Context, tracer trace.Tracer, topic string, srv serverInfo) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNameSend, topic)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingOperationName(MessagingOperationNameSend),
		semconv.MessagingOperationTypeSend,
	}
	attrs = append(attrs, srv.spanAttrs()...)
	return tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
}

// startPollSpan creates a span for a Kafka poll operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startPollSpan(ctx context.Context, tracer trace.Tracer, topic, group string, srv serverInfo) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNamePoll, topic)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingConsumerGroupName(group),
		semconv.MessagingOperationName(MessagingOperationNamePoll),
		semconv.MessagingOperationTypeReceive,
	}
	attrs = append(attrs, srv.spanAttrs()...)
	return tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

// startCommitSpan creates a span for a Kafka commit operation with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startCommitSpan(ctx context.Context, tracer trace.Tracer, topic, group string, srv serverInfo) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNameCommit, topic)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(topic),
		semconv.MessagingConsumerGroupName(group),
		semconv.MessagingOperationName(MessagingOperationNameCommit),
		semconv.MessagingOperationTypeSettle,
	}
	attrs = append(attrs, srv.spanAttrs()...)
	return tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

// injectTraceContext extracts the trace context from the current span and injects it into the headers
func injectTraceContext(ctx context.Context, headers map[string][]byte) map[string][]byte {
	// Create a new map to avoid modifying the original
	propagatedHeaders := make(map[string][]byte, len(headers))
	maps.Copy(propagatedHeaders, headers)

	// Use a MapCarrier to inject trace context
	carrier := propagation.MapCarrier{}
	tracePropagator.Inject(ctx, carrier)

	// Add trace context headers to the Kafka headers
	for k, v := range carrier {
		propagatedHeaders[k] = []byte(v)
	}

	return propagatedHeaders
}

// headerCarrier converts Kafka message headers into a propagation.MapCarrier
// for use with a TextMapPropagator's Extract.
func headerCarrier(headers map[string][]byte) propagation.MapCarrier {
	carrier := propagation.MapCarrier{}
	for k, v := range headers {
		carrier[k] = string(v)
	}
	return carrier
}

// extractSpanContext parses the OpenTelemetry SpanContext carried in a Kafka
// message's headers (written by injectTraceContext at produce time), for use
// as a trace.Link on the processing span.
//
// Returns a zero-value SpanContext (check IsValid()) if the message carries
// no trace context.
func extractSpanContext(headers map[string][]byte) trace.SpanContext {
	// context.Background(), not the caller's ctx: if headers carry no valid
	// traceparent, Extract returns its input unchanged, which would otherwise
	// make this return whatever span the caller has active — linking to the
	// consumer's own span instead of correctly reporting "no producer span."
	ctx := propagation.TraceContext{}.Extract(context.Background(), headerCarrier(headers))
	return trace.SpanContextFromContext(ctx)
}

// extractBaggage extracts W3C Baggage from a Kafka message's headers into
// ctx. Unlike extractSpanContext, this uses the Baggage propagator alone
// rather than the full tracePropagator: extracting into ctx (not a
// throwaway context) with the full composite would also attach the
// producer's span as ctx's current span, reintroducing the parent-child
// relationship extractSpanContext's Link is meant to replace.
func extractBaggage(ctx context.Context, headers map[string][]byte) context.Context {
	return propagation.Baggage{}.Extract(ctx, headerCarrier(headers))
}

// startProcessingSpan creates a span for processing a Kafka message with all standard attributes.
// Caller must call span.End() when the operation completes.
//
//nolint:spancheck // Span is returned for caller to manage
func startProcessingSpan(
	ctx context.Context,
	tracer trace.Tracer,
	group string,
	msg *Message,
	srv serverInfo,
) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("%s %s", MessagingOperationNameProcess, msg.Topic)

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationName(msg.Topic),
		semconv.MessagingConsumerGroupName(group),
		semconv.MessagingOperationName(MessagingOperationNameProcess),
		semconv.MessagingOperationTypeProcess,
	}
	attrs = append(attrs, srv.spanAttrs()...)

	// Only set partition and offset if they have valid values
	if msg.Partition >= 0 {
		attrs = append(attrs, semconv.MessagingDestinationPartitionID(fmt.Sprintf("%d", msg.Partition)))
	}

	if msg.Offset >= 0 {
		attrs = append(attrs, semconv.MessagingKafkaOffset(int(msg.Offset)))
	}

	spanOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if sc := extractSpanContext(msg.Headers); sc.IsValid() {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}

	ctx = extractBaggage(ctx, msg.Headers)
	return tracer.Start(ctx, spanName, spanOpts...)
}

// recordSentMessage records a metric for a sent message with standard attributes.
func recordSentMessage(
	ctx context.Context,
	counter messagingconv.ClientSentMessages,
	topic, partition string,
	srv serverInfo,
	err error,
) {
	attrs := make([]attribute.KeyValue, 0, 5)

	if topic != "" {
		attrs = append(attrs, counter.AttrDestinationName(topic))
	}

	if partition != "" {
		attrs = append(attrs, counter.AttrDestinationPartitionID(partition))
	}

	if srv.address != "" {
		attrs = append(attrs, counter.AttrServerAddress(srv.address))
		if srv.port > 0 {
			attrs = append(attrs, counter.AttrServerPort(srv.port))
		}
	}

	if err != nil {
		attrs = append(attrs, counter.AttrErrorType(messagingconv.ErrorTypeAttr(getErrorType(err))))
	}

	counter.Add(ctx, 1, MessagingOperationNameSend, messagingconv.SystemKafka, attrs...)
}

// recordConsumedMessage records a metric for a consumed message with standard attributes.
func recordConsumedMessage(
	ctx context.Context,
	counter messagingconv.ClientConsumedMessages,
	topic, group, partition string,
	srv serverInfo,
	err error,
) {
	attrs := make([]attribute.KeyValue, 0, 5)

	if topic != "" {
		attrs = append(attrs, counter.AttrDestinationName(topic))
	}

	if group != "" {
		attrs = append(attrs, counter.AttrConsumerGroupName(group))
	}

	if partition != "" {
		attrs = append(attrs, counter.AttrDestinationPartitionID(partition))
	}

	if srv.address != "" {
		attrs = append(attrs, counter.AttrServerAddress(srv.address))
		if srv.port > 0 {
			attrs = append(attrs, counter.AttrServerPort(srv.port))
		}
	}

	if err != nil {
		attrs = append(attrs, counter.AttrErrorType(messagingconv.ErrorTypeAttr(getErrorType(err))))
	}

	counter.Add(ctx, 1, MessagingOperationNamePoll, messagingconv.SystemKafka, attrs...)
}

// recordOperationDuration records the duration (seconds) of a client send or
// receive operation with standard attributes.
func recordOperationDuration(
	ctx context.Context,
	hist messagingconv.ClientOperationDuration,
	operationName string,
	operationType messagingconv.OperationTypeAttr,
	topic, group, partition string,
	srv serverInfo,
	seconds float64,
	err error,
) {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs, hist.AttrOperationType(operationType))

	if topic != "" {
		attrs = append(attrs, hist.AttrDestinationName(topic))
	}

	if group != "" {
		attrs = append(attrs, hist.AttrConsumerGroupName(group))
	}

	if partition != "" {
		attrs = append(attrs, hist.AttrDestinationPartitionID(partition))
	}

	if srv.address != "" {
		attrs = append(attrs, hist.AttrServerAddress(srv.address))
		if srv.port > 0 {
			attrs = append(attrs, hist.AttrServerPort(srv.port))
		}
	}

	if err != nil {
		attrs = append(attrs, hist.AttrErrorType(messagingconv.ErrorTypeAttr(getErrorType(err))))
	}

	hist.Record(ctx, seconds, operationName, messagingconv.SystemKafka, attrs...)
}

// recordProcessDuration records the duration (seconds) of processing a delivered
// message with standard attributes.
func recordProcessDuration(
	ctx context.Context,
	hist messagingconv.ProcessDuration,
	topic, group, partition string,
	srv serverInfo,
	seconds float64,
	err error,
) {
	attrs := make([]attribute.KeyValue, 0, 5)

	if topic != "" {
		attrs = append(attrs, hist.AttrDestinationName(topic))
	}

	if group != "" {
		attrs = append(attrs, hist.AttrConsumerGroupName(group))
	}

	if partition != "" {
		attrs = append(attrs, hist.AttrDestinationPartitionID(partition))
	}

	if srv.address != "" {
		attrs = append(attrs, hist.AttrServerAddress(srv.address))
		if srv.port > 0 {
			attrs = append(attrs, hist.AttrServerPort(srv.port))
		}
	}

	if err != nil {
		attrs = append(attrs, hist.AttrErrorType(messagingconv.ErrorTypeAttr(getErrorType(err))))
	}

	hist.Record(ctx, seconds, MessagingOperationNameProcess, messagingconv.SystemKafka, attrs...)
}

// setProducerSuccess sets span attributes and status for a successful send.
func setProducerSuccess(span trace.Span, partition string, offset int64) {
	span.SetAttributes(
		semconv.MessagingDestinationPartitionID(partition),
		semconv.MessagingKafkaOffset(int(offset)),
	)
	span.SetStatus(codes.Ok, "message sent successfully")
}

// setSpanError records err on the span: error status, an exception event, and
// the error.type attribute, mirroring the metric-side error.type handling.
func setSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	span.SetAttributes(attribute.String(string(semconv.ErrorTypeKey), getErrorType(err)))
}

// setPollSuccess sets span attributes and status for successful poll.
func setPollSuccess(span trace.Span, messageCount int, partition string, offset int64) {
	if messageCount > 0 {
		attrs := []attribute.KeyValue{}

		// Only set batch count for actual batches (2+ messages)
		if messageCount > 1 {
			attrs = append(attrs, semconv.MessagingBatchMessageCount(messageCount))
		}

		// partition.id may describe a single message or a same-partition batch.
		if partition != "" {
			attrs = append(attrs, semconv.MessagingDestinationPartitionID(partition))
		}

		// kafka.offset is a single-message attribute: set it only when the span
		// describes exactly one message.
		if messageCount == 1 {
			attrs = append(attrs, semconv.MessagingKafkaOffset(int(offset)))
		}

		if len(attrs) > 0 {
			span.SetAttributes(attrs...)
		}
		span.SetStatus(codes.Ok, "messages received")
	} else {
		span.SetStatus(codes.Ok, "no messages available")
	}
}

// setPollError sets span attributes and status for failed poll.
func setPollError(span trace.Span, err error, isTimeout bool) {
	if isTimeout {
		span.SetStatus(codes.Ok, "poll timeout")
		return
	}
	setSpanError(span, err)
}
