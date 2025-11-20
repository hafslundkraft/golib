# Kafkarator

A Go library for connecting to and interacting with Kafka services, with support for both TLS-secured and non-TLS connections.

The main reason for you to use this package, instead of just using a library such as `github.com/segmentio/kafka-go` (
which is used internally here), is that this package integrates with the module `github.com/hafslundkraft/golib/telemetry`,
providing automatic OpenTelemetry trace propagation as well as standard metrics.

## Installation

```bash
go get github.com/hafslundkraft/golib/kafkarator
```

## Configuration

### Using Environment Variables

The library can be configured using environment variables through the `ConfigFromEnvVars()` function:

```go
config, err := kafkarator.ConfigFromEnvVars()
if err != nil {
    log.Fatal(err)
}

conn, err := kafkarator.New(config)
if err != nil {
    log.Fatal(err)
}
```

#### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated list of Kafka broker addresses | `broker1:9092,broker2:9092` |
| `KAFKA_CERT_FILE` | Path to the client certificate file | `/path/to/client-cert.pem` |
| `KAFKA_KEY_FILE` | Path to the client key file | `/path/to/client-key.pem` |
| `KAFKA_CA_FILE` | Path to the Certificate Authority file | `/path/to/ca-cert.pem` |

### Programmatic Configuration

Alternatively, you can create a `Config` struct directly:

```go
config := kafkarator.Config{
    Brokers:  []string{"broker1:9092", "broker2:9092"},
    CertFile: "/path/to/client-cert.pem",
    KeyFile:  "/path/to/client-key.pem",
    CAFile:   "/path/to/ca-cert.pem",
}

conn, err := kafkarator.New(config)
if err != nil {
    log.Fatal(err)
}
```

### Non-TLS Configuration (for local development/testing)

For local development or testing without TLS, you can omit the certificate fields:

```go
config := kafkarator.Config{
    Brokers: []string{"localhost:9092"},
}

conn, err := kafkarator.New(config)
if err != nil {
    log.Fatal(err)
}
```

## Usage

### Testing the Connection

```go
ctx := context.Background()
if err := conn.Test(ctx); err != nil {
    log.Fatalf("Failed to connect to Kafka: %v", err)
}
```

### Creating a Producer

```go
producer, err := conn.Producer("my-topic")
if err != nil {
    log.Fatal(err)
}

msg := []byte("hello world")
headers := map[string][]byte{
    "Content-Type": []byte("application/json"),
}

err = producer.Produce(ctx, msg, headers)
if err != nil {
    log.Fatal(err)
}
```

### Creating a Consumer

```go
consumer, err := conn.Consumer("my-topic", "my-consumer-group")
if err != nil {
    log.Fatal(err)
}

messageChan, err := consumer.Consume(ctx)
if err != nil {
    log.Fatal(err)
}

for msg := range messageChan {
    fmt.Printf("Received: %s\n", string(msg.Value))
    fmt.Printf("Topic: %s, Partition: %d, Offset: %d\n",
        msg.Topic, msg.Partition, msg.Offset)
}
```

### Getting Topic Partition Count

```go
// Get the number of partitions for a topic
partitionCount, err := conn.TopicPartitions(ctx, "my-topic")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Topic has %d partitions\n", partitionCount)
```

## OpenTelemetry Trace Propagation

This library automatically propagates OpenTelemetry trace context through Kafka messages when a telemetry provider is configured. This enables distributed tracing across your Kafka-based microservices.

### Prerequisites

Before trace propagation can work, you must configure the global OpenTelemetry text map propagator. This is typically done once at application startup:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

func init() {
    // Configure the global propagator for W3C Trace Context
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))
}
```

**Note**: The `github.com/hafslundkraft/golib/telemetry` library may already configure this for you. Check your telemetry initialization code to avoid duplicate configuration.

### Producer Side

When you produce a message with an active trace context, the library automatically injects the trace context into the Kafka message headers:

```go
import (
    "context"
    "github.com/hafslundkraft/golib/kafkarator"
    "github.com/hafslundkraft/golib/telemetry"
)

func main() {
    ctx := context.Background()

    // Initialize telemetry
    tel, shutdown := telemetry.New(ctx, "my-service")
    defer shutdown(ctx)

    // Create Kafka connection with telemetry
    config := kafkarator.Config{
        Brokers: []string{"localhost:9092"},
    }
    conn, err := kafkarator.New(config, tel)
    if err != nil {
        log.Fatal(err)
    }

    // Create producer
    producer, err := conn.Producer("my-topic")
    if err != nil {
        log.Fatal(err)
    }

    // Start a span (or use existing span from incoming request)
    ctx, span := tel.Tracer().Start(ctx, "produce-message")
    defer span.End()

    // Produce message - trace context is automatically injected
    msg := []byte(`{"event": "user.created"}`)
    err = producer.Produce(ctx, msg, nil)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Consumer Side

On the consumer side, extract the trace context from received messages to continue the distributed trace:

```go
func main() {
    ctx := context.Background()

    // Initialize telemetry
    tel, shutdown := telemetry.New(ctx, "consumer-service")
    defer shutdown(ctx)

    // Create Kafka connection with telemetry
    config := kafkarator.Config{
        Brokers: []string{"localhost:9092"},
    }
    conn, err := kafkarator.New(config, tel)
    if err != nil {
        log.Fatal(err)
    }

    // Create consumer
    consumer, err := conn.Consumer("my-topic", "my-consumer-group")
    if err != nil {
        log.Fatal(err)
    }

    messageChan, err := consumer.Consume(ctx)
    if err != nil {
        log.Fatal(err)
    }

    for msg := range messageChan {
        // Extract trace context from message headers
        msgCtx := msg.ExtractTraceContext(ctx)

        // Start a new span as a child of the extracted trace
        msgCtx, span := tel.Tracer().Start(msgCtx, "process-message")

        // Process the message - all operations will be part of the same trace
        processMessage(msgCtx, msg.Value)

        span.End()
    }
}

func processMessage(ctx context.Context, data []byte) {
    // This function and any downstream calls using ctx will be part of the trace
    // ...
}
```

### How It Works

1. **Producer**: When `Produce()` is called with a context that has an active span, the library uses OpenTelemetry's `TextMapPropagator` to inject trace context (trace ID, span ID, trace flags) into the Kafka message headers.

2. **Consumer**: When processing messages, call `msg.ExtractTraceContext(ctx)` to extract the trace context from the message headers. This creates a new context that continues the distributed trace.

3. **No Telemetry**: If no telemetry provider is configured (i.e., `tel` is `nil`), the library works normally without trace propagation.

### Trace Context Headers

The library uses the W3C Trace Context standard headers:
- `traceparent`: Contains trace ID, span ID, and trace flags
- `tracestate`: Contains vendor-specific trace information (if configured)

These headers are automatically managed by OpenTelemetry and don't require manual intervention.

## Trace Metrics
The following metric counters and gauges are automatically maintained:
* **messages_produced_total** (counter): The total number of messages that have been written to the Kafka topic.
* **kafka_lag_partition_N** (gauge): N here is the total number of partitions on the topic - 1. For instance,
    if the topic has 16 partitions, the gauges named `kafka_lag_partition_0`, `kafka_lag_partition_1` ... `kafka_lag_partition_15`
    will be maintained. Each gauge measures the number of messages remaining on the partition that service hasn't read yet.

## Testing

Run the tests with:

```bash
go test -v
```

The tests use [testcontainers-go](https://golang.testcontainers.org/) to spin up a real Kafka instance for integration testing.

## License

See LICENSE file for details.
