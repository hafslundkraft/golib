# Kafkarator

A Go library for connecting to and interacting with Kafka services, with support for both TLS-secured and SASL (OAuth) connections.

The reason for you to use this package, instead of just using a library such as `github.com/confluentinc/confluent-kafka-go/v2` (
which is used internally here), is that this package integrates with the module `github.com/hafslundkraft/golib/telemetry`,
providing automatic OpenTelemetry trace propagation as well as standardized metrics.

The main abstraction is the *Connection* which is created with *New*, as well as simple serialization and deserialization for Avro schemas.

Since the library uses github.com/confluentinc/confluent-kafka-go/v2 which uses the librdkafka (a C library), CGO_ENABLED must be set to 1 when building.

## Usage at a glance
Ceremony exists in this package as with most packages: it must be configured, errors must be handled, etc. All such details
are documented further down. Here, we want to give you an impression of what the package can offer once everything is
set up. For extra clarity, we've omitted all error handling from these sample, we assume that you know how to do that!

### Writing messages
In order to use the serializer, a schema for the topic must be available in the schema registry. If no SubjectNameProvider is given, then the default convention of topic name and "-value" will be used.

```go
ctx := context.Background()

writer, _ := conn.Writer()
serializer, _ := conn.Serializer() 
defer writer.Close(ctx)

key := []byte("key")
headers := map[string][]byte{
	"my-key": []byte("my-value"),
}
value := map[string]any{
	"id": "hello",
}

topic := "my-topic"
encoded, err := serializer.Serialize(ctx, topic, value)
if err != nil {
	// handle error
}

message := kafkarator.Message{
	Topic: topic,
    Key: key,
    Headers: headers,
    Value: encoded,
}

err = writer.Write(ctx, &message)
if err != nil {
	// handle error
}

```

### Reading messages

There are three ways to read messages from Kafka, each suited for different use cases:

| Method | What It Does | When to Use | Offset Commits | `maxMessages` |
|--------|--------------|-------------|-----------------|---------------|
| **ChannelReader** | Simple Go channel streaming | Quick prototypes, simple apps | Automatic per message | 1 |
| **Reader** | Read in batches with full control | High-traffic apps, need control | Manual via `Committer` | n |
| **Processor** | `Reader` with automatic tracing built-in | Need to trace message flow | Automatic after batch | n |


#### ChannelReader
In order to use the deserializer, a schema for the topic must be available in the schema registry. Receive messages, one at a time, as quickly as possible. Suitable for low-volume scenarios. Control around when the reader commits the high watermark is sacrificed; each message is committed automatically.


```go
ctx := context.Background()
deserializer := conn.Deserializer()
messageChan, _ := conn.ChannelReader(ctx, "my_topic", "my-consumer-group")

go func() {
    for {
        msg, ok := <-messageChan
        if !ok {
            // channel closed
        return
	}
	decoded, _ := deserializer.Deserialize(ctx, "my-topic", msg)
    handleMessage(decoded)
}
}()
```

#### Reader
In order to use the deserializer, a schema for the topic must be available in the schema registry.
Read messages in batches, commit offsets only when you want. Good for high-volume scenarios where you need full control over error handling and commits. 


```go
ctx := context.Background()
reader, err := conn.Reader("my-topic", "my-consumer-group")
deserializer := conn.Deserializer()
defer reader.Close(ctx)

messages, committer, _ := reader.Read(ctx, 1000, 1*time.Second)

// Process all messages
handleManyMessages(messages)

// You decide when to save progress
_ = committer(ctx)
```


#### Processor
In order to use the deserializer, a schema for the topic must be available in the schema registry.
The Processor wraps the Reader and automatically tracks messages as they flow through your system using OpenTelemetry. It reads trace information from message headers (like `traceparent`), creates spans for each message, and only saves your progress when all messages in a batch succeed.

```go
ctx := context.Background()
deserializer := conn.Deserializer()

// Define handler to process each message
handler := func(ctx context.Context, msg *kafkarator.Message) error {
    decoded, err := deserializer.Deserialize(ctx, msg.Topic, msg.Value)
    if err != nil {
        return err
    }
    return handleMessage(ctx, decoded)
}

// Create processor with automatic tracing
processor, err := conn.Processor("my-topic", "my-consumer-group", handler)
defer processor.Close(ctx)

processed, err := processor.ProcessNext(ctx, 10, 1*time.Second)
```

For a complete example with testcontainers and Avro serialization, see [examples/kafkarator_processor_demo](../examples/kafkarator_processor_demo).

## Installation

```bash
go get github.com/hafslundkraft/golib/kafkarator
```

## Configuration

kafkarator is instrumented with OpenTelemetry for logging, metrics and tracing. Telemetry is provided through a small interface:

```go
type TelemetryProvider interface {
	Logger() Logger
	Meter() metric.Meter
	Tracer() trace.Tracer
}

type Logger interface {
	ErrorContext(ctx context.Context, msg string, args ...any)
}

```
kafkarator does not initialize OpenTelemetry itself, this is the responsibility of the application using kafkarator.

If using the golib/telemetry, then you can pass the provider directly as shown in the examples below.

### Using Environment Variables

The library can be configured using environment variables through the `ConfigFromEnvVars()` function:

```go
config, err := kafkarator.ConfigFromEnvVars()
if err != nil {
    log.Fatal(err)
}

tel, _ := telemetry.New(
    ctx, "my-service"
    )

conn, err := kafkarator.New(config, telemetryProvider)
if err != nil {
    log.Fatal(err)
}
```
By default, kafkarator uses Azure DefaultAzureCredential to obtain OAuth access tokens.
When using default Azure provider, you must set the OAuth scope as an env variable: 

| Variable | Description | Example |
|----------|-------------|---------|
| `AZURE_KAFKA_SCOPE` | Azure scope to use for fetching tokens to authenticate with to Aiven | `api://aaaa-bbbb-cccc-dddd` |


You can proivde your own optional TokenSource to use instead. kafkarator allows oauth2.TokenSource as additional token sources.
```go
ts := oauth2.StaticTokenSource(&oauth2.Token{
    AccessToken: "my-token", 
    Expiry: time.Now().Add(1 * time.Hour)
})

conn, err := kafkarator.New(config, telemetry, kafkarator.WithTokenSource(ts))
if err != nil {
    log.Fatal(err)
}
```
#### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `ENV` | Environment determines which Kafka service and authentication mode | `prod` |
| `USE_SCHEMA_REGISTRY` | Boolean on whether schema registry should be used or not | `true` |
| `KAFKA_AUTH_TYPE` | Determines how to authenticate with to Aiven | `sasl` or `tls`|

##### TLS mode

These environment variables are necessary as well for TLS mode
| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_CERT_FILE` | Path to the client certificate file | `/path/to/client-cert.pem` |
| `KAFKA_KEY_FILE` | Path to the client key file | `/path/to/client-key.pem` |
| `KAFKA_CA_CERT` | Either path to the Certificate Authority file or the certificate itself | `/path/to/ca-cert.pem` |

##### SASL mode

These environment variables are necessary as well for SASL mode. AZURE_KAFKA_SCOPE does not need to be set if using custom token source.

| Variable | Description | Example |
|----------|-------------|---------|
| `AZURE_KAFKA_SCOPE` | Azure scope to use for fetching tokens to authenticate with to Aiven | `api://aaaa-bbbb-cccc-dddd` |
| `KAFKA_CA_CERT` | Either path to the Certificate Authority file or the certificate itself | `/path/to/ca-cert.pem` |


#### Optional Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BROKER` | Kafka broker address to use | `broker1:9092` |
| `KAFKA_SCHEMA_REGISTRY_URL` | URL to the desired schema registry you want to use | `https://url.com:9090` |
| `KAFKA_USER` | Username to authenticate with to the desired schema registry | `username` |
| `KAFKA_PASSWORD` | Password to authenticate with to Aiven Schema Registry | `pass` |

If any of the above variables are not set, they will default to:

Test environment:
- KAFKA_BROKER = kafka-test-ture-test.com
- KAFKA_SCHEMA_REGISTRY_URL = kafka-test-ture-test.com:18360
- KAFKA_USER: object ID from Azure as username for the application
- KAFKA_PASSWORD: password associated with the user in Aiven

Prod environment:
- KAFKA_BROKER = kafka-prod-ture-prod.com
- KAFKA_SCHEMA_REGISTRY_URL = kafka-prod-ture-prod.com:11132
- KAFKA_USER: object ID from Azure as username for the application
- KAFKA_PASSWORD: password associated with the user in Aiven


### Programmatic Configuration

Alternatively, you can create a `Config` struct directly:

```go
config := kafkarator.Config{
    Broker:  "broker1:9092",
    CertFile: "/path/to/client-cert.pem",
    KeyFile:  "/path/to/client-key.pem",
    CACert:   "/path/to/ca-cert.pem",
    UseSchemaRegistry: false,
}

conn, err := kafkarator.New(config, telemetry)
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

### OpenTelemetry Trace Propagation

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

### Trace Context Headers

The library uses the W3C Trace Context standard headers:
- `traceparent`: Contains trace ID, span ID, and trace flags
- `tracestate`: Contains vendor-specific trace information (if configured)

These headers are automatically managed by OpenTelemetry and don't require manual intervention.

## Observability

kafkarator follows [OpenTelemetry Semantic Conventions for Messaging](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/) to provide standardized observability.

### Metrics

The library automatically records the following metrics:

| Metric | Type | Description | Attributes |
|--------|------|-------------|------------|
| `messaging.client.sent.messages` | Counter | Number of messages sent to Kafka | `messaging.system=kafka`, `messaging.operation.name=send`, `messaging.destination.name` (topic), `messaging.destination.partition.id`, `error.type` (on failure) |
| `messaging.client.consumed.messages` | Counter | Number of messages consumed from Kafka | `messaging.system=kafka`, `messaging.operation.name=poll`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `messaging.destination.partition.id` |
| `messaging.client.poll.failures` | Counter | Number of poll failures | `messaging.system=kafka`, `messaging.operation.name=poll`, `messaging.operation.type=receive`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `error.type` |
| `messaging.kafka.consumer.lag` | Gauge | Consumer lag per partition | `messaging.system=kafka`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `messaging.destination.partition.id` |

### Traces

The library creates spans for all Kafka operations:

**Producer spans** (SpanKind: PRODUCER):
- **Name**: `send <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=send`, `messaging.operation.name=send`, `messaging.destination.name` (topic), `messaging.destination.partition.id`, `messaging.kafka.offset`, `messaging.kafka.message.key` (if present)

**Consumer spans** (SpanKind: CLIENT):
- **Name**: `poll <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=receive`, `messaging.operation.name=poll`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `messaging.batch.message_count` (for multi-message batches), `messaging.destination.partition.id`, `messaging.kafka.offset`

**Commit spans** (SpanKind: CLIENT):
- **Name**: `commit <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=settle`, `messaging.operation.name=commit`, `messaging.destination.name` (topic), `messaging.consumer.group.name`

### Error Handling

Errors are recorded with low-cardinality error types suitable for metrics:
- **Kafka errors**: `kafka_error_<code>` (e.g., `kafka_error_-191` for partition EOF)
- **Other errors**: `_OTHER`

**Note**: Timeouts are not treated as errors and result in OK span status.

## Testing

Run the tests with:

```bash
go test -v
```

The tests use [testcontainers-go](https://golang.testcontainers.org/) to spin up a real Kafka instance for integration testing.

## License

See LICENSE file for details.
