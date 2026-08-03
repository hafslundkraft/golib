# Kafkarator

![Version](https://img.shields.io/github/v/tag/hafslundkraft/golib?filter=kafkarator/v*&label=version)

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
serializer := conn.Serializer()
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
| **Processor** | `Reader` with automatic tracing built-in | Need to trace message flow | Automatic after batch | n |
| **ChannelReader** | Simple Go channel streaming | Quick prototypes, simple apps | Automatic per message | 1 |
| **Reader** | Read in batches with full control | High-traffic apps, need control | Manual via `Committer` | n |


#### Processor
In order to use the deserializer, a schema for the topic must be available in the schema registry.
The Processor wraps the Reader and automatically tracks messages as they flow through your system using OpenTelemetry. It reads trace information from message headers (like `traceparent`), creates spans for each message, and only saves your progress when all messages in a batch succeed.

Processor options and their defaults:

| Option | Default | Description |
|--------|---------|-------------|
| `WithProcessorReadTimeout(d)` | `10s` | Max time `ProcessNext` blocks waiting for messages. Must be non-negative. |
| `WithProcessorMaxMessages(n)` | `10` | Max messages processed per `ProcessNext` call. Must be `>= 1`. |
| `WithProcessorAutoOffsetReset(v)` | `OffsetEarliest` | Where to start when the consumer group has no committed offset. Valid values: `OffsetEarliest`, `OffsetLatest`. |

Invalid option values now cause `conn.Processor(...)` to return an error (previously silently coerced or, for `WithProcessorAutoOffsetReset`, caused a nil-func panic).

```go
ctx := context.Background()
deserializer := conn.Deserializer()

// Define handler to process each message
handler := func(ctx context.Context, msg *kafkarator.Message) error {
    var decoded MyStruct
    if err := deserializer.Deserialize(ctx, msg.Topic, msg.Value, &decoded); err != nil {
        return err
    }
    return handleMessage(ctx, decoded)
}

// Create processor with automatic tracing
processor, err := conn.Processor(
    "my-topic",
    handler,
    kafkarator.WithProcessorMaxMessages(10),                        // Process up to 10 messages per call to ProcessNext
    kafkarator.WithProcessorReadTimeout(5*time.Second),             // 5 second read timeout
    kafkarator.WithProcessorAutoOffsetReset(kafkarator.OffsetLatest), // Start from latest if no committed offset
)
defer processor.Close(ctx)

// Process next collection of messages, automatically handling trace context and commits
processed, err := processor.ProcessNext(ctx)
```


#### ChannelReader
In order to use the deserializer, a schema for the topic must be available in the schema registry. Receive messages, one at a time, as quickly as possible. Suitable for low-volume scenarios. Control around when the reader commits the high watermark is sacrificed; each message is committed automatically.


```go
ctx := context.Background()
deserializer := conn.Deserializer()
messageChan, _ := conn.ChannelReader(ctx, "my_topic")

go func() {
    for {
        msg, ok := <-messageChan
        if !ok {
            // channel closed
            return
        }
        var decoded MyStruct
        if err := deserializer.Deserialize(ctx, "my-topic", msg.Value, &decoded); err != nil {
            // handle error
            continue
        }
        handleMessage(decoded)
    }
}()
```

#### Reader
In order to use the deserializer, a schema for the topic must be available in the schema registry.
Read messages in batches, commit offsets only when you want. Good for high-volume scenarios where you need full control over error handling and commits. 


```go
ctx := context.Background()
reader, err := conn.Reader("my-topic")
deserializer := conn.Deserializer()
defer reader.Close(ctx)

messages, committer, _ := reader.Read(ctx, 1000, 1*time.Second)

// Process all messages
handleManyMessages(messages)

// You decide when to save progress
_ = committer(ctx)
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

tel, _ := telemetry.New(ctx)

conn, err := kafkarator.NewConnection(config, telemetryProvider)
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

conn, err := kafkarator.NewConnection(config, telemetry, kafkarator.WithTokenSource(ts))
if err != nil {
    log.Fatal(err)
}
```
#### Required Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `ENV` | Environment determines which Kafka service and authentication mode | `prod` |
| `KAFKA_AUTH_TYPE` | Determines how to authenticate with to Aiven | `sasl` or `tls`|
| `KAFKA_BROKER` | Kafka broker address to use | `broker1:9092` |
| `KAFKA_CA_CERT` | Either path to the Certificate Authority file, the raw PEM, or a base64-encoded PEM | `/path/to/ca-cert.pem` |
| `HAPPI_SYSTEM_NAME` | Happi system name. Used for consumer group id generation | `my-system` |
| `HAPPI_WORKLOAD_NAME` | Happi workload/app/job name. Used for consumer group id generation | `my-worker` |
| `HAPPI_ENV` | Happi environment name. Used for consumer group id generation | `prod` |

##### TLS mode

These environment variables are necessary as well for TLS mode
| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_CERT_FILE` | Path to the client certificate file | `/path/to/client-cert.pem` |
| `KAFKA_KEY_FILE` | Path to the client key file | `/path/to/client-key.pem` |

##### SASL mode

These environment variables are necessary as well for SASL mode. `AZURE_KAFKA_SCOPE` does not need to be set if using a custom token source.

| Variable | Description | Example |
|----------|-------------|---------|
| `AZURE_KAFKA_SCOPE` | Azure scope to use for fetching tokens to authenticate with to Aiven | `api://aaaa-bbbb-cccc-dddd` |


#### Optional Environment Variables

Schema Registry is optional. If `KAFKA_SCHEMA_REGISTRY_URL` is unset, `ConfigFromEnvVars`/`NewConnection` succeed and the resulting `Connection` will not have a Schema Registry client (calling `Deserializer()` / `Serializer()` will panic). If `KAFKA_SCHEMA_REGISTRY_URL` **is** set, both `KAFKA_USERNAME` and `KAFKA_PASSWORD` become required.

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_SCHEMA_REGISTRY_URL` | URL to the desired schema registry | `https://url.com:9090` |
| `KAFKA_USERNAME` | Username to authenticate with the schema registry (required if URL is set) | `username` |
| `KAFKA_PASSWORD` | Password to authenticate with the schema registry (required if URL is set) | `pass` |


### Programmatic Configuration

Alternatively, you can create a `Config` struct directly:

```go
config := kafkarator.Config{
    Broker:  "broker1:9092",
    CertFile: "/path/to/client-cert.pem",
    KeyFile:  "/path/to/client-key.pem",
    CACert:   "/path/to/ca-cert.pem",
}

conn, err := kafkarator.NewConnection(config, telemetry)
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

kafkarator reads and writes this trace context using its own propagator, independent of whatever (if anything) the host application has configured globally via `otel.SetTextMapPropagator` — no setup is required for trace propagation to work.

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

### Traces

The library creates spans for all Kafka operations:

**Producer spans** (SpanKind: PRODUCER):
- **Name**: `send <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=send`, `messaging.operation.name=send`, `messaging.destination.name` (topic), `messaging.destination.partition.id`, `messaging.kafka.offset`, `messaging.kafka.message.key` (if present)

**Poll spans** (SpanKind: CLIENT):
- **Name**: `poll <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=receive`, `messaging.operation.name=poll`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `messaging.batch.message_count` (for multi-message batches), `messaging.destination.partition.id`, `messaging.kafka.offset`

**Process spans** (SpanKind: CONSUMER):
- **Name**: `process <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=process`, `messaging.operation.name=process`, `messaging.destination.name` (topic), `messaging.consumer.group.name`, `messaging.destination.partition.id`, `messaging.kafka.offset`

**Commit spans** (SpanKind: CLIENT):
- **Name**: `commit <topic-name>`
- **Attributes**: `messaging.system=kafka`, `messaging.operation.type=settle`, `messaging.operation.name=commit`, `messaging.destination.name` (topic), `messaging.consumer.group.name`

### How Spans Are Correlated

A process span carries an OpenTelemetry [Link](https://opentelemetry.io/docs/specs/otel/trace/api/#link) back to the producer's send span — it does **not** parent to it, and it starts its own trace rather than extending the producer's.

This is deliberate: Kafka topics are durable and replayable. A message can be reprocessed long after it was produced (consumer group reset, backfill, DLQ retry, or just ordinary lag), and by then the producer's trace may already be closed or evicted from the tracing backend's retention window. Parent-child spans assume a bounded request/response lifetime; grafting a new span onto a trace that may be arbitrarily old — or gone — produces incorrect or unqueryable traces. A link records the relationship without depending on the producer's trace still being "live," and correctly represents fan-out too: several consumer groups can each process the same message, each getting their own trace linked back to the same producer span.

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
