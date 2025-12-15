# Kafkarator

A Go library for connecting to and interacting with Kafka services, with support for both TLS-secured and non-TLS connections.

The reason for you to use this package, instead of just using a library such as `github.com/segmentio/kafka-go` (
which is used internally here), is that this package integrates with the module `github.com/hafslundkraft/golib/telemetry`,
providing automatic OpenTelemetry trace propagation as well as standardized metrics.

The main abstraction is the *Connection* which is created with *New*. 

Since the library uses github.com/confluentinc/confluent-kafka-go/v2 which uses the librdkafka which is a C library, CGO_ENABLED must be set to 1.

## Usage at a glance
Ceremony exists in this package as with most packages: it must be configured, errors must be handled, etc. All such details
are documented further down. Here, we want to give you an impression of what the package can offer once everything is
set up. For extra clarity, we've omitted all error handling from these sample, we assume that you know how to do that!

### Writing messages
In order to use the serializer, a schema for the topic must be available in the schema registry.

```go
ctx := context.Background()
writer, _ := conn.Writer("my_topic")
serializer, _ := conn.Serializer("my_topic")
defer writer.Close(ctx)

key := []byte("key")
message := []byte("whatever you want marshaled as a byte slice. marshaling is your job!")
headers := map[string][]byte{
    "my-key": []byte("my-value"),
}


encoded, _ := serializer.Serialize(ctx, message)
_ = writer.Write(ctx, key, encoded, headers)
```

### Reading messages with channel
Receive messages, one at a time, as quickly as possible. Suitable for low-volume scenarios. Control around when
the reader commits the high watermark is sacrificed; each message is committed automatically.
```go
ctx := context.Background()
deserializer := conn.Deserializer("my-topic")
messageChan, _ := conn.ChannelReader(ctx, "my_topic", "my-consumer-group")

go func() {
    for {
        msg, ok := <-messageChan
        if !ok {
            // channel closed
        return
	}
	decoded, _ := deserializer.Deserialize(ctx, msg)
    handleMessage(decoded)
}
}()
```

### Reading messages with reader
In order to use the deserializer, a schema for the topic must be available in the schema registry.
Read messages in batches, commit offsets only when you want. This is suitable for high-volume scenarios.
```go
ctx := context.Background()
reader, err := conn.Reader("my-topic", "my-consumer-group")
deserializer := conn.Deserializer("my-topic")
defer reader.Close(ctx)

messages, committer, _ := reader.Read(ctx, 1000, 1*time.Second)
_ = committer(ctx)
handleManyMessages(messages)
```

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
| `ENV` | Environment determines which Kafka service and authentication mode | `prod` |
| `USE_SCHEMA_REGISTRY` | Boolean on whether schema registry should be used or not | `true` |
| `KAFKA_AUTH_TYPE` | Determines how to authenticate with to Aiven | `sasl` or `tls`|

##### TLS mode

These environment variables are necessary as well for TLS mode
| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_CERT_FILE` | Path to the client certificate file | `/path/to/client-cert.pem` |
| `KAFKA_KEY_FILE` | Path to the client key file | `/path/to/client-key.pem` |
| `KAFKA_CA_FILE` | Path to the Certificate Authority file | `/path/to/ca-cert.pem` |

##### SASL mode

These environment variables are necessary as well for SASL mode

| Variable | Description | Example |
|----------|-------------|---------|
| `AZURE_KAFKA_SCOPE` | Azure scope to use for fetching tokens to authenticate with to Aiven | `api://aaaa-bbbb-cccc-dddd` |


#### Optional Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated list of Kafka broker addresses to use | `broker1:9092,broker2:9092` |
| `KAFKA_SCHEMA_REGISTRY_URL` | URL to the desired schema registry you want to use | `https://url.com:9090` |
| `KAFKA_USER` | Username to authenticate with to the desired schema registry | `username` |
| `KAFKA_PASSWORD` | Password to authenticate with to Aiven Schema Registry | `pass` |

If any of the above variables are not set, they will default to:

Test environment:
- KAFKA_BROKERS = kafka-test-ture-test.com
- KAFKA_SCHEMA_REGISTRY_URL = kafka-test-ture-test.com:18360
- KAFKA_USER: object ID as username for the application
- KAFKA_PASSWORD: password associated with the user in Aiven

Prod environment:
- KAFKA_BROKERS = kafka-prod-ture-prod.com
- KAFKA_SCHEMA_REGISTRY_URL = kafka-prod-ture-prod.com:11132
- KAFKA_USER: object ID as username for the application
- KAFKA_PASSWORD: password associated with the user in Aiven


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

## Trace Metrics
The following metric counters and gauges are automatically maintained:
* **messages_produced_total** (counter): The total number of messages that have been written to the Kafka topic.
* **kafka_lag_partition** (gauge): Measures the number of messages remaining on the partition that service hasn't read 
    yet. The partition is question is added as a attribute on the gauge.

## Testing

Run the tests with:

```bash
go test -v
```

The tests use [testcontainers-go](https://golang.testcontainers.org/) to spin up a real Kafka instance for integration testing.

## License

See LICENSE file for details.
