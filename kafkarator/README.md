# Kafkarator

A Go library for connecting to and interacting with Kafka services, with support for both TLS-secured and non-TLS connections.

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

## Testing

Run the tests with:

```bash
go test -v
```

The tests use [testcontainers-go](https://golang.testcontainers.org/) to spin up a real Kafka instance for integration testing.

## License

See LICENSE file for details.
