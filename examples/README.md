# Examples

This directory contains runnable demonstrations of the golib features.

## Telemetry Local Demo

Demonstrates local telemetry usage with logging, tracing, and metrics.

### Running

```bash
cd telemetry_local_demo
go run .
```

### What it demonstrates

- Creating a telemetry provider with local output
- Using structured logging with different severity levels
- Creating and using spans with attributes
- Recording metrics with counters

## Kafkarator Processor Demo

Full end-to-end Kafka example with testcontainers, Avro serialization, and message processing.

### Running

```bash
cd kafkarator_processor_demo
go run .
```

### What it demonstrates

- Starting a local Kafka container for testing
- Creating a Kafkarator connection with schema registry
- Producing messages with Avro serialization
- Consuming and processing messages
- Integrated telemetry for Kafka operations
