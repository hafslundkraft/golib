// Package main demonstrates how to use kafkarator to read and write messages to Kafka.
//
// This example shows:
//   - Writing messages to Kafka with Avro serialization
//   - Reading messages from Kafka with Avro deserialization
//   - Processing messages with a handler
//
// The setup code (testcontainers, telemetry, mock schema registry) is in helpers.go
// and can be ignored when learning the basic kafkarator patterns.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
)

const (
	schema = `{
		"type": "record",
		"name": "DemoMessage",
		"namespace": "kafkarator.demo",
		"fields": [
			{"name": "text", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	kafkaImage   = "confluentinc/confluent-local:7.5.0"
	topic        = "kafkarator-demo-topic"
	messageCount = 5
	readTimeout  = 10 * time.Second
)

func main() {
	ctx := context.Background()

	// ========================================
	// SETUP (boilerplate for demo purposes)
	// ========================================
	// In production, you'll have Kafka already running and configured.
	// This section starts a local Kafka for testing.

	log.Println("Starting Kafka container...")
	kafkaContainer := startKafkaContainer(ctx)
	defer func() {
		log.Println("Cleaning up: terminating Kafka container...")
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Printf("Failed to terminate container: %v", err)
		}
		log.Println("Cleanup complete")
	}()

	broker := getBrokerAddress(ctx, kafkaContainer)
	log.Printf("Kafka broker: %s", broker)

	// Setup telemetry (observability)
	tp, shutdown := telemetry.New(ctx, "kafkarator-demo", telemetry.WithLocal(true))
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown telemetry: %v", err)
		}
	}()

	logger := tp.Logger()
	logger.InfoContext(ctx, "Starting kafkarator demo...")

	// Create kafkarator connection (with Avro schema support)
	conn := setupKafkaConnection(broker, tp)
	logger.InfoContext(ctx, "Connection created")

	// ========================================
	// MAIN DEMO: Using kafkarator
	// ========================================

	// 1. Write messages to Kafka
	if err := writeMessages(ctx, conn, logger); err != nil {
		logger.ErrorContext(ctx, "Failed to write messages", "error", err)
		return
	}

	// 2. Read and process messages from Kafka
	if err := readAndProcessMessages(ctx, conn, logger); err != nil {
		logger.ErrorContext(ctx, "Failed to read messages", "error", err)
		return
	}

	logger.InfoContext(ctx, "Demo completed successfully!")
	time.Sleep(2 * time.Second) // Allow telemetry to flush
}

// writeMessages demonstrates how to write messages to Kafka using kafkarator.
//
// Key steps:
//  1. Get a Writer from the connection
//  2. Get a Serializer to convert Go structs to Avro
//  3. Create your message as a Go map/struct
//  4. Serialize it with the schema
//  5. Write it to Kafka
func writeMessages(
	ctx context.Context,
	conn *kafkarator.Connection,
	logger *slog.Logger,
) error {
	logger.InfoContext(ctx, "Writing messages to Kafka...")

	// Step 1: Get a writer from the kafkarator connection
	writer, err := conn.Writer()
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer writer.Close(ctx)

	// Step 2: Get a serializer for Avro encoding
	serializer := conn.Serializer()

	// Write demo messages
	for i := 0; i < messageCount; i++ {
		// Step 3: Create your message data (matches the Avro schema)
		msg := map[string]any{
			"text":      fmt.Sprintf("Hello, kafkarator! Message #%d", i),
			"timestamp": time.Now().UnixMilli(),
		}

		// Step 4: Serialize using Avro schema
		payload, err := serializer.Serialize(ctx, topic, msg)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to serialize message", "error", err, "message-num", i)
			continue
		}

		// Step 5: Write to Kafka
		if err := writer.Write(ctx, &kafkarator.Message{
			Topic: topic,
			Key:   []byte("demo"),
			Value: payload,
		}); err != nil {
			logger.ErrorContext(ctx, "Failed to write message", "error", err, "message-num", i)
			continue
		}

		logger.InfoContext(ctx, "Produced message", "message-num", i)
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// readAndProcessMessages demonstrates how to read and process messages from Kafka using the Processor.
//
// Key steps:
//  1. Create a handler function that processes individual messages
//  2. Create a Processor with the handler (includes automatic tracing and offset management)
//  3. Call ProcessNext in a loop to process group of messages
func readAndProcessMessages(ctx context.Context, conn *kafkarator.Connection, logger *slog.Logger) error {
	logger.InfoContext(ctx, "Reading messages from Kafka...")

	// Step 1: Create handler with deserializer (it will deserialize Avro messages to get the values)
	handler := newMessageHandler(logger, conn.Deserializer())

	// Step 2: Create a Processor - it handles tracing and offset management automatically
	processor, err := conn.Processor(
		topic,
		handler.handle, // Pass the handler function
		kafkarator.WithProcessorReadTimeout(10*time.Second),
		kafkarator.WithProcessorMaxMessages(10),
	)
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}
	defer processor.Close(ctx)

	// Step 3: Process messages in a loop until we've processed enough
	deadline := time.Now().Add(readTimeout)
	for handler.count < messageCount && time.Now().Before(deadline) {
		// ProcessNext reads messages and processes them based on processor configuration
		processed, err := processor.ProcessNext(ctx)
		if err != nil {
			return fmt.Errorf("process messages: %w", err)
		}

		if processed == 0 {
			logger.InfoContext(ctx, "No messages available, waiting...")
			time.Sleep(100 * time.Millisecond)
		}
	}

	logger.InfoContext(ctx, "Processing complete", "total-processed", handler.count)
	return nil
}
