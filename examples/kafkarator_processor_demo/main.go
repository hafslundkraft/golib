package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
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

	kafkaImage    = "confluentinc/confluent-local:7.5.0"
	topic         = "kafkarator-demo-topic"
	consumerGroup = "kafkarator-demo-group"
)

func main() {
	ctx := context.Background()

	// Start Kafka container
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

	// Setup telemetry with local mode
	tp, shutdown := telemetry.New(ctx, "kafkarator-demo", telemetry.WithLocal(true))
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("Failed to shutdown telemetry: %v", err)
		}
	}()

	logger := tp.Logger()
	logger.InfoContext(ctx, "Starting kafkarator demo...")

	// Create kafkarator connection with mock schema registry
	conn := setupKafkaConnection(broker, tp)
	logger.InfoContext(ctx, "Connection created")

	// Produce messages
	logger.InfoContext(ctx, "Producing messages...")
	writer, err := conn.Writer()
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create writer", "error", err)
		return
	}
	serializer := conn.Serializer()
	defer writer.Close(ctx)

	sendDemoMessages(ctx, writer, serializer, logger)

	// Consume and process messages
	logger.InfoContext(ctx, "Consuming messages...")
	consumeAndProcessMessages(ctx, conn, logger)

	logger.InfoContext(ctx, "Demo completed successfully!")
	time.Sleep(2 * time.Second) // Allow telemetry to flush
}

// sendDemoMessages sends demo messages to Kafka with Avro serialization
func sendDemoMessages(
	ctx context.Context,
	writer *kafkarator.Writer,
	serializer kafkarator.ValueSerializer,
	logger *slog.Logger,
) {
	for i := 0; i < 5; i++ {
		msg := map[string]any{
			"text":      fmt.Sprintf("Hello, kafkarator! Message #%d", i),
			"timestamp": time.Now().UnixMilli(),
		}

		payload, err := serializer.Serialize(ctx, topic, msg)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to serialize message", "error", err, "message-num", i)
			continue
		}

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
}

// consumeAndProcessMessages reads messages from Kafka and processes them using a handler
func consumeAndProcessMessages(ctx context.Context, conn *kafkarator.Connection, logger *slog.Logger) {
	// Create channel reader (simpler than batching Reader)
	messageChan, err := conn.ChannelReader(ctx, topic, consumerGroup)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create channel reader", "error", err)
		return
	}

	handler := newMessageHandler(conn, logger)

	// Process up to 5 messages with timeout
	timeout := time.After(10 * time.Second)
	for handler.count < 5 {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				return // Channel closed
			}
			handler.handle(ctx, &msg)
		case <-timeout:
			logger.InfoContext(ctx, "Timeout waiting for messages", "processed", handler.count)
			return
		}
	}
}

// messageHandler processes incoming messages (similar to Python's MessageHandler)
type messageHandler struct {
	count        int
	deserializer kafkarator.ValueDeserializer
	logger       *slog.Logger
}

func newMessageHandler(conn *kafkarator.Connection, logger *slog.Logger) *messageHandler {
	return &messageHandler{
		deserializer: conn.Deserializer(),
		logger:       logger,
	}
}

func (h *messageHandler) handle(ctx context.Context, msg *kafkarator.Message) {
	decoded, err := h.deserializer.Deserialize(ctx, msg.Topic, msg.Value)
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to deserialize message", "error", err, "topic", msg.Topic)
		return
	}
	if decoded == nil {
		h.logger.WarnContext(ctx, "Received nil decoded message", "topic", msg.Topic)
		return
	}

	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		h.logger.WarnContext(ctx, "Decoded message is not a map",
			"topic", msg.Topic,
			"type", fmt.Sprintf("%T", decoded),
		)
		return
	}

	h.count++
	h.logger.InfoContext(ctx, "Message handled",
		"count", h.count,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"key", string(msg.Key),
		"text", decodedMap["text"],
		"timestamp", decodedMap["timestamp"],
	)
}

// Helpers

func startKafkaContainer(ctx context.Context) *testkafka.KafkaContainer {
	container, err := testkafka.Run(ctx, kafkaImage, testkafka.WithClusterID("demo-cluster"))
	if err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}
	return container
}

func getBrokerAddress(ctx context.Context, container *testkafka.KafkaContainer) string {
	brokers, err := container.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		log.Fatalf("Failed to get broker: %v", err)
	}
	return brokers[0]
}

func setupKafkaConnection(broker string, tp *telemetry.Provider) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      "",
			SchemaRegistryUser:     "none",
			SchemaRegistryPassword: "dummy",
		},
	}

	// Register schema in mock registry
	mockSR := newMockSchemaRegistry()
	mockSR.addSchema(topic+"-value", schema)

	conn, err := kafkarator.New(config, tp, kafkarator.WithSchemaRegistryClient(mockSR))
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	return conn
}

type mockSchemaRegistry struct {
	latest map[string]sr.SchemaMetadata
	byID   map[string]map[int]sr.SchemaInfo
	nextID int
}

func newMockSchemaRegistry() *mockSchemaRegistry {
	return &mockSchemaRegistry{
		latest: make(map[string]sr.SchemaMetadata),
		byID:   make(map[string]map[int]sr.SchemaInfo),
		nextID: 1,
	}
}

func (m *mockSchemaRegistry) addSchema(subject, schema string) {
	schemaInfo := sr.SchemaInfo{Schema: schema}
	m.latest[subject] = sr.SchemaMetadata{
		SchemaInfo: schemaInfo,
		ID:         m.nextID,
		Subject:    subject,
		Version:    1,
	}
	if m.byID[subject] == nil {
		m.byID[subject] = make(map[int]sr.SchemaInfo)
	}
	m.byID[subject][m.nextID] = schemaInfo
	m.nextID++
}

func (m *mockSchemaRegistry) GetLatestSchemaMetadata(subject string) (sr.SchemaMetadata, error) {
	if md, ok := m.latest[subject]; ok {
		return md, nil
	}
	return sr.SchemaMetadata{}, fmt.Errorf("schema not found: %s", subject)
}

func (m *mockSchemaRegistry) GetBySubjectAndID(subject string, id int) (sr.SchemaInfo, error) {
	if subjectSchemas, ok := m.byID[subject]; ok {
		if info, ok := subjectSchemas[id]; ok {
			return info, nil
		}
	}
	return sr.SchemaInfo{}, fmt.Errorf("schema not found for subject %s and id %d", subject, id)
}
