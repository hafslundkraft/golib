package main

import (
	"context"
	"fmt"
	"log"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// startKafkaContainer starts a Kafka testcontainer
func startKafkaContainer(ctx context.Context) *testkafka.KafkaContainer {
	container, err := testkafka.Run(ctx, kafkaImage, testkafka.WithClusterID("demo-cluster"))
	if err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}
	return container
}

// getBrokerAddress retrieves the broker address from the Kafka container
func getBrokerAddress(ctx context.Context, container *testkafka.KafkaContainer) string {
	brokers, err := container.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		log.Fatalf("Failed to get broker: %v", err)
	}
	return brokers[0]
}

// setupKafkaConnection creates a kafkarator connection with mock schema registry
func setupKafkaConnection(broker string, tp *telemetry.Provider) *kafkarator.Connection {
	config := &kafkarator.Config{
		Broker:   broker,
		AuthMode: kafkarator.AuthNone,
		SchemaRegistryConfig: kafkarator.SchemaRegistryConfig{
			SchemaRegistryURL:      "",
			SchemaRegistryUser:     "none",
			SchemaRegistryPassword: "dummy",
		},
		Env:          "test",
		SystemName:   "kafkarator",
		WorkloadName: "processor-demo",
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

// mockSchemaRegistry is a simple in-memory mock for schema registry
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
