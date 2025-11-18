package kafkarator

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"testing"
)

const kafkaImage = "confluentinc/confluent-local:7.5.0"

func Test_connection_Consumer(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(
		ctx,
		kafkaImage,
		kafka.WithClusterID("test-cluster"),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Errorf("failed to terminate kafka container: %v", err)
		}
	}()
	if err != nil {
		t.Errorf("failed to start kafka container: %v", err)
	}


}
