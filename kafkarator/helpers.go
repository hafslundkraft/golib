package kafkarator

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func defaultSubjectNameProvider(topic string) (string, error) {
	if topic == "" {
		return "", fmt.Errorf("topic is empty")
	}
	return topic + "-value", nil
}

func convertHeaders(hdrs []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = h.Value
	}
	return m
}
