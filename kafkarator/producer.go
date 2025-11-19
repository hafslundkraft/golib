package kafkarator

import (
	"context"
	"fmt"
	"github.com/hafslundkraft/golib/telemetry"

	"github.com/segmentio/kafka-go"
)

type producer struct {
	writer *kafka.Writer
	tel    telemetry.Provider
}

func (p *producer) Produce(ctx context.Context, msg []byte, headers map[string][]byte) error {
	if err := p.writer.WriteMessages(ctx, kafkaMessage(msg, headers)); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

func kafkaMessage(b []byte, headers map[string][]byte) kafka.Message {
	headerList := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		headerList = append(headerList, kafka.Header{Key: k, Value: v})
	}

	return kafka.Message{
		Value:   b,
		Headers: headerList,
	}
}
