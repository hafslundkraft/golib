package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hafslundkraft/golib/kafkarator"
)

// messageHandler processes incoming messages
type messageHandler struct {
	count        int
	logger       *slog.Logger
	deserializer kafkarator.ValueDeserializer
}

func newMessageHandler(logger *slog.Logger, deserializer kafkarator.ValueDeserializer) *messageHandler {
	return &messageHandler{
		logger:       logger,
		deserializer: deserializer,
	}
}

func (h *messageHandler) handle(
	ctx context.Context,
	msg *kafkarator.Message,
) error {
	// Deserialize the Avro message to get the actual values
	decoded, err := h.deserializer.Deserialize(ctx, msg.Topic, msg.Value)
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to deserialize message", "error", err, "topic", msg.Topic)
		return fmt.Errorf("deserialize message: %w", err)
	}
	if decoded == nil {
		h.logger.WarnContext(ctx, "Received nil decoded message", "topic", msg.Topic)
		return fmt.Errorf("decoded message is nil for topic %s", msg.Topic)
	}

	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		h.logger.WarnContext(ctx, "Decoded message is not a map",
			"topic", msg.Topic,
			"type", fmt.Sprintf("%T", decoded),
		)
		return fmt.Errorf("decoded message is not a map, got %T", decoded)
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
	return nil
}
