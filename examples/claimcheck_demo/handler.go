package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

type messageHandler struct {
	count  int
	logger *slog.Logger
}

func (h *messageHandler) HandleMessage(ctx context.Context, msg *claimcheck.Message) error {
	meta, err := msg.PeekEnvelope(ctx)
	if err != nil {
		return fmt.Errorf("peek envelope: %w", err)
	}
	h.logger.InfoContext(ctx, "Envelope received",
		"batch_id", meta.BatchID,
		"records", meta.RecordCount,
		"bytes", meta.ByteSize,
	)

	for r, err := range claimcheck.Records[SensorReading](ctx, msg) {
		if err != nil {
			return fmt.Errorf("stream records: %w", err)
		}
		h.logger.InfoContext(ctx, "  record",
			"sensor_id", r.SensorID,
			"value", r.Value,
			"ts_ms", r.TsMs,
		)
	}
	return nil
}
