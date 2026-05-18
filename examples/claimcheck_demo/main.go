// Package main demonstrates the claim-check pattern using the claimcheck
// library.
//
// This example shows:
//   - Writing many records to S3/Parquet and producing a small envelope to Kafka
//   - Processing envelopes from Kafka and streaming records back from S3
//
// S3 is replaced by an in-memory FakeS3Client so the demo runs without any
// object-storage infrastructure. Kafka is provided by a Redpanda testcontainer.
//
// Shared setup helpers (container and schema setup) are in internal/demohelpers.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
	"github.com/hafslundkraft/golib/telemetry"

	"github.com/hafslundkraft/golib/examples/internal/demohelpers"
)

// payloadSchema is the Avro schema for the records we write as the payload.
// It must be registered under "{topic}-claim-check-payload" before writing.
const payloadSchema = `{
	"type": "record",
	"name": "SensorReading",
	"namespace": "happi.demo",
	"fields": [
		{"name": "sensor_id", "type": "string"},
		{"name": "value",     "type": "double"},
		{"name": "ts_ms",     "type": "long"}
	]
}`

const claimCheckenvelopechema = `{
	"type": "record",
	"name": "ClaimCheckEnvelope",
	"namespace": "happi.kafkarator.claimcheck",
	"fields": [
		{"name": "batch_id",     "type": "string"},
		{"name": "storage_uri",  "type": "string"},
		{"name": "topic",        "type": "string"},
		{"name": "record_count", "type": "long"},
		{"name": "byte_size",    "type": "long"},
		{"name": "created_at",   "type": "long"}
	]
}`

const topic = "sensor-readings"

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v23.3.3"

// SensorReading is the typed representation of a record in the payload.
// Field names must match the Avro field names in payloadSchema.
type SensorReading struct {
	SensorID string  `parquet:"sensor_id"`
	Value    float64 `parquet:"value"`
	TsMs     int64   `parquet:"ts_ms"`
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute) // stop the demo after 1 minute
	defer cancel()
	log.Println("Starting Redpanda container...")
	container := demohelpers.StartRedpandaContainer(ctx, redpandaImage)
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Printf("container terminate: %v", err)
		}
	}()

	broker := demohelpers.GetRedpandaBrokerAddress(ctx, container)
	srURL := demohelpers.GetRedpandaSchemaRegistryAddress(ctx, container)
	log.Printf("broker=%s  schema-registry=%s", broker, srURL)
	tp, shutdown := telemetry.New(ctx, telemetry.WithLocal(true))
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	// Register the payload schema so the Stager can look it up.
	demohelpers.RegisterSchema(srURL, demohelpers.ClaimCheckPayloadSubject(topic), payloadSchema)

	// Register the envelope schema and create the kafkarator connection.
	conn := demohelpers.SetupKafkaConnection(
		broker,
		srURL,
		topic,
		claimCheckenvelopechema,
		"claimcheck",
		"claimcheck-demo",
		tp,
	)

	// Shared in-memory S3 used by both writer and processor.
	s3 := claimcheck.NewFakeS3Client()
	logger := tp.Logger()

	logger.InfoContext(ctx, "Writing records...")
	if err := writeRecords(ctx, conn, s3, logger); err != nil {
		logger.ErrorContext(ctx, "write failed", "error", err)
		return
	}

	logger.InfoContext(ctx, "Processing envelope from Kafka...")
	if err := processMessages(ctx, conn, s3, logger); err != nil {
		logger.ErrorContext(ctx, "process failed", "error", err)
		return
	}

	logger.InfoContext(ctx, "Demo complete")
	time.Sleep(time.Second) // let telemetry flush
}

// writeRecords creates a claim-check Writer, opens a batch, writes a handful
// of sensor readings, then publishes the envelope to Kafka.
func writeRecords(
	ctx context.Context,
	conn *kafkarator.Connection,
	s3 claimcheck.S3Client,
	logger *slog.Logger,
) error {
	w, err := claimcheck.NewWriter(conn,
		claimcheck.WithWriterS3Client(s3),
	)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	defer w.Close(ctx) //nolint:errcheck // close error in defer is intentionally discarded

	batch, err := w.NewBatch(ctx, topic)
	if err != nil {
		return fmt.Errorf("open batch: %w", err)
	}
	defer batch.Cleanup() // no-op after a successful Commit

	now := time.Now().UnixMilli()
	readings := []SensorReading{
		{SensorID: "temp-1", Value: 23.4, TsMs: now},
		{SensorID: "temp-2", Value: 24.1, TsMs: now},
		{SensorID: "hum-1", Value: 61.0, TsMs: now},
		{SensorID: "temp-1", Value: 23.6, TsMs: now + 1000},
		{SensorID: "hum-1", Value: 60.5, TsMs: now + 1000},
	}

	for _, r := range readings {
		if err := batch.Write(r); err != nil {
			return fmt.Errorf("write records: %w", err)
		}
	}

	if err := batch.Produce(ctx); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	logger.InfoContext(ctx, "Batch produced", "records", len(readings), "topic", topic)
	return nil
}

// processMessages creates a claim-check Processor and processes one envelope.
// For each envelope it streams all records from S3 and logs them.
func processMessages(
	ctx context.Context,
	conn *kafkarator.Connection,
	s3 claimcheck.S3Client,
	logger *slog.Logger,
) error {
	handler := &messageHandler{
		logger: logger,
	}
	proc, err := claimcheck.NewProcessor(conn, topic, handler.HandleMessage,
		claimcheck.WithProcessorS3Client(s3),
		claimcheck.WithProcessorReadTimeout(1*time.Minute),
	)
	if err != nil {
		return fmt.Errorf("create processor: %w", err)
	}
	defer proc.Close(ctx) //nolint:errcheck // close error in defer is intentionally discarded

	for {
		n, err := proc.ProcessNext(ctx)
		if err != nil {
			return fmt.Errorf("process next: %w", err)
		}

		logger.InfoContext(ctx, "ProcessNext completed", "messages-processed", n)

		if n > 0 {
			break // exit after processing one batch, in production you'd typically keep processing indefinitely
		}

		if err := ctx.Err(); err != nil {
			break // context canceled, exit gracefully
		}
	}

	return nil
}
