# claimcheck — claim-check transport for large payloads

`claimcheck` is the Go equivalent of the Python `hafslund.happi.kafkarator.claim_check`
module. Use it when the logical payload should be stored externally as
Parquet in S3-compatible object storage while Kafka carries a small
envelope message pointing to it.

## Install

```bash
go get github.com/hafslundkraft/golib/kafkarator/claimcheck
```

The `claimcheck` package is a separate Go module from `kafkarator` so
consumers who don't need the claim-check pattern don't pull in the
Parquet, S3, and STS dependencies.

## S3 authentication

S3 authentication is handled automatically when `HAPPI_IDP_ISSUER_URL` is
set (injected by the Happi operator alongside `AWS_ENDPOINT_URL_S3` and
`AWS_ENDPOINT_URL_STS`). The library exchanges the platform IDP token
for a Ceph-compatible JWT before each S3 operation and refreshes it
transparently.

The IAM role ARN is derived automatically from `HAPPI_SYSTEM_NAME`,
`HAPPI_ENV`, and the bucket name — no manifest configuration required.
The bucket name is derived from the topic name by
`DefaultBucketResolver` as `cc-{sha256(topic)[:16]}`.

For local testing or when running outside Happi, pass a custom S3 client
(e.g. `claimcheck.NewFakeS3Client()` or an AWS SDK client pointed at a
local Ceph/MinIO) with `WithWriterS3Client` / `WithProcessorS3Client`.

## Tracing

S3 operations are instrumented on the tracer provided by the
`kafkarator.Connection`'s `TelemetryProvider`. Envelope
resolution creates a `claim_check resolve <topic>` client span with
attributes for `batch_id`, `record_count`, `byte_size`, and `storage_uri`.

Override the tracer used for S3 fetches with `WithProcessorTracer`.

## Schema Registry subjects

Two subjects are used by the claim-check flow:

- `{topic}-value` — Avro schema for the envelope message on Kafka.
  Corresponds to the `claimcheck.Envelope` struct.
- `{topic}-claim-check-payload` — Avro schema for the logical payload
  written to Parquet. Register this before opening a batch.

## Usage

```go
package main

import (
    "context"
    "log"

    "github.com/hafslundkraft/golib/kafkarator"
    "github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// SensorReading is the typed representation of one record in the payload.
// Field names must match the Avro field names in the payload schema
// registered under "{topic}-claim-check-payload".
type SensorReading struct {
    SensorID string  `parquet:"sensor_id"`
    Value    float64 `parquet:"value"`
    TsMs     int64   `parquet:"ts_ms"`
}

func main() {
    ctx := context.Background()
    cfg, err := kafkarator.ConfigFromEnvVars()
    if err != nil {
        log.Fatal(err)
    }
    conn, err := kafkarator.NewConnection(cfg, telemetry /* your TelemetryProvider */)
    if err != nil {
        log.Fatal(err)
    }

    const topic = "sensor-readings"

    // ---- Writer ----
    w, err := claimcheck.NewWriter(conn)
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close(ctx)

    batch, err := w.NewBatch(ctx, topic, claimcheck.WithBatchKey([]byte("batch-1")))
    if err != nil {
        log.Fatal(err)
    }
    defer batch.Cleanup() // no-op after a successful Produce

    for _, r := range []SensorReading{
        {SensorID: "temp-1", Value: 23.4, TsMs: 1_700_000_000_000},
        {SensorID: "hum-1",  Value: 61.0, TsMs: 1_700_000_000_000},
    } {
        if err := batch.Write(r); err != nil {
            log.Fatal(err)
        }
    }

    if err := batch.Produce(ctx); err != nil {
        log.Fatal(err)
    }

    // ---- Processor ----
    handler := func(ctx context.Context, msg *claimcheck.Message) error {
        for r, err := range claimcheck.Records[SensorReading](ctx, msg) {
            if err != nil {
                return err
            }
            log.Printf("  record sensor=%s value=%f", r.SensorID, r.Value)
        }
        return nil
    }

    proc, err := claimcheck.NewProcessor(conn, topic, handler)
    if err != nil {
        log.Fatal(err)
    }
    defer proc.Close(ctx)

    if _, err := proc.ProcessNext(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### Writer

`claimcheck.NewWriter(conn, opts...)` returns a `*Writer` bound to a
`kafkarator.Connection`. Each batch is a Parquet-in-S3 write followed by
a synchronous envelope produce to Kafka — `Produce` blocks until the
broker acknowledges the envelope, so the S3 object is never considered
produced without a durable Kafka message.

Writer options:

| Option | Default | Description |
|--------|---------|-------------|
| `WithWriterS3Client(s3)` | production client from `HAPPI_*` env | Inject a fixed S3 writer (e.g. `FakeS3Client`). |
| `WithWriterSchemaFetcher(f)` | Schema Registry-backed | Override the payload-schema fetcher. |
| `WithWriterBucketResolver(fn)` | `DefaultBucketResolver` | Override the topic→bucket naming convention. Must match the reader side. |
| `WithWriterRowGroupSize(n)` | 100 000 | Records per Parquet row group. |
| `WithWriterPartSize(n)` | 5 MiB | S3 multipart part size in bytes. Must be ≥ 5 MiB. |

Batch options (per `NewBatch` call):

| Option | Description |
|--------|-------------|
| `WithBatchKey(key)` | Kafka message key on the envelope. |
| `WithBatchHeaders(hdrs)` | Additional Kafka headers on the envelope. |

Records passed to `batch.Write` can be any value whose fields map to
the registered payload schema — a concrete struct with `parquet:"..."`
tags, or a `map[string]any`.

When a record is a `map[string]any`, `batch.Write` validates required
(non-nullable) schema fields recursively, including fields inside records,
arrays, and maps. Missing or nil required fields return an error matching
`claimcheck.ErrRequiredField`; the error message includes the field path. A
validation error permanently poisons the batch, so later `Write` calls and
`Produce` return the same error. Struct records are not checked because their
fields are present by construction.

Always `defer batch.Cleanup()`. It aborts the S3 multipart upload if
`Produce` was never called (or errored) and is a no-op once `Produce`
has succeeded.

### Processor

`claimcheck.NewProcessor(conn, topic, handler, opts...)` returns a
`*Processor` that consumes envelopes from `topic` and presents each
Kafka message as a `*claimcheck.Message` with lazy S3 access. Default
`MaxMessages` is 1 because each envelope triggers a full S3 + Parquet
fetch.

Processor options:

| Option | Default | Description |
|--------|---------|-------------|
| `WithProcessorS3Client(s3)` | production client from `HAPPI_*` env | Inject a fixed S3 reader (e.g. `FakeS3Client`). |
| `WithProcessorBucketResolver(fn)` | `DefaultBucketResolver` | Override the topic→bucket naming convention. Must match the writer side. |
| `WithProcessorMaxMessages(n)` | 1 | Max Kafka messages received per `ProcessNext`. |
| `WithProcessorReadTimeout(d)` | 10 s | Max time `ProcessNext` blocks waiting for a message. |
| `WithProcessorAutoOffsetReset(v)` | `OffsetEarliest` | Where to start when no committed offset exists. |
| `WithProcessorTracer(t)` | connection's tracer | Tracer used to instrument S3 payload fetches. |

Accessing the payload from a handler:

- `claimcheck.Records[T](ctx, msg)` — typed row iterator. `T` must be a
  struct whose exported fields carry `parquet:"..."` tags matching the
  Parquet column names. This is the recommended API.
- `msg.PeekEnvelope(ctx)` — decode the envelope without fetching the
  S3 payload. Useful for logging / metrics.
- `msg.Payload(ctx)` — low-level escape hatch returning a
  `*PayloadReader` over the raw Parquet bytes. Caller must `Close`.
- `msg.IsEmpty()` — true when the Kafka message has no value
  (null payload / tombstone).

Returning a non-nil error from the handler stops processing and
prevents the offset commit for the batch.

## Local demo

A fuller local demo with Redpanda (Kafka + Schema Registry) and an
in-memory `FakeS3Client` is at
[`examples/claimcheck_demo`](../../examples/claimcheck_demo). It writes
a few `SensorReading` records to Parquet, produces the envelope, and
then processes the envelope back through a handler using the typed
`Records[SensorReading]` iterator.
