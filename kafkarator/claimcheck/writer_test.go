package claimcheck_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

func TestWriter_StampsProducerSystemOnEnvelope(t *testing.T) {
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSystemForTest("billing"),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{
			schemaStr: simpleSchema("id"),
			version:   1,
			id:        42,
		}),
	)

	batch, err := w.NewBatch(context.Background(), "billing.test.invoices")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "billing", env.System, "envelope must record the producing system for readers' ARN construction")
}

func TestMultipartWriter_CompleteFlushesSmallPayload(t *testing.T) {
	// Write < minPartSize bytes; Complete must still upload them.
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{
			schemaStr: simpleSchema("id"),
			version:   1,
			id:        42,
		}),
	)

	batch, err := w.NewBatch(context.Background(), "test-topic")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "test-topic", env.Topic)
	assert.Equal(t, int64(1), env.RecordCount)
	assert.Positive(t, env.ByteSize)

	bucket, key := bucketAndKey(t, env.StorageURI)
	assert.NotEmpty(t, s3.Store[bucket+"/"+key])
}

func TestMultipartWriter_AbortLeavesNoObject(t *testing.T) {
	s3 := claimcheck.NewFakeS3Client()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{
			schemaStr: simpleSchema("id"),
			version:   1,
			id:        1,
		}),
	)

	batch, err := w.NewBatch(context.Background(), "abort-topic")
	require.NoError(t, err)
	require.NoError(t, batch.Write(map[string]any{"id": int32(99)}))
	batch.Cleanup()

	assert.Empty(t, s3.Store, "abort must leave no object in store")
}

func TestDefaultBucketResolver_UsesHashConvention(t *testing.T) {
	topic := "my-topic-v1"
	sum := sha256.Sum256([]byte(topic))
	expected := "cc-" + hex.EncodeToString(sum[:])[:16]

	assert.Equal(t, expected, claimcheck.DefaultBucketResolver(topic))
}

func TestStager_UsesPayloadSubject(t *testing.T) {
	fetcher := &fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}
	s3 := claimcheck.NewFakeS3Client()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(fetcher),
	)

	batch, err := w.NewBatch(context.Background(), "demo-topic")
	require.NoError(t, err)
	batch.Cleanup()

	assert.Equal(t, "demo-topic-claim-check-payload", fetcher.subject)
}

func TestStager_BucketDerivedFromResolver(t *testing.T) {
	topic := "hash-check-topic"
	expected := claimcheck.DefaultBucketResolver(topic)

	var capturedBucket string
	s3 := claimcheck.NewFakeS3Client()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("x"), version: 1, id: 1}),
		claimcheck.WithWriterBucketResolver(func(t string) string {
			capturedBucket = claimcheck.DefaultBucketResolver(t)
			return capturedBucket
		}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	batch.Cleanup()

	assert.Equal(t, expected, capturedBucket)
}

func TestBatch_WriteProduceAndReadRecords(t *testing.T) {
	type Event struct {
		ID   int32  `parquet:"id"`
		Name string `parquet:"name"`
	}

	const topic = "event-topic"
	schemaStr := `{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	input := []Event{{1, "alice"}, {2, "bob"}, {3, "carol"}}
	for _, r := range input {
		require.NoError(t, batch.Write(r))
	}
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{env: env})

	var got []Event
	for r, err := range claimcheck.Records[Event](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 3)
	assert.Equal(t, Event{1, "alice"}, got[0])
	assert.Equal(t, Event{3, "carol"}, got[2])
}

func TestBatch_KeyAndHeadersPropagated(t *testing.T) {
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("v"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), "keyed-topic",
		claimcheck.WithBatchKey([]byte("my-key")),
		claimcheck.WithBatchHeaders(map[string][]byte{"x-source": []byte("test")}),
	)
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"v": int32(42)}))
	require.NoError(t, batch.Produce(context.Background()))

	assert.Equal(t, []byte("my-key"), kw.last.Key)
	assert.Equal(t, []byte("test"), kw.last.Headers["x-source"])
}

func TestBatch_FlushesMultipleRowGroups(t *testing.T) {
	const topic = "rowgroup-topic"
	schemaStr := `{"type":"record","name":"RG","fields":[{"name":"id","type":"int"}]}`
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
		claimcheck.WithWriterRowGroupSize(2),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	for i := range 6 {
		require.NoError(t, batch.Write(map[string]any{"id": int32(i)}))
	}
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{env: env})
	pr, err := msg.Payload(context.Background())
	require.NoError(t, err)
	defer pr.Close()

	f, err := parquet.OpenFile(pr, pr.Size())
	require.NoError(t, err)

	var totalRows int64
	for _, rg := range f.RowGroups() {
		totalRows += rg.NumRows()
	}
	assert.GreaterOrEqual(t, len(f.RowGroups()), 2)
	assert.Equal(t, int64(6), totalRows)
}

func TestBatch_ParquetFooterEmbeddsAvroMetadata(t *testing.T) {
	const topic = "meta-topic"
	schemaStr := `{"type":"record","name":"M","fields":[{"name":"v","type":"long"}]}`
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 3, id: 55}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	require.NoError(t, batch.Write(map[string]any{"v": int64(42)}))
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	bucket, key := bucketAndKey(t, env.StorageURI)
	data := s3.Store[bucket+"/"+key]
	require.NotEmpty(t, data)

	f, err := parquet.OpenFile(claimcheck.NewBytesReaderAt(data), int64(len(data)))
	require.NoError(t, err)

	kv := parquetKV(f)
	assert.Equal(t, schemaStr, kv["avro.schema"])
	assert.Equal(t, topic+"-claim-check-payload", kv["avro.schema.subject"])
	assert.Equal(t, "3", kv["avro.schema.version"])
	assert.Equal(t, "55", kv["avro.schema.id"])
}

func TestBatch_ProduceAfterCleanupReturnsError(t *testing.T) {
	s3 := claimcheck.NewFakeS3Client()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), "double-close")
	require.NoError(t, err)

	batch.Cleanup()
	assert.Error(t, batch.Produce(context.Background()))
}

func TestBatch_CleanupAfterProduceIsNoop(t *testing.T) {
	// The idiomatic pattern is `defer batch.Cleanup()` — must be silent after a successful Produce.
	s3 := claimcheck.NewFakeS3Client()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("v"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), "noop-abort")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"v": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))
}

// TestPayloadReader_ReadAtDoesNotMoveSequentialPosition verifies that
// ReadAt (used by parquet.OpenFile for the footer) does not disturb the
// sequential read position, satisfying the io.ReaderAt contract.
func TestPayloadReader_ReadAtDoesNotMoveSequentialPosition(t *testing.T) {
	type R struct {
		ID int32 `parquet:"id"`
	}
	const topic = "readat-independence-topic"
	schemaStr := `{"type":"record","name":"R","fields":[{"name":"id","type":"int"}]}`
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, batch.Write(map[string]any{"id": int32(i)}))
	}
	require.NoError(t, batch.Produce(context.Background()))

	env := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{env: env})

	// Open PayloadReader — parquet.OpenFile will issue ReadAt calls for the footer.
	pr, err := msg.Payload(context.Background())
	require.NoError(t, err)
	defer pr.Close()

	f, err := parquet.OpenFile(pr, pr.Size())
	require.NoError(t, err)

	// Now use a generic reader which reads sequentially via ReadAt.
	r := parquet.NewGenericReader[R](pr)
	defer r.Close()

	buf := make([]R, 10)
	n, err := r.Read(buf)
	require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected read error: %v", err)
	assert.Equal(t, 5, n, "all rows should be readable after ReadAt footer reads")

	// Verify that ReadAt for the last byte (footer size check) also still works.
	var oneByte [1]byte
	nn, err := pr.ReadAt(oneByte[:], pr.Size()-1)
	assert.Equal(t, 1, nn)
	assert.NoError(t, err)

	_ = f
}

func TestWriter_SharedTopicStampsAndBuildsForDataDefinitions(t *testing.T) {
	const topic = "test.water--obs.measurements--v1" // shared product

	var gotSystem, gotBucket string
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3FactoryForTest(func(system, bucket string) (claimcheck.S3Writer, error) {
			gotSystem, gotBucket = system, bucket
			return claimcheck.NewFakeS3Client(), nil
		}),
		// Producer's own system is "billing" — must be ignored for a shared topic.
		claimcheck.WithWriterSystemForTest("billing"),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()
	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	assert.Equal(t, "data-definitions", gotSystem, "write client must be built for the bucket owner, not the producer")
	assert.Equal(t, claimcheck.DefaultBucketResolver(topic), gotBucket)
	env := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "data-definitions", env.System, "stamp must match the write role")
}

func TestWriter_InternalTopicStampsOwnSystem(t *testing.T) {
	const topic = "test.sys--billing.invoices--v1" // internal product

	var gotSystem string
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3FactoryForTest(func(system, _ string) (claimcheck.S3Writer, error) {
			gotSystem = system
			return claimcheck.NewFakeS3Client(), nil
		}),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()
	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	assert.Equal(t, "billing", gotSystem)
	env := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "billing", env.System)
}

func TestWriter_WithWriterSystemOverridesBoth(t *testing.T) {
	const topic = "test.water--obs.measurements--v1" // resolver would say data-definitions

	var gotSystem string
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3FactoryForTest(func(system, _ string) (claimcheck.S3Writer, error) {
			gotSystem = system
			return claimcheck.NewFakeS3Client(), nil
		}),
		claimcheck.WithWriterSystem("override-sys"),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()
	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	assert.Equal(t, "override-sys", gotSystem, "override must win over the resolver for the write role")
	env := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "override-sys", env.System, "override must win over the resolver for the stamp")
}

// parquetKV extracts the Parquet key-value metadata footer as a map.
func parquetKV(f *parquet.File) map[string]string {
	m := make(map[string]string, len(f.Metadata().KeyValueMetadata))
	for _, e := range f.Metadata().KeyValueMetadata {
		m[e.Key] = e.Value
	}
	return m
}
