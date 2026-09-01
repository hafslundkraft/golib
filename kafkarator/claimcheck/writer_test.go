package claimcheck_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
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
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{
			schemaStr: simpleSchema("id"),
			version:   1,
			id:        42,
		}),
	)

	// Topic derives the owning system "billing"; it is never set by the caller.
	batch, err := w.NewBatch(context.Background(), "test.sys--billing.invoices--v1")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(
		t,
		"billing",
		envelope.System,
		"envelope must record the topic-derived owning system for readers' ARN construction",
	)
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

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.events--v1")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "test.sys--demo.events--v1", envelope.Topic)
	assert.Equal(t, int64(1), envelope.RecordCount)
	assert.Positive(t, envelope.ByteSize)

	bucket, key := bucketAndKey(t, envelope.StorageURI)
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

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.aborts--v1")
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

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.subjects--v1")
	require.NoError(t, err)
	batch.Cleanup()

	assert.Equal(t, "test.sys--demo.subjects--v1-claim-check-payload", fetcher.subject)
}

func TestStager_BucketDerivedFromResolver(t *testing.T) {
	topic := "test.sys--demo.buckets--v1"
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

	const topic = "test.sys--demo.records--v1"
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

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	var got []Event
	for r, err := range claimcheck.Records[Event](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 3)
	assert.Equal(t, Event{1, "alice"}, got[0])
	assert.Equal(t, Event{3, "carol"}, got[2])
}

func TestBatch_WriteProduceAndReadDecimals(t *testing.T) {
	type Payment struct {
		ID     int32  `parquet:"id"`
		Amount []byte `parquet:"amount"`
	}

	const topic = "test.sys--demo.payments--v1"
	schemaStr := `{"type":"record","name":"Payment","fields":[` +
		`{"name":"id","type":"int"},` +
		`{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}]}`

	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	// Unscaled big-endian two's complement, the encoding both Avro and Parquet
	// use: 123456 at scale 2 is 1234.56, and 0xCE is -50, i.e. -0.50.
	input := []Payment{
		{1, []byte{0x01, 0xE2, 0x40}},
		{2, []byte{0xCE}},
	}
	for _, r := range input {
		require.NoError(t, batch.Write(r))
	}
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	var got []Payment
	for r, err := range claimcheck.Records[Payment](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 2)
	assert.Equal(t, input, got, "decimal bytes must survive the round-trip unchanged")

	// The written file must advertise DECIMAL, not plain bytes, or downstream
	// readers lose the scale.
	pr, err := msg.Payload(context.Background())
	require.NoError(t, err)
	defer pr.Close() //nolint:errcheck // test cleanup

	f, err := parquet.OpenFile(pr, pr.Size())
	require.NoError(t, err)
	var amount parquet.Field
	for _, fld := range f.Schema().Fields() {
		if fld.Name() == "amount" {
			amount = fld
		}
	}
	require.NotNil(t, amount, "amount field missing from written file")
	assert.Equal(t, "DECIMAL(9,2)", amount.Type().String())
}

func TestBatch_WriteProduceAndReadNullableDecimals(t *testing.T) {
	type Payment struct {
		ID     int32  `parquet:"id"`
		Amount []byte `parquet:"amount"`
	}

	const topic = "test.sys--demo.payments-nullable--v1"
	schemaStr := `{"type":"record","name":"Payment","fields":[` +
		`{"name":"id","type":"int"},` +
		`{"name":"amount","type":["null",{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}]}` +
		`]}`

	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	input := []Payment{
		{ID: 1, Amount: []byte{0x01, 0xE2, 0x40}},
		{ID: 2, Amount: nil},
	}
	for _, r := range input {
		require.NoError(t, batch.Write(r))
	}
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	var got []Payment
	for r, err := range claimcheck.Records[Payment](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 2)
	assert.Equal(t, input[0], got[0], "non-null decimal bytes must survive round-trip")
	assert.Equal(t, input[1].ID, got[1].ID)
	assert.Empty(t, got[1].Amount, "nullable decimal should decode as an empty byte slice when null")
}

func TestBatch_WriteProduceAndReadFixedDecimals(t *testing.T) {
	// Amount is []byte, not [16]byte: parquet-go writes a [N]byte field via
	// reflect.Value.Bytes, which panics on the unaddressable array inside a
	// struct passed by value. That applies to every fixed-backed column, not
	// just decimals.
	type Money struct {
		Amount []byte `parquet:"amount"`
	}

	const topic = "test.sys--demo.money--v1"
	schemaStr := `{"type":"record","name":"Money","fields":[` +
		`{"name":"amount","type":{"type":"fixed","name":"Dec","size":16,` +
		`"logicalType":"decimal","precision":38,"scale":9}}]}`

	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	// 1234.560000000 at scale 9, unscaled and zero-padded to the fixed 16 bytes.
	amount := make([]byte, 16)
	big.NewInt(1234560000000).FillBytes(amount)
	require.NoError(t, batch.Write(Money{amount}))
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	var got []Money
	for r, err := range claimcheck.Records[Money](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}
	require.Len(t, got, 1)
	assert.Equal(t, amount, got[0].Amount)
}

func TestBatch_KeyAndHeadersPropagated(t *testing.T) {
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("v"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.keyed--v1",
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
	const topic = "test.sys--demo.rowgroups--v1"
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

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})
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
	const topic = "test.sys--demo.meta--v1"
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

	envelope := unmarshalEnvelope(t, kw.last.Value)
	bucket, key := bucketAndKey(t, envelope.StorageURI)
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

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.doubleclose--v1")
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

	batch, err := w.NewBatch(context.Background(), "test.sys--demo.noopabort--v1")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"v": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))
}

func TestBatch_RejectedRecordClosesTheBatch(t *testing.T) {
	// All its records or none of them. Without this, a caller that ignores the
	// Write error produces a batch that looks complete but is missing rows, and
	// nothing downstream can tell.
	const topic = "test.sys--demo.guardbuffer--v1"
	spy := &abortSpyS3{FakeS3Client: claimcheck.NewFakeS3Client()}
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(spy),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: guardSchema, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	rejected := batch.Write(map[string]any{"note": "rejected"})
	require.Error(t, rejected)

	// Every later call reports the same failure, and Produce uploads nothing.
	assert.Equal(t, rejected, batch.Write(map[string]any{"id": int32(2)}))

	err = batch.Produce(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, rejected, "Produce reports the record that closed the batch")

	assert.Nil(t, kw.last, "no envelope may reach Kafka")
	assert.Equal(t, 1, spy.aborts, "the upload must be aborted")
	assert.Empty(t, spy.Store, "no object may be left in the bucket")
}

// TestPayloadReader_ReadAtDoesNotMoveSequentialPosition verifies that
// ReadAt (used by parquet.OpenFile for the footer) does not disturb the
// sequential read position, satisfying the io.ReaderAt contract.
func TestPayloadReader_ReadAtDoesNotMoveSequentialPosition(t *testing.T) {
	type R struct {
		ID int32 `parquet:"id"`
	}
	const topic = "test.sys--demo.readat--v1"
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

	envelope := unmarshalEnvelope(t, kw.last.Value)
	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

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
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()
	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Produce(context.Background()))

	assert.Equal(t, "data-definitions", gotSystem, "write client must be built for the bucket owner, not the producer")
	assert.Equal(t, claimcheck.DefaultBucketResolver(topic), gotBucket)
	envelope := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "data-definitions", envelope.System, "stamp must match the write role")
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
	envelope := unmarshalEnvelope(t, kw.last.Value)
	assert.Equal(t, "billing", envelope.System)
}

func TestWriter_NonConventionalTopicErrors(t *testing.T) {
	const topic = "billing.test" // no name segment -> no derivable system

	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3FactoryForTest(func(string, string) (claimcheck.S3Writer, error) {
			t.Fatal("S3 writer factory must not be called when the system cannot be derived")
			return nil, nil
		}),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: simpleSchema("id"), version: 1, id: 1}),
	)

	_, err := w.NewBatch(context.Background(), topic)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot derive owning system")
}

// parquetKV extracts the Parquet key-value metadata footer as a map.
func parquetKV(f *parquet.File) map[string]string {
	m := make(map[string]string, len(f.Metadata().KeyValueMetadata))
	for _, e := range f.Metadata().KeyValueMetadata {
		m[e.Key] = e.Value
	}
	return m
}
