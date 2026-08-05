package claimcheck_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// These tests exercise the full write path against the full read path: an Avro
// schema is converted to a Parquet schema, records are written through Batch,
// uploaded to the in-process S3 fake, and read back through the same
// claimcheck.Records iterator a consumer uses. Where avro_parquet_test.go
// asserts the shape of the converted schema, these assert that values survive
// the round trip.

// wideSchema covers every Avro construct avroSchemaToParquet claims to support
// except fixed and uuid, which are covered separately below.
const wideSchema = `{"type":"record","name":"Wide","fields":[
{"name":"b","type":"boolean"},
{"name":"i","type":"int"},
{"name":"l","type":"long"},
{"name":"f","type":"float"},
{"name":"d","type":"double"},
{"name":"by","type":"bytes"},
{"name":"s","type":"string"},
{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
{"name":"dt","type":{"type":"int","logicalType":"date"}},
{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
{"name":"opt","type":["null","string"]},
{"name":"arr","type":{"type":"array","items":"string"}},
{"name":"mp","type":{"type":"map","values":"long"}},
{"name":"en","type":{"type":"enum","name":"E","symbols":["A","B"]}},
{"name":"rec","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`

type innerRow struct {
	X int32 `parquet:"x"`
}

type wideRow struct {
	B   bool             `parquet:"b"`
	I   int32            `parquet:"i"`
	L   int64            `parquet:"l"`
	F   float32          `parquet:"f"`
	D   float64          `parquet:"d"`
	By  []byte           `parquet:"by"`
	S   string           `parquet:"s"`
	Ts  int64            `parquet:"ts,timestamp"`
	Dt  int32            `parquet:"dt,date"`
	Tm  int32            `parquet:"tm,time(millisecond)"`
	Opt *string          `parquet:"opt,optional"`
	Arr []string         `parquet:"arr,list"`
	Mp  map[string]int64 `parquet:"mp"`
	En  string           `parquet:"en"`
	Rec innerRow         `parquet:"rec"`
}

// roundTrip writes records through a batch and reads them back as T, exercising
// the same path a producer and consumer pair takes.
func roundTrip[T any](t *testing.T, topic, schemaStr string, records ...any) []T {
	t.Helper()
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()

	for _, r := range records {
		require.NoError(t, batch.Write(r))
	}
	require.NoError(t, batch.Produce(context.Background()))

	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3,
		&fakeEnvelopeDeserializer{envelope: unmarshalEnvelope(t, kw.last.Value)})

	var got []T
	for row, err := range claimcheck.Records[T](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, row)
	}
	return got
}

func TestE2E_EveryAvroTypeSurvivesRoundTrip(t *testing.T) {
	opt := "present"
	record := map[string]any{
		"b":   true,
		"i":   int32(-7),
		"l":   int64(1) << 40,
		"f":   float32(1.5),
		"d":   2.25,
		"by":  []byte{0xde, 0xad},
		"s":   "hei",
		"ts":  time.UnixMilli(1700000000123).UTC(),
		"dt":  int32(20000),
		"tm":  int32(3600000),
		"opt": &opt,
		"arr": []string{"a", "b"},
		"mp":  map[string]int64{"k": 9},
		"en":  "B",
		"rec": map[string]any{"x": int32(42)},
	}

	got := roundTrip[wideRow](t, "test.sys--demo.wide--v1", wideSchema, record)

	require.Len(t, got, 1)
	row := got[0]
	assert.True(t, row.B)
	assert.Equal(t, int32(-7), row.I)
	assert.Equal(t, int64(1)<<40, row.L)
	assert.Equal(t, float32(1.5), row.F)
	assert.InDelta(t, 2.25, row.D, 0)
	assert.Equal(t, []byte{0xde, 0xad}, row.By)
	assert.Equal(t, "hei", row.S)
	assert.Equal(t, int64(1700000000123), row.Ts, "timestamp-millis is stored as epoch millis")
	assert.Equal(t, int32(20000), row.Dt)
	assert.Equal(t, int32(3600000), row.Tm)
	require.NotNil(t, row.Opt)
	assert.Equal(t, "present", *row.Opt)
	assert.Equal(t, []string{"a", "b"}, row.Arr)
	assert.Equal(t, map[string]int64{"k": 9}, row.Mp)
	assert.Equal(t, "B", row.En, "enums round trip as their symbol string")
	assert.Equal(t, int32(42), row.Rec.X)
}

func TestE2E_OmittedNullableFieldReadsBackAsNil(t *testing.T) {
	// The required-field guard permits omitting a nullable field; it must then
	// arrive as null rather than as the column's zero value, so a consumer can
	// tell "not set" from "set to empty".
	const schemaStr = `{"type":"record","name":"N","fields":[
	{"name":"id","type":"int"},
	{"name":"note","type":["null","string"]}]}`
	type row struct {
		ID   int32   `parquet:"id"`
		Note *string `parquet:"note,optional"`
	}

	got := roundTrip[row](t, "test.sys--demo.nullable--v1", schemaStr,
		map[string]any{"id": int32(1)},
		map[string]any{"id": int32(2), "note": nil},
		map[string]any{"id": int32(3), "note": "set"},
	)

	require.Len(t, got, 3)
	assert.Nil(t, got[0].Note, "omitted")
	assert.Nil(t, got[1].Note, "explicit nil")
	require.NotNil(t, got[2].Note)
	assert.Equal(t, "set", *got[2].Note)
}

func TestE2E_StructAndMapRecordsProduceIdenticalRows(t *testing.T) {
	// Both documented record shapes must agree — a struct with parquet tags and
	// a map keyed by field name.
	const schemaStr = `{"type":"record","name":"S","fields":[
	{"name":"id","type":"int"},
	{"name":"name","type":"string"},
	{"name":"note","type":["null","string"]}]}`
	type row struct {
		ID   int32   `parquet:"id"`
		Name string  `parquet:"name"`
		Note *string `parquet:"note,optional"`
	}
	note := "n"

	got := roundTrip[row](t, "test.sys--demo.shapes--v1", schemaStr,
		row{ID: 1, Name: "alice", Note: &note},
		map[string]any{"id": int32(1), "name": "alice", "note": &note},
	)

	require.Len(t, got, 2)
	assert.Equal(t, got[0], got[1])
}

func TestE2E_ValuesSurviveRowGroupBoundaries(t *testing.T) {
	// TestBatch_FlushesMultipleRowGroups asserts the row count across groups;
	// this asserts the values themselves, including per-row nullability.
	const schemaStr = `{"type":"record","name":"RG","fields":[
	{"name":"id","type":"int"},
	{"name":"note","type":["null","string"]}]}`
	type row struct {
		ID   int32   `parquet:"id"`
		Note *string `parquet:"note,optional"`
	}

	const rows = 7
	records := make([]any, 0, rows)
	for i := range rows {
		r := map[string]any{"id": int32(i)}
		if i%2 == 0 {
			note := "even"
			r["note"] = &note
		}
		records = append(records, r)
	}

	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: schemaStr, version: 1, id: 1}),
		claimcheck.WithWriterRowGroupSize(2),
	)
	const topic = "test.sys--demo.rgvalues--v1"
	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	defer batch.Cleanup()
	for _, r := range records {
		require.NoError(t, batch.Write(r))
	}
	require.NoError(t, batch.Produce(context.Background()))

	msg := claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3,
		&fakeEnvelopeDeserializer{envelope: unmarshalEnvelope(t, kw.last.Value)})

	var got []row
	for r, err := range claimcheck.Records[row](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, r)
	}

	require.Len(t, got, rows)
	for i, r := range got {
		assert.Equal(t, int32(i), r.ID, "rows keep their order across row groups")
		if i%2 == 0 {
			require.NotNil(t, r.Note, "row %d", i)
			assert.Equal(t, "even", *r.Note)
		} else {
			assert.Nil(t, r.Note, "row %d", i)
		}
	}
}

// fixedSchema uses the two Avro constructs that map to
// FIXED_LEN_BYTE_ARRAY: uuid and fixed.
const fixedSchema = `{"type":"record","name":"Fx","fields":[
{"name":"u","type":{"type":"string","logicalType":"uuid"}},
{"name":"fx","type":{"type":"fixed","name":"F4","size":4}}]}`

type fixedRow struct {
	U  [16]byte `parquet:"u,uuid"`
	Fx [4]byte  `parquet:"fx"`
}

func TestE2E_FixedAndUUIDFieldsRoundTrip(t *testing.T) {
	id := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	// Byte-array columns must be fed as a slice from a map, or via a pointer to
	// a struct — see TestE2E_FixedFieldRejectsStructValues.
	got := roundTrip[fixedRow](t, "test.sys--demo.fixed--v1", fixedSchema,
		map[string]any{"u": id[:], "fx": []byte{1, 2, 3, 4}},
		&fixedRow{U: [16]byte(id), Fx: [4]byte{1, 2, 3, 4}},
	)

	require.Len(t, got, 2)
	for i, row := range got {
		assert.Equal(t, id, uuid.UUID(row.U), "record %d", i)
		assert.Equal(t, [4]byte{1, 2, 3, 4}, row.Fx, "record %d", i)
	}
}

func TestE2E_FixedFieldRejectsStructValues(t *testing.T) {
	// Known limitation: Batch.Write boxes the record in an `any` and the writer
	// is a GenericWriter[any], so a struct passed by value has unaddressable
	// array fields. parquet-go panics on those; flushRowGroup turns it into an
	// error. Pass &record, or a map with a []byte, instead.
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(claimcheck.NewFakeS3Client()),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: fixedSchema, version: 1, id: 1}),
	)
	batch, err := w.NewBatch(context.Background(), "test.sys--demo.fixedvalue--v1")
	require.NoError(t, err)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(fixedRow{Fx: [4]byte{1, 2, 3, 4}}))

	err = batch.Produce(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unaddressable byte array")
}
