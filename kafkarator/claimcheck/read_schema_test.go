package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// The array in kundeSchema is the shape Go struct tags get wrong: Parquet stores
// it as a three-level LIST group.
const kundeSchema = `{"type":"record","name":"Kunde","fields":[` +
	`{"name":"navn","type":"string"},` +
	`{"name":"adresser","type":{"type":"array","items":"string"}}]}`

// nestedSchema puts the array one level down, so the reported path has to name
// both the record and the field inside it.
const nestedSchema = `{"type":"record","name":"Maaling","fields":[` +
	`{"name":"sensor","type":{"type":"record","name":"Sensor","fields":[` +
	`{"name":"verdier","type":{"type":"array","items":"double"}}]}}]}`

type kunde struct {
	Navn     string   `parquet:"navn"`
	Adresser []string `parquet:"adresser,list"`
}

type kundeUtenListTag struct {
	Navn     string   `parquet:"navn"`
	Adresser []string `parquet:"adresser"`
}

type kundeBareNavn struct {
	Navn string `parquet:"navn"`
}

type kundeMedSkrivefeil struct {
	Navn    string   `parquet:"navn"`
	Adreser []string `parquet:"adreser,list"`
}

type sensor struct {
	Verdier []float64 `parquet:"verdier"`
}

type maaling struct {
	Sensor sensor `parquet:"sensor"`
}

func TestCheckModelSchema(t *testing.T) {
	tests := []struct {
		name       string
		avroSchema string
		model      any
		wantErr    string
	}{
		{
			name:       "matching struct",
			avroSchema: kundeSchema,
			model:      kunde{},
		},
		{
			name:       "column projection reads a subset",
			avroSchema: kundeSchema,
			model:      kundeBareNavn{},
		},
		{
			name:       "slice without list tag",
			avroSchema: kundeSchema,
			model:      kundeUtenListTag{},
			wantErr:    `field "adresser" does not match the payload schema: the payload holds a list, T describes a repeated column`,
		},
		{
			name:       "field the payload does not have",
			avroSchema: kundeSchema,
			model:      kundeMedSkrivefeil{},
			wantErr:    `field "adreser" does not match the payload schema: the payload has no such field`,
		},
		{
			name:       "nested slice names the full path",
			avroSchema: nestedSchema,
			model:      maaling{},
			wantErr:    `field "sensor.verdier" does not match the payload schema`,
		},
		{
			name:       "any reads through the payload's own schema",
			avroSchema: kundeSchema,
			model:      nil,
		},
		{
			name:       "pointer to a matching struct",
			avroSchema: kundeSchema,
			model:      &kunde{},
		},
		{
			name:       "map is not a supported model",
			avroSchema: kundeSchema,
			model:      map[string]any{},
			wantErr:    "Records requires a struct with parquet field tags",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := claimcheck.AvroSchemaToParquet(tc.avroSchema)
			require.NoError(t, err)

			err = claimcheck.CheckModelSchema(schema, tc.model)

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// newKundeMessage writes one record through the real write path and returns a
// Message over the resulting payload.
func newKundeMessage(t *testing.T, record any) *claimcheck.Message {
	t.Helper()

	const topic = "test.sys--demo.kunder--v1"
	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: kundeSchema, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), topic)
	require.NoError(t, err)
	t.Cleanup(batch.Cleanup)

	require.NoError(t, batch.Write(record))
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	return claimcheck.NewMessage(topic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})
}

func TestRecordsRejectsSliceWithoutListTag(t *testing.T) {
	msg := newKundeMessage(t, kunde{Navn: "Kari", Adresser: []string{"Storgata 1", "Lilleveien 4"}})

	var rows int
	for row, err := range claimcheck.Records[kundeUtenListTag](context.Background(), msg) {
		rows++
		require.ErrorIs(t, err, claimcheck.ErrSchemaMismatch)
		assert.Empty(t, row.Adresser, "the mismatch this guards against is an empty slice returned as if it were data")
	}
	assert.Equal(t, 1, rows, "the error must be yielded once and end the iteration")
}

func TestRecordsReadsSliceWithListTag(t *testing.T) {
	input := kunde{Navn: "Kari", Adresser: []string{"Storgata 1", "Lilleveien 4"}}
	msg := newKundeMessage(t, input)

	var got []kunde
	for row, err := range claimcheck.Records[kunde](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, row)
	}

	require.Len(t, got, 1)
	assert.Equal(t, input, got[0])
}

func TestRecordsOnEmptyMessageYieldsNothing(t *testing.T) {
	msg := claimcheck.NewMessage("test.sys--demo.kunder--v1", nil, nil, nil, nil, nil)

	for range claimcheck.Records[kundeUtenListTag](context.Background(), msg) {
		t.Fatal("a tombstone must not reach the schema check")
	}
}
