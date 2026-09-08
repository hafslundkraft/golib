package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// The array in listSchema is the shape Go struct tags get wrong: Parquet stores
// it as a three-level LIST group.
const listSchema = `{"type":"record","name":"L","fields":[` +
	`{"name":"name","type":"string"},` +
	`{"name":"tags","type":{"type":"array","items":"string"}}]}`

// nestedListSchema puts the array one level down, so the reported path has to
// name both the record and the field inside it.
const nestedListSchema = `{"type":"record","name":"N","fields":[` +
	`{"name":"inner","type":{"type":"record","name":"I","fields":[` +
	`{"name":"values","type":{"type":"array","items":"double"}}]}}]}`

type listRow struct {
	Name string   `parquet:"name"`
	Tags []string `parquet:"tags,list"`
}

type listRowUntagged struct {
	Name string   `parquet:"name"`
	Tags []string `parquet:"tags"`
}

type listRowSubset struct {
	Name string `parquet:"name"`
}

type listRowMisspelled struct {
	Name string   `parquet:"name"`
	Tag  []string `parquet:"tag,list"`
}

type nestedInner struct {
	Values []float64 `parquet:"values"`
}

type nestedRow struct {
	Inner nestedInner `parquet:"inner"`
}

func TestCheckModelSchema(t *testing.T) {
	tests := []struct {
		name       string
		avroSchema string
		model      any
		wantErr    string
	}{
		{
			name:       "matching_struct",
			avroSchema: listSchema,
			model:      listRow{},
		},
		{
			name:       "column_projection_reads_a_subset",
			avroSchema: listSchema,
			model:      listRowSubset{},
		},
		{
			name:       "slice_without_list_tag",
			avroSchema: listSchema,
			model:      listRowUntagged{},
			wantErr:    `field "tags" does not match the payload schema: the payload holds a list, T describes a repeated column`,
		},
		{
			name:       "field_the_payload_does_not_have",
			avroSchema: listSchema,
			model:      listRowMisspelled{},
			wantErr:    `field "tag" does not match the payload schema: the payload has no such field`,
		},
		{
			name:       "nested_slice_names_the_full_path",
			avroSchema: nestedListSchema,
			model:      nestedRow{},
			wantErr:    `field "inner.values" does not match the payload schema`,
		},
		{
			name:       "any_reads_through_the_payloads_own_schema",
			avroSchema: listSchema,
			model:      nil,
		},
		{
			name:       "pointer_to_a_matching_struct",
			avroSchema: listSchema,
			model:      &listRow{},
		},
		{
			name:       "map_is_not_a_supported_model",
			avroSchema: listSchema,
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

const listTopic = "test.sys--demo.list--v1"

// newListMessage writes one record through the real write path and returns a
// Message over the resulting payload.
func newListMessage(t *testing.T, record any) *claimcheck.Message {
	t.Helper()

	s3 := claimcheck.NewFakeS3Client()
	kw := &captureKW{}
	w := claimcheck.NewTestWriter(kw, &jsonSerializer{},
		claimcheck.WithWriterS3Client(s3),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: listSchema, version: 1, id: 1}),
	)

	batch, err := w.NewBatch(context.Background(), listTopic)
	require.NoError(t, err)
	t.Cleanup(batch.Cleanup)

	require.NoError(t, batch.Write(record))
	require.NoError(t, batch.Produce(context.Background()))

	envelope := unmarshalEnvelope(t, kw.last.Value)
	return claimcheck.NewMessage(listTopic, nil, kw.last.Value, nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})
}

func TestRecords_RejectsSliceWithoutListTag(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})

	var rows int
	for _, err := range claimcheck.Records[listRowUntagged](context.Background(), msg) {
		rows++
		require.ErrorIs(t, err, claimcheck.ErrSchemaMismatch)
	}
	assert.Equal(t, 1, rows, "the error must be yielded once and end the iteration")
}

func TestRecords_ReadsSliceWithListTag(t *testing.T) {
	input := listRow{Name: "a", Tags: []string{"x", "y"}}
	msg := newListMessage(t, input)

	var got []listRow
	for row, err := range claimcheck.Records[listRow](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, row)
	}

	require.Len(t, got, 1)
	assert.Equal(t, input, got[0])
}

func TestRecords_OnEmptyMessageYieldsNothing(t *testing.T) {
	msg := claimcheck.NewMessage(listTopic, nil, nil, nil, nil, nil)

	for range claimcheck.Records[listRowUntagged](context.Background(), msg) {
		t.Fatal("a tombstone must not reach the schema check")
	}
}
