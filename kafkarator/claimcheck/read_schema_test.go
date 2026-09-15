package claimcheck_test

import (
	"context"
	"io"
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

// groupListSchema is an array of records, so the leaf sits past the LIST wrapper.
const groupListSchema = `{"type":"record","name":"G","fields":[` +
	`{"name":"groups","type":{"type":"array","items":{"type":"record","name":"Group","fields":[` +
	`{"name":"value","type":"string"},` +
	`{"name":"count","type":"long"}]}}}]}`

type group struct {
	Value string `parquet:"value"`
	Count int64  `parquet:"count"`
}

type groupRow struct {
	Groups []group `parquet:"groups,list"`
}

type groupRowUntagged struct {
	Groups []group `parquet:"groups"`
}

// scalarTagsSchema is the reverse of listSchema: the producer stores one value
// where the consumer in listRowUntagged expects many.
const scalarTagsSchema = `{"type":"record","name":"S","fields":[` +
	`{"name":"name","type":"string"},` +
	`{"name":"tags","type":"string"}]}`

// matrixSchema nests one LIST inside another, so the wrappers stack.
const matrixSchema = `{"type":"record","name":"M","fields":[` +
	`{"name":"matrix","type":{"type":"array","items":{"type":"array","items":"double"}}}]}`

// matrixRow cannot read matrixSchema: a ",list" tag only wraps the outermost
// slice, so the inner one asks for a column the payload does not have.
type matrixRow struct {
	Matrix [][]float64 `parquet:"matrix,list"`
}

// customerSchema has a plain nested record: the payload keeps "customer" as a
// group, not as a leaf.
const customerSchema = `{"type":"record","name":"C","fields":[` +
	`{"name":"customer","type":{"type":"record","name":"Cust","fields":[` +
	`{"name":"name","type":"string"},{"name":"id","type":"long"}]}}]}`

// flatCustomerSchema is the reverse: "customer" is a leaf.
const flatCustomerSchema = `{"type":"record","name":"F","fields":[` +
	`{"name":"customer","type":"string"}]}`

type customerInner struct {
	Name string `parquet:"name"`
	ID   int64  `parquet:"id"`
}

type customerRow struct {
	Customer customerInner `parquet:"customer"`
}

// customerRowScalar reads the group as a leaf, so nothing it asks for exists.
type customerRowScalar struct {
	Customer string `parquet:"customer"`
}

const mapSchema = `{"type":"record","name":"MP","fields":[` +
	`{"name":"attrs","type":{"type":"map","values":"string"}}]}`

type mapRow struct {
	Attrs map[string]string `parquet:"attrs"`
}

// wrapperNamesSchema nests ordinary records that happen to be named "list" and
// "element", the names Parquet gives the levels of a LIST group.
const wrapperNamesSchema = `{"type":"record","name":"W","fields":[` +
	`{"name":"outer","type":{"type":"record","name":"Outer","fields":[` +
	`{"name":"list","type":{"type":"record","name":"L","fields":[` +
	`{"name":"element","type":{"type":"record","name":"E","fields":[` +
	`{"name":"name","type":"string"}]}}]}}]}}]}`

type wrapperElement struct {
	Name string `parquet:"name"`
}

type wrapperList struct {
	Element wrapperElement `parquet:"element"`
}

type wrapperOuter struct {
	List wrapperList `parquet:"list"`
}

type wrapperRow struct {
	Outer wrapperOuter `parquet:"outer"`
}

// groupRowAhead is a consumer on a newer schema version than the payload: every
// field it adds is one the producer has not started writing yet, in each of the
// shapes evolution can take.
type groupAhead struct {
	Value string `parquet:"value"`
	Count int64  `parquet:"count"`
	// new scalar inside the element of a LIST
	Quality *string `parquet:"quality,optional"`
	// new group inside the element of a LIST
	Source groupSource `parquet:"source"`
}

type groupSource struct {
	System *string `parquet:"system,optional"`
}

type groupRowAhead struct {
	Groups []groupAhead `parquet:"groups,list"`
	// new scalar, new group and new list at the top level
	Tenant  *string      `parquet:"tenant,optional"`
	Address groupAddress `parquet:"address"`
	Labels  []string     `parquet:"labels,list"`
}

type groupAddress struct {
	City *string `parquet:"city,optional"`
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
			wantErr:    `Records[claimcheck_test.listRowUntagged] reads column "tags", but the payload stores it as "tags.list.element"`,
		},
		{
			// Indistinguishable from a field the producer has not added yet, so
			// it reads as the zero value rather than failing the whole message.
			name:       "field_the_payload_does_not_have",
			avroSchema: listSchema,
			model:      listRowMisspelled{},
		},
		{
			name:       "nested_slice_names_the_full_path",
			avroSchema: nestedListSchema,
			model:      nestedRow{},
			wantErr:    `Records[claimcheck_test.nestedRow] reads column "inner.values", but the payload stores it as "inner.values.list.element"`,
		},
		{
			name:       "struct_slice_with_list_tag",
			avroSchema: groupListSchema,
			model:      groupRow{},
		},
		{
			// "groups.value" is not a prefix of "groups.list.element.value" the
			// way "tags" is of "tags.list.element": the leaf sits past the wrapper.
			name:       "struct_slice_without_list_tag",
			avroSchema: groupListSchema,
			model:      groupRowUntagged{},
			wantErr:    `Records[claimcheck_test.groupRowUntagged] reads column "groups.value", but the payload stores it as "groups.list.element.value"`,
		},
		{
			// Both sides put "tags" at the same path, and parquet-go fills the
			// slice with the single value. Nothing is lost, so nothing to reject.
			name:       "slice_where_the_payload_stores_a_scalar",
			avroSchema: scalarTagsSchema,
			model:      listRowUntagged{},
		},
		{
			name:       "list_inside_a_list",
			avroSchema: matrixSchema,
			model:      matrixRow{},
			wantErr: `Records[claimcheck_test.matrixRow] reads column "matrix.list.element",` +
				` but the payload stores it as "matrix.list.element.list.element"`,
		},
		{
			// Neither side shares a leaf path, so only the ancestor tells this
			// apart from a field the producer has not added yet.
			name:       "scalar_where_the_payload_stores_a_group",
			avroSchema: customerSchema,
			model:      customerRowScalar{},
			wantErr: `Records[claimcheck_test.customerRowScalar] reads column "customer",` +
				` but the payload stores it as "customer.id" or "customer.name"`,
		},
		{
			name:       "group_where_the_payload_stores_a_scalar",
			avroSchema: flatCustomerSchema,
			model:      customerRow{},
			wantErr: `Records[claimcheck_test.customerRow] reads column "customer.name",` +
				` but the payload stores it as "customer"`,
		},
		{
			name:       "records_named_like_list_wrappers",
			avroSchema: wrapperNamesSchema,
			model:      wrapperRow{},
		},
		{
			// parquet-go wraps a Go map in the same "key_value" group the payload
			// uses, so a map field needs no tag of its own.
			name:       "map_needs_no_tag",
			avroSchema: mapSchema,
			model:      mapRow{},
		},
		{
			// Schema evolution in every shape at once. None of these fields has
			// any column in the payload, so none of them is a reshaped field.
			name:       "consumer_ahead_of_the_producer",
			avroSchema: groupListSchema,
			model:      groupRowAhead{},
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
			// parquet-go reads into a struct or a single pointer to one, and
			// panics on a deeper pointer.
			name:       "pointer_to_a_pointer_is_not_a_supported_model",
			avroSchema: listSchema,
			model:      new(*listRow),
			wantErr:    "Records requires a struct with parquet field tags, got **claimcheck_test.listRow",
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

// Records[any] is the escape hatch the schema-mismatch error points at: it reads
// through the payload's own schema, so the list comes back whole without a
// ",list" tag.
func TestRecords_AnyReadsThroughThePayloadSchema(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})

	var got []any
	for row, err := range claimcheck.Records[any](context.Background(), msg) {
		require.NoError(t, err)
		got = append(got, row)
	}

	require.Len(t, got, 1)
	assert.Equal(t, map[string]any{
		"name": "a",
		"tags": []any{"x", "y"},
	}, got[0])
}

// Only the empty interface reads through the payload schema. An interface with
// methods gets the model-type error, not a decode failure further down: a row
// decoded into a map cannot satisfy it.
func TestRecords_RejectsInterfaceWithMethods(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})

	var rows int
	for _, err := range claimcheck.Records[io.Reader](context.Background(), msg) {
		rows++
		require.ErrorContains(t, err, "Records requires a struct with parquet field tags, got io.Reader")
	}
	assert.Equal(t, 1, rows, "the error must be yielded once and end the iteration")
}

// The model check has to run before parquet-go builds its reader: the reader
// panics on a pointer this deep rather than returning an error.
func TestRecords_RejectsDoublePointerWithoutPanicking(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})
	wantErr := "Records requires a struct with parquet field tags, got **claimcheck_test.listRow"

	var rows int
	for _, err := range claimcheck.Records[**listRow](context.Background(), msg) {
		rows++
		require.ErrorContains(t, err, wantErr)
	}
	assert.Equal(t, 1, rows, "the error must be yielded once and end the iteration")
}
