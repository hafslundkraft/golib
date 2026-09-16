package claimcheck_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// listSchema holds an array, which Parquet stores as a three-level LIST group.
const listSchema = `{"type":"record","name":"L","fields":[` +
	`{"name":"name","type":"string"},` +
	`{"name":"tags","type":{"type":"array","items":"string"}}]}`

// nestedListSchema puts the array one level down, so the reported path names
// the enclosing record too.
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

type listRowMisspelled struct {
	Name string   `parquet:"name"`
	Tag  []string `parquet:"tag,list"`
}

type nestedRow struct {
	Inner struct {
		Values []float64 `parquet:"values"`
	} `parquet:"inner"`
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

// scalarTagsSchema is the reverse of listSchema: one value where
// listRowUntagged expects many.
const scalarTagsSchema = `{"type":"record","name":"S","fields":[` +
	`{"name":"name","type":"string"},` +
	`{"name":"tags","type":"string"}]}`

// matrixSchema nests one LIST inside another, so the wrappers stack.
const matrixSchema = `{"type":"record","name":"M","fields":[` +
	`{"name":"matrix","type":{"type":"array","items":{"type":"array","items":"double"}}}]}`

// matrixRow cannot read matrixSchema: ",list" wraps only the outer slice, so
// the inner one asks for a column the payload does not have.
type matrixRow struct {
	Matrix [][]float64 `parquet:"matrix,list"`
}

// customerSchema keeps "customer" as a group, not a leaf.
const customerSchema = `{"type":"record","name":"C","fields":[` +
	`{"name":"customer","type":{"type":"record","name":"Cust","fields":[` +
	`{"name":"name","type":"string"},{"name":"id","type":"long"}]}}]}`

// flatCustomerSchema is the reverse: "customer" is a leaf.
const flatCustomerSchema = `{"type":"record","name":"F","fields":[` +
	`{"name":"customer","type":"string"}]}`

type customerRow struct {
	Customer struct {
		Name string `parquet:"name"`
		ID   int64  `parquet:"id"`
	} `parquet:"customer"`
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

// wrapperNamesSchema nests ordinary records named "list" and "element", the
// names Parquet gives a LIST group's own levels.
const wrapperNamesSchema = `{"type":"record","name":"W","fields":[` +
	`{"name":"outer","type":{"type":"record","name":"Outer","fields":[` +
	`{"name":"list","type":{"type":"record","name":"L","fields":[` +
	`{"name":"element","type":{"type":"record","name":"E","fields":[` +
	`{"name":"name","type":"string"}]}}]}}]}}]}`

type wrapperRow struct {
	Outer struct {
		List struct {
			Element struct {
				Name string `parquet:"name"`
			} `parquet:"element"`
		} `parquet:"list"`
	} `parquet:"outer"`
}

// groupRowAhead is a consumer ahead of groupListSchema: every field it adds is
// one the producer does not write yet, in each shape evolution can take.
type groupRowAhead struct {
	Groups []struct {
		Value string `parquet:"value"`
		Count int64  `parquet:"count"`
		// a new scalar and group inside a LIST element
		Quality *string `parquet:"quality,optional"`
		Source  struct {
			System *string `parquet:"system,optional"`
		} `parquet:"source"`
	} `parquet:"groups,list"`
	// a new scalar, group and list at the top level
	Tenant  *string `parquet:"tenant,optional"`
	Address struct {
		City *string `parquet:"city,optional"`
	} `parquet:"address"`
	Labels []string `parquet:"labels,list"`
}

// checkModel runs the model check against the payload schema built from avroSchema.
func checkModel(t *testing.T, avroSchema string, model any) error {
	t.Helper()

	schema, err := claimcheck.AvroSchemaToParquet(avroSchema)
	require.NoError(t, err)

	return claimcheck.CheckModelSchema(schema, model) //nolint:wrapcheck // test helper, wrapping adds no value
}

func TestCheckModelSchema(t *testing.T) {
	tests := []struct {
		name       string
		avroSchema string
		model      any
		// An empty wantErr means the model has to pass.
		wantErr string
		// wantMismatch is whether the error wraps [claimcheck.ErrSchemaMismatch],
		// which consumers match on. An unsupported model is a programming error
		// instead, and must not look like a schema mismatch.
		wantMismatch bool
	}{
		{
			name:       "matching_struct",
			avroSchema: listSchema,
			model:      listRow{},
		},
		{
			name:         "slice_without_list_tag",
			avroSchema:   listSchema,
			model:        listRowUntagged{},
			wantErr:      `Records[claimcheck_test.listRowUntagged] reads column "tags", but the payload stores it as "tags.list.element"`,
			wantMismatch: true,
		},
		{
			// "tag" is a prefix of the payload's "tags" without being a path
			// segment of it, so it stays a field the payload does not have.
			name:       "field_the_payload_does_not_have",
			avroSchema: listSchema,
			model:      listRowMisspelled{},
		},
		{
			name:         "nested_slice_names_the_full_path",
			avroSchema:   nestedListSchema,
			model:        nestedRow{},
			wantErr:      `Records[claimcheck_test.nestedRow] reads column "inner.values", but the payload stores it as "inner.values.list.element"`,
			wantMismatch: true,
		},
		{
			name:       "struct_slice_with_list_tag",
			avroSchema: groupListSchema,
			model:      groupRow{},
		},
		{
			// Unlike "tags" vs "tags.list.element", "groups.value" is no prefix of
			// "groups.list.element.value": the leaf sits past the wrapper.
			name:         "struct_slice_without_list_tag",
			avroSchema:   groupListSchema,
			model:        groupRowUntagged{},
			wantErr:      `Records[claimcheck_test.groupRowUntagged] reads column "groups.value", but the payload stores it as "groups.list.element.value"`,
			wantMismatch: true,
		},
		{
			// Both sides put "tags" at the same path, and parquet-go fills the
			// slice from the single value. Nothing lost, so nothing to reject.
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
			wantMismatch: true,
		},
		{
			// A missing ",list" tag in the other shape a payload can take: a
			// nested record read as a flat field. Without the check it reads as ""
			// on every row, indistinguishable from a customer with no name.
			name:       "consumer_flattened_a_group_into_a_value",
			avroSchema: customerSchema,
			model:      customerRowScalar{},
			// parquet-go sorts a group's fields, so the two paths come out
			// alphabetically rather than in customerSchema's order.
			wantErr: `Records[claimcheck_test.customerRowScalar] reads column "customer",` +
				` but the payload stores it as "customer.id" or "customer.name"`,
			wantMismatch: true,
		},
		{
			// The reverse: a nested model against a payload keeping the field flat.
			name:       "consumer_nested_a_value_into_a_group",
			avroSchema: flatCustomerSchema,
			model:      customerRow{},
			wantErr: `Records[claimcheck_test.customerRow] reads column "customer.name",` +
				` but the payload stores it as "customer"`,
			wantMismatch: true,
		},
		{
			name:       "records_named_like_list_wrappers",
			avroSchema: wrapperNamesSchema,
			model:      wrapperRow{},
		},
		{
			// parquet-go wraps a Go map in the same "key_value" group the payload
			// uses, so a map field needs no tag.
			name:       "map_needs_no_tag",
			avroSchema: mapSchema,
			model:      mapRow{},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkModel(t, tc.avroSchema, tc.model)

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, tc.wantMismatch, errors.Is(err, claimcheck.ErrSchemaMismatch),
				"errors.Is(err, ErrSchemaMismatch) should be %v for %q", tc.wantMismatch, err)
		})
	}
}

// producerAheadSchema is listSchema plus one field of every shape evolution can
// take: a scalar, a group and a list.
const producerAheadSchema = `{"type":"record","name":"L","fields":[` +
	`{"name":"name","type":"string"},` +
	`{"name":"tags","type":{"type":"array","items":"string"}},` +
	`{"name":"tenant","type":["null","string"]},` +
	`{"name":"address","type":{"type":"record","name":"A","fields":[` +
	`{"name":"city","type":["null","string"]}]}},` +
	`{"name":"labels","type":{"type":"array","items":"string"}}]}`

// producerDroppedListSchema is listSchema after the producer removed "tags".
const producerDroppedListSchema = `{"type":"record","name":"L","fields":[` +
	`{"name":"name","type":"string"}]}`

// producerDroppedFieldInGroupSchema is customerSchema after the producer removed
// "customer.id" but kept its sibling.
const producerDroppedFieldInGroupSchema = `{"type":"record","name":"C","fields":[` +
	`{"name":"customer","type":{"type":"record","name":"Cust","fields":[` +
	`{"name":"name","type":"string"}]}}]}`

// TestCheckModelSchema_SchemaEvolution pins down the version skews the check has
// to let through, so a consumer keeps reading a payload from a producer on
// another schema version. Added and removed fields pass in either direction,
// because the check only looks at columns the model asks for.
//
// A field whose type changed shape is absent: Avro has no promotion from a value
// to a record, so a compatibility-enforcing registry rejects that change before
// such a payload exists. TestCheckModelSchema covers it as a consumer mistake.
func TestCheckModelSchema_SchemaEvolution(t *testing.T) {
	tests := []struct {
		name       string
		avroSchema string
		model      any
	}{
		{
			// Projection: fields the consumer never asks for cannot mismatch.
			name:       "producer_added_fields_the_consumer_does_not_read",
			avroSchema: producerAheadSchema,
			model:      listRow{},
		},
		{
			// The mirror image: every field the consumer adds is absent from the
			// payload, and absent is not a mismatch.
			name:       "consumer_added_fields_the_producer_does_not_write",
			avroSchema: groupListSchema,
			model:      groupRowAhead{},
		},
		{
			// "tags" is gone, so the slice reads as nil rather than failing the
			// message.
			name:       "producer_removed_a_field_the_consumer_reads",
			avroSchema: producerDroppedListSchema,
			model:      listRow{},
		},
		{
			// "customer.id" is gone, "customer.name" stays. Sibling leaves are
			// different fields, so the survivor is no reshaped "customer.id".
			name:       "producer_removed_one_field_inside_a_group",
			avroSchema: producerDroppedFieldInGroupSchema,
			model:      customerRow{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.NoError(t, checkModel(t, tc.avroSchema, tc.model))
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

// onlyError reads msg as T and returns the single error the iteration yields.
// A rejected model has to end the iteration, so more than one yield is a bug
// even when the error itself is right.
func onlyError[T any](t *testing.T, msg *claimcheck.Message) error {
	t.Helper()

	var errs []error
	for _, err := range claimcheck.Records[T](context.Background(), msg) {
		errs = append(errs, err)
	}
	require.Len(t, errs, 1)
	return errs[0]
}

func TestRecords_RejectsSliceWithoutListTag(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})

	require.ErrorIs(t, onlyError[listRowUntagged](t, msg), claimcheck.ErrSchemaMismatch)
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
// through the payload's own schema, so the list comes back whole untagged.
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

// Both have to be caught before parquet-go builds its reader. Only the empty
// interface reads through the payload schema; a row decoded into a map cannot
// satisfy methods. A pointer this deep makes the reader panic, not error.
func TestRecords_RejectsUnsupportedModels(t *testing.T) {
	msg := newListMessage(t, listRow{Name: "a", Tags: []string{"x", "y"}})

	require.ErrorContains(t, onlyError[io.Reader](t, msg),
		"Records requires a struct with parquet field tags, got io.Reader")
	require.ErrorContains(t, onlyError[**listRow](t, msg),
		"Records requires a struct with parquet field tags, got **claimcheck_test.listRow")
}
