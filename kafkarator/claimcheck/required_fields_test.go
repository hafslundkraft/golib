package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// guardSchema has one required and one nullable field.
const guardSchema = `{"type":"record","name":"G","fields":[` +
	`{"name":"id","type":"int"},` +
	`{"name":"note","type":["null","string"]}]}`

// newGuardBatch opens a batch over guardSchema. The caller must Cleanup.
func newGuardBatch(t *testing.T) *claimcheck.Batch {
	t.Helper()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(claimcheck.NewFakeS3Client()),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: guardSchema, version: 1, id: 1}),
	)
	batch, err := w.NewBatch(context.Background(), "test.sys--demo.guard--v1")
	require.NoError(t, err)
	return batch
}

func TestBatch_WriteRejectsMissingRequiredField(t *testing.T) {
	batch := newGuardBatch(t)
	defer batch.Cleanup()

	err := batch.Write(map[string]any{"note": "no id"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `missing required field "id"`)
}

func TestBatch_WriteRejectsNilRequiredField(t *testing.T) {
	// Key presence alone is not enough: parquet-go writes a nil required field as
	// the column's zero value, and a typed nil pointer the same. One batch per
	// case — the first rejection closes the batch.
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"untyped_nil", nil},
		{"typed_nil_pointer", (*int32)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := newGuardBatch(t)
			defer batch.Cleanup()

			assert.Error(t, batch.Write(map[string]any{"id": tc.value, "note": "x"}))
		})
	}
}

func TestBatch_WriteAllowsMissingNullableField(t *testing.T) {
	// Only non-optional fields are required; an omitted nullable field is null.
	batch := newGuardBatch(t)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(map[string]any{"id": int32(1)}))
	require.NoError(t, batch.Write(map[string]any{"id": int32(2), "note": nil}))
}

func TestBatch_WriteDoesNotCheckStructs(t *testing.T) {
	// A struct always carries every field, so there is nothing to check — and
	// its zero value is not distinguishable from a deliberate one.
	type G struct {
		ID   int32   `parquet:"id"`
		Note *string `parquet:"note,optional"`
	}
	batch := newGuardBatch(t)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(G{}))
}

// nestedGuardSchema exercises every position a required field can sit in: under
// an array element, under a map value, and under a nullable record. Each record
// type is named only once — golib does not yet resolve named type references,
// so a reused name cannot be written as one.
const nestedGuardSchema = `{"type":"record","name":"R","fields":[` +
	`{"name":"id","type":"string"},` +
	`{"name":"groups","type":{"type":"array","items":{"type":"record","name":"G","fields":[` +
	`{"name":"unit","type":"string"},` +
	`{"name":"values","type":{"type":"array","items":"double"}}]}}},` +
	`{"name":"byName","type":{"type":"map","values":{"type":"record","name":"M","fields":[` +
	`{"name":"unit","type":"string"}]}}},` +
	`{"name":"optGroup","type":["null",{"type":"record","name":"O","fields":[` +
	`{"name":"unit","type":"string"}]}]},` +
	`{"name":"optValues","type":["null",{"type":"array","items":["null","double"]}]}]}`

// newNestedBatch opens a batch over nestedGuardSchema. The caller must Cleanup.
func newNestedBatch(t *testing.T) *claimcheck.Batch {
	t.Helper()
	w := claimcheck.NewTestWriter(&captureKW{}, &jsonSerializer{},
		claimcheck.WithWriterS3Client(claimcheck.NewFakeS3Client()),
		claimcheck.WithWriterSchemaFetcher(&fakeSchemaFetcher{schemaStr: nestedGuardSchema, version: 1, id: 1}),
	)
	batch, err := w.NewBatch(context.Background(), "test.sys--demo.nested--v1")
	require.NoError(t, err)
	return batch
}

// nestedGroup returns a valid element of the groups array.
func nestedGroup() map[string]any {
	return map[string]any{"unit": "MW", "values": []any{1.0, 2.0}}
}

// nestedRecord returns a valid record, with overrides applied.
func nestedRecord(overrides map[string]any) map[string]any {
	record := map[string]any{
		"id":     "a",
		"groups": []any{nestedGroup()},
		"byName": map[string]any{"k": map[string]any{"unit": "MW"}},
	}
	for name, value := range overrides {
		record[name] = value
	}
	return record
}

func TestBatch_WriteAcceptsValidNestedRecord(t *testing.T) {
	batch := newNestedBatch(t)
	defer batch.Cleanup()

	require.NoError(t, batch.Write(nestedRecord(nil)))
}

func TestBatch_WriteRejectsNestedRequiredField(t *testing.T) {
	// One batch per case: the first rejection closes the batch.
	for _, tc := range []struct {
		name    string
		record  map[string]any
		wantErr string
	}{
		{
			name:    "nil_array_element",
			record:  nestedRecord(map[string]any{"groups": []any{nil}}),
			wantErr: `required field "groups[0]" is nil`,
		},
		{
			name: "nil_leaf_in_array_element",
			record: nestedRecord(map[string]any{
				"groups": []any{map[string]any{"unit": nil, "values": []any{1.0}}},
			}),
			wantErr: `required field "groups[0].unit" is nil`,
		},
		{
			name: "missing_leaf_in_array_element",
			record: nestedRecord(map[string]any{
				"groups": []any{map[string]any{"values": []any{1.0}}},
			}),
			wantErr: `missing required field "groups[0].unit"`,
		},
		{
			name: "nil_array_in_array_element",
			record: nestedRecord(map[string]any{
				"groups": []any{map[string]any{"unit": "MW", "values": nil}},
			}),
			wantErr: `required field "groups[0].values" is nil`,
		},
		{
			name: "nil_among_leaf_elements",
			record: nestedRecord(map[string]any{
				"groups": []any{map[string]any{"unit": "MW", "values": []any{1.0, nil}}},
			}),
			wantErr: `required field "groups[0].values[1]" is nil`,
		},
		{
			// A nil map where a record belongs holds no keys, so its required
			// members read as absent rather than the map reading as nil.
			name:    "nil_map_as_array_element",
			record:  nestedRecord(map[string]any{"groups": []map[string]any{nil}}),
			wantErr: `missing required field "groups[0].unit"`,
		},
		{
			name:    "nil_map_value",
			record:  nestedRecord(map[string]any{"byName": map[string]any{"k": nil}}),
			wantErr: `required field "byName[k]" is nil`,
		},
		{
			name: "nil_leaf_under_map_key",
			record: nestedRecord(map[string]any{"byName": map[string]any{
				"first":  map[string]any{"unit": "MW"},
				"second": map[string]any{"unit": nil},
			}}),
			wantErr: `required field "byName[second].unit" is nil`,
		},
		{
			name: "nil_leaf_in_present_nullable_record",
			record: nestedRecord(map[string]any{
				"optGroup": map[string]any{"unit": nil},
			}),
			wantErr: `required field "optGroup.unit" is nil`,
		},
		{
			name:    "second_element_is_the_one_named",
			record:  nestedRecord(map[string]any{"groups": []any{nestedGroup(), nestedGroup(), nil}}),
			wantErr: `required field "groups[2]" is nil`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := newNestedBatch(t)
			defer batch.Cleanup()

			err := batch.Write(tc.record)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestBatch_WriteAcceptsNestedValuesThatAreLegal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record map[string]any
	}{
		{"nil_nullable_record", nestedRecord(map[string]any{"optGroup": nil})},
		{"nil_nullable_array", nestedRecord(map[string]any{"optValues": nil})},
		{"nil_element_in_nullable_array", nestedRecord(map[string]any{"optValues": []any{1.0, nil}})},
		{"empty_required_array", nestedRecord(map[string]any{"groups": []any{}})},
		{"empty_required_map", nestedRecord(map[string]any{"byName": map[string]any{}})},
		// A nil slice is Go's empty collection, so a required array reads as [].
		{"nil_slice_for_required_array", nestedRecord(map[string]any{"groups": []any(nil)})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := newNestedBatch(t)
			defer batch.Cleanup()

			require.NoError(t, batch.Write(tc.record))
		})
	}
}
