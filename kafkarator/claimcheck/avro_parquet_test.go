package claimcheck_test

import (
	"strings"
	"testing"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

func avroRecord(fields ...string) string {
	return `{"type":"record","name":"R","namespace":"test","fields":[` +
		strings.Join(fields, ",") + `]}`
}

func avroField(name, typeJSON string) string {
	return `{"name":"` + name + `","type":` + typeJSON + `}`
}

func mustBuildSchema(t *testing.T, avroJSON string) *parquet.Schema {
	t.Helper()
	s, err := claimcheck.AvroSchemaToParquet(avroJSON)
	require.NoError(t, err)
	return s
}

func findField(t *testing.T, schema *parquet.Schema, name string) parquet.Field {
	t.Helper()
	for _, f := range schema.Fields() {
		if f.Name() == name {
			return f
		}
	}
	t.Fatalf("field %q not found in schema (fields: %v)", name, fieldNames(schema.Fields()))
	panic("unreachable")
}

func fieldNames(fields []parquet.Field) []string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name()
	}
	return names
}

func TestAvroParquet_TopLevelValidation(t *testing.T) {
	t.Run("non_record_type_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(`{"type":"string"}`)
		assert.Error(t, err)
	})

	t.Run("array_at_top_level_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(`{"type":"array","items":"string"}`)
		assert.Error(t, err)
	})

	t.Run("missing_type_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(`{}`)
		assert.Error(t, err)
	})

	t.Run("invalid_json_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(`not json`)
		assert.Error(t, err)
	})
}

func TestAvroParquet_Primitives(t *testing.T) {
	tests := []struct {
		avroType    string
		wantKind    parquet.Kind
		wantTypeStr string // empty = don't check
	}{
		{`"boolean"`, parquet.Boolean, ""},
		{`"int"`, parquet.Int32, ""},
		{`"long"`, parquet.Int64, ""},
		{`"float"`, parquet.Float, ""},
		{`"double"`, parquet.Double, ""},
		{`"bytes"`, parquet.ByteArray, "BYTE_ARRAY"},
		{`"string"`, parquet.ByteArray, "STRING"},
	}

	for _, tc := range tests {
		t.Run(tc.avroType, func(t *testing.T) {
			schema := mustBuildSchema(t, avroRecord(avroField("f", tc.avroType)))
			f := findField(t, schema, "f")
			assert.Equal(t, tc.wantKind, f.Type().Kind())
			if tc.wantTypeStr != "" {
				assert.Equal(t, tc.wantTypeStr, f.Type().String())
			}
		})
	}

	t.Run("unknown_primitive_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", `"integer"`)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "integer")
	})
}

func TestAvroParquet_LogicalTypes(t *testing.T) {
	tests := []struct {
		name        string
		avroType    string
		wantTypeStr string
	}{
		{
			"timestamp_millis",
			`{"type":"long","logicalType":"timestamp-millis"}`,
			"TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)",
		},
		{
			"timestamp_micros",
			`{"type":"long","logicalType":"timestamp-micros"}`,
			"TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)",
		},
		{
			"local_timestamp_millis",
			`{"type":"long","logicalType":"local-timestamp-millis"}`,
			"TIMESTAMP(isAdjustedToUTC=false,unit=MILLIS)",
		},
		{
			"local_timestamp_micros",
			`{"type":"long","logicalType":"local-timestamp-micros"}`,
			"TIMESTAMP(isAdjustedToUTC=false,unit=MICROS)",
		},
		{
			"date",
			`{"type":"int","logicalType":"date"}`,
			"DATE",
		},
		{
			"time_millis",
			`{"type":"int","logicalType":"time-millis"}`,
			"TIME(isAdjustedToUTC=true,unit=MILLIS)",
		},
		{
			"time_micros",
			`{"type":"long","logicalType":"time-micros"}`,
			"TIME(isAdjustedToUTC=true,unit=MICROS)",
		},
		{
			"uuid",
			`{"type":"string","logicalType":"uuid"}`,
			"UUID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := mustBuildSchema(t, avroRecord(avroField("f", tc.avroType)))
			f := findField(t, schema, "f")
			assert.Equal(t, tc.wantTypeStr, f.Type().String())
		})
	}

	t.Run("unknown_logical_type_returns_error", func(t *testing.T) {
		avroType := `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", avroType)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decimal")
	})

	t.Run("unknown_logical_type_name_in_error", func(t *testing.T) {
		avroType := `{"type":"int","logicalType":"custom-type"}`
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", avroType)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "custom-type")
	})
}

func TestAvroParquet_ComplexTypes(t *testing.T) {
	t.Run("nested_record", func(t *testing.T) {
		inner := `{"type":"record","name":"Inner","fields":[` +
			avroField("x", `"int"`) + "," +
			avroField("y", `"string"`) + `]}`
		schema := mustBuildSchema(t, avroRecord(avroField("nested", inner)))
		nested := findField(t, schema, "nested")
		require.False(t, nested.Leaf(), "nested record should not be a leaf")

		innerNames := fieldNames(nested.Fields())
		assert.Contains(t, innerNames, "x")
		assert.Contains(t, innerNames, "y")
	})

	t.Run("deeply_nested_record", func(t *testing.T) {
		leaf := `{"type":"record","name":"Leaf","fields":[` + avroField("v", `"double"`) + `]}`
		mid := `{"type":"record","name":"Mid","fields":[` + avroField("leaf", leaf) + `]}`
		schema := mustBuildSchema(t, avroRecord(avroField("mid", mid)))
		midField := findField(t, schema, "mid")
		require.False(t, midField.Leaf())
		var leafField parquet.Field
		for _, f := range midField.Fields() {
			if f.Name() == "leaf" {
				leafField = f
			}
		}
		require.NotNil(t, leafField, "leaf field not found inside mid")
		assert.Contains(t, fieldNames(leafField.Fields()), "v")
	})

	t.Run("array_of_strings", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(
			avroField("tags", `{"type":"array","items":"string"}`),
		))
		f := findField(t, schema, "tags")
		// parquet-go List() uses three-level encoding: the outer node is a
		// LIST group, not directly Repeated. Check the logical type string.
		assert.Equal(t, "LIST", f.Type().String())
	})

	t.Run("array_of_records", func(t *testing.T) {
		item := `{"type":"record","name":"Item","fields":[` + avroField("id", `"int"`) + `]}`
		schema := mustBuildSchema(t, avroRecord(
			avroField("items", `{"type":"array","items":`+item+`}`),
		))
		f := findField(t, schema, "items")
		assert.Equal(t, "LIST", f.Type().String())
	})

	t.Run("map_of_longs", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(
			avroField("counts", `{"type":"map","values":"long"}`),
		))
		f := findField(t, schema, "counts")
		// parquet MAP is encoded as a repeated group
		assert.False(t, f.Leaf(), "map should not be a leaf node")
	})

	t.Run("enum_maps_to_string", func(t *testing.T) {
		avroEnum := `{"type":"enum","name":"Colour","symbols":["RED","GREEN","BLUE"]}`
		schema := mustBuildSchema(t, avroRecord(avroField("colour", avroEnum)))
		f := findField(t, schema, "colour")
		assert.Equal(t, "STRING", f.Type().String())
	})

	t.Run("fixed", func(t *testing.T) {
		avroFixed := `{"type":"fixed","name":"MD5","size":16}`
		schema := mustBuildSchema(t, avroRecord(avroField("hash", avroFixed)))
		f := findField(t, schema, "hash")
		assert.Equal(t, parquet.FixedLenByteArray, f.Type().Kind())
		assert.Equal(t, 16, f.Type().Length())
	})

	t.Run("unknown_complex_type_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", `{"type":"frobnitz"}`)))
		assert.Error(t, err)
	})
}

func TestAvroParquet_Unions(t *testing.T) {
	t.Run("nullable_string", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(avroField("name", `["null","string"]`)))
		f := findField(t, schema, "name")
		assert.True(t, f.Optional(), "nullable field should be optional")
		assert.Equal(t, "STRING", f.Type().String())
	})

	t.Run("nullable_long", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(avroField("ts", `["null","long"]`)))
		f := findField(t, schema, "ts")
		assert.True(t, f.Optional())
		assert.Equal(t, parquet.Int64, f.Type().Kind())
	})

	t.Run("nullable_record", func(t *testing.T) {
		inner := `{"type":"record","name":"Inner","fields":[` + avroField("v", `"int"`) + `]}`
		schema := mustBuildSchema(t, avroRecord(avroField("obj", `["null",`+inner+`]`)))
		f := findField(t, schema, "obj")
		assert.True(t, f.Optional())
		assert.Contains(t, fieldNames(f.Fields()), "v")
	})

	t.Run("all_null_union", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(avroField("f", `["null"]`)))
		f := findField(t, schema, "f")
		assert.True(t, f.Optional())
	})

	t.Run("non_nullable_field_is_required", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(avroField("f", `"int"`)))
		f := findField(t, schema, "f")
		assert.False(t, f.Optional())
		assert.True(t, f.Required())
	})

	t.Run("multi_branch_union_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", `["null","string","int"]`)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "multi-branch")
	})

	t.Run("multi_branch_non_null_union_returns_error", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", `["string","int"]`)))
		assert.Error(t, err)
	})

	t.Run("multi_branch_union_error_includes_type", func(t *testing.T) {
		_, err := claimcheck.AvroSchemaToParquet(avroRecord(avroField("f", `["null","string","long"]`)))
		assert.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "multi-branch")
	})
}

func TestAvroParquet_FullSchema(t *testing.T) {
	t.Run("multiple_fields", func(t *testing.T) {
		schema := mustBuildSchema(t, avroRecord(
			avroField("id", `"long"`),
			avroField("name", `"string"`),
			avroField("value", `"double"`),
			avroField("active", `"boolean"`),
			avroField("created_at", `{"type":"long","logicalType":"timestamp-millis"}`),
		))
		assert.Equal(t, parquet.Int64, findField(t, schema, "id").Type().Kind())
		assert.Equal(t, "STRING", findField(t, schema, "name").Type().String())
		assert.Equal(t, parquet.Double, findField(t, schema, "value").Type().Kind())
		assert.Equal(t, parquet.Boolean, findField(t, schema, "active").Type().Kind())
		assert.Equal(t,
			"TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)",
			findField(t, schema, "created_at").Type().String(),
		)
	})

	t.Run("all_field_names_present", func(t *testing.T) {
		names := []string{"z", "a", "m", "b"}
		fields := make([]string, len(names))
		for i, n := range names {
			fields[i] = avroField(n, `"int"`)
		}
		schema := mustBuildSchema(t, avroRecord(fields...))
		got := fieldNames(schema.Fields())
		for _, name := range names {
			assert.Contains(t, got, name)
		}
	})
}
