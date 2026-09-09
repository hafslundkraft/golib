package claimcheck

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	parquet "github.com/parquet-go/parquet-go"
)

// ErrSchemaMismatch reports a column T reads that the payload does not store
// under that path. parquet-go derives the reader's schema from T alone and never
// consults the file, so without this check the column reads as the zero value in
// silence. The error names the path T asked for and the paths the payload stores
// under the same top-level field; either side may be the wrong one.
//
// The common case is an array field missing its ",list" tag: Parquet stores an
// array under "field.list.element", an untagged Go slice asks for "field".
//
// Match it with errors.Is.
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

type schemaMismatchError struct {
	// want is the column path T reads, as "field.list.element".
	want string
	// have is the payload's paths under want's first segment; empty when the
	// payload has no such field.
	have []string
}

func (e *schemaMismatchError) Error() string {
	if len(e.have) == 0 {
		return fmt.Sprintf(
			"claimcheck: T reads column %q, which the payload does not have", e.want)
	}
	if len(e.have) == 1 {
		return fmt.Sprintf(
			"claimcheck: T reads column %q, but the payload stores it as %q",
			e.want, e.have[0])
	}
	return fmt.Sprintf(
		"claimcheck: T reads column %q, but the payload stores it under %v",
		e.want, e.have)
}

func (e *schemaMismatchError) Is(target error) bool { return target == ErrSchemaMismatch }

// checkModelSchema compares the payload's schema against the one parquet-go will
// derive from T. An interface T is read through the file's own schema: nothing to
// compare. Any other non-struct T, map[string]any in particular, has no derivable
// schema and makes parquet-go panic, so it is rejected here.
func checkModelSchema(file *parquet.Schema, model reflect.Type) error {
	if model.Kind() == reflect.Interface {
		return nil
	}
	for model.Kind() == reflect.Pointer {
		model = model.Elem()
	}
	if model.Kind() != reflect.Struct {
		return fmt.Errorf(
			"claimcheck: Records requires a struct with parquet field tags, got %s;"+
				" use msg.Payload for schema-driven access to the raw Parquet bytes",
			model)
	}
	return checkColumns(file, parquet.SchemaOf(reflect.Zero(model).Interface()))
}

// checkColumns requires every leaf column T reads to exist in the payload under
// the same path. Containment rather than equality, since a payload column T does
// not name is legal projection. Leaf paths suffice because a path carries the
// structure above it: a list's ".list.element", a map's ".key_value.value".
//
// Physical types are deliberately not compared: parquet-go converts between
// compatible widths and errors loudly when it cannot ("STRING to DOUBLE").
//
// parquet-go's own comparisons cannot stand in: [parquet.Convert] returns nil for
// every mismatch here, and [parquet.SameNodes] / [parquet.EqualNodes] reject
// legal projection.
func checkColumns(file, model *parquet.Schema) error {
	fileColumns := file.Columns()

	have := make(map[string]bool, len(fileColumns))
	for _, column := range fileColumns {
		have[strings.Join(column, ".")] = true
	}

	for _, column := range model.Columns() {
		path := strings.Join(column, ".")
		if have[path] {
			continue
		}
		return &schemaMismatchError{want: path, have: columnsUnder(fileColumns, column[0])}
	}
	return nil
}

// columnsUnder returns the payload's column paths below one top-level field, so
// the error can show where the payload keeps it.
func columnsUnder(columns [][]string, field string) []string {
	var under []string
	for _, column := range columns {
		if column[0] == field {
			under = append(under, strings.Join(column, "."))
		}
	}
	return under
}
