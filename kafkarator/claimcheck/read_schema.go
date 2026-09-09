package claimcheck

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	parquet "github.com/parquet-go/parquet-go"
)

// ErrSchemaMismatch means T reads a column the payload does not have at that
// path. parquet-go builds the reader's schema from T, never from the file.
// Without this check, the column reads as the zero value with no error.
//
// The usual cause is a slice field missing its ",list" tag. Parquet stores a list
// under "field.list.element"; an untagged Go slice asks for "field". The error
// shows both paths, since either side can be the outdated one.
//
// Match it with errors.Is.
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

type schemaMismatchError struct {
	// want is the column path T reads, such as "field.list.element".
	want string
	// have holds the payload's paths under want's first segment. Empty if the
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

// checkModelSchema compares the payload's schema with the one parquet-go builds
// from T. An interface T is skipped: it reads through the file's own schema, so
// there is nothing to compare. Other non-struct types, map[string]any above all,
// have no schema to build, and parquet-go panics on them instead of returning an
// error. They are rejected here.
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

// checkColumns requires every column T reads to exist in the payload at the same
// path. Containment, not equality: a payload column T leaves out is legal column
// projection. Leaf paths are enough to compare, since a leaf path spells out the
// structure above it ("tags.list.element" for a list of strings).
//
// Physical types are left to parquet-go, which converts compatible widths and
// errors clearly when it cannot convert.
//
// Neither [parquet.Convert] (zero-fills missing columns) nor [parquet.SameNodes]
// (requires equal field counts) can be used here.
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

// columnsUnder returns the payload's column paths under one top-level field. The
// error uses them to show where the payload stores that field.
func columnsUnder(columns [][]string, field string) []string {
	var under []string
	for _, column := range columns {
		if column[0] == field {
			under = append(under, strings.Join(column, "."))
		}
	}
	return under
}
