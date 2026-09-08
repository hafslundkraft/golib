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
// consults the file, so a column the two place differently is read as the zero
// value in silence, in a row whose other fields are correct. The common case is
// an array field missing its ",list" tag: Parquet stores an array as a
// three-level LIST group under "field.list.element", an untagged Go slice asks
// for a single repeated column at "field", and the read yields an empty slice
// with no error.
//
// The error names the path T asked for and the paths the payload stores under
// the same top-level field, which is what has to change in T. Which side is
// wrong cannot be told apart from here: T may never have matched, or the
// payload's schema may have moved on since T was written.
//
// Match it with errors.Is.
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

type schemaMismatchError struct {
	// want is the column path T reads, as "field.list.element".
	want string
	// have is the payload's column paths under want's first segment, empty when
	// the payload has no such field at all.
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
// derive from T.
//
// An interface T is read through the file's own schema: nothing to compare. Any
// other non-struct T, map[string]any above all, has no derivable schema, and
// parquet-go panics on it deep inside the reader; that is reported as an error.
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
// the same path. Containment rather than equality: a payload column T does not
// name is column projection, which is legal, while a column only T has can never
// receive data.
//
// Comparing leaf paths is enough because a path carries the whole structure above
// it — a list's ".list.element", a map's ".key_value.value", a record's field
// names. Physical types are deliberately not compared: parquet-go converts
// between compatible widths, and errors loudly when it cannot ("STRING to
// DOUBLE"), so a width mismatch is not part of the silence this guards against.
//
// parquet-go's own comparisons cannot stand in for this. [parquet.Convert]
// returns a nil error for every mismatch here, filling a column it cannot map
// with nulls — that is exactly the silence to break. [parquet.SameNodes] and
// [parquet.EqualNodes] are false even for a T that reads correctly, because they
// require both schemas to name the same fields and so reject projection.
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
// the error can show where the payload actually keeps it.
func columnsUnder(columns [][]string, field string) []string {
	var under []string
	for _, column := range columns {
		if column[0] == field {
			under = append(under, strings.Join(column, "."))
		}
	}
	return under
}
