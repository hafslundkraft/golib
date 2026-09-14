package claimcheck

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	parquet "github.com/parquet-go/parquet-go"
)

// ErrSchemaMismatch means the row struct passed to [Records] and the payload
// keep the same field at different column paths. parquet-go builds the reader
// schema from the struct alone, so a field like that quietly reads as the zero
// value instead of failing.
//
// The usual cause is a slice field missing its ",list" tag. Parquet keeps a list
// at "field.list.element", but an untagged Go slice asks for "field". The error
// prints both paths, because either side could be the outdated one. Maps need no
// tag: parquet-go wraps a Go map in the same "key_value" group the payload uses.
//
// A struct field reading a column the payload keeps as a group is the same kind
// of mistake: "customer" against a payload storing "customer.name".
//
// If the payload has no sign of the column at all, that is not a mismatch. It is
// a field added to the schema after the payload was written, and reading it as
// the zero value is what makes schema evolution work.
//
// Match it with errors.Is.
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

type schemaMismatchError struct {
	// rowType is the name of the Go struct the reader schema was built from.
	rowType string
	// want is the column path the struct reads, such as "field.list.element".
	want string
	// have lists the payload paths that keep the same field in a different shape.
	have []string
}

func (e *schemaMismatchError) Error() string {
	quoted := make([]string, len(e.have))
	for i, path := range e.have {
		quoted[i] = strconv.Quote(path)
	}
	return fmt.Sprintf(
		"claimcheck: Records[%s] reads column %q, but the payload stores it as %s",
		e.rowType, e.want, strings.Join(quoted, " or "))
}

func (e *schemaMismatchError) Is(target error) bool { return target == ErrSchemaMismatch }

// checkModelSchema compares the payload schema with the schema parquet-go builds
// from model, the type [Records] was instantiated with. The empty interface is
// skipped, because Records[any] reads through the payload's own schema. Any
// other model is rejected unless it is a struct: parquet-go cannot build a
// schema for it, and a row decoded into a map cannot satisfy an interface that
// has methods.
func checkModelSchema(payload *parquet.Schema, model reflect.Type) error {
	if model.Kind() == reflect.Interface && model.NumMethod() == 0 {
		return nil
	}
	// parquet-go's generic reader takes a struct or a single pointer to one, and
	// panics on anything else, so a deeper pointer has to be rejected here.
	row := model
	if row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	if row.Kind() != reflect.Struct {
		return fmt.Errorf(
			"claimcheck: Records requires a struct with parquet field tags, got %s;"+
				" use Records[any] for schema-driven rows, or msg.Payload to access the raw Parquet bytes",
			model)
	}
	reader := parquet.SchemaOf(reflect.Zero(row).Interface())
	return checkColumns(payload, reader, model.String())
}

// checkColumns fails when the reader schema and the payload put the same field
// in a different shape. Two other cases are fine and pass: the payload keeps the
// column somewhere unrelated (the reader is reading a subset of the columns), or
// the payload does not have the column at all (it was written before the field
// existed). parquet-go itself checks that the physical types match.
//
// readerType is the name of the Go struct reader was built from, used in the
// error message.
func checkColumns(payload, reader *parquet.Schema, readerType string) error {
	payloadColumns := payload.Columns()

	have := make(map[string]bool, len(payloadColumns))
	columns := make([]payloadColumn, 0, len(payloadColumns))
	for _, column := range payloadColumns {
		path := strings.Join(column, ".")
		have[path] = true
		columns = append(columns, payloadColumn{path: path, field: withoutListWrappers(column)})
	}

	for _, column := range reader.Columns() {
		path := strings.Join(column, ".")
		if have[path] {
			continue
		}
		if paths := reshapedIn(columns, withoutListWrappers(column)); len(paths) > 0 {
			return &schemaMismatchError{rowType: readerType, want: path, have: paths}
		}
	}
	return nil
}

// payloadColumn is one leaf column of the payload, in the two shapes the
// comparison needs: the full path for the error message, and the path without
// LIST wrappers for matching.
type payloadColumn struct{ path, field string }

// reshapedIn returns the payload paths that keep the field a reader column
// reads, but in another shape. The names have to match once the LIST wrappers
// are gone, or one has to sit under the other: a reader asking for a scalar
// "customer" reads the same field as a payload storing "customer.name", it just
// disagrees about whether it is a leaf or a group.
func reshapedIn(payload []payloadColumn, field string) []string {
	var paths []string
	for _, column := range payload {
		if column.field == field || under(column.field, field) || under(field, column.field) {
			paths = append(paths, column.path)
		}
	}
	return paths
}

// under reports whether the field path inner is nested below outer. The dot
// keeps it to whole path segments, so "tag" does not count as under "tags".
func under(outer, inner string) bool { return strings.HasPrefix(inner, outer+".") }

// withoutListWrappers drops the "list.element" groups Parquet puts around a
// repeated field, so the same field compares equal whichever side carries the
// wrapper: "groups.list.element.value" and "groups.value" both reduce to
// "groups.value".
func withoutListWrappers(column []string) string {
	trimmed := make([]string, 0, len(column))
	for i := 0; i < len(column); i++ {
		if column[i] == "list" && i+1 < len(column) && column[i+1] == "element" {
			i++
			continue
		}
		trimmed = append(trimmed, column[i])
	}
	return strings.Join(trimmed, ".")
}
