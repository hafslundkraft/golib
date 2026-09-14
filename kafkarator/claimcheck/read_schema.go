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
// keep the same field at different column paths, so the field would quietly read
// as the zero value. Match it with errors.Is.
//
// The usual cause is a slice field missing its ",list" tag: Parquet keeps a list
// at "field.list.element", but an untagged Go slice asks for "field". Maps need
// no tag. A struct field reading a column the payload keeps as a group is the
// same kind of mistake: "customer" against a payload storing "customer.name".
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

// schemaMismatchError prints both column paths, because either side could be
// the outdated one.
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

func (e *schemaMismatchError) Unwrap() error { return ErrSchemaMismatch }

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
	payloadColumns := leafColumns(payload)

	have := make(map[string]bool, len(payloadColumns))
	for _, column := range payloadColumns {
		have[column.path] = true
	}

	for _, want := range leafColumns(reader) {
		if have[want.path] {
			continue
		}
		if paths := reshapedIn(payloadColumns, want.field); len(paths) > 0 {
			return &schemaMismatchError{rowType: readerType, want: want.path, have: paths}
		}
	}
	return nil
}

// leafColumn is one leaf column of a schema, in the two shapes the comparison
// needs: the full path for the error message, and the path without LIST wrappers
// for matching.
type leafColumn struct{ path, field string }

// reshapedIn returns the payload paths that keep the field a reader column
// reads, but in another shape. The names have to match once the LIST wrappers
// are gone, or one has to sit under the other: a reader asking for a scalar
// "customer" reads the same field as a payload storing "customer.name", it just
// disagrees about whether it is a leaf or a group.
func reshapedIn(payload []leafColumn, field string) []string {
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

// leafColumns walks the schema and returns one entry per leaf column. The field
// name drops the "list.element" levels Parquet puts around a repeated field, so
// the same field compares equal whichever side carries the wrapper:
// "groups.list.element.value" and "groups.value" both reduce to "groups.value".
//
// The wrappers are found through the LIST annotation on the group, not through
// the "list" and "element" names, which are also valid names for ordinary
// fields.
func leafColumns(schema *parquet.Schema) []leafColumn {
	var columns []leafColumn

	var walk func(node parquet.Node, path, field string)
	walk = func(node parquet.Node, path, field string) {
		if node.Leaf() {
			columns = append(columns, leafColumn{path: path, field: field})
			return
		}
		if isListGroup(node) {
			// A LIST group holds one repeated level holding one element. Both
			// levels belong in the column path, neither in the field name.
			for _, list := range node.Fields() {
				for _, element := range list.Fields() {
					walk(element, dotted(dotted(path, list.Name()), element.Name()), field)
				}
			}
			return
		}
		for _, child := range node.Fields() {
			walk(child, dotted(path, child.Name()), dotted(field, child.Name()))
		}
	}

	walk(schema, "", "")
	return columns
}

func isListGroup(node parquet.Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

func dotted(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
