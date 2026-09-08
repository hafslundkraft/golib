package claimcheck

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	parquet "github.com/parquet-go/parquet-go"
)

// ErrSchemaMismatch reports a disagreement in shape between T and the payload's
// own schema. parquet-go derives the reader's schema from T alone and never
// consults the file, so a column the two describe differently is read as the
// zero value in silence, in a row whose other fields are correct. The common
// case is an array field missing its ",list" tag: Parquet stores an array as a
// three-level LIST group, an untagged Go slice describes a single repeated
// column, and the mismatch yields an empty slice with no error.
//
// The error names the field and both shapes; which side is wrong cannot be told
// apart from here.
//
// Match it with errors.Is.
var ErrSchemaMismatch = errors.New("claimcheck: schema mismatch")

type schemaMismatchError struct {
	path   string
	reason string
}

func (e *schemaMismatchError) Error() string {
	return fmt.Sprintf("claimcheck: field %q does not match the payload schema: %s", e.path, e.reason)
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
	return compareSchemas(file, parquet.SchemaOf(reflect.Zero(model).Interface()))
}

// compareSchemas reports the first field on which the payload schema and the
// schema derived from T disagree in shape. Physical types are left to parquet-go,
// which converts between compatible widths; an optional column read into a
// non-pointer field loses nulls but is long-established usage.
//
// parquet-go's own comparisons cannot stand in for this walk. [parquet.Convert]
// returns a nil error for every mismatch below, filling a column it cannot map
// with nulls — that is the silence this package exists to break.
// [parquet.SameNodes] and [parquet.EqualNodes] are false even for a T that reads
// correctly, because they require both schemas to name the same fields and so
// reject column projection.
func compareSchemas(file, model *parquet.Schema) error {
	return compareGroups(file, model, nil)
}

// compareGroups compares two group nodes field by field, driven by T's fields: a
// payload field T does not name is column projection, which is legal, while a
// field only T has can never receive data.
func compareGroups(file, model parquet.Node, path []pathSegment) error {
	fileFields := make(map[string]parquet.Node, len(file.Fields()))
	for _, f := range file.Fields() {
		fileFields[f.Name()] = f
	}

	for _, m := range model.Fields() {
		path := descend(path, pathSegment{name: m.Name()})
		f, ok := fileFields[m.Name()]
		if !ok {
			return &schemaMismatchError{
				path:   renderPath(path),
				reason: "the payload has no such field",
			}
		}
		if err := compareNodes(f, m, path); err != nil {
			return err
		}
	}
	return nil
}

func compareNodes(file, model parquet.Node, path []pathSegment) error {
	fileShape, modelShape := shapeOf(file), shapeOf(model)
	if fileShape != modelShape {
		return &schemaMismatchError{
			path:   renderPath(path),
			reason: fmt.Sprintf("the payload holds %s, T describes %s", fileShape, modelShape),
		}
	}

	switch fileShape {
	case shapeList:
		return compareBelow(listElement(file), listElement(model), path)
	case shapeMap:
		return compareBelow(mapValue(file), mapValue(model), path)
	case shapeRecord:
		return compareGroups(file, model, path)
	}
	return nil
}

// compareBelow compares a list's element or a map's value, named "[]" in the
// field path. A nil node is a group not shaped the way its logical type promises,
// which stops the walk rather than guessing.
func compareBelow(file, model parquet.Node, path []pathSegment) error {
	if file == nil || model == nil {
		return nil
	}
	return compareNodes(file, model, descend(path, pathSegment{position: true}))
}

// descend extends a field path by one segment. Appending in place would have
// siblings overwrite each other's segment whenever the slice has spare capacity.
func descend(path []pathSegment, segment pathSegment) []pathSegment {
	return slices.Concat(path, []pathSegment{segment})
}

// shape is a node's structural kind — the part of a schema that must agree for a
// column to be found at all.
type shape int

const (
	shapeValue shape = iota
	shapeColumn
	shapeRecord
	shapeList
	shapeMap
)

func (s shape) String() string {
	switch s {
	case shapeValue:
		return "a value"
	case shapeColumn:
		return "a repeated column"
	case shapeRecord:
		return "a record"
	case shapeList:
		return "a list"
	case shapeMap:
		return "a map"
	default:
		return fmt.Sprintf("shape(%d)", int(s))
	}
}

func shapeOf(node parquet.Node) shape {
	if node.Leaf() {
		if node.Repeated() {
			return shapeColumn
		}
		return shapeValue
	}
	if logical := node.Type().LogicalType(); logical != nil {
		switch {
		case logical.List != nil:
			return shapeList
		case logical.Map != nil:
			return shapeMap
		}
	}
	return shapeRecord
}
