package claimcheck

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	parquet "github.com/parquet-go/parquet-go"
)

// ErrRequiredField reports a record that omits a required schema field, or sets
// one to nil, at any depth. Such a record cannot be written as it stands:
// parquet-go writes a nil in a required column as the column's zero value, where
// "" and 0 are indistinguishable from real data, and a nil inside a required
// array corrupts the page it lands in, so the file reads back short a value and
// other Parquet readers reject it outright.
//
// Match it with errors.Is to tell a malformed record apart from an
// infrastructure failure: a batch poisoned by this error will fail the same way
// for every later record, so retrying it cannot help.
var ErrRequiredField = errors.New("claimcheck: required field")

// checkRequired applies the schema's compiled checks to one record, returning
// [ErrRequiredField] for the first breach it finds. Neither the Avro schema,
// which only builds the writer, nor parquet-go itself validates rows, so this is
// the one place to catch it.
func (b *Batch) checkRequired(record any) error {
	return checkMembers(record, b.checks)
}

// valueCheck is how one value is validated, wherever it sits — in a named field,
// as an array element, or under a map key.
type valueCheck struct {
	required bool
	// checker validates what lies below the value, and is nil when nothing below
	// it can breach the rule.
	checker valueChecker
}

// fieldCheck is a valueCheck for a named member of a record or struct.
type fieldCheck struct {
	name string
	valueCheck
}

// valueChecker validates the contents of one field's value. It returns
// *requiredFieldError, whose path the caller extends on the way out.
type valueChecker func(value any) error

// pathSegment is one step of a field path. A position is an array index or a map
// key, and renders as "[x]"; anything else is a field name joined with ".".
type pathSegment struct {
	position bool
	name     string
}

// requiredFieldError names the field that breached the rule. Segments are
// appended innermost first as the error travels out, so no caller has to thread
// a path down through the walk.
type requiredFieldError struct {
	segments []pathSegment
	missing  bool
}

func (e *requiredFieldError) Error() string {
	if e.missing {
		return fmt.Sprintf("claimcheck: record is missing required field %q", e.path())
	}
	return fmt.Sprintf("claimcheck: required field %q is nil", e.path())
}

// Is reports the error as [ErrRequiredField], so callers can match the class
// without the type being exported.
func (e *requiredFieldError) Is(target error) bool {
	return target == ErrRequiredField
}

func (e *requiredFieldError) push(segment pathSegment) {
	e.segments = append(e.segments, segment)
}

func (e *requiredFieldError) path() string {
	var b strings.Builder
	for i := len(e.segments) - 1; i >= 0; i-- {
		segment := e.segments[i]
		if segment.position {
			b.WriteByte('[')
			b.WriteString(segment.name)
			b.WriteByte(']')
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		b.WriteString(segment.name)
	}
	return b.String()
}

// extend adds segment to err's path when err is a requiredFieldError, and
// returns err either way.
func extend(err error, segment pathSegment) error {
	var breach *requiredFieldError
	if errors.As(err, &breach) {
		breach.push(segment)
	}
	return err
}

// compileSchema builds the checks for a schema's top-level fields, once per
// batch. Compiling beats deriving the checks per value, because reading a node's
// type and fields allocates.
func compileSchema(schema *parquet.Schema) []fieldCheck {
	return compileMembers(schema.Fields())
}

func compileMembers(fields []parquet.Field) []fieldCheck {
	members := make([]fieldCheck, 0, len(fields))
	for _, f := range fields {
		check := compileNode(f)
		if check == nil {
			continue
		}
		members = append(members, fieldCheck{name: f.Name(), valueCheck: *check})
	}
	if len(members) == 0 {
		return nil
	}
	return members
}

// compileNode reports how to check one node, or nil when nothing about it can go
// wrong at all — which holds when the node is optional and carries nothing
// required below it. Deciding that here, bottom-up, keeps the rule in one place.
func compileNode(node parquet.Node) *valueCheck {
	checker := compileContents(node)
	required := !node.Optional()
	if checker == nil && !required {
		return nil
	}
	return &valueCheck{required: required, checker: checker}
}

// compileContents builds a checker for what lies below node, or nil if nothing
// below it can breach the rule.
func compileContents(node parquet.Node) valueChecker {
	if node.Leaf() {
		return nil
	}
	if logical := node.Type().LogicalType(); logical != nil {
		switch {
		case logical.List != nil:
			return compileArray(node)
		case logical.Map != nil:
			return compileMap(node)
		}
	}
	members := compileMembers(node.Fields())
	if members == nil {
		return nil
	}
	return func(value any) error { return checkMembers(value, members) }
}

func compileArray(node parquet.Node) valueChecker {
	element := listElement(node)
	if element == nil {
		return nil
	}
	check := compileNode(element)
	if check == nil {
		return nil
	}
	return func(value any) error { return checkElements(value, *check) }
}

func compileMap(node parquet.Node) valueChecker {
	value := mapValue(node)
	if value == nil {
		return nil
	}
	check := compileNode(value)
	if check == nil {
		return nil
	}
	return func(v any) error { return checkMapValues(v, *check) }
}

// listElement returns the element node of a LIST group — the single field of its
// single repeated field — or nil if the group is not shaped that way, which
// disables checking below it. avro_parquet.go builds every list with
// parquet.List, so an unexpected shape means the schema did not come from there.
func listElement(node parquet.Node) parquet.Node {
	repeated := node.Fields()
	if len(repeated) != 1 {
		return nil
	}
	elements := repeated[0].Fields()
	if len(elements) != 1 {
		return nil
	}
	return elements[0]
}

// mapValue returns the value node of a MAP group — the second field of its
// single repeated key_value field — or nil if the group is not shaped that way,
// which disables checking below it, as in [listElement].
func mapValue(node parquet.Node) parquet.Node {
	repeated := node.Fields()
	if len(repeated) != 1 {
		return nil
	}
	pair := repeated[0].Fields()
	if len(pair) != 2 {
		return nil
	}
	return pair[1]
}

// checkMembers validates the named members of a map[string]any — a record, or a
// struct in the schema. Any other Go value is left alone, parquet-go reporting a
// shape that contradicts the schema better than this could. That holds at every
// depth, not only at the top level: a nested record supplied as a struct, or as
// a map of any type other than map[string]any, goes unchecked.
func checkMembers(value any, members []fieldCheck) error {
	record, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for _, member := range members {
		child, present := record[member.name]
		if !present {
			if member.required {
				return &requiredFieldError{
					segments: []pathSegment{{name: member.name}},
					missing:  true,
				}
			}
			continue
		}
		if isNil(child) {
			if member.required {
				return &requiredFieldError{segments: []pathSegment{{name: member.name}}}
			}
			continue
		}
		if member.checker == nil {
			continue
		}
		if err := member.checker(child); err != nil {
			return extend(err, pathSegment{name: member.name})
		}
	}
	return nil
}

// checkElements validates the elements of an array against element, the check
// compiled for the array's element node. A slice whose element type can never be
// nil, and which carries no check below it, is skipped without being walked.
func checkElements(value any, element valueCheck) error {
	items := reflect.ValueOf(value)
	if kind := items.Kind(); kind != reflect.Slice && kind != reflect.Array {
		return nil
	}
	if element.checker == nil && !canBeNil(items.Type().Elem()) {
		return nil
	}
	for i := range items.Len() {
		item := items.Index(i).Interface()
		if isNil(item) {
			if element.required {
				return &requiredFieldError{
					segments: []pathSegment{{position: true, name: strconv.Itoa(i)}},
				}
			}
			continue
		}
		if element.checker == nil {
			continue
		}
		if err := element.checker(item); err != nil {
			return extend(err, pathSegment{position: true, name: strconv.Itoa(i)})
		}
	}
	return nil
}

// checkMapValues validates the values of a map against entry, the check compiled
// for the map's value node, naming each value by its key.
//
// Map iteration order is randomized, so a map with more than one breach reports
// an arbitrary one of them rather than the first by any stable ordering.
func checkMapValues(value any, entry valueCheck) error {
	pairs := reflect.ValueOf(value)
	if pairs.Kind() != reflect.Map {
		return nil
	}
	if entry.checker == nil && !canBeNil(pairs.Type().Elem()) {
		return nil
	}
	for iter := pairs.MapRange(); iter.Next(); {
		item := iter.Value().Interface()
		if isNil(item) {
			if entry.required {
				return &requiredFieldError{
					segments: []pathSegment{{position: true, name: fmt.Sprint(iter.Key().Interface())}},
				}
			}
			continue
		}
		if entry.checker == nil {
			continue
		}
		if err := entry.checker(item); err != nil {
			return extend(err, pathSegment{position: true, name: fmt.Sprint(iter.Key().Interface())})
		}
	}
	return nil
}

// canBeNil reports whether a value of type t could ever satisfy isNil, so that a
// collection whose elements never can is skipped without being walked. Only an
// interface or a pointer qualifies: isNil counts a nil slice or map as the empty
// collection rather than as no value.
func canBeNil(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer:
		return true
	default:
		return false
	}
}

// isNil reports whether v carries no value. Only nil interfaces and nil
// pointers count: a nil slice or map is the empty collection, which is a legal
// value for a required bytes, array, or map field.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}
