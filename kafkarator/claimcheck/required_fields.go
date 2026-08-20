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
// one to nil, at any depth. parquet-go writes such a record without complaint,
// producing a structurally valid file with the right row and element counts, in
// which the nil holds whatever the write buffer happened to carry: the column's
// zero value, indistinguishable from real data, or a value from an earlier record.
//
// Match it with errors.Is: a batch poisoned by this error fails the same way for
// every later record, so retrying cannot help.
var ErrRequiredField = errors.New("claimcheck: required field")

// checkRequired applies the schema's compiled checks to one record.
func (b *Batch) checkRequired(record any) error {
	return checkMembers(record, b.checks)
}

// valueCheck is what must hold for one value, in a named field, an array
// element, or under a map key.
type valueCheck struct {
	required bool
	// checker validates what lies below the value, and is nil when nothing below
	// it can be missing or nil.
	checker valueChecker
}

type fieldCheck struct {
	name string
	valueCheck
}

// valueChecker validates one value's contents, returning *requiredFieldError,
// whose path the caller extends on the way out.
type valueChecker func(value any) error

// pathSegment is one step of a field path: a position — an array index or a map
// key — rendering as "[x]", or a field name, joined with ".".
type pathSegment struct {
	position bool
	name     string
}

// requiredFieldError names the field that was missing or nil. Segments are
// appended innermost first, as the error travels out.
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

func extend(err error, segment pathSegment) error {
	var breach *requiredFieldError
	if errors.As(err, &breach) {
		breach.push(segment)
	}
	return err
}

// compileSchema builds the checks for a schema's top-level fields, once per
// batch: reading a node's type and fields allocates.
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

// compileNode reports how to check one node, or nil when the node is optional
// and carries nothing required below it.
func compileNode(node parquet.Node) *valueCheck {
	checker := compileContents(node)
	required := !node.Optional()
	if checker == nil && !required {
		return nil
	}
	return &valueCheck{required: required, checker: checker}
}

// compileContents builds a checker for what lies below node, or nil if nothing
// below it can be missing or nil.
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
// single repeated field — or nil, disabling checking below it, if the group is
// not shaped that way.
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
// single repeated key_value field — or nil, disabling checking below it, if the
// group is not shaped that way.
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

// checkMembers validates the named members of a map[string]any, at any depth.
// Any other Go value is left alone, including a record supplied as a struct.
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
// compiled for the array's element node.
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
// for the map's value node, naming each value by its key. Map iteration order is
// randomized, so a map with more than one breach reports an arbitrary one.
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

// canBeNil reports whether a value of type t could ever satisfy isNil.
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
