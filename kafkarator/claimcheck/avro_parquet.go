package claimcheck

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"

	parquet "github.com/parquet-go/parquet-go"
)

// AvroSchemaToParquet converts an Avro record schema JSON string into a
// parquet-go Schema. The resulting schema drives both the BatchWriter (write
// path) and the Resolver (read path).
//
// Supported Avro constructs:
//
//	Primitives : boolean, int, long, float, double, bytes, string
//	Logical    : timestamp-millis/micros, local-timestamp-millis/micros,
//	             date, time-millis/micros, uuid (→ UTF-8 string),
//	             decimal (bytes- or fixed-backed)
//	record     : → nested Group
//	array      : → parquet.List
//	map        : → parquet.Map (string keys)
//	enum       : → UTF-8 string
//	fixed      : → FIXED_LEN_BYTE_ARRAY(size)
//	["null", X]: → Optional X
//
// Multi-branch unions (other than ["null", X]) and unknown logicalTypes
// return an error.
func avroSchemaToParquet(avroSchemaStr string) (*parquet.Schema, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(avroSchemaStr), &raw); err != nil {
		return nil, fmt.Errorf("claimcheck: parse avro schema: %w", err)
	}

	group, err := avroRecordToGroup(raw)
	if err != nil {
		return nil, err
	}

	name, _ := raw["name"].(string)
	if name == "" {
		name = "Record"
	}

	return parquet.NewSchema(name, group), nil
}

// avroRecordToGroup converts an Avro record schema dict to a parquet.Group.
func avroRecordToGroup(schema map[string]any) (parquet.Group, error) {
	if schema["type"] != "record" {
		return nil, fmt.Errorf("claimcheck: avro schema must be type \"record\", got %q", schema["type"])
	}

	rawFields, ok := schema["fields"]
	if !ok {
		return nil, fmt.Errorf("claimcheck: avro record schema is missing \"fields\"")
	}
	fields, ok := rawFields.([]any)
	if !ok {
		return nil, fmt.Errorf("claimcheck: avro \"fields\" must be an array")
	}

	group := parquet.Group{}
	for _, rawField := range fields {
		field, ok := rawField.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("claimcheck: avro field must be an object, got %T", rawField)
		}
		name, ok := field["name"].(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("claimcheck: avro field missing or empty \"name\"")
		}
		node, err := avroTypeToNode(field["type"])
		if err != nil {
			return nil, fmt.Errorf("claimcheck: field %q: %w", name, err)
		}
		group[name] = node
	}

	return group, nil
}

// avroTypeToNode dispatches on the Avro type expression (string, []any, or
// map[string]any) and returns the corresponding parquet Node.
func avroTypeToNode(avroType any) (parquet.Node, error) {
	switch t := avroType.(type) {
	case string:
		return avroPrimitiveToNode(t)
	case []any:
		return avroUnionToNode(t)
	case map[string]any:
		return avroComplexToNode(t)
	default:
		return nil, fmt.Errorf("unsupported avro type expression: %T %v", avroType, avroType)
	}
}

// avroPrimitiveToNode maps a plain Avro primitive string to a parquet Node.
func avroPrimitiveToNode(typeName string) (parquet.Node, error) {
	switch typeName {
	case "null":
		// A bare "null" type — represent as an optional byte array that is
		// always nil. Callers are expected to wrap it in Optional via a union.
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), nil
	case "boolean":
		return parquet.Leaf(parquet.BooleanType), nil
	case "int":
		return parquet.Leaf(parquet.Int32Type), nil
	case "long":
		return parquet.Leaf(parquet.Int64Type), nil
	case "float":
		return parquet.Leaf(parquet.FloatType), nil
	case "double":
		return parquet.Leaf(parquet.DoubleType), nil
	case "bytes":
		return parquet.Leaf(parquet.ByteArrayType), nil
	case "string":
		return parquet.String(), nil
	default:
		return nil, fmt.Errorf("unsupported avro primitive type: %q", typeName)
	}
}

// avroUnionToNode handles Avro union types. Only ["null", X] unions (nullable
// fields) are supported; multi-branch unions return an error.
func avroUnionToNode(union []any) (parquet.Node, error) {
	nonNull := make([]any, 0, len(union))
	for _, t := range union {
		if t != "null" {
			nonNull = append(nonNull, t)
		}
	}
	if len(nonNull) == 0 {
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), nil
	}
	if len(nonNull) > 1 {
		return nil, fmt.Errorf("multi-branch unions are not supported: %v", union)
	}
	inner, err := avroTypeToNode(nonNull[0])
	if err != nil {
		return nil, err
	}
	return parquet.Optional(inner), nil
}

// avroComplexToNode handles Avro complex type dicts (record, array, map, enum,
// fixed) and logical type annotations.
func avroComplexToNode(schema map[string]any) (parquet.Node, error) {
	if logical, _ := schema["logicalType"].(string); logical != "" {
		return avroLogicalToNode(logical, schema)
	}

	baseType, _ := schema["type"].(string)
	switch baseType {
	case "record":
		return avroRecordToGroup(schema)

	case "array":
		itemNode, err := avroTypeToNode(schema["items"])
		if err != nil {
			return nil, fmt.Errorf("array items: %w", err)
		}
		return parquet.List(itemNode), nil

	case "map":
		valueNode, err := avroTypeToNode(schema["values"])
		if err != nil {
			return nil, fmt.Errorf("map values: %w", err)
		}
		return parquet.Map(parquet.String(), valueNode), nil

	case "enum":
		// Avro enums are serialized as their symbol string.
		return parquet.String(), nil

	case "fixed":
		// A size of 0 divides by zero deep inside the parquet writer, and a
		// negative one trips an out-of-range panic there; both must fail here.
		size, ok := avroIntAttr(schema, "size")
		if !ok || size < 1 {
			return nil, fmt.Errorf(
				"avro fixed type \"size\" must be a positive integer, got %v", schema["size"])
		}
		return parquet.Leaf(parquet.FixedLenByteArrayType(size)), nil

	default:
		// Could be a primitive wrapped in a {"type": "string"} dict.
		inner, err := avroPrimitiveToNode(baseType)
		if err != nil {
			return nil, fmt.Errorf("unsupported avro complex type: %v", schema)
		}
		return inner, nil
	}
}

// avroLogicalToNode maps Avro logicalType annotations to parquet Nodes. schema
// is the annotated type dict, which decimal needs for its precision and scale.
func avroLogicalToNode(logical string, schema map[string]any) (parquet.Node, error) {
	switch logical {
	case "decimal":
		return avroDecimalToNode(schema)
	case "timestamp-millis":
		return parquet.Timestamp(parquet.Millisecond), nil
	case "timestamp-micros":
		return parquet.Timestamp(parquet.Microsecond), nil
	case "local-timestamp-millis":
		// No UTC adjustment — use TimestampAdjusted with isAdjustedToUTC=false.
		return parquet.TimestampAdjusted(parquet.Millisecond, false), nil
	case "local-timestamp-micros":
		return parquet.TimestampAdjusted(parquet.Microsecond, false), nil
	case "date":
		return parquet.Date(), nil
	case "time-millis":
		return parquet.Time(parquet.Millisecond), nil
	case "time-micros":
		return parquet.Time(parquet.Microsecond), nil
	case "uuid":
		return parquet.String(), nil
	default:
		return nil, fmt.Errorf("unsupported avro logicalType: %q", logical)
	}
}

// maxDecimalFixedSize bounds the fixed width a decimal may declare. 256 bytes
// allows a precision of ~616 digits, far past any real decimal (Spark caps at
// 38, Arrow at 76), and keeps the precision check below from doing unbounded
// bignum work on a malformed schema. Plain Avro "fixed" is not capped — it
// holds arbitrary byte blobs, where a large width is legitimate.
const maxDecimalFixedSize = 256

// avroDecimalToNode maps an Avro decimal annotation to a parquet Decimal node.
// Avro backs a decimal with either "bytes" (variable length) or "fixed" (size
// bytes), and both hold the unscaled value as a big-endian two's-complement
// integer — the same encoding Parquet uses, so the bytes pass through as-is.
func avroDecimalToNode(schema map[string]any) (parquet.Node, error) {
	precision, ok := avroIntAttr(schema, "precision")
	if !ok {
		return nil, fmt.Errorf("avro decimal missing or invalid \"precision\"")
	}
	if precision < 1 {
		return nil, fmt.Errorf("avro decimal \"precision\" must be >= 1, got %d", precision)
	}

	// Avro treats scale as optional and defaults it to 0.
	scale := 0
	if _, present := schema["scale"]; present {
		if scale, ok = avroIntAttr(schema, "scale"); !ok {
			return nil, fmt.Errorf("avro decimal has invalid \"scale\"")
		}
	}
	if scale < 0 || scale > precision {
		return nil, fmt.Errorf(
			"avro decimal \"scale\" must be between 0 and precision (%d), got %d", precision, scale)
	}

	switch baseType, _ := schema["type"].(string); baseType {
	case "bytes":
		return parquet.Decimal(scale, precision, parquet.ByteArrayType), nil
	case "fixed":
		size, ok := avroIntAttr(schema, "size")
		if !ok || size < 1 || size > maxDecimalFixedSize {
			return nil, fmt.Errorf(
				"avro fixed decimal \"size\" must be an integer between 1 and %d, got %v",
				maxDecimalFixedSize, schema["size"])
		}
		if maxPrecision := maxFixedPrecision(size); precision > maxPrecision {
			return nil, fmt.Errorf(
				"avro decimal \"precision\" %d does not fit in %d fixed bytes (max %d)",
				precision, size, maxPrecision)
		}
		return parquet.Decimal(scale, precision, parquet.FixedLenByteArrayType(size)), nil
	default:
		return nil, fmt.Errorf("avro decimal must be backed by \"bytes\" or \"fixed\", got %q", baseType)
	}
}

// avroIntAttr reads an integer Avro schema attribute. JSON numbers decode as
// float64, so every integer attribute arrives that way; a fractional value, or
// one beyond int32, is not a valid Avro integer and must not be truncated into
// one silently.
func avroIntAttr(schema map[string]any, name string) (int, bool) {
	v, ok := schema[name].(float64)
	if !ok || v != math.Trunc(v) || v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int(v), true
}

// maxFixedPrecision returns the largest decimal precision that size bytes of
// big-endian two's-complement integer can hold: 4 bytes give 9 digits, 16 give
// 38. The largest value in size bytes is 2^(8*size-1)-1, which is never all
// nines, so the limit is always one digit fewer than that value has.
func maxFixedPrecision(size int) int {
	maxVal := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(8*size-1)), nil)
	maxVal.Sub(maxVal, big.NewInt(1))
	return len(maxVal.String()) - 1
}
