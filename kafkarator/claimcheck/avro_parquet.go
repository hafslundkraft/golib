package claimcheck

import (
	"encoding/json"
	"fmt"

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
//	             date, time-millis/micros, uuid
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
		return avroLogicalToNode(logical)
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
		size, ok := schema["size"].(float64)
		if !ok {
			return nil, fmt.Errorf("avro fixed type missing or invalid \"size\"")
		}
		return parquet.Leaf(parquet.FixedLenByteArrayType(int(size))), nil

	default:
		// Could be a primitive wrapped in a {"type": "string"} dict.
		inner, err := avroPrimitiveToNode(baseType)
		if err != nil {
			return nil, fmt.Errorf("unsupported avro complex type: %v", schema)
		}
		return inner, nil
	}
}

// avroLogicalToNode maps Avro logicalType annotations to parquet Nodes.
func avroLogicalToNode(logical string) (parquet.Node, error) {
	switch logical {
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
		return parquet.UUID(), nil
	default:
		return nil, fmt.Errorf("unsupported avro logicalType: %q", logical)
	}
}
