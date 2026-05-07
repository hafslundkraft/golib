# Avro → Parquet type mapping

Documents how Avro schema types are converted to Parquet column types in both
`hafslund.happi.py` (Python) and `golib/kafkarator/claimcheck` (Go). The two
implementations produce identical on-disk Parquet files.

---

## Primitives

| Avro type | Python (PyArrow) | Go (parquet-go) | Parquet on disk |
|-----------|-----------------|-----------------|-----------------|
| `null` | `pa.null()` | `Optional(Leaf(ByteArrayType))` | optional BYTE_ARRAY |
| `boolean` | `pa.bool_()` | `Leaf(BooleanType)` | BOOLEAN |
| `int` | `pa.int32()` | `Leaf(Int32Type)` | INT32 |
| `long` | `pa.int64()` | `Leaf(Int64Type)` | INT64 |
| `float` | `pa.float32()` | `Leaf(FloatType)` | FLOAT |
| `double` | `pa.float64()` | `Leaf(DoubleType)` | DOUBLE |
| `bytes` | `pa.binary()` | `Leaf(ByteArrayType)` | BYTE_ARRAY |
| `string` | `pa.string()` | `String()` | BYTE_ARRAY (STRING) |

---

## Logical types

| Avro logicalType | Python (PyArrow) | Go (parquet-go) | Parquet on disk |
|------------------|-----------------|-----------------|-----------------|
| `timestamp-millis` | `pa.timestamp("ms", tz="UTC")` | `Timestamp(Millisecond)` | INT64 TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS) |
| `timestamp-micros` | `pa.timestamp("us", tz="UTC")` | `Timestamp(Microsecond)` | INT64 TIMESTAMP(isAdjustedToUTC=true,unit=MICROS) |
| `local-timestamp-millis` | `pa.timestamp("ms")` | `TimestampAdjusted(Millisecond, false)` | INT64 TIMESTAMP(isAdjustedToUTC=false,unit=MILLIS) |
| `local-timestamp-micros` | `pa.timestamp("us")` | `TimestampAdjusted(Microsecond, false)` | INT64 TIMESTAMP(isAdjustedToUTC=false,unit=MICROS) |
| `date` | `pa.date32()` | `Date()` | INT32 DATE |
| `time-millis` | `pa.time32("ms")` | `Time(Millisecond)` | INT32 TIME(isAdjustedToUTC=true,unit=MILLIS) |
| `time-micros` | `pa.time64("us")` | `Time(Microsecond)` | INT64 TIME(isAdjustedToUTC=true,unit=MICROS) |
| `uuid` | `pa.string()` | `UUID()` | BYTE_ARRAY (UUID) |

---

## Complex types

| Avro type | Python (PyArrow) | Go (parquet-go) | Parquet on disk |
|-----------|-----------------|-----------------|-----------------|
| `record` | `pa.struct([...])` | `Group{...}` (nested group) | required group |
| `array` | `pa.list_(item)` | `List(item)` | LIST (three-level encoding) |
| `map` | `pa.map_(pa.string(), value)` | `Map(String(), value)` | MAP group |
| `enum` | `pa.dictionary(pa.int32(), pa.string())` ¹ | `String()` | BYTE_ARRAY (STRING) |
| `fixed` | `pa.binary(size)` | `Leaf(FixedLenByteArrayType(size))` | FIXED_LEN_BYTE_ARRAY(size) |

> ¹ **Enum note:** Python uses `pa.dictionary(int32, string)` as the Arrow
> in-memory type (efficient for analytics). When written to Parquet the logical
> type is STRING, not ENUM. Go uses `String()` directly, producing the same
> on-disk result. The difference is Arrow-layer only and invisible to any Parquet
> reader.

---

## Unions

| Avro union | Python (PyArrow) | Go (parquet-go) | Parquet on disk |
|-----------|-----------------|-----------------|-----------------|
| `["null", X]` | nullable field of type X | `Optional(node for X)` | optional column of type X |
| `["null"]` | `pa.null()` | `Optional(Leaf(ByteArrayType))` | optional BYTE_ARRAY |
| multi-branch | `NotImplementedError` | `error` | — |

---

## Compression

Both libraries default to **Snappy** compression for all columns.

| | Default codec |
|--|--|
| Python (`pq.write_table`) | Snappy |
| Go (`parquet.GenericWriter` + `parquet.Compression(&parquet.Snappy)`) | Snappy |

Compression is transparent to readers — any Parquet library will decompress
automatically on read regardless of which library wrote the file.

---

## Footer metadata

Both libraries embed the following keys in the Parquet file footer to allow the
read path to reconstruct the schema without a Schema Registry call:

| Key | Value |
|-----|-------|
| `avro.schema` | Full Avro schema JSON string |
| `avro.schema.subject` | `{topic}-claim-check-payload` |
| `avro.schema.version` | Schema Registry version number |
| `avro.schema.id` | Schema Registry ID |

---

## Cross-library compatibility

A file written by either library can be read by the other. The on-disk Parquet
format is the contract; the Arrow intermediate layer used by Python is invisible
to the Go reader, and vice versa.
