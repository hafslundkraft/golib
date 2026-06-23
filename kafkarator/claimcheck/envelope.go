package claimcheck

// Envelope is the small message placed on the Kafka topic. It carries enough
// information to locate and stream the payload from object storage without
// fetching the payload itself.
type Envelope struct {
	// BatchID is a UUID v4 that uniquely identifies this batch. Use it as a
	// correlation ID in logs.
	BatchID string `avro:"batch_id"`

	// StorageURI is the s3://bucket/key URI of the Parquet file in object
	// storage.
	StorageURI string `avro:"storage_uri"`

	// Topic is the Kafka topic this envelope was produced to.
	Topic string `avro:"topic"`

	// System is the Happi system name that produced this envelope. Because a
	// claim-check bucket is owned by the producing system, readers use System to
	// assume the producer's Ceph IAM role when fetching the payload. Legacy
	// envelopes written before this field existed decode with System == "", in
	// which case the reader falls back to its own system name.
	System string `avro:"system"`

	// RecordCount is the number of logical records in the Parquet file.
	RecordCount int64 `avro:"record_count"`

	// ByteSize is the total size of the Parquet file in bytes.
	ByteSize int64 `avro:"byte_size"`

	// CreatedAt is the UTC timestamp at which the batch was finalized,
	// expressed as milliseconds since the Unix epoch.
	CreatedAt int64 `avro:"created_at"`
}
