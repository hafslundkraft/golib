package claimcheck

import (
	"context"
	"io"
)

// CompletedPart holds the part number and ETag returned by UploadPart,
// required to finalise a multipart upload.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// S3Writer is the write-side object-storage interface required by the claim-check
// write path. Satisfied by FakeS3Client for testing.
type S3Writer interface {
	// CreateMultipartUpload initiates a multipart upload and returns the upload ID.
	CreateMultipartUpload(ctx context.Context, bucket, key string) (uploadID string, err error)

	// UploadPart uploads one part of a multipart upload. The caller is
	// responsible for reading body exactly once; the implementation must not
	// buffer the entire body.
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (etag string, err error)

	// CompleteMultipartUpload assembles all previously uploaded parts into the
	// final object.
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) error

	// AbortMultipartUpload discards all uploaded parts. Implementations must
	// not return an error if the upload ID is unknown (best-effort clean-up).
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// PutObject stores body as a single object. Used for small payloads that
	// do not require multipart upload.
	PutObject(ctx context.Context, bucket, key string, body io.Reader) error

	// DeleteObject deletes the object at key from bucket. Implementations must
	// not return an error if the object does not exist.
	DeleteObject(ctx context.Context, bucket, key string) error
}

// S3Reader is the read-side object-storage interface required by the claim-check
// read path. Satisfied by FakeS3Client for testing.
type S3Reader interface {
	// GetObject returns the object body and its total size in bytes. If
	// byteRange is non-nil it must be a valid HTTP Range value (e.g.
	// "bytes=1024-"); the returned size is always the full object size
	// regardless of the range.
	GetObject(ctx context.Context, bucket, key string, byteRange *string) (body io.ReadCloser, size int64, err error)
}

// S3Client combines S3Writer and S3Reader. Satisfied by FakeS3Client and
// production AWS/Ceph clients. Use this when a single client is passed to
// both a Writer and a Processor.
type S3Client interface {
	S3Writer
	S3Reader
}
