package claimcheck

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// FakeS3Client is an in-process S3 mock that satisfies S3Client. All objects
// are stored in Store as "bucket/key" → bytes and can be inspected directly
// in tests after a write.
type FakeS3Client struct {
	mu      sync.Mutex
	Store   map[string][]byte
	uploads map[string]*fakeUpload // keyed by upload ID
	nextID  int
}

type fakeUpload struct {
	bucket string
	key    string
	parts  map[int][]byte
}

// NewFakeS3Client returns an initialized FakeS3Client.
func NewFakeS3Client() *FakeS3Client {
	return &FakeS3Client{
		Store:   make(map[string][]byte),
		uploads: make(map[string]*fakeUpload),
	}
}

func (f *FakeS3Client) storeKey(bucket, key string) string {
	return bucket + "/" + key
}

// CreateMultipartUpload implements S3Client.
func (f *FakeS3Client) CreateMultipartUpload(_ context.Context, bucket, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("upload-%d", f.nextID)
	f.uploads[id] = &fakeUpload{bucket: bucket, key: key, parts: make(map[int][]byte)}
	return id, nil
}

// UploadPart implements S3Client.
func (f *FakeS3Client) UploadPart(
	_ context.Context,
	bucket, key, uploadID string,
	partNumber int,
	body io.Reader,
) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("fake s3: read part body: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[uploadID]
	if !ok {
		return "", fmt.Errorf("fake s3: unknown upload ID %q", uploadID)
	}
	u.parts[partNumber] = data
	return fmt.Sprintf("etag-%d", partNumber), nil
}

// CompleteMultipartUpload implements S3Client.
func (f *FakeS3Client) CompleteMultipartUpload(
	_ context.Context,
	bucket, key, uploadID string,
	parts []CompletedPart,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[uploadID]
	if !ok {
		return fmt.Errorf("fake s3: unknown upload ID %q", uploadID)
	}
	// Sort parts by number and assemble.
	nums := make([]int, 0, len(parts))
	for _, p := range parts {
		nums = append(nums, p.PartNumber)
	}
	sort.Ints(nums)
	var buf []byte
	for _, n := range nums {
		buf = append(buf, u.parts[n]...)
	}
	f.Store[f.storeKey(u.bucket, u.key)] = buf
	delete(f.uploads, uploadID)
	return nil
}

// AbortMultipartUpload implements S3Client.
func (f *FakeS3Client) AbortMultipartUpload(_ context.Context, bucket, key, uploadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.uploads, uploadID) // no-op if already gone
	return nil
}

// DeleteObject implements S3Client.
func (f *FakeS3Client) DeleteObject(_ context.Context, bucket, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.Store, f.storeKey(bucket, key))
	return nil
}

// GetObject implements S3Client.
func (f *FakeS3Client) GetObject(
	_ context.Context,
	bucket, key string,
	byteRange *string,
) (io.ReadCloser, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.Store[f.storeKey(bucket, key)]
	if !ok {
		return nil, 0, fmt.Errorf("fake s3: object not found: %s/%s", bucket, key)
	}
	if byteRange != nil {
		sliced, err := applyByteRange(*byteRange, data)
		if err != nil {
			return nil, 0, err
		}
		data = sliced
	}
	// Copy before releasing the lock so the returned reader does not share the
	// backing array with Store. Without this, a concurrent DeleteObject or
	// CompleteMultipartUpload could race on the same memory.
	result := make([]byte, len(data))
	copy(result, data)
	return io.NopCloser(bytes.NewReader(result)), int64(len(result)), nil
}

// applyByteRange parses an HTTP Range header value such as "bytes=1024-" or
// "bytes=0-511" and returns the corresponding slice of data.
func applyByteRange(rangeHeader string, data []byte) ([]byte, error) {
	rangeVal := strings.TrimPrefix(rangeHeader, "bytes=")
	startStr, endStr, _ := strings.Cut(rangeVal, "-")
	var start, end int
	if _, err := fmt.Sscanf(startStr, "%d", &start); err != nil {
		return nil, fmt.Errorf("fake s3: invalid range start %q: %w", startStr, err)
	}
	if endStr == "" {
		end = len(data)
	} else {
		if _, err := fmt.Sscanf(endStr, "%d", &end); err != nil {
			return nil, fmt.Errorf("fake s3: invalid range end %q: %w", endStr, err)
		}
		end++ // HTTP range is inclusive, Go slice is exclusive
	}
	if start < 0 || start > len(data) || end > len(data) || start > end {
		return nil, fmt.Errorf("fake s3: range %q out of bounds for object of size %d", rangeHeader, len(data))
	}
	return data[start:end], nil
}
