package claimcheck

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
)

const (
	defaultRowGroupSize = 100_000
	minPartSize         = 5 * 1024 * 1024 // 5 MiB — S3 multipart minimum
)

// multipartWriter is an io.Writer that buffers bytes and flushes S3 multipart
// parts whenever the buffer reaches partSize. The full Parquet file is never
// held entirely in memory.
type multipartWriter struct {
	ctx        context.Context //nolint:containedctx // stored to satisfy io.Writer which has no ctx parameter
	s3         S3Writer
	bucket     string
	key        string
	partSize   int
	uploadID   string
	parts      []CompletedPart
	buf        []byte
	totalBytes int64
	logger     *slog.Logger
}

func newMultipartWriter(
	ctx context.Context,
	s3 S3Writer,
	bucket, key string,
	partSize int,
	logger *slog.Logger,
) (*multipartWriter, error) {
	if partSize < minPartSize {
		return nil, fmt.Errorf("claimcheck: partSize %d is below the S3 minimum of %d bytes", partSize, minPartSize)
	}
	uploadID, err := s3.CreateMultipartUpload(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("claimcheck: create multipart upload: %w", err)
	}
	return &multipartWriter{
		ctx:      ctx,
		s3:       s3,
		bucket:   bucket,
		key:      key,
		partSize: partSize,
		uploadID: uploadID,
		logger:   logger,
	}, nil
}

// Write implements io.Writer.
func (w *multipartWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	if len(w.buf) >= w.partSize {
		if err := w.flushPart(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Size returns the total number of bytes written so far, including bytes
// buffered but not yet uploaded as a part.
func (w *multipartWriter) Size() int64 {
	return w.totalBytes + int64(len(w.buf))
}

func (w *multipartWriter) flushPart() error {
	if len(w.buf) == 0 {
		return nil
	}
	partNumber := len(w.parts) + 1
	etag, err := w.s3.UploadPart(w.ctx, w.bucket, w.key, w.uploadID, partNumber, bytes.NewReader(w.buf))
	if err != nil {
		return fmt.Errorf("claimcheck: upload part %d: %w", partNumber, err)
	}
	w.parts = append(w.parts, CompletedPart{PartNumber: partNumber, ETag: etag})
	w.totalBytes += int64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

// Complete flushes any remaining buffered bytes and finalizes the multipart upload.
func (w *multipartWriter) Complete(ctx context.Context) error {
	if err := w.flushPart(); err != nil {
		return err
	}
	if err := w.s3.CompleteMultipartUpload(ctx, w.bucket, w.key, w.uploadID, w.parts); err != nil {
		return fmt.Errorf("claimcheck: complete multipart upload: %w", err)
	}
	return nil
}

// Abort discards all uploaded parts. Best-effort: errors are logged as
// warnings so that the original error context is preserved by the caller.
func (w *multipartWriter) Abort() {
	ctx := context.Background()
	if err := w.s3.AbortMultipartUpload(ctx, w.bucket, w.key, w.uploadID); err != nil {
		w.logger.WarnContext(ctx, "claimcheck: abort multipart upload failed; orphaned parts may accumulate",
			slog.String("bucket", w.bucket),
			slog.String("key", w.key),
			slog.String("upload-id", w.uploadID),
			slog.Any("error", err),
		)
	}
}
