package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

func TestPeekEnvelope_ReturnsMetadataWithoutFetchingPayload(t *testing.T) {
	envelope := &claimcheck.Envelope{
		BatchID:     "batch-1",
		StorageURI:  "s3://bucket/topic/batch-1.parquet",
		Topic:       "peek-topic",
		RecordCount: 99,
		ByteSize:    1024,
		CreatedAt:   1_700_000_000_000,
	}
	s3 := claimcheck.NewFakeS3Client()
	msg := claimcheck.NewMessage("peek-topic", nil, []byte("wire"), nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	meta, err := msg.PeekEnvelope(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "batch-1", meta.BatchID)
	assert.Equal(t, int64(99), meta.RecordCount)
	assert.Equal(t, int64(1024), meta.ByteSize)
	assert.Empty(t, s3.Store, "PeekEnvelope must not touch S3")
}

func TestMessage_Empty(t *testing.T) {
	type R struct {
		X int32 `parquet:"x"`
	}

	s3 := claimcheck.NewFakeS3Client()
	msg := claimcheck.NewMessage("t", nil, nil, nil, s3, &fakeEnvelopeDeserializer{})

	assert.True(t, msg.IsEmpty())

	_, err := msg.PeekEnvelope(context.Background())
	require.Error(t, err)

	_, err = msg.Payload(context.Background())
	require.Error(t, err)

	var count int
	for _, err := range claimcheck.Records[R](context.Background(), msg) {
		require.NoError(t, err)
		count++
	}
	assert.Zero(t, count)
	assert.Empty(t, s3.Store, "empty message must not touch S3")
}

func TestMessage_Payload_RejectsTamperedBucket(t *testing.T) {
	envelope := &claimcheck.Envelope{
		BatchID:     "batch-1",
		StorageURI:  "s3://cc-wrongbucket/real-topic/batch-1.parquet",
		Topic:       "real-topic",
		RecordCount: 1,
		ByteSize:    100,
	}
	s3 := claimcheck.NewFakeS3Client()
	msg := claimcheck.NewMessage("real-topic", nil, []byte("wire"), nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	_, err := msg.Payload(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tampered")
}

func TestMessage_Payload_RejectsTamperedKeyPrefix(t *testing.T) {
	correctBucket := claimcheck.DefaultBucketResolver("real-topic")
	envelope := &claimcheck.Envelope{
		BatchID:     "batch-1",
		StorageURI:  "s3://" + correctBucket + "/other-topic/batch-1.parquet",
		Topic:       "real-topic",
		RecordCount: 1,
		ByteSize:    100,
	}
	s3 := claimcheck.NewFakeS3Client()
	msg := claimcheck.NewMessage("real-topic", nil, []byte("wire"), nil, s3, &fakeEnvelopeDeserializer{envelope: envelope})

	_, err := msg.Payload(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tampered")
}
