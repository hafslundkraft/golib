package claimcheck_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// envelopeForTopic builds an envelope whose StorageURI points at the
// convention-derived bucket for topic, with the given producer system name.
func envelopeForTopic(topic, system string) *claimcheck.Envelope {
	bucket := claimcheck.DefaultBucketResolver(topic)
	return &claimcheck.Envelope{
		BatchID:     "batch-1",
		StorageURI:  "s3://" + bucket + "/" + topic + "/batch-1.parquet",
		Topic:       topic,
		RecordCount: 1,
		ByteSize:    1,
		System:      system,
	}
}

// TestResolver_UsesEnvelopeSystem verifies that the reader builds its S3 client
// (and thus its IAM role ARN) for the system stamped on the envelope, which the
// writer derived from the topic — that is the bucket owner whose role must be
// assumed. It covers both an internal product (sys--<system>) and a shared
// product (<domain>--<sub>, owned by data-definitions).
func TestResolver_UsesEnvelopeSystem(t *testing.T) {
	for _, tc := range []struct {
		name   string
		topic  string
		system string
	}{
		{"internal product", "test.sys--billing.invoices--v1", "billing"},
		{"shared product (data-definitions)", "test.water--obs.measurements--v1", "data-definitions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope := envelopeForTopic(tc.topic, tc.system)

			var gotSystem, gotBucket string
			factory := func(system, bucket string) (claimcheck.S3Reader, error) {
				gotSystem, gotBucket = system, bucket
				return claimcheck.NewFakeS3Client(), nil
			}

			err := claimcheck.ResolveForTest(
				factory,
				&fakeEnvelopeDeserializer{envelope: envelope},
				tc.topic,
				[]byte("wire"),
			)
			require.NoError(t, err)

			assert.Equal(t, tc.system, gotSystem, "reader must assume the system stamped on the envelope")
			assert.Equal(t, claimcheck.DefaultBucketResolver(tc.topic), gotBucket)
		})
	}
}

// TestResolver_EmptySystemErrors verifies that an envelope without a system
// (malformed or pre-system-field) is rejected rather than resolved via any
// fallback. This holds regardless of whether the topic follows the convention.
func TestResolver_EmptySystemErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		topic string
	}{
		{"conventional shared topic", "test.water--obs.measurements--v1"},
		{"conventional internal topic", "test.sys--billing.invoices--v1"},
		{"non-conventional topic", "billing.test.invoices"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope := envelopeForTopic(tc.topic, "") // no system

			factory := func(string, string) (claimcheck.S3Reader, error) {
				t.Fatal("factory must not be called for an envelope with no system")
				return nil, nil
			}

			err := claimcheck.ResolveForTest(
				factory,
				&fakeEnvelopeDeserializer{envelope: envelope},
				tc.topic,
				[]byte("wire"),
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "has no system")
		})
	}
}
