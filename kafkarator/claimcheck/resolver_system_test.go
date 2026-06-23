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

// TestResolver_UsesEnvelopeSystem verifies that when an envelope carries a
// producer system name, the reader builds its S3 client (and thus its IAM role
// ARN) for that issuer system — not the consumer's own system.
func TestResolver_UsesEnvelopeSystem(t *testing.T) {
	const topic = "billing.test.invoices"
	env := envelopeForTopic(topic, "billing") // produced by "billing"

	var gotSystem, gotBucket string
	factory := func(system, bucket string) (claimcheck.S3Reader, error) {
		gotSystem, gotBucket = system, bucket
		return claimcheck.NewFakeS3Client(), nil
	}

	// Consumer's own system is "analytics" — must be ignored in favour of the issuer.
	err := claimcheck.ResolveForTest(factory, "analytics", &fakeEnvelopeDeserializer{env: env}, topic, []byte("wire"))
	require.NoError(t, err)

	assert.Equal(t, "billing", gotSystem, "reader must assume the producer's role, not the consumer's")
	assert.Equal(t, claimcheck.DefaultBucketResolver(topic), gotBucket)
}

// TestResolver_FallsBackToDefaultSystem verifies that legacy envelopes without a
// system field fall back to the consumer's own system name.
func TestResolver_FallsBackToDefaultSystem(t *testing.T) {
	const topic = "billing.test.invoices"
	env := envelopeForTopic(topic, "") // legacy envelope, no system

	var gotSystem string
	factory := func(system, _ string) (claimcheck.S3Reader, error) {
		gotSystem = system
		return claimcheck.NewFakeS3Client(), nil
	}

	err := claimcheck.ResolveForTest(factory, "analytics", &fakeEnvelopeDeserializer{env: env}, topic, []byte("wire"))
	require.NoError(t, err)

	assert.Equal(t, "analytics", gotSystem, "missing system must fall back to the consumer's own system")
}
