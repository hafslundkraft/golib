package claimcheck_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkarator "github.com/hafslundkraft/golib/kafkarator"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

// TestClaimCheckARN_FromEnvVars_E2E exercises the full path that a real Happi
// workload follows: the operator-injected environment is read by
// kafkarator.ConfigFromEnvVars, the resulting SystemName/Env flow into the
// claimcheck S3 client construction, and the Ceph IAM role ARN is derived from
// them together with the topic-derived bucket.
//
// It mirrors the env block injected into a pod such as sine-wave-writer:
//
//	HAPPI_SYSTEM_NAME=snappi-demo
//	HAPPI_ENV=dev
//	HAPPI_WORKLOAD_NAME=sine-wave-writer
//	HAPPI_IDP_ISSUER_URL=https://login.dev.happi.hafslund.no/realms/happi
func TestClaimCheckARN_FromEnvVars_E2E(t *testing.T) {
	// Happi-injected environment, mirroring a real pod. Secret-backed values
	// (KAFKA_CA_CERT) are given dummy contents — they are not validated by
	// ConfigFromEnvVars, only required to be non-empty.
	t.Setenv("KAFKA_AUTH_TYPE", "sasl")
	t.Setenv("KAFKA_BROKER", "kafkabroker")
	t.Setenv("KAFKA_CA_CERT", "dummy-ca-cert")
	t.Setenv("KAFKA_SCHEMA_REGISTRY_URL", "https://kafkabroker:23100")
	t.Setenv("KAFKA_USERNAME", "username")
	t.Setenv("KAFKA_PASSWORD", "dummy-password")
	t.Setenv("HAPPI_IDP_ISSUER_URL", "https://dummy-issuer-url")

	// use vars from happi.docs
	t.Setenv("HAPPI_SYSTEM_NAME", "billing")
	t.Setenv("HAPPI_ENV", "test")
	t.Setenv("HAPPI_WORKLOAD_NAME", "my-app")

	// kafkarator reads the environment exactly as it does in production.
	cfg, err := kafkarator.ConfigFromEnvVars()
	require.NoError(t, err)
	require.Equal(t, "billing", cfg.SystemName)
	require.Equal(t, "test", cfg.Env)

	const topic = "billing.test.my-app"
	bucket := claimcheck.DefaultBucketResolver(topic)

	// Build a S3 client through the real construction path and
	//    read back the role ARN it computed.
	for _, tc := range []struct {
		name   string
		access string
	}{
		{"writer", "rw"},
		{"reader", "r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotARN, err := claimcheck.RoleARNForClient(bucket, tc.access, cfg.SystemName, cfg.Env)
			require.NoError(t, err)

			wantARN := fmt.Sprintf(
				"arn:aws:iam:::role/happi/billing/test/%s/billing.test.%s.%s",
				bucket, bucket, tc.access,
			)
			assert.Equal(t, wantARN, gotARN)
		})
	}
}

// TestClaimCheckARN_ProducerAndConsumerAgree_E2E proves the fix end-to-end: a
// consumer running in a DIFFERENT system than the producer must assume the
// producer's role to read the payload, because the bucket is owned by the
// producing system. The producer's writer role and the consumer's reader role
// must therefore resolve to the same Ceph IAM role, differing only in the
// trailing access component (rw vs r).
func TestClaimCheckARN_ProducerAndConsumerAgree_E2E(t *testing.T) {
	const (
		producerSystem = "billing"
		consumerSystem = "analytics" // deliberately NOT the producer's system
		env            = "test"
		topic          = "billing.test.invoices"
	)
	bucket := claimcheck.DefaultBucketResolver(topic)

	// Producer side: a writer in "billing" assumes the rw role for the bucket.
	producerARN, err := claimcheck.ClaimCheckRoleARN(producerSystem, env, bucket, "rw")
	require.NoError(t, err)

	// Consumer side: a processor whose OWN system is "analytics" resolves an
	// envelope produced by "billing". Capture the (system, bucket) the reader
	// factory is asked to build so we can derive the reader ARN production would.
	envelope := envelopeForTopic(topic, producerSystem)
	var usedSystem, usedBucket string
	factory := func(system, b string) (claimcheck.S3Reader, error) {
		usedSystem, usedBucket = system, b
		return claimcheck.NewFakeS3Client(), nil
	}
	require.NoError(t, claimcheck.ResolveForTest(
		factory, consumerSystem, &fakeEnvelopeDeserializer{envelope: envelope}, topic, []byte("wire"),
	))

	// The consumer used the producer's system, not its own.
	require.Equal(t, producerSystem, usedSystem)
	require.NotEqual(t, consumerSystem, usedSystem)
	require.Equal(t, bucket, usedBucket)

	consumerARN, err := claimcheck.ClaimCheckRoleARN(usedSystem, env, usedBucket, "r")
	require.NoError(t, err)

	// Both sides target the same role: identical except for the access suffix.
	assert.Equal(t, "arn:aws:iam:::role/happi/billing/test/"+bucket+"/billing.test."+bucket+".rw", producerARN)
	assert.Equal(t, "arn:aws:iam:::role/happi/billing/test/"+bucket+"/billing.test."+bucket+".r", consumerARN)
	assert.Equal(t,
		strings.TrimSuffix(producerARN, ".rw"),
		strings.TrimSuffix(consumerARN, ".r"),
		"producer and consumer must resolve to the same role, differing only in access",
	)
}

// TestClaimCheckARN_SharedProduct_E2E proves that for a shared data-product
// neither the producer nor the consumer owns the bucket: both derive
// "data-definitions" from the topic and resolve to the same role.
func TestClaimCheckARN_SharedProduct_E2E(t *testing.T) {
	const (
		consumerSystem = "analytics"
		env            = "test"
		topic          = "test.water--obs.measurements--v1"
	)

	owner := claimcheck.DefaultSystemResolver(topic)
	require.Equal(t, "data-definitions", owner)

	bucket := claimcheck.DefaultBucketResolver(topic)

	// Producer side: the writer assumes the owner's rw role.
	producerARN, err := claimcheck.ClaimCheckRoleARN(owner, env, bucket, "rw")
	require.NoError(t, err)

	// Consumer side: the writer stamped the resolved owner onto the envelope.
	envelope := envelopeForTopic(topic, owner)
	var usedSystem, usedBucket string
	factory := func(system, b string) (claimcheck.S3Reader, error) {
		usedSystem, usedBucket = system, b
		return claimcheck.NewFakeS3Client(), nil
	}
	require.NoError(t, claimcheck.ResolveForTest(
		factory, consumerSystem, &fakeEnvelopeDeserializer{envelope: envelope}, topic, []byte("wire"),
	))
	require.Equal(t, owner, usedSystem)
	require.NotEqual(t, consumerSystem, usedSystem)

	consumerARN, err := claimcheck.ClaimCheckRoleARN(usedSystem, env, usedBucket, "r")
	require.NoError(t, err)

	wantBase := fmt.Sprintf(
		"arn:aws:iam:::role/happi/data-definitions/test/%s/data-definitions.test.%s",
		bucket,
		bucket,
	)
	assert.Equal(t, wantBase+".rw", producerARN)
	assert.Equal(t, wantBase+".r", consumerARN)
	assert.Equal(t,
		strings.TrimSuffix(producerARN, ".rw"),
		strings.TrimSuffix(consumerARN, ".r"),
		"producer and consumer must resolve to the same data-definitions role",
	)
}
