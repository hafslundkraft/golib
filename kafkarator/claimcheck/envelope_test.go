package claimcheck_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

func TestClaimCheckRoleARN_CorrectFormat(t *testing.T) {
	arn, err := claimcheck.ClaimCheckRoleARN("billing", "test", "invoices", "rw")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam:::role/happi/billing/test/invoices/billing.test.invoices.rw", arn)
}

func TestClaimCheckRoleARN_RejectsEmptyArgs(t *testing.T) {
	_, err := claimcheck.ClaimCheckRoleARN("", "prod", "bucket", "rw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system")
}

func TestClaimCheckRoleARN_RejectsSlashInSystem(t *testing.T) {
	_, err := claimcheck.ClaimCheckRoleARN("my/app", "prod", "cc-bucket", "rw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system")
}

func TestClaimCheckRoleARN_RejectsSlashInEnv(t *testing.T) {
	_, err := claimcheck.ClaimCheckRoleARN("myapp", "prod/stage", "cc-bucket", "rw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "env")
}

func TestClaimCheckRoleARN_AcceptsHyphensAndUnderscores(t *testing.T) {
	arn, err := claimcheck.ClaimCheckRoleARN("my-app_1", "prod-eu", "cc-abc123", "r")
	require.NoError(t, err)
	assert.Contains(t, arn, "my-app_1")
	assert.Contains(t, arn, "prod-eu")
}
