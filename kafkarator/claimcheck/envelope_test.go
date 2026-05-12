package claimcheck_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hafslundkraft/golib/kafkarator/claimcheck"
)

func TestClaimCheckRoleARN_CorrectFormat(t *testing.T) {
	arn, err := claimcheck.ClaimCheckRoleARN("myapp", "prod", "cc-abc123", "rw")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam:::role/happi/myapp/prod/cc-abc123/myapp.prod.cc-abc123.rw", arn)
}

func TestClaimCheckRoleARN_RejectsEmptyArgs(t *testing.T) {
	_, err := claimcheck.ClaimCheckRoleARN("", "prod", "bucket", "rw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system")
}
