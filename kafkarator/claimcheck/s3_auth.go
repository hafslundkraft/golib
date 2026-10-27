package claimcheck

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/identity"
)

const (
	envIDPIssuerURL   = "HAPPI_IDP_ISSUER_URL"
	defaultIDPS3Scope = "ceph_rgw"

	envIDPS3Scope = "HAPPI_IDP_S3_SCOPE"
)

// isValidARNComponent reports whether s contains only characters that are safe
// to embed in a Ceph IAM role ARN path component (ASCII alphanumeric, '-', '_').
func isValidARNComponent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// claimCheckRoleARN computes the Ceph IAM role ARN for a claim-check bucket.
//
//	arn:aws:iam:::role/happi/{system}/{env}/{bucket}/{system}.{env}.{bucket}.{access}
//
// access is "rw" for writers and "r" for readers.
func claimCheckRoleARN(system, env, bucket, access string) (string, error) {
	var missing []string
	for name, val := range map[string]string{
		"system": system, "env": env, "bucket": bucket, "access": access,
	} {
		if strings.TrimSpace(val) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf(
			"claimCheckRoleARN: required argument(s) empty or whitespace: %s",
			strings.Join(missing, ", "),
		)
	}
	if !isValidARNComponent(system) {
		return "", fmt.Errorf(
			"claimCheckRoleARN: system %q contains characters not allowed in an ARN path component",
			system,
		)
	}
	if !isValidARNComponent(env) {
		return "", fmt.Errorf("claimCheckRoleARN: env %q contains characters not allowed in an ARN path component", env)
	}
	return fmt.Sprintf("arn:aws:iam:::role/happi/%s/%s/%s/%s.%s.%s.%s",
		system, env, bucket, system, env, bucket, access), nil
}

// newIdentityTokenRetriever builds an stscreds.IdentityTokenRetriever backed by
// identity.WorkloadCredential. The retriever is independent of bucket and role,
// so it can be shared across multiple S3 clients.
func newIdentityTokenRetriever() (stscreds.IdentityTokenRetriever, error) {
	idpIssuerURL := os.Getenv(envIDPIssuerURL)
	if strings.TrimSpace(idpIssuerURL) == "" {
		return nil, fmt.Errorf("claimcheck: %s env var is not set", envIDPIssuerURL)
	}
	parsed, err := url.Parse(idpIssuerURL)
	if err != nil || parsed.Scheme != "https" {
		return nil, fmt.Errorf("claimcheck: %s must be an https:// URL, got %q", envIDPIssuerURL, idpIssuerURL)
	}

	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: init workload credential: %w", err)
	}

	idpScope := defaultIDPS3Scope
	if v := os.Getenv(envIDPS3Scope); v != "" {
		idpScope = v
	}

	src := cred.TokenSource(context.Background(), identity.WithScopes(idpScope))
	return tokenSourceRetriever{src: src}, nil
}

// tokenSourceRetriever adapts an oauth2.TokenSource to the AWS SDK's
// stscreds.IdentityTokenRetriever interface, returning the access token in
// memory rather than via a file.
type tokenSourceRetriever struct {
	src interface {
		Token() (*oauth2.Token, error)
	}
}

func (r tokenSourceRetriever) GetIdentityToken() ([]byte, error) {
	tok, err := r.src.Token()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: get identity token: %w", err)
	}
	return []byte(tok.AccessToken), nil
}
