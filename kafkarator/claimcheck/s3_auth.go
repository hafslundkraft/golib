package claimcheck

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"golang.org/x/oauth2"

	"github.com/hafslundkraft/golib/identity"
)

const (
	envIDPIssuerURL   = "HAPPI_IDP_ISSUER_URL"
	defaultIDPS3Scope = "ceph_rgw"

	envIDPS3Scope = "HAPPI_IDP_S3_SCOPE"

	tokenRequestTimeout = 10 * time.Second
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

// newIDPTokenSource returns a cached Happi IDP token source for the given scope.
// identity keeps ctx for the source's lifetime, so it must be long-lived, and
// oauth2 reads its HTTP client from it — that is how httpClient's timeout
// reaches token refreshes, since Token() takes no ctx.
func newIDPTokenSource(cred *identity.WorkloadCredential, scope string, httpClient *http.Client) oauth2.TokenSource {
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, httpClient)
	return cred.TokenSource(ctx, identity.WithScopes(scope))
}

// newIdentityTokenRetriever builds an stscreds.IdentityTokenRetriever backed by
// identity.WorkloadCredential. The retriever is independent of bucket and role,
// so it can be shared across multiple S3 clients.
func newIdentityTokenRetriever() (stscreds.IdentityTokenRetriever, error) {
	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: init workload credential: %w", err)
	}

	// identity does not enforce https, and the pod's service account token is sent here.
	if u, err := url.Parse(cred.TokenURL); err != nil || u.Scheme != "https" {
		return nil, fmt.Errorf("claimcheck: %s must be an https:// URL, got token URL %q",
			envIDPIssuerURL, cred.TokenURL)
	}

	idpScope := defaultIDPS3Scope
	if v := os.Getenv(envIDPS3Scope); v != "" {
		idpScope = v
	}

	httpClient := &http.Client{Timeout: tokenRequestTimeout}
	return tokenSourceRetriever{src: newIDPTokenSource(cred, idpScope, httpClient)}, nil
}

// tokenSourceRetriever adapts an oauth2.TokenSource to the AWS SDK's
// stscreds.IdentityTokenRetriever interface, returning the access token in
// memory rather than via a file.
type tokenSourceRetriever struct {
	src oauth2.TokenSource
}

func (r tokenSourceRetriever) GetIdentityToken() ([]byte, error) {
	tok, err := r.src.Token()
	if err != nil {
		return nil, fmt.Errorf("claimcheck: get identity token: %w", err)
	}
	return []byte(tok.AccessToken), nil
}
