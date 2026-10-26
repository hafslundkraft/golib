// Command kafkarator_happi_idp_demo shows how to authenticate a kafkarator SASL
// connection against Aiven Kafka using the Happi IdP (client-assertion flow in
// the identity package) instead of the default Azure AD credential.
//
// Required environment:
//
//	KAFKA_AUTH_TYPE=sasl
//	KAFKA_BROKER             Aiven broker address
//	KAFKA_CA_CERT            Aiven CA (path, PEM, or base64 PEM)
//	HAPPI_SYSTEM_NAME        used for consumer group id generation
//	HAPPI_WORKLOAD_NAME      used for consumer group id generation
//	HAPPI_ENV                used for consumer group id generation
//	HAPPI_IDP_ISSUER_URL     Happi IdP issuer (OIDC)
//	KAFKA_HAPPI_AUD          audience/resource the token must carry; must equal
//	                         the Aiven kafka.sasl_oauthbearer_expected_audience
//
// Optional:
//
//	HAPPI_SERVICE_ACCOUNT_TOKEN_PATH  defaults to /happi/idp-token
//
// Build requires CGO (librdkafka): CGO_ENABLED=1.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hafslundkraft/golib/identity"
	"github.com/hafslundkraft/golib/kafkarator"
	"github.com/hafslundkraft/golib/telemetry"
)

func main() {
	ctx := context.Background()

	aud := os.Getenv("KAFKA_HAPPI_AUD")
	if aud == "" {
		log.Fatal("KAFKA_HAPPI_AUD is not set")
	}

	// 1. Happi IdP credential. Reads the projected service-account token and
	//    uses it as a client assertion against HAPPI_IDP_ISSUER_URL.
	cred, err := identity.NewWorkloadCredential()
	if err != nil {
		log.Fatalf("create workload credential: %v", err)
	}

	// 2. Token source. WithResource sets the `resource` endpoint param, which
	//    the IdP maps into the token's `aud` claim.
	ts := cred.TokenSource(ctx, identity.WithResource(aud))

	// 3. Fetch one token up front and print its key claims so they can be
	//    checked against the Aiven sasl_oauthbearer_* settings.
	tok, err := ts.Token()
	if err != nil {
		log.Fatalf("fetch happi token: %v", err)
	}
	if err := printTokenClaims(tok.AccessToken); err != nil {
		log.Fatalf("decode token: %v", err)
	}

	// 4. Load Kafka config from the environment (KAFKA_AUTH_TYPE=sasl).
	cfg, err := kafkarator.ConfigFromEnvVars()
	if err != nil {
		log.Fatalf("load kafka config: %v", err)
	}

	tp, shutdown := telemetry.New(ctx)
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	// 5. Build the connection with the Happi token source instead of Azure AD.
	conn, err := kafkarator.NewConnection(cfg, tp, kafkarator.WithTokenSource(ts))
	if err != nil {
		log.Printf("create connection: %v", err)
		return
	}

	// 6. Exercise the full OAUTHBEARER handshake against the broker.
	if err := conn.Test(ctx); err != nil {
		log.Printf("kafka connection test failed: %v", err)
		return
	}

	log.Println("kafka connection test succeeded using Happi IdP token")
}

// printTokenClaims base64url-decodes a JWT payload and logs the claims relevant
// to Aiven OAuthbearer validation.
func printTokenClaims(rawToken string) error {
	parts := strings.SplitN(rawToken, ".", 3)
	if len(parts) != 3 {
		return fmt.Errorf("expected 3 JWT parts, got %d", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fmt.Errorf("base64 decode payload: %w", err)
	}

	var claims struct {
		Issuer   string `json:"iss"`
		Audience any    `json:"aud"`
		Subject  string `json:"sub"`
		Expiry   int64  `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return fmt.Errorf("unmarshal claims: %w", err)
	}

	log.Printf("token iss: %s", claims.Issuer)
	log.Printf("token aud: %v", claims.Audience)
	log.Printf("token sub: %s", claims.Subject)
	log.Printf("token exp: %s", time.Unix(claims.Expiry, 0).UTC())

	return nil
}
