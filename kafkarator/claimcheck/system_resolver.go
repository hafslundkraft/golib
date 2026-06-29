package claimcheck

import "strings"

// dataDefinitionsSystem is the Happi system that owns the claim-check buckets
// for shared data-products. It is the single source of truth: if the
// provisioned Ceph role path ever changes (e.g. becomes env-suffixed), change
// it here and nowhere else.
const dataDefinitionsSystem = "data-definitions"

// SystemResolver maps a topic name to the Happi system that owns its
// claim-check bucket (the system whose Ceph IAM role must be assumed).
type SystemResolver func(topic string) string

// DefaultSystemResolver parses snappirator's topic convention
//
//	<env>.<domain-segment>.<unqualified...>
//
// and returns the owning system:
//   - domain segment "sys--<system>"   -> "<system>"         (internal product)
//   - domain segment "<domain>--<sub>" -> "data-definitions" (shared product)
//   - otherwise                        -> ""  (caller falls back to its own system)
//
// env is intentionally ignored: the shared system is the bare constant, and the
// ARN env comes from the connection config.
func DefaultSystemResolver(topic string) string {
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) < 3 {
		return ""
	}
	domain := parts[1]
	if system, ok := strings.CutPrefix(domain, "sys--"); ok {
		return system // "" when domain is exactly "sys--" (malformed) -> caller falls back
	}
	if strings.Contains(domain, "--") {
		return dataDefinitionsSystem
	}
	return ""
}
