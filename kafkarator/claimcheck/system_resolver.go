package claimcheck

import "strings"

// dataDefinitionsSystem is the Happi system that owns the claim-check buckets
// for shared data-products. It is the single source of truth: if the
// provisioned Ceph role path ever changes (e.g. becomes env-suffixed), change
// it here and nowhere else.
const dataDefinitionsSystem = "data-definitions"

// owningSystem parses snappirator's topic convention
//
//	<env>.<domain-segment>.<unqualified...>
//
// and returns the Happi system that owns the topic's claim-check bucket (the
// system whose Ceph IAM role must be assumed):
//   - domain segment "sys--<system>"   -> "<system>"         (internal product)
//   - domain segment "<domain>--<sub>" -> "data-definitions" (shared product)
//   - otherwise                        -> ""  (non-conventional topic; the caller errors)
//
// The system is derived solely from the topic and is never overridable. env is
// intentionally ignored: the shared system is the bare constant, and the ARN env
// comes from the connection config.
func owningSystem(topic string) string {
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) < 3 {
		return ""
	}
	domain := parts[1]
	if system, ok := strings.CutPrefix(domain, "sys--"); ok {
		return system // "" when domain is exactly "sys--" (malformed) -> caller errors
	}
	return dataDefinitionsSystem // shared product (domain--sub) -> data-definitions
}
