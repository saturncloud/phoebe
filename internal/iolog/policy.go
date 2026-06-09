package iolog

import (
	"hash/fnv"

	"github.com/saturncloud/phoebe/internal/identity"
)

// Policy decides, per request, whether to capture its bodies. It is the FLAGGED
// FORK of M5: the opt-in + sampling gate that the proxy consults exactly once
// per request before deciding to buffer anything.
//
// The Policy interface is the seam. It exists so the interim StaticPolicy below
// can be swapped for a control-plane-backed policy WITHOUT touching the proxy —
// exactly like registry.LookupFunc is the seam for the control-plane resolver.
type Policy interface {
	// ShouldLog reports whether this request's bodies should be captured.
	// It MUST be cheap (it runs on the hot path) and deterministic given the
	// same identity + request id, so a request's log decision is reproducible.
	ShouldLog(id identity.Identity, requestID string) bool
}

// StaticPolicy is the INTERIM, statically-configured opt-in + sampling policy.
//
// ─────────────────────────────────────────────────────────────────────────────
// FLAGGED DECISION (M5) — interim static policy, control-plane TODO
// ─────────────────────────────────────────────────────────────────────────────
// The REAL per-tenant opt-in config — which tenants enabled I/O logging and at
// what sample rate — belongs in the control plane (Atlas), looked up per
// auth_id/group_id the same way registry.CachedResolver looks up upstreams via
// its LookupFunc seam. That control-plane API is unspecified, so M5 ships this
// static stand-in:
//
//   - Enabled         global on/off. DEFAULT FALSE — logging is OFF by default
//     (fail closed: bodies are sensitive, capturing them must be
//     a deliberate opt-in, never the default).
//   - SampleRate      global fraction in [0,1]. DEFAULT 0.0 — even when Enabled,
//     nothing is captured until a rate is set.
//   - AllowAuthIDs    auth_ids that are opted in. A request whose AuthID is in
//   - AllowGroupIDs   this set (or whose GroupID is in AllowGroupIDs) is subject
//     to sampling; everything else is never logged. An EMPTY
//     allowlist opts in NO ONE (fail-closed). Fleet-wide debug
//     capture requires the EXPLICIT AllowAllTenants flag.
//
// Replace StaticPolicy with a ControlPlanePolicy (per-tenant Enabled + rate
// fetched + cached from Atlas) once that contract is confirmed. The proxy
// depends only on the Policy interface, so that swap is a one-line wiring change.
// ─────────────────────────────────────────────────────────────────────────────
type StaticPolicy struct {
	// Enabled is the global kill switch. False (the zero value) = logging off.
	Enabled bool

	// SampleRate is the fraction of opted-in requests to capture, in [0,1].
	// 0.0 (the zero value) never logs; 1.0 always logs an opted-in request.
	SampleRate float64

	// AllowAuthIDs / AllowGroupIDs are the per-tenant opt-in allowlists. A request
	// is eligible iff its AuthID is in AllowAuthIDs OR its GroupID is in
	// AllowGroupIDs. EMPTY (the zero value) opts in NO ONE — this is fail-closed:
	// the most sensitive data in the system (prompts/completions, possibly PII)
	// must never be captured by accident. Forgetting to set an allowlist must mean
	// "capture nobody," not "capture everybody."
	AllowAuthIDs  map[string]struct{}
	AllowGroupIDs map[string]struct{}

	// AllowAllTenants is the EXPLICIT fleet-wide opt-in (subject to Enabled+rate).
	// Capturing every tenant's bodies is a deliberate, dangerous choice (debug
	// only), so it must be stated outright — it is NOT the emergent meaning of an
	// empty allowlist. False (the zero value) keeps the allowlist authoritative.
	AllowAllTenants bool
}

// NewStaticPolicy builds a StaticPolicy from plain config values, converting the
// allowlist slices into sets. An empty allowlist opts in NO ONE; pass
// allowAllTenants=true for the explicit (debug-only) fleet-wide opt-in.
func NewStaticPolicy(enabled bool, sampleRate float64, allowAuthIDs, allowGroupIDs []string, allowAllTenants bool) StaticPolicy {
	return StaticPolicy{
		Enabled:         enabled,
		SampleRate:      sampleRate,
		AllowAuthIDs:    toSet(allowAuthIDs),
		AllowGroupIDs:   toSet(allowGroupIDs),
		AllowAllTenants: allowAllTenants,
	}
}

func toSet(xs []string) map[string]struct{} {
	if len(xs) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if x != "" {
			s[x] = struct{}{}
		}
	}
	return s
}

// ShouldLog applies the gate: global enabled → tenant opt-in → sample. It is
// pure and side-effect free, so a request's decision is fully reproducible from
// its identity + request id.
func (p StaticPolicy) ShouldLog(id identity.Identity, requestID string) bool {
	// Fail closed: off unless explicitly enabled.
	if !p.Enabled {
		return false
	}
	// Rate 0 short-circuits before any hashing.
	if p.SampleRate <= 0 {
		return false
	}
	// Per-tenant opt-in. An empty allowlist means all tenants are eligible.
	if !p.optedIn(id) {
		return false
	}
	// Rate >= 1 always logs an opted-in request (skip the hash).
	if p.SampleRate >= 1 {
		return true
	}
	return sampled(requestID, p.SampleRate)
}

// optedIn reports whether this identity is eligible for sampling. FAIL-CLOSED:
// with no allowlists and no explicit AllowAllTenants, NO ONE is eligible.
// Fleet-wide capture requires the explicit AllowAllTenants flag; an empty
// allowlist never means "everyone." Otherwise the request must match by AuthID
// OR GroupID.
func (p StaticPolicy) optedIn(id identity.Identity) bool {
	if p.AllowAllTenants {
		return true
	}
	if _, ok := p.AllowAuthIDs[id.AuthID]; ok {
		return true
	}
	if _, ok := p.AllowGroupIDs[id.GroupID]; ok {
		return true
	}
	return false
}

// sampled is a DETERMINISTIC sample gate: it hashes the request id to a fraction
// in [0,1) and keeps the request iff that fraction < rate.
//
// Deterministic-by-hash (not math/rand) is chosen on purpose:
//   - Reproducible: the same request id always yields the same decision, so a
//     "why wasn't this request logged?" question is answerable after the fact.
//   - Testable: no injected rand source needed; tests assert exact boundaries.
//   - Uniform: FNV-1a over the request id spreads ids evenly, so the kept
//     fraction tracks the configured rate across many requests.
//
// (math/rand/v2 would also be fine on go1.23 — Go's PRNG has no JS-style
// determinism pitfalls — but a hash needs no shared state and is reproducible,
// which wins for a debug-logging gate.)
func sampled(requestID string, rate float64) bool {
	if requestID == "" {
		// No id to hash — fall back to "not sampled" rather than logging
		// everything. A missing X-Request-Id is unexpected; don't let it
		// silently flip the gate to always-on.
		return false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	// Map the 64-bit hash into [0,1). Divide by 2^64 using the top 53 bits to
	// stay within float64's exact-integer range.
	frac := float64(h.Sum64()>>11) / float64(1<<53)
	return frac < rate
}

// compile-time interface check.
var _ Policy = StaticPolicy{}
