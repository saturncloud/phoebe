package iolog

import (
	"fmt"
	"testing"

	"github.com/saturncloud/phoebe/internal/identity"
)

func idWith(authID, groupID string) identity.Identity {
	return identity.Identity{AuthID: authID, GroupID: groupID, ResourceID: "model-1"}
}

// TestPolicy_OffByDefault is the load-bearing invariant: a zero-value Policy (no
// config at all) NEVER logs. This is the fail-closed/private default.
func TestPolicy_OffByDefault(t *testing.T) {
	var p StaticPolicy // zero value: Enabled=false, SampleRate=0
	if p.ShouldLog(idWith("a1", "g1"), "req-1") {
		t.Fatal("zero-value policy must not log (off by default)")
	}
}

// TestPolicy_EnabledButZeroRate verifies Enabled alone captures nothing.
func TestPolicy_EnabledButZeroRate(t *testing.T) {
	p := NewStaticPolicy(true, 0.0, nil, nil)
	for i := range 100 {
		if p.ShouldLog(idWith("a1", "g1"), fmt.Sprintf("req-%d", i)) {
			t.Fatalf("rate 0.0 must never log, but req-%d logged", i)
		}
	}
}

// TestPolicy_RateOneAlwaysLogs verifies the upper boundary: rate 1.0 logs every
// eligible request.
func TestPolicy_RateOneAlwaysLogs(t *testing.T) {
	p := NewStaticPolicy(true, 1.0, nil, nil)
	for i := range 100 {
		if !p.ShouldLog(idWith("a1", "g1"), fmt.Sprintf("req-%d", i)) {
			t.Fatalf("rate 1.0 must always log, but req-%d did not", i)
		}
	}
}

// TestPolicy_AllowlistHit verifies per-tenant opt-in: with an allowlist set,
// only matching tenants are eligible (and then sampled).
func TestPolicy_AllowlistHit(t *testing.T) {
	// rate 1.0 so eligibility is the only variable.
	p := NewStaticPolicy(true, 1.0, []string{"opted-auth"}, nil)

	if !p.ShouldLog(idWith("opted-auth", "g1"), "req-1") {
		t.Error("auth_id in allowlist should log")
	}
	if p.ShouldLog(idWith("other-auth", "g1"), "req-2") {
		t.Error("auth_id NOT in allowlist must not log")
	}
}

// TestPolicy_AllowlistByGroup verifies group-id opt-in works independently.
func TestPolicy_AllowlistByGroup(t *testing.T) {
	p := NewStaticPolicy(true, 1.0, nil, []string{"opted-group"})

	if !p.ShouldLog(idWith("a1", "opted-group"), "req-1") {
		t.Error("group_id in allowlist should log")
	}
	if p.ShouldLog(idWith("a1", "other-group"), "req-2") {
		t.Error("group_id NOT in allowlist must not log")
	}
}

// TestPolicy_EmptyAllowlistMeansAll verifies an empty allowlist makes every
// tenant eligible (operator-wide debug sampling).
func TestPolicy_EmptyAllowlistMeansAll(t *testing.T) {
	p := NewStaticPolicy(true, 1.0, nil, nil)
	if !p.ShouldLog(idWith("any-auth", "any-group"), "req-1") {
		t.Error("empty allowlist must treat all tenants as eligible")
	}
}

// TestPolicy_DeterministicSampling verifies the same request id always yields
// the same decision — reproducibility, the reason we hash instead of rand.
func TestPolicy_DeterministicSampling(t *testing.T) {
	p := NewStaticPolicy(true, 0.5, nil, nil)
	id := idWith("a1", "g1")
	for i := range 50 {
		reqID := fmt.Sprintf("stable-req-%d", i)
		first := p.ShouldLog(id, reqID)
		for range 5 {
			if p.ShouldLog(id, reqID) != first {
				t.Fatalf("sampling not deterministic for %s", reqID)
			}
		}
	}
}

// TestPolicy_SampleRateApproximate verifies the kept fraction roughly tracks the
// configured rate across many distinct request ids (uniformity of the hash).
func TestPolicy_SampleRateApproximate(t *testing.T) {
	const n = 20000
	const rate = 0.25
	p := NewStaticPolicy(true, rate, nil, nil)
	id := idWith("a1", "g1")
	kept := 0
	for i := range n {
		if p.ShouldLog(id, fmt.Sprintf("req-%d", i)) {
			kept++
		}
	}
	frac := float64(kept) / float64(n)
	// Allow a generous tolerance band; we're checking the hash isn't degenerate,
	// not asserting a precise rate.
	if frac < rate-0.03 || frac > rate+0.03 {
		t.Errorf("kept fraction %.3f too far from rate %.3f", frac, rate)
	}
}

// TestPolicy_EmptyRequestIDNotSampled verifies a missing request id fails closed
// (does not flip the gate to always-on).
func TestPolicy_EmptyRequestIDNotSampled(t *testing.T) {
	// rate < 1 so the hash path (not the >=1 short-circuit) is exercised.
	p := NewStaticPolicy(true, 0.9999, nil, nil)
	if p.ShouldLog(idWith("a1", "g1"), "") {
		t.Error("empty request id must not be sampled (fail closed)")
	}
}

// TestPolicy_ImplementsInterface is a runtime interface check: both the zero
// value and a constructed StaticPolicy satisfy Policy.
func TestPolicy_ImplementsInterface(t *testing.T) {
	var p Policy = StaticPolicy{}
	if p.ShouldLog(idWith("a", "g"), "r") {
		t.Error("zero-value policy via interface must not log")
	}
	p = NewStaticPolicy(false, 0, nil, nil)
	if p.ShouldLog(idWith("a", "g"), "r") {
		t.Error("disabled policy via interface must not log")
	}
}
