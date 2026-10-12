package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
)

// B7: reserved KV tokens are the estimated input plus the reserved output,
// held from admission until the request finishes (NOT released at the first
// response byte, unlike prefill tokens).
func TestReservedKVTokensHeldUntilFinish(t *testing.T) {
	// request(): 10 estimated input + 20 reserved output = 30 KV tokens.
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedKVTokens: ptr64(59)}})
	ctx := context.Background()
	first, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Admit(ctx, request("a", "m"))
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Dimension != "kv" || rejected.Contractual {
		t.Fatalf("second request err=%v, want a capacity rejection on kv (30+30 > 59)", err)
	}
	if err := first.PrefillDone(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Admit(ctx, request("a", "m")); !errors.As(err, &rejected) || rejected.Dimension != "kv" {
		t.Fatalf("after first byte err=%v, want kv still held", err)
	}
	if err := first.Complete(ctx, 5); err != nil {
		t.Fatal(err)
	}
	second, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatalf("after finish the KV reservation must be released: %v", err)
	}
	_ = second.Complete(ctx, 0)
}

// B6: prefill tokens are released at the first response byte.
func TestPrefillTokensReleasedAtFirstByte(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxPrefillTokens: ptr64(15)}})
	ctx := context.Background()
	first, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	var rejected *Rejected
	if _, err := a.Admit(ctx, request("a", "m")); !errors.As(err, &rejected) || rejected.Dimension != "prefill_tokens" {
		t.Fatalf("err=%v, want a prefill_tokens rejection (10+10 > 15)", err)
	}
	if err := first.PrefillDone(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatalf("prefill tokens must be released at the first byte: %v", err)
	}
	_ = first.Complete(ctx, 0)
	_ = second.Complete(ctx, 0)
}

// A rolling deploy leaves leases written by the previous release (prompt
// BYTES under 'prompt', no kv or prefill_tokens). Releasing one must return
// its bytes and must not drive the new counters below what other leases hold.
func TestLegacyLeaseRecordReleasesCleanly(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedKVTokens: ptr64(100)}})
	ctx := context.Background()
	current, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	scopeID := a.scopes(request("a", "m"))[0].ID
	legacy := map[string]any{
		"scopes": []scope{a.scopes(request("a", "m"))[0]}, "prompt": 400, "estimated_input": 10, "output": 20,
		"adapter": 0, "prefill": true, "cold": false, "admitted_at": 0,
	}
	raw, _ := json.Marshal(legacy)
	mr.HSet(a.leases, "legacy-lease", string(raw))
	mr.HSet(a.counters, scopeID+"|prompt", "400")
	legacyLease := &Lease{owner: a, id: "legacy-lease"}
	if err := legacyLease.Complete(ctx, 0); err != nil {
		t.Fatalf("releasing a legacy record failed: %v", err)
	}
	if mr.HGet(a.counters, scopeID+"|prompt") != "" {
		t.Fatalf("legacy prompt bytes not released: %q", mr.HGet(a.counters, scopeID+"|prompt"))
	}
	if got := mr.HGet(a.counters, scopeID+"|kv"); got != "30" {
		t.Fatalf("kv=%q after releasing a legacy record, want the current lease's 30 untouched", got)
	}
	_ = current.Complete(ctx, 0)
	if got := mr.HGet(a.counters, scopeID+"|kv"); got != "" {
		t.Fatalf("kv=%q after every lease finished, want released", got)
	}
}
