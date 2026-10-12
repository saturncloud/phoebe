package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

// During a rolling deploy an old replica can die mid-request, leaving a lease
// under the previous release's keys that only the reaper can release. The new
// reaper must drain it (every counter it holds, legacy prompt bytes included)
// so it cannot strand capacity forever.
func TestExpiredLegacyKeyLeaseIsReapedByNewReplica(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxActiveRequests: ptr64(1)}})
	ctx := context.Background()
	req := request("a", "m")
	req.Adapter = false
	sc := a.scopes(req)[0]
	legacy := map[string]any{
		"scopes": []scope{sc}, "prompt": 400, "estimated_input": 10, "output": 20,
		"adapter": 0, "prefill": true, "cold": false, "admitted_at": 0,
	}
	raw, _ := json.Marshal(legacy)
	mr.HSet(a.legacyLeases, "old-replica-lease", string(raw))
	if _, err := mr.ZAdd(a.legacyExpiries, 1, "old-replica-lease"); err != nil {
		t.Fatal(err)
	}
	// The counters the old replica's admit wrote for that lease.
	for dim, v := range map[string]string{
		"active": "1", "prefills": "1", "reserved_decode_slots": "1", "output": "20", "genreserved": "20",
		"total_prompt_reserved": "10", "uncached_prompt_reserved": "10", "prompt": "400",
	} {
		mr.HSet(a.counters, sc.ID+"|"+dim, v)
	}
	lease, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatalf("the expired legacy lease still holds the only active slot: %v", err)
	}
	if mr.Exists(a.legacyLeases) || mr.Exists(a.legacyExpiries) {
		t.Fatal("expired legacy lease not removed from the legacy keys")
	}
	want := map[string]string{
		"active": "1", "prefills": "1", "reserved_decode_slots": "1", "output": "20", "genreserved": "20",
		"total_prompt_reserved": "10", "uncached_prompt_reserved": "10", "prompt": "", "prefill_tokens": "10", "kv": "30",
	}
	for dim, v := range want {
		if got := mr.HGet(a.counters, sc.ID+"|"+dim); got != v {
			t.Errorf("%s=%q, want %q (only the new lease's reservation)", dim, got, v)
		}
	}
	_ = lease.Complete(ctx, 0)
}

// A new lease lives only under the versioned keys, through its whole life, so
// a not-yet-upgraded replica (which reaps only the legacy keys and does not
// know prefill_tokens or kv) can never release it and strand those counters.
func TestNewLeaseNeverWrittenToLegacyKeys(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: limits(5), LeaseTTL: time.Minute})
	ctx := context.Background()
	if a.leases == a.legacyLeases || a.expiries == a.legacyExpiries {
		t.Fatal("lease keys must be versioned apart from the legacy keys")
	}
	lease, err := a.Admit(ctx, request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	assertOnlyVersioned := func(stage string) {
		t.Helper()
		if mr.Exists(a.legacyLeases) || mr.Exists(a.legacyExpiries) {
			t.Fatalf("%s: a new lease appeared in the legacy keys", stage)
		}
	}
	if ok, _ := a.client.HExists(ctx, a.leases, lease.id).Result(); !ok {
		t.Fatal("new lease not stored under the versioned lease key")
	}
	assertOnlyVersioned("admit")
	if err := lease.PrefillDone(ctx); err != nil {
		t.Fatal(err)
	}
	assertOnlyVersioned("prefill done")
	if err := lease.BeginColdHold(ctx); err != nil {
		t.Fatal(err)
	}
	assertOnlyVersioned("cold hold")
	if renewed, err := renewScript.Run(ctx, a.client, a.keys(), lease.id, time.Minute.Milliseconds()).Int64(); err != nil || renewed != 1 {
		t.Fatalf("renew=%d err=%v, want the versioned lease renewed", renewed, err)
	}
	assertOnlyVersioned("renew")
	if err := lease.Complete(ctx, 1); err != nil {
		t.Fatal(err)
	}
	assertOnlyVersioned("finish")
}

// A request alone larger than a positive physical cap is unsatisfiable (the
// proxy answers 413), not a retryable capacity rejection; a zero cap stays
// retryable.
func TestRequestLargerThanCapacityIsUnsatisfiable(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		limits        config.AdmissionLimits
		dimension     string
		unsatisfiable bool
	}{
		{"prefill tokens", config.AdmissionLimits{MaxPrefillTokens: ptr64(9)}, "prefill_tokens_exceeds_limit", true},
		{"kv tokens", config.AdmissionLimits{MaxReservedKVTokens: ptr64(29)}, "kv_exceeds_limit", true},
		{"zero prefill cap stays retryable", config.AdmissionLimits{MaxPrefillTokens: ptr64(0)}, "prefill_tokens", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testAdmitter(t, config.AdmissionSettings{Graph: tc.limits})
			_, err := a.Admit(ctx, request("a", "m")) // 10 input + 20 output
			var rejected *Rejected
			if !errors.As(err, &rejected) || rejected.Dimension != tc.dimension || rejected.Unsatisfiable != tc.unsatisfiable || rejected.Contractual {
				t.Fatalf("err=%+v, want dimension %s unsatisfiable=%v", rejected, tc.dimension, tc.unsatisfiable)
			}
		})
	}
}
