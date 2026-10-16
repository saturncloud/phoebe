package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/testutil"
)

// B7: reserved KV tokens are the estimated input plus the reserved output,
// held from admission until the request finishes (NOT released at the first
// response byte, unlike prefill tokens).
func TestReservedKVTokensHeldUntilFinish(t *testing.T) {
	// request(): 10 estimated input + 20 reserved output = 30 KV tokens.
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedKVTokens: testutil.Ptr[int64](59)}})
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
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxPrefillTokens: testutil.Ptr[int64](15)}})
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

// During a rolling deploy an old replica can die mid-request, leaving a lease
// under the previous release's keys that only the reaper can release. The new
// reaper must drain it (every counter it holds, legacy prompt bytes included)
// so it cannot strand capacity forever.
func TestExpiredLegacyKeyLeaseIsReapedByNewReplica(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](1)}})
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

// Q24: capacity scopes never answer "can never fit". A request larger than a
// positive prefill or KV limit is admitted alone and refused as retryable when
// the scope's counter is in use; an explicit zero cap refuses even alone.
func TestCapacityLimitsAreLoadLimits(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		limits    config.AdmissionLimits
		dimension string
		zeroCap   bool
	}{
		{"prefill tokens", config.AdmissionLimits{MaxPrefillTokens: testutil.Ptr[int64](9)}, "prefill_tokens", false},
		{"kv tokens", config.AdmissionLimits{MaxReservedKVTokens: testutil.Ptr[int64](29)}, "kv", false},
		{"zero prefill cap stays retryable even alone", config.AdmissionLimits{MaxPrefillTokens: testutil.Ptr[int64](0)}, "prefill_tokens", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testAdmitter(t, config.AdmissionSettings{Graph: tc.limits})
			first, err := a.Admit(ctx, request("a", "m")) // 10 input + 20 output
			var rejected *Rejected
			if tc.zeroCap {
				if !errors.As(err, &rejected) || rejected.Dimension != tc.dimension || rejected.Unsatisfiable {
					t.Fatalf("err=%+v, want a retryable %s rejection", rejected, tc.dimension)
				}
				return
			}
			if err != nil {
				t.Fatalf("oversized but alone must be admitted: %v", err)
			}
			defer func() { _ = first.Complete(ctx, 0) }()
			_, err = a.Admit(ctx, request("b", "m"))
			if !errors.As(err, &rejected) || rejected.Dimension != tc.dimension || rejected.Unsatisfiable || rejected.Contractual {
				t.Fatalf("err=%+v, want a retryable %s rejection while the scope is in use", rejected, tc.dimension)
			}
		})
	}
}

// TightestReservedCaps reads the same scopes Admit checks: the smallest
// positive KV cap wins, the lane share counts after its weight, published
// graph capacity counts, and an explicit zero cap is ignored (that scope
// rejects in admission with its retryable answer).
func TestTightestReservedCapsReadsKVFromAdmitScopes(t *testing.T) {
	zero, five, nine, lane := int64(0), int64(5000), int64(9000), int64(1500)
	cfg := config.AdmissionSettings{
		Platform:     config.AdmissionLimits{MaxReservedKVTokens: &nine},
		Organization: config.AdmissionLimits{MaxReservedKVTokens: &zero},
		Lanes:        map[string]config.AdmissionLane{"default": {Weight: 2, Limits: config.AdmissionLimits{MaxReservedKVTokens: &lane}}},
	}
	r := Request{Graph: "g", Organization: "o", Model: "m"}
	if got := TightestReservedCaps(cfg, r).KV; got != 3000 {
		t.Fatalf("cap=%d, want the weighted lane share 3000", got)
	}
	r.GraphCapacity = config.AdmissionLimits{MaxReservedKVTokens: testutil.Ptr[int64](2500)}
	if got := TightestReservedCaps(cfg, r).KV; got != 2500 {
		t.Fatalf("cap=%d, want the published graph capacity 2500", got)
	}
	if got := TightestReservedCaps(config.AdmissionSettings{Graph: config.AdmissionLimits{MaxReservedKVTokens: &five}}, Request{}).KV; got != 5000 {
		t.Fatalf("cap=%d, want 5000", got)
	}
	if got := TightestReservedCaps(config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedKVTokens: &zero}}, Request{}).KV; got != 0 {
		t.Fatalf("cap=%d, an explicit zero cap must be ignored", got)
	}
}

// The output cap is read from the same scope walk: the smallest positive
// maxReservedOutputTokens wins, the lane share after its weight, published
// graph capacity counts, and an explicit zero is ignored.
func TestTightestReservedCapsReadsOutputFromAdmitScopes(t *testing.T) {
	zero, nine, lane := int64(0), int64(9000), int64(1500)
	cfg := config.AdmissionSettings{
		Platform:     config.AdmissionLimits{MaxReservedOutputTokens: &nine},
		Organization: config.AdmissionLimits{MaxReservedOutputTokens: &zero},
		Lanes:        map[string]config.AdmissionLane{"default": {Weight: 2, Limits: config.AdmissionLimits{MaxReservedOutputTokens: &lane}}},
	}
	r := Request{Graph: "g", Organization: "o", Model: "m"}
	if got := TightestReservedCaps(cfg, r); got.Output != 3000 || got.KV != 0 {
		t.Fatalf("caps=%+v, want output = the weighted lane share 3000 and no KV cap", got)
	}
	r.GraphCapacity = config.AdmissionLimits{MaxReservedOutputTokens: testutil.Ptr[int64](2048)}
	if got := TightestReservedCaps(cfg, r).Output; got != 2048 {
		t.Fatalf("output cap=%d, want the published graph capacity 2048", got)
	}
	if got := TightestReservedCaps(config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedOutputTokens: &zero}}, Request{}).Output; got != 0 {
		t.Fatalf("output cap=%d, an explicit zero cap must be ignored", got)
	}
}

// A request larger than a CONTRACT scope's whole limit is unsatisfiable (the
// proxy's 400) even when an earlier scope is full, so the answer does not
// depend on load. Capacity scopes have no such answer (Q24,
// TestCapacityLimitsAreLoadLimits).
func TestOversizeContractRequestIsUnsatisfiableEvenWhenEarlierScopeFull(t *testing.T) {
	ctx := context.Background()
	cfg := config.AdmissionSettings{Platform: config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](1)}}
	a, _ := testAdmitter(t, cfg)
	small := request("a", "m")
	small.PrefillTokens, small.EstimatedInputTokens, small.ReservedOutputTokens = 1, 1, 1
	holder, err := a.Admit(ctx, small)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Complete(ctx, 0) }()
	req := request("a", "m") // 20 output
	req.OwnerLimits = RateLimits{GeneratedTokens: testutil.Ptr[int64](19)}
	_, err = a.Admit(ctx, req)
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Scope != "contract_owner" || rejected.Dimension != "generated_tokens_exceeds_limit" ||
		!rejected.Unsatisfiable || !rejected.Contractual || rejected.RetryAfter != 0 {
		t.Fatalf("err=%+v, want unsatisfiable contract_owner generated_tokens_exceeds_limit, not the full platform scope", rejected)
	}
}

// n / best_of: the prompt's KV is shared, each output sequence needs its own.
func TestReservedKVChargesEveryOutputSequence(t *testing.T) {
	ctx := context.Background()
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: config.AdmissionLimits{MaxReservedKVTokens: testutil.Ptr[int64](1000)}})
	req := request("a", "m") // 10 input + 20 output
	req.OutputSequences = 4
	lease, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	scopeID := a.scopes(req)[0].ID
	if got := mr.HGet(a.counters, scopeID+"|kv"); got != "90" {
		t.Fatalf("kv=%q, want 10 + 20x4 = 90", got)
	}
	_ = lease.Complete(ctx, 0)
	if got := mr.HGet(a.counters, scopeID+"|kv"); got != "" {
		t.Fatalf("kv=%q after finish, want released", got)
	}
}

// TestReleaseSubtractsExactlyWhatAdmitAdded: the lease record must carry the
// kv charge exactly, so finish subtracts the same amount admit added. Redis's
// cjson keeps only 14 significant digits for a JSON number, so the record
// stores kv as a decimal string. (miniredis encodes numbers exactly; the
// real-Valkey twin of this test is the one that reproduces the rounding.)
func TestReleaseSubtractsExactlyWhatAdmitAdded(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{})
	assertReleaseSubtractsExactlyWhatAdmitAdded(context.Background(), t, a, func(field string) string {
		return mr.HGet(a.counters, field)
	})
}

func assertReleaseSubtractsExactlyWhatAdmitAdded(ctx context.Context, t *testing.T, a *RedisAdmitter, hget func(string) string) {
	t.Helper()
	req := request("a", "m")
	// 3 + 4e9 x 40000 = 160000000000003: 15 significant digits, which a
	// 14-digit encoding rounds DOWN to 1.6e14, leaving 3 behind. (Rounding up
	// would drive the counter negative, and add() deletes a field at <= 0,
	// hiding the drift.)
	req.PrefillTokens, req.EstimatedInputTokens = 3, 3
	req.ReservedOutputTokens, req.OutputSequences = 4_000_000_000, 40_000
	for _, finish := range []string{"complete", "prefill then complete"} {
		lease, err := a.Admit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		scopeID := a.scopes(req)[0].ID
		if got := hget(scopeID + "|kv"); got != "160000000000003" {
			t.Fatalf("kv=%q, want 160000000000003", got)
		}
		if finish != "complete" {
			// PrefillDone re-encodes the record; the kv must survive it.
			if err := lease.PrefillDone(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := lease.Complete(ctx, 0); err != nil {
			t.Fatal(err)
		}
		if got := hget(scopeID + "|kv"); got != "" {
			t.Fatalf("%s: kv=%q after release, want exactly zero (field removed)", finish, got)
		}
	}
	// The output and generated-token reservations are output x sequences
	// too, and must also survive the record exactly: 3999999999 x 40001 =
	// 160003999959999 has 15 significant digits.
	req.ReservedOutputTokens, req.OutputSequences = 3_999_999_999, 40_001
	for _, finish := range []string{"complete", "prefill then complete"} {
		lease, err := a.Admit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		scopeID := a.scopes(req)[0].ID
		for _, dim := range []string{"output", "genreserved"} {
			if got := hget(scopeID + "|" + dim); got != "160003999959999" {
				t.Fatalf("%s=%q, want 160003999959999", dim, got)
			}
		}
		if finish != "complete" {
			if err := lease.PrefillDone(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := lease.Complete(ctx, 0); err != nil {
			t.Fatal(err)
		}
		for _, dim := range []string{"output", "genreserved", "kv"} {
			if got := hget(scopeID + "|" + dim); got != "" {
				t.Fatalf("%s: %s=%q after release, want exactly zero (field removed)", finish, dim, got)
			}
		}
	}
}

// TestZeroChargeLeaseReleasesWithoutNegativeZero: a lease whose charges are
// zero (prefill_tokens=0, so the new-format record has no 'prompt' field to
// release, or output=0) must release through finish, abandon and reap and
// return every counter to zero. release_record once computed -(0), a Lua
// negative zero. Redis 5 formats a Lua number argument with "%.17g", giving
// "-0", which HINCRBY rejects, aborting the script with the lease and
// counters stranded (reproduced against redis:5). Valkey 8 formats an
// integral number as an integer and accepts it; miniredis does not
// reproduce the failure either. The real-server twin of this test, pointed
// at a Redis 5 server, fails without the fix.
func TestZeroChargeLeaseReleasesWithoutNegativeZero(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	assertZeroChargeLeaseReleasesCleanly(context.Background(), t, a)
}

func assertZeroChargeLeaseReleasesCleanly(ctx context.Context, t *testing.T, a *RedisAdmitter) {
	t.Helper()
	c := a.client
	req := request("a", "m")
	req.PrefillTokens = 0
	scopes := a.scopes(req)
	gen := req.ReservedOutputTokens * max(req.OutputSequences, 1)

	assertReleased := func(name string) {
		t.Helper()
		if n := c.HLen(ctx, a.leases).Val(); n != 0 {
			t.Fatalf("%s: %d leases remain after release, want 0", name, n)
		}
		if n := c.ZCard(ctx, a.expiries).Val(); n != 0 {
			t.Fatalf("%s: %d lease expiries remain after release, want 0", name, n)
		}
		if left := c.HGetAll(ctx, a.counters).Val(); len(left) != 0 {
			t.Fatalf("%s: counters not returned to zero: %v", name, left)
		}
	}
	release := func(path string, lease *Lease) {
		t.Helper()
		switch path {
		case "finish":
			if err := lease.Complete(ctx, 0); err != nil {
				t.Fatalf("finish: %v", err)
			}
		case "abandon":
			if err := abandonScript.Run(ctx, c, a.keys(), lease.id).Err(); err != nil {
				t.Fatalf("abandon: %v", err)
			}
		case "reap":
			// Expire the lease now; any later script reaps it first.
			if err := c.ZAdd(ctx, a.expiries, redis.Z{Score: 0, Member: lease.id}).Err(); err != nil {
				t.Fatal(err)
			}
			if err := renewScript.Run(ctx, c, a.keys(), "no-such-lease", 1000).Err(); err != nil {
				t.Fatalf("reap: %v", err)
			}
		}
	}

	for _, path := range []string{"finish", "abandon", "reap"} {
		// prefill_tokens=0, still in the prefill phase: the record carries no
		// 'prompt' field, so both prompt and prefill_tokens release as zero.
		lease, err := a.Admit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(c.HGet(ctx, a.leases, lease.id).Val()), &rec); err != nil {
			t.Fatal(err)
		}
		if _, ok := rec["prompt"]; ok || rec["prefill"] != true {
			t.Fatalf("want a new-format prefill-phase record without 'prompt', got %v", rec)
		}
		release(path, lease)
		assertReleased("prefill_tokens=0 via " + path)

		// output=0: the proxy never admits one, but a stored record with a
		// zero output charge must still release cleanly. Rewrite the record
		// and the counters it holds to match.
		lease, err = a.Admit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		rec = nil
		if err := json.Unmarshal([]byte(c.HGet(ctx, a.leases, lease.id).Val()), &rec); err != nil {
			t.Fatal(err)
		}
		rec["output"] = "0"
		rec["kv"] = fmt.Sprintf("%d", req.EstimatedInputTokens)
		raw, _ := json.Marshal(rec)
		if err := c.HSet(ctx, a.leases, lease.id, string(raw)).Err(); err != nil {
			t.Fatal(err)
		}
		for _, s := range scopes {
			if err := c.HDel(ctx, a.counters, s.ID+"|output", s.ID+"|genreserved").Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.HIncrBy(ctx, a.counters, s.ID+"|kv", -gen).Err(); err != nil {
				t.Fatal(err)
			}
		}
		release(path, lease)
		assertReleased("output=0 via " + path)
	}
}

// D60: the admission script never returns *_exceeds_limit for a capacity
// scope. If a reply ever carries one (contract flag 0), Admit fails closed to
// a retryable rejection with a Retry-After, never a non-contractual
// Unsatisfiable that the proxy would answer with a permanent 413. A contract
// reply with the same suffix stays unsatisfiable.
func TestCapacityExceedsLimitReplyFailsClosedToRetryable(t *testing.T) {
	original := admitScript
	t.Cleanup(func() { admitScript = original })
	for _, tc := range []struct {
		name          string
		reply         string
		unsatisfiable bool
	}{
		{"capacity reply is retryable", `return {0,'graph','kv_exceeds_limit',0}`, false},
		{"contract reply stays unsatisfiable", `return {0,'contract','generated_tokens_exceeds_limit',1}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admitScript = redis.NewScript(tc.reply)
			a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute})
			_, err := a.Admit(context.Background(), request("a", "m"))
			var rejected *Rejected
			if !errors.As(err, &rejected) {
				t.Fatalf("err=%v, want a rejection", err)
			}
			if rejected.Unsatisfiable != tc.unsatisfiable {
				t.Fatalf("rejection=%+v, want Unsatisfiable=%v", rejected, tc.unsatisfiable)
			}
			if !tc.unsatisfiable && (rejected.Contractual || rejected.RetryAfter <= 0) {
				t.Fatalf("rejection=%+v, want a retryable non-contractual rejection with a Retry-After", rejected)
			}
			if tc.unsatisfiable && (!rejected.Contractual || rejected.RetryAfter != 0) {
				t.Fatalf("rejection=%+v, want a contractual unsatisfiable rejection with no Retry-After", rejected)
			}
		})
	}
}
