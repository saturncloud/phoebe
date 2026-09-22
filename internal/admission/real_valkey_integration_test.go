//go:build admissionintegration

package admission

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
)

// TestRealValkeyAtomicAdmission exercises the production Lua transaction
// against Valkey itself. Unit tests use miniredis for fast failure-path
// coverage; this gate catches differences in Lua, TIME, hashes, and sorted sets.
func TestRealValkeyAtomicAdmission(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	one := redis.NewClient(&redis.Options{Addr: addr})
	two := redis.NewClient(&redis.Options{Addr: addr})
	if err := one.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping real Valkey: %v", err)
	}

	cfg := config.AdmissionSettings{
		KeyPrefix: fmt.Sprintf("phoebe-admission-integration-%d", time.Now().UnixNano()),
		LeaseTTL:  150 * time.Millisecond,
		Platform:  limits(1),
	}
	a, b := New(one, cfg), New(two, cfg)
	t.Cleanup(func() {
		_ = one.Del(context.Background(), a.counters, a.leases, a.expiries, a.windowExpiries).Err()
		_ = one.Close()
		_ = two.Close()
	})

	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := a
			if i%2 == 1 {
				owner = b
			}
			_, err := owner.Admit(ctx, request("org", "model"))
			if err == nil {
				accepted.Add(1)
				return
			}
			if _, ok := err.(*Rejected); !ok {
				t.Errorf("unexpected admission error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("real Valkey accepted %d contenders, want exactly 1", got)
	}

	// Simulate owner loss: do not complete the accepted lease. A later mutation
	// must reap it using Valkey TIME and make capacity available again.
	time.Sleep(200 * time.Millisecond)
	lease, err := b.Admit(ctx, request("other", "model"))
	if err != nil {
		t.Fatalf("real Valkey did not reap expired lease: %v", err)
	}
	if err := lease.Complete(ctx, 3); err != nil {
		t.Fatalf("complete real Valkey lease: %v", err)
	}
}

// TestRealValkeyLeaseLifecycleTransitions exercises the lease transition
// scripts (prefill, cold hold, renew) against a real server, asserting the
// prefill and cold counters actually release capacity and that a short
// KeepAlive renewal advances the lease expiry.
func TestRealValkeyLeaseLifecycleTransitions(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping real Valkey: %v", err)
	}

	l := limits(2)
	l.MaxConcurrentPrefills = 1
	l.MaxColdHolds = 1
	l.WakesPerWindow = 10
	cfg := config.AdmissionSettings{
		KeyPrefix: fmt.Sprintf("phoebe-admission-lifecycle-%d", time.Now().UnixNano()),
		LeaseTTL:  300 * time.Millisecond,
		Platform:  l,
	}
	a := New(client, cfg)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), a.counters, a.leases, a.expiries, a.windowExpiries).Err()
		_ = client.Close()
	})

	// Prefill cap: the second admit is blocked until the first lease's prefill
	// reservation is released by PrefillDone.
	first, err := a.Admit(ctx, request("org-a", "model"))
	if err != nil {
		t.Fatalf("admit first: %v", err)
	}
	if _, err := a.Admit(ctx, request("org-b", "model")); err == nil {
		t.Fatal("second concurrent prefill admitted past the cap")
	} else if _, ok := err.(*Rejected); !ok {
		t.Fatalf("prefill contention error = %T %v, want Rejected", err, err)
	}
	if err := first.PrefillDone(ctx); err != nil {
		t.Fatalf("prefill transition: %v", err)
	}
	second, err := a.Admit(ctx, request("org-b", "model"))
	if err != nil {
		t.Fatalf("released prefill counter did not reopen capacity: %v", err)
	}

	// Cold-hold cap: BeginColdHold is exclusive at one hold; EndColdHold must
	// release the counter for the next holder.
	if err := first.BeginColdHold(ctx); err != nil {
		t.Fatalf("begin cold hold: %v", err)
	}
	if err := second.BeginColdHold(ctx); err == nil {
		t.Fatal("concurrent cold hold admitted past the cap")
	} else if _, ok := err.(*Rejected); !ok {
		t.Fatalf("cold-hold contention error = %T %v, want Rejected", err, err)
	}
	if err := first.EndColdHold(ctx); err != nil {
		t.Fatalf("end cold hold: %v", err)
	}
	if err := second.BeginColdHold(ctx); err != nil {
		t.Fatalf("released cold-hold counter did not reopen capacity: %v", err)
	}
	if err := second.EndColdHold(ctx); err != nil {
		t.Fatalf("end second cold hold: %v", err)
	}

	// A short KeepAlive renewal must advance the lease expiry on the real
	// server's TIME and must not report an error for a healthy lease.
	expiry0, err := client.ZScore(ctx, a.expiries, first.id).Result()
	if err != nil {
		t.Fatalf("read initial expiry: %v", err)
	}
	keepaliveCtx, stopKeepalive := context.WithCancel(ctx)
	renewErr := make(chan error, 1)
	go first.KeepAlive(keepaliveCtx, func(e error) {
		select {
		case renewErr <- e:
		default:
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		expiry, zerr := client.ZScore(ctx, a.expiries, first.id).Result()
		if zerr != nil {
			t.Fatalf("read renewed expiry: %v", zerr)
		}
		if expiry > expiry0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("KeepAlive did not renew the lease on the real server")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopKeepalive()
	select {
	case e := <-renewErr:
		t.Fatalf("healthy keepalive reported an error: %v", e)
	default:
	}

	if err := first.Complete(ctx, 1); err != nil {
		t.Fatalf("complete first: %v", err)
	}
	if err := second.Complete(ctx, 1); err != nil {
		t.Fatalf("complete second: %v", err)
	}
}

func TestRealValkeyReapsRetiredScopeWindows(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping real Valkey: %v", err)
	}

	l := limits(64)
	l.Window = 100 * time.Millisecond
	cfg := config.AdmissionSettings{
		KeyPrefix: fmt.Sprintf("phoebe-admission-window-churn-%d", time.Now().UnixNano()),
		LeaseTTL:  time.Second, Platform: l, Graph: l, Organization: l, OrganizationModel: l,
	}
	a := New(client, cfg)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), a.counters, a.leases, a.expiries, a.windowExpiries).Err()
		_ = client.Close()
	})

	for i := 0; i < 20; i++ {
		lease, err := a.Admit(ctx, request(fmt.Sprintf("org-%d", i), fmt.Sprintf("model-%d", i)))
		if err != nil {
			t.Fatalf("admit churn scope %d: %v", i, err)
		}
		if err := lease.CompleteUsage(ctx, Usage{TotalPromptTokens: 10, GeneratedTokens: 1}); err != nil {
			t.Fatalf("complete churn scope %d: %v", i, err)
		}
	}
	before, err := client.HLen(ctx, a.counters).Result()
	if err != nil || before == 0 {
		t.Fatalf("window fields before expiry = %d, err=%v", before, err)
	}

	time.Sleep(150 * time.Millisecond)
	lease, err := a.Admit(ctx, request("live-org", "live-model"))
	if err != nil {
		t.Fatalf("trigger expiry reap: %v", err)
	}
	if err := lease.Complete(ctx, 0); err != nil {
		t.Fatalf("complete reap trigger: %v", err)
	}
	after, err := client.HLen(ctx, a.counters).Result()
	if err != nil {
		t.Fatalf("window fields after expiry: %v", err)
	}
	if after >= before {
		t.Fatalf("retired window fields were not reclaimed: before=%d after=%d", before, after)
	}
}

func TestRealValkeySettlesIndependentContractsExactly(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr})
	cfg := config.AdmissionSettings{
		KeyPrefix: fmt.Sprintf("phoebe-admission-settlement-%d", time.Now().UnixNano()),
		LeaseTTL:  time.Second,
		Platform:  limits(64),
	}
	a := New(client, cfg)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), a.counters, a.leases, a.expiries, a.windowExpiries).Err()
		_ = client.Close()
	})

	contractRequest := func(owner string, input, output int64) Request {
		r := request("org", "model")
		r.Owner = owner
		r.EstimatedInputTokens = input
		r.ReservedOutputTokens = output
		r.OrganizationLimits = RateLimits{
			TotalPromptTokens: 12, UncachedPromptTokens: 8, GeneratedTokens: 7,
		}
		r.OwnerLimits = RateLimits{
			TotalPromptTokens: 9, UncachedPromptTokens: 5, GeneratedTokens: 6,
		}
		return r
	}

	first, err := a.Admit(ctx, contractRequest("owner-a", 4, 5))
	if err != nil {
		t.Fatalf("admit first request: %v", err)
	}
	// Exact usage is total=6, uncached=2, generated=3. This deliberately
	// exceeds the input estimate while refunding cached and generated capacity.
	if err := first.CompleteUsage(ctx, Usage{
		TotalPromptTokens: 6, CachedPromptTokens: 4, GeneratedTokens: 3,
	}); err != nil {
		t.Fatalf("settle first request: %v", err)
	}

	ownerBoundary, err := a.Admit(ctx, contractRequest("owner-a", 3, 3))
	if err != nil {
		t.Fatalf("exact owner boundary rejected after settlement: %v", err)
	}
	if _, err := a.Admit(ctx, contractRequest("owner-a", 1, 1)); err == nil {
		t.Fatal("owner total/uncached/generated boundary was not enforced")
	} else if rejected, ok := err.(*Rejected); !ok || rejected.Scope != "contract_owner" {
		t.Fatalf("owner boundary rejection = %T %v", err, err)
	}
	// A zero-usage completion refunds the outstanding conservative reservation.
	if err := ownerBoundary.CompleteUsage(ctx, Usage{}); err != nil {
		t.Fatalf("refund owner reservation: %v", err)
	}
	refunded, err := a.Admit(ctx, contractRequest("owner-a", 3, 3))
	if err != nil {
		t.Fatalf("refunded owner reservation remained charged: %v", err)
	}
	if err := refunded.CompleteUsage(ctx, Usage{}); err != nil {
		t.Fatalf("complete refunded-boundary request: %v", err)
	}

	// Other owners get independent owner scopes while sharing the remaining org
	// budget. Together these reservations reach each exact organization bound.
	ownerB, err := a.Admit(ctx, contractRequest("owner-b", 3, 3))
	if err != nil {
		t.Fatalf("owner-b admission: %v", err)
	}
	ownerC, err := a.Admit(ctx, contractRequest("owner-c", 3, 1))
	if err != nil {
		t.Fatalf("owner-c exact organization boundary: %v", err)
	}
	if _, err := a.Admit(ctx, contractRequest("owner-d", 1, 1)); err == nil {
		t.Fatal("aggregate organization boundary was not enforced")
	} else if rejected, ok := err.(*Rejected); !ok || rejected.Scope != "contract_organization" {
		t.Fatalf("organization boundary rejection = %T %v", err, err)
	}
	_ = ownerB.CompleteUsage(ctx, Usage{})
	_ = ownerC.CompleteUsage(ctx, Usage{})
}
