package admission

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
)

type scriptFailureHook struct {
	hash      string
	remaining atomic.Int64
	after     bool
}

func newScriptFailureHook(hash string, failures int64, after bool) *scriptFailureHook {
	h := &scriptFailureHook{hash: hash, after: after}
	h.remaining.Store(failures)
	return h
}

func (h *scriptFailureHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *scriptFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *scriptFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		matches := cmd.Name() == "evalsha" && len(args) > 1 && fmt.Sprint(args[1]) == h.hash
		if !matches || h.remaining.Add(-1) < 0 {
			if matches {
				h.remaining.Add(1)
			}
			return next(ctx, cmd)
		}
		if h.after {
			if err := next(ctx, cmd); err != nil {
				return err
			}
		}
		return io.ErrUnexpectedEOF
	}
}

func limits(active int64) config.AdmissionLimits {
	return config.AdmissionLimits{MaxActiveRequests: active, MaxConcurrentPrefills: active,
		MaxReservedDecodeSlots: active,
		MaxPromptBytes:         1024, MaxReservedOutputTokens: 1024, MaxActiveAdapters: active,
		RequestsPerWindow: 100, TotalPromptTokensPerWindow: 1000,
		UncachedPromptTokensPerWindow: 1000, GeneratedTokensPerWindow: 1000, MaxColdHolds: active,
		WakesPerWindow: 100, Window: time.Minute}
}

func testAdmitter(t *testing.T, cfg config.AdmissionSettings) (*RedisAdmitter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "test"
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = time.Minute
	}
	return New(client, cfg), mr
}

func request(org, model string) Request {
	return Request{Graph: "graph-a", Organization: org, Model: model, PromptBytes: 10, EstimatedInputTokens: 10, ReservedOutputTokens: 20, Adapter: true}
}

func TestConcurrentReplicasShareOneAtomicCapacityPool(t *testing.T) {
	cfg := config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute, KeyPrefix: "concurrent"}
	a, mr := testAdmitter(t, cfg)
	b := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), cfg)
	var accepted atomic.Int64
	var leasesMu sync.Mutex
	var leases []*Lease
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := a
			if i%2 == 1 {
				owner = b
			}
			l, err := owner.Admit(context.Background(), request("org-a", "m"))
			if err == nil {
				accepted.Add(1)
				leasesMu.Lock()
				leases = append(leases, l)
				leasesMu.Unlock()
			} else {
				var r *Rejected
				if !errors.As(err, &r) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted=%d, want exactly 1 across replicas", got)
	}
	for _, l := range leases {
		_ = l.Complete(context.Background(), 0)
	}
}

func TestRetiredScopeWindowFieldsAreReaped(t *testing.T) {
	l := limits(64)
	l.Window = time.Minute
	a, mr := testAdmitter(t, config.AdmissionSettings{
		Platform: l, Graph: l, Organization: l, OrganizationModel: l,
	})
	mr.SetTime(time.UnixMilli(120_000))
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		lease, err := a.Admit(ctx, request(fmt.Sprintf("org-%d", i), fmt.Sprintf("model-%d", i)))
		if err != nil {
			t.Fatalf("admit churn scope %d: %v", i, err)
		}
		if err := lease.CompleteUsage(ctx, Usage{TotalPromptTokens: 10, GeneratedTokens: 1}); err != nil {
			t.Fatalf("complete churn scope %d: %v", i, err)
		}
	}
	before, err := a.client.HLen(ctx, a.counters).Result()
	if err != nil {
		t.Fatalf("window fields before expiry: %v", err)
	}
	if before == 0 {
		t.Fatal("expected fixed-window fields before expiry")
	}

	mr.SetTime(time.UnixMilli(180_001))
	lease, err := a.Admit(ctx, request("live-org", "live-model"))
	if err != nil {
		t.Fatalf("trigger expiry reap: %v", err)
	}
	if err := lease.Complete(ctx, 0); err != nil {
		t.Fatalf("complete reap trigger: %v", err)
	}
	after, err := a.client.HLen(ctx, a.counters).Result()
	if err != nil {
		t.Fatalf("window fields after expiry: %v", err)
	}
	if after >= before {
		t.Fatalf("retired window fields were not reclaimed: before=%d after=%d", before, after)
	}
}

func TestOrganizationIsolationAndRelease(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(10), Organization: limits(1)})
	a1, err := a.Admit(context.Background(), request("org-a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("org-a", "m2")); err == nil {
		t.Fatal("same org exceeded active limit")
	}
	b1, err := a.Admit(context.Background(), request("org-b", "m"))
	if err != nil {
		t.Fatalf("other tenant was isolated incorrectly: %v", err)
	}
	if err = a1.Complete(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("org-a", "m2")); err != nil {
		t.Fatalf("released reservation leaked: %v", err)
	}
	_ = b1.Complete(context.Background(), 0)
}

func TestWeightedTierLanesAreIsolated(t *testing.T) {
	cfg := config.AdmissionSettings{Platform: config.AdmissionLimits{MaxActiveRequests: 3, Window: time.Minute},
		Tiers: map[string]config.AdmissionTier{
			"default":   {Weight: 1, Limits: config.AdmissionLimits{MaxActiveRequests: 1, Window: time.Minute}},
			"protected": {Weight: 2, Limits: config.AdmissionLimits{MaxActiveRequests: 1, Window: time.Minute}},
		}, OrganizationTiers: map[string]string{"org-p": "protected"}}
	a, _ := testAdmitter(t, cfg)
	d1, err := a.Admit(context.Background(), request("org-d", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("org-d2", "m")); err == nil {
		t.Fatal("default lane exceeded its protected share")
	}
	p1, err := a.Admit(context.Background(), request("org-p", "m"))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := a.Admit(context.Background(), request("org-p", "m2"))
	if err != nil {
		t.Fatalf("weighted protected lane did not receive two slots: %v", err)
	}
	_ = d1.Complete(context.Background(), 0)
	_ = p1.Complete(context.Background(), 0)
	_ = p2.Complete(context.Background(), 0)
}

func TestPrefillReservationReleasesAtResponseHeaders(t *testing.T) {
	l := limits(2)
	l.MaxConcurrentPrefills = 1
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	first, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("second concurrent prefill admitted")
	}
	if err = first.PrefillDone(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("prefill release did not open capacity: %v", err)
	}
	_ = first.Complete(context.Background(), 0)
	_ = second.Complete(context.Background(), 0)
}

func TestDecodeReservationHeldUntilCompletion(t *testing.T) {
	l := limits(2)
	l.MaxReservedDecodeSlots = 1
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	first, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if err = first.PrefillDone(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("decode slot released at the prefill boundary")
	}
	if err = first.Complete(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	second, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("completed decode leaked its slot: %v", err)
	}
	_ = second.Complete(context.Background(), 0)
}

func TestGeneratedWindowReservesThenChargesActual(t *testing.T) {
	l := limits(5)
	l.GeneratedTokensPerWindow = 25
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	first, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("outstanding output reservations may not overbook generated window")
	}
	if err = first.Complete(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	// 10 actual + 20 requested still exceeds the 25-token window.
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("actual generated-token window was not charged")
	}
}

func TestFixedWindowRejectionReportsRemainingWindow(t *testing.T) {
	l := limits(5)
	l.RequestsPerWindow = 1
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: l})
	mr.SetTime(time.UnixMilli(118_500)) // 1.5 seconds remain in the 60-second bucket.
	first, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Complete(context.Background(), 0) //nolint:errcheck
	_, err = a.Admit(context.Background(), request("b", "m"))
	var rejected *Rejected
	if !errors.As(err, &rejected) {
		t.Fatalf("err=%v, want Rejected", err)
	}
	if rejected.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("retry_after=%s, want 1.5s remaining in fixed window", rejected.RetryAfter)
	}
}

func TestPromptWindowsSettleAuthoritativeCachedUsage(t *testing.T) {
	t.Run("total prompt includes cached tokens", func(t *testing.T) {
		l := limits(5)
		l.TotalPromptTokensPerWindow = 100
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		first, err := a.Admit(context.Background(), request("a", "m"))
		if err != nil {
			t.Fatal(err)
		}
		if err := first.CompleteUsage(context.Background(), Usage{
			TotalPromptTokens: 100, CachedPromptTokens: 90, GeneratedTokens: 1,
		}); err != nil {
			t.Fatal(err)
		}
		_, err = a.Admit(context.Background(), request("b", "m"))
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Dimension != "total_prompt_tokens" {
			t.Fatalf("err=%v, want total_prompt_tokens rejection", err)
		}
	})

	t.Run("uncached prompt excludes cache hits", func(t *testing.T) {
		l := limits(5)
		l.TotalPromptTokensPerWindow = 1000
		l.UncachedPromptTokensPerWindow = 20
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		first, err := a.Admit(context.Background(), request("a", "m"))
		if err != nil {
			t.Fatal(err)
		}
		if err := first.CompleteUsage(context.Background(), Usage{
			TotalPromptTokens: 100, CachedPromptTokens: 80,
		}); err != nil {
			t.Fatal(err)
		}
		_, err = a.Admit(context.Background(), request("b", "m"))
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Dimension != "uncached_prompt_tokens" {
			t.Fatalf("err=%v, want uncached_prompt_tokens rejection", err)
		}
	})
}

func TestEstimatedPromptTokensReserveThenReconcileActual(t *testing.T) {
	t.Run("concurrent estimates cannot overbook", func(t *testing.T) {
		l := limits(5)
		l.TotalPromptTokensPerWindow = 15
		l.UncachedPromptTokensPerWindow = 15
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		first, err := a.Admit(context.Background(), request("a", "m"))
		if err != nil {
			t.Fatal(err)
		}
		defer first.Complete(context.Background(), 0) //nolint:errcheck
		_, err = a.Admit(context.Background(), request("b", "m"))
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Dimension != "total_prompt_tokens" {
			t.Fatalf("err=%v, want total_prompt_tokens reservation rejection", err)
		}
	})

	t.Run("overestimate is refunded and cache-aware actual is charged", func(t *testing.T) {
		l := limits(5)
		l.TotalPromptTokensPerWindow = 10
		l.UncachedPromptTokensPerWindow = 10
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		first, err := a.Admit(context.Background(), request("a", "m"))
		if err != nil {
			t.Fatal(err)
		}
		if err := first.CompleteUsage(context.Background(), Usage{TotalPromptTokens: 4, CachedPromptTokens: 2}); err != nil {
			t.Fatal(err)
		}
		next := request("b", "m")
		next.EstimatedInputTokens = 6
		second, err := a.Admit(context.Background(), next)
		if err != nil {
			t.Fatalf("refunded estimate did not reopen capacity: %v", err)
		}
		_ = second.Complete(context.Background(), 0)
	})

	t.Run("underestimate records debt", func(t *testing.T) {
		l := limits(5)
		l.TotalPromptTokensPerWindow = 10
		l.UncachedPromptTokensPerWindow = 10
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		first := request("a", "m")
		first.EstimatedInputTokens = 2
		lease, err := a.Admit(context.Background(), first)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.CompleteUsage(context.Background(), Usage{TotalPromptTokens: 9}); err != nil {
			t.Fatal(err)
		}
		next := request("b", "m")
		next.EstimatedInputTokens = 2
		_, err = a.Admit(context.Background(), next)
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Dimension != "total_prompt_tokens" {
			t.Fatalf("err=%v, want total_prompt_tokens debt rejection", err)
		}
	})
}

func TestPromptUsageClampsMalformedCachedSubset(t *testing.T) {
	l := limits(5)
	l.TotalPromptTokensPerWindow = 10
	l.UncachedPromptTokensPerWindow = 1
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	req := request("a", "m")
	req.EstimatedInputTokens = 1
	first, err := a.Admit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.CompleteUsage(context.Background(), Usage{
		TotalPromptTokens: 10, CachedPromptTokens: 20,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = a.Admit(context.Background(), request("b", "m"))
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Dimension != "total_prompt_tokens" {
		t.Fatalf("err=%v, want total_prompt_tokens rejection", err)
	}
}

func TestColdHoldAndWakeBurstLimits(t *testing.T) {
	l := limits(5)
	l.MaxColdHolds = 1
	l.WakesPerWindow = 1
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	one, _ := a.Admit(context.Background(), request("a", "m"))
	two, _ := a.Admit(context.Background(), request("b", "m"))
	if err := one.BeginColdHold(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := two.BeginColdHold(context.Background()); err == nil {
		t.Fatal("concurrent cold hold exceeded")
	}
	if err := one.EndColdHold(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := two.BeginColdHold(context.Background()); err == nil {
		t.Fatal("wake churn window exceeded after hold released")
	}
	_ = one.Complete(context.Background(), 0)
	_ = two.Complete(context.Background(), 0)
}

func TestExpiredLeaseIsReapedAfterReplicaDeath(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(1), LeaseTTL: 20 * time.Millisecond})
	if _, err := a.Admit(context.Background(), request("a", "m")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	l, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("expired crashed-replica lease leaked: %v", err)
	}
	_ = l.Complete(context.Background(), 0)
}

func TestExpiredLeaseReapingIsBoundedAndConverges(t *testing.T) {
	l := limits(300)
	l.MaxPromptBytes = 100_000
	l.MaxReservedOutputTokens = 100_000
	l.RequestsPerWindow = 0
	l.TotalPromptTokensPerWindow = 0
	l.UncachedPromptTokensPerWindow = 0
	l.GeneratedTokensPerWindow = 0
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: l, LeaseTTL: time.Minute})
	mr.SetTime(time.UnixMilli(120_000))
	ctx := context.Background()
	for i := 0; i < 250; i++ {
		if _, err := a.Admit(ctx, request("crashed-org", fmt.Sprintf("model-%d", i))); err != nil {
			t.Fatalf("seed expired lease %d: %v", i, err)
		}
	}

	// Tighten the client-supplied scope contract so stale reservations make the
	// first two attempts fail closed while each Lua mutation reaps at most 100.
	a.cfg.Platform.MaxActiveRequests = 1
	mr.SetTime(time.UnixMilli(180_001))
	for attempt, wantRemaining := range []int64{150, 50} {
		if _, err := a.Admit(ctx, request("live-org", "live-model")); err == nil {
			t.Fatalf("attempt %d admitted before the expired backlog was drained", attempt+1)
		} else {
			var rejected *Rejected
			if !errors.As(err, &rejected) {
				t.Fatalf("attempt %d error = %v, want conservative rejection", attempt+1, err)
			}
		}
		remaining, err := a.client.ZCard(ctx, a.expiries).Result()
		if err != nil {
			t.Fatalf("attempt %d expiry cardinality: %v", attempt+1, err)
		}
		if remaining != wantRemaining {
			t.Fatalf("attempt %d left %d expired leases, want %d", attempt+1, remaining, wantRemaining)
		}
	}

	lease, err := a.Admit(ctx, request("live-org", "live-model"))
	if err != nil {
		t.Fatalf("admission did not recover after bounded reaping converged: %v", err)
	}
	if err := lease.Complete(ctx, 0); err != nil {
		t.Fatalf("complete recovered lease: %v", err)
	}
}

func TestKeepAlivePreventsLongStreamExpiry(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: limits(1), LeaseTTL: 30 * time.Millisecond})
	l, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	initialExpiry, err := a.client.ZScore(context.Background(), a.expiries, l.id).Result()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.KeepAlive(ctx, func(e error) { t.Errorf("keepalive: %v", e) }) }()
	// Wait for an observed renewal instead of assuming a busy CI runner will
	// schedule the keepalive goroutine inside a sub-100ms sleep window.
	deadline := time.Now().Add(time.Second)
	var latestExpiry float64
	for {
		renewedExpiry, zerr := a.client.ZScore(context.Background(), a.expiries, l.id).Result()
		if zerr != nil {
			t.Fatal(zerr)
		}
		if renewedExpiry > initialExpiry {
			latestExpiry = renewedExpiry
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease was not renewed")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("live renewed stream was reaped")
	}
	cancel()
	<-done
	// Lua uses Redis TIME, so advance Miniredis' server clock beyond the exact
	// renewed ZSET score rather than sleeping or changing unrelated key TTLs.
	mr.SetTime(time.UnixMilli(int64(latestExpiry) + 1))
	next, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("stopped keepalive was not reaped: %v", err)
	}
	_ = next.Complete(context.Background(), 0)
}

func TestKeepAliveReportsLeaseReapedBeforeRenewal(t *testing.T) {
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: limits(1), LeaseTTL: 30 * time.Millisecond})
	l, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	expiry, err := a.client.ZScore(context.Background(), a.expiries, l.id).Result()
	if err != nil {
		t.Fatal(err)
	}
	mr.SetTime(time.UnixMilli(int64(expiry) + 1))
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.KeepAlive(ctx, func(err error) { errCh <- err })
	select {
	case keepaliveErr := <-errCh:
		if !errors.Is(keepaliveErr, ErrUnavailable) {
			t.Fatalf("keepalive error=%v, want ErrUnavailable", keepaliveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("reaped lease was silently treated as a normal keepalive stop")
	}
}

func TestCompletionRetryRetainsAuthoritativeUsage(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})
	t.Cleanup(func() { _ = client.Close() })
	if err := finishScript.Load(context.Background(), client).Err(); err != nil {
		t.Fatal(err)
	}
	client.AddHook(newScriptFailureHook(finishScript.Hash(), 1, false))
	l := limits(5)
	l.TotalPromptTokensPerWindow = 10
	a := New(client, config.AdmissionSettings{Platform: l, LeaseTTL: time.Minute, KeyPrefix: "completion-retry"})
	lease, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.CompleteUsage(context.Background(), Usage{TotalPromptTokens: 10, GeneratedTokens: 3}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first completion err=%v, want transient ErrUnavailable", err)
	}
	// This is the proxy's deferred safety call. It must retry the retained exact
	// counts, not replace them with zero.
	if err := lease.Complete(context.Background(), 0); err != nil {
		t.Fatalf("completion retry: %v", err)
	}
	_, err = a.Admit(context.Background(), request("b", "m"))
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Dimension != "total_prompt_tokens" {
		t.Fatalf("err=%v, want retained total_prompt_tokens charge", err)
	}
}

func TestAdmitRecoversCommittedLeaseAfterLostReply(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})
	t.Cleanup(func() { _ = client.Close() })
	if err := admitScript.Load(context.Background(), client).Err(); err != nil {
		t.Fatal(err)
	}
	client.AddHook(newScriptFailureHook(admitScript.Hash(), 1, true))
	a := New(client, config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute, KeyPrefix: "lost-reply-recover"})
	lease, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatalf("idempotent recovery failed: %v", err)
	}
	if _, err := a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("recovered lease did not own exactly one capacity slot")
	}
	if err := lease.Complete(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestAdmitCleansCommittedLeaseWhenRecoveryReplyAlsoLost(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})
	t.Cleanup(func() { _ = client.Close() })
	for _, script := range []*redis.Script{admitScript, abandonScript} {
		if err := script.Load(context.Background(), client).Err(); err != nil {
			t.Fatal(err)
		}
	}
	client.AddHook(newScriptFailureHook(admitScript.Hash(), 2, true))
	l := limits(1)
	l.RequestsPerWindow = 1
	a := New(client, config.AdmissionSettings{Platform: l, LeaseTTL: time.Minute, KeyPrefix: "lost-reply-cleanup"})
	if _, err := a.Admit(context.Background(), request("a", "m")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want indeterminate admission failure", err)
	}
	lease, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("compensating cleanup stranded capacity: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

func TestImpossibleRequestRejectedBeforeReservation(t *testing.T) {
	l := limits(5)
	l.MaxPromptBytes = 9
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	_, err := a.Admit(context.Background(), request("a", "m"))
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Dimension != "prompt" {
		t.Fatalf("err=%v, want prompt rejection", err)
	}
}

func TestStateFailureReturnsUnavailableForProxyBypass(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: 0})
	a := New(client, config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute, KeyPrefix: "down"})
	_, err := a.Admit(context.Background(), request("a", "m"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
}

func TestBlackholedValkeyReturnsUnavailableWithinAdmissionBudget(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	client := NewValkeyClient(listener.Addr().String())
	t.Cleanup(func() { _ = client.Close() })
	a := New(client, config.AdmissionSettings{KeyPrefix: "blackhole", LeaseTTL: time.Minute, Platform: limits(1)})

	started := time.Now()
	_, err = a.Admit(context.Background(), request("org", "model"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("admission took %s against accept/no-reply Valkey", elapsed)
	}
}
