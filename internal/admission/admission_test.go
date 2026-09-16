package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
)

func limits(active int64) config.AdmissionLimits {
	return config.AdmissionLimits{MaxActiveRequests: active, MaxConcurrentPrefills: active,
		MaxPromptBytes: 1024, MaxReservedOutputTokens: 1024, MaxActiveAdapters: active,
		RequestsPerWindow: 100, GeneratedTokensPerWindow: 1000, MaxColdHolds: active,
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
	return Request{Graph: "graph-a", Organization: org, Model: model, PromptBytes: 10, ReservedOutputTokens: 20, Adapter: true}
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

func TestKeepAlivePreventsLongStreamExpiry(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(1), LeaseTTL: 30 * time.Millisecond})
	l, err := a.Admit(context.Background(), request("a", "m"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.KeepAlive(ctx, func(e error) { t.Errorf("keepalive: %v", e) }) }()
	time.Sleep(75 * time.Millisecond)
	if _, err = a.Admit(context.Background(), request("b", "m")); err == nil {
		t.Fatal("live renewed stream was reaped")
	}
	cancel()
	<-done
	time.Sleep(40 * time.Millisecond)
	next, err := a.Admit(context.Background(), request("b", "m"))
	if err != nil {
		t.Fatalf("stopped keepalive was not reaped: %v", err)
	}
	_ = next.Complete(context.Background(), 0)
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

func TestStateFailureFailsClosed(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: 0})
	a := New(client, config.AdmissionSettings{Platform: limits(1), LeaseTTL: time.Minute, KeyPrefix: "down"})
	_, err := a.Admit(context.Background(), request("a", "m"))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want ErrUnavailable", err)
	}
}
