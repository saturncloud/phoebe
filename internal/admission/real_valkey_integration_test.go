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
