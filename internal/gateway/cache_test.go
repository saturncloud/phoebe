package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// countingResolver counts inner calls and serves from a fixed map (missing key
// = ErrNotFound), or a fixed error when err is set.
type countingResolver struct {
	calls int32
	m     map[[2]string]Resolution
	err   error
}

func (c *countingResolver) Resolve(_ context.Context, org, model string) (Resolution, error) {
	atomic.AddInt32(&c.calls, 1)
	if c.err != nil {
		return Resolution{}, c.err
	}
	if r, ok := c.m[[2]string{org, model}]; ok {
		return r, nil
	}
	return Resolution{}, ErrNotFound
}

// testCache builds a Cache over inner with a mutable fake clock, returning the
// cache and the clock-advance func.
func testCache(inner Resolver, pos, neg time.Duration) (*Cache, func(time.Duration)) {
	c := NewCache(inner, pos, neg)
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }
	return c, func(d time.Duration) { now = now.Add(d) }
}

// TestCache_PositiveHitWithinTTL: a resolved (org, model) is served from cache
// until the positive TTL elapses, then re-resolved.
func TestCache_PositiveHitWithinTTL(t *testing.T) {
	inner := &countingResolver{m: map[[2]string]Resolution{
		{"org-1", "bot"}: {ResourceID: "tfm-1", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g"},
	}}
	c, advance := testCache(inner, 30*time.Second, 5*time.Second)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		r, err := c.Resolve(ctx, "org-1", "bot")
		if err != nil || r.ResourceID != "tfm-1" {
			t.Fatalf("Resolve #%d: r=%+v err=%v", i, r, err)
		}
	}
	if n := atomic.LoadInt32(&inner.calls); n != 1 {
		t.Fatalf("inner calls = %d, want 1 (positive hit cached)", n)
	}

	// Just before expiry: still cached. At/after expiry: re-resolved.
	advance(29 * time.Second)
	if _, err := c.Resolve(ctx, "org-1", "bot"); err != nil {
		t.Fatalf("Resolve pre-expiry: %v", err)
	}
	if n := atomic.LoadInt32(&inner.calls); n != 1 {
		t.Fatalf("inner calls = %d, want 1 before TTL expiry", n)
	}
	advance(2 * time.Second)
	if _, err := c.Resolve(ctx, "org-1", "bot"); err != nil {
		t.Fatalf("Resolve post-expiry: %v", err)
	}
	if n := atomic.LoadInt32(&inner.calls); n != 2 {
		t.Fatalf("inner calls = %d, want 2 after TTL expiry", n)
	}
}

// TestCache_NegativeHitWithinShorterTTL: ErrNotFound is cached, but only for
// the (shorter) negative TTL — a just-created model becomes resolvable fast.
func TestCache_NegativeHitWithinShorterTTL(t *testing.T) {
	inner := &countingResolver{m: map[[2]string]Resolution{}}
	c, advance := testCache(inner, 30*time.Second, 5*time.Second)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := c.Resolve(ctx, "org-1", "ghost"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Resolve #%d err = %v, want ErrNotFound", i, err)
		}
	}
	if n := atomic.LoadInt32(&inner.calls); n != 1 {
		t.Fatalf("inner calls = %d, want 1 (negative hit cached)", n)
	}

	// The model appears in the DB; after the NEGATIVE TTL (not the positive
	// one) it resolves.
	inner.m[[2]string{"org-1", "ghost"}] = Resolution{ResourceID: "tfm-9", GraphK8sName: "g"}
	advance(6 * time.Second)
	r, err := c.Resolve(ctx, "org-1", "ghost")
	if err != nil || r.ResourceID != "tfm-9" {
		t.Fatalf("post-negative-TTL Resolve: r=%+v err=%v", r, err)
	}
	if n := atomic.LoadInt32(&inner.calls); n != 2 {
		t.Fatalf("inner calls = %d, want 2", n)
	}
}

// TestCache_ErrorsNeverCached: a DB failure is surfaced per call and NOT
// cached — the next request retries immediately instead of pinning the outage
// for a TTL, and a failure never masquerades as a 404.
func TestCache_ErrorsNeverCached(t *testing.T) {
	inner := &countingResolver{err: errors.New("db down")}
	c, _ := testCache(inner, 30*time.Second, 5*time.Second)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		_, err := c.Resolve(ctx, "org-1", "bot")
		if err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("Resolve #%d err = %v, want a non-NotFound error", i, err)
		}
		if n := atomic.LoadInt32(&inner.calls); n != int32(i) {
			t.Fatalf("inner calls = %d, want %d (errors must not be cached)", n, i)
		}
	}

	// Recovery is immediate: the moment the DB answers, the cache serves it.
	inner.err = nil
	inner.m = map[[2]string]Resolution{{"org-1", "bot"}: {ResourceID: "tfm-1", GraphK8sName: "g"}}
	if r, err := c.Resolve(ctx, "org-1", "bot"); err != nil || r.ResourceID != "tfm-1" {
		t.Fatalf("post-recovery Resolve: r=%+v err=%v", r, err)
	}
}

// TestCache_KeysAreOrgScoped: the cache key includes the org — org B's lookup
// of the same model name is a separate entry (never a cross-tenant cache hit).
func TestCache_KeysAreOrgScoped(t *testing.T) {
	inner := &countingResolver{m: map[[2]string]Resolution{
		{"org-a", "bot"}: {ResourceID: "tfm-a", GraphK8sName: "g"},
	}}
	c, _ := testCache(inner, 30*time.Second, 5*time.Second)
	ctx := context.Background()

	if r, err := c.Resolve(ctx, "org-a", "bot"); err != nil || r.ResourceID != "tfm-a" {
		t.Fatalf("org-a: r=%+v err=%v", r, err)
	}
	// org-b asking for the same model name must NOT hit org-a's entry.
	if _, err := c.Resolve(ctx, "org-b", "bot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("org-b err = %v, want ErrNotFound (no cross-tenant cache hit)", err)
	}
	if n := atomic.LoadInt32(&inner.calls); n != 2 {
		t.Fatalf("inner calls = %d, want 2 (distinct keys)", n)
	}
}

// TestCache_BoundedUnderUniqueKeyFlood: past maxCacheEntries live entries, new
// results are served correctly but not stored (and expired entries are pruned)
// — an attacker streaming unique unknown model names cannot grow the map
// without bound.
func TestCache_BoundedUnderUniqueKeyFlood(t *testing.T) {
	inner := &countingResolver{m: map[[2]string]Resolution{}}
	c, advance := testCache(inner, 30*time.Second, 5*time.Minute) // long negative TTL: entries stay live
	ctx := context.Background()

	for i := 0; i < maxCacheEntries+100; i++ {
		_, err := c.Resolve(ctx, "org-1", fmt.Sprintf("ghost-%d", i))
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Resolve #%d err = %v, want ErrNotFound", i, err)
		}
	}
	c.mu.Lock()
	n := len(c.entries)
	c.mu.Unlock()
	if n > maxCacheEntries {
		t.Fatalf("cache grew to %d entries, cap is %d", n, maxCacheEntries)
	}

	// Once the flood's entries expire, storage resumes (prune-then-insert).
	advance(6 * time.Minute)
	if _, err := c.Resolve(ctx, "org-1", "late-ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late Resolve err = %v, want ErrNotFound", err)
	}
	c.mu.Lock()
	_, stored := c.entries[cacheKey{org: "org-1", model: "late-ghost"}]
	c.mu.Unlock()
	if !stored {
		t.Fatal("expired entries should be pruned and the new entry stored")
	}
}

// TestNewCache_DefaultTTLs: non-positive TTLs take the package defaults.
func TestNewCache_DefaultTTLs(t *testing.T) {
	c := NewCache(&countingResolver{}, 0, -1)
	if c.positiveTTL != DefaultPositiveTTL || c.negativeTTL != DefaultNegativeTTL {
		t.Fatalf("TTLs = %v/%v, want defaults %v/%v",
			c.positiveTTL, c.negativeTTL, DefaultPositiveTTL, DefaultNegativeTTL)
	}
}
