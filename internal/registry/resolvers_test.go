package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- helpers ----------------------------------------------------------------

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("mustURL(%q): %v", raw, err)
	}
	return u
}

func assertErrNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func assertURL(t *testing.T, got *url.URL, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected URL %q, got nil", want)
	}
	if got.String() != want {
		t.Fatalf("URL mismatch: got %q, want %q", got.String(), want)
	}
}

// ---- ConventionResolver tests -----------------------------------------------

func TestConventionResolver_Basic(t *testing.T) {
	r, err := NewConventionResolver(ConventionConfig{
		Template: "http://model-{id}.inference.svc.cluster.local:8000",
	})
	if err != nil {
		t.Fatal(err)
	}

	u, err := r.Resolve("llama3-70b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertURL(t, u, "http://model-llama3-70b.inference.svc.cluster.local:8000")
}

func TestConventionResolver_IDWithSpecialChars(t *testing.T) {
	r, err := NewConventionResolver(ConventionConfig{
		Template: "http://model-{id}.ns.svc:8080",
	})
	if err != nil {
		t.Fatal(err)
	}
	// IDs are resource names — typically alphanumeric with hyphens
	u, err := r.Resolve("my-model-42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertURL(t, u, "http://model-my-model-42.ns.svc:8080")
}

func TestConventionResolver_EmptyTemplate(t *testing.T) {
	_, err := NewConventionResolver(ConventionConfig{Template: ""})
	if err == nil {
		t.Fatal("expected error for empty template")
	}
}

func TestConventionResolver_TemplateProducesNoHost(t *testing.T) {
	// A template that produces a path-only URL has no host → ErrNotFound.
	r, err := NewConventionResolver(ConventionConfig{Template: "/just/a/path/{id}"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Resolve("anything")
	assertErrNotFound(t, err)
}

func TestConventionResolver_InvalidURLAfterSubstitution(t *testing.T) {
	// Template with spaces → url.Parse fails
	r, err := NewConventionResolver(ConventionConfig{Template: "http://model {id}.ns.svc:8000"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Resolve("x")
	if err == nil {
		t.Fatal("expected error for invalid URL after substitution")
	}
}

func TestConventionResolver_DifferentIDs(t *testing.T) {
	r, err := NewConventionResolver(ConventionConfig{
		Template: "http://svc-{id}.prod.svc.cluster.local:9000",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"alpha", "beta", "gamma"} {
		u, err := r.Resolve(id)
		if err != nil {
			t.Fatalf("resolve(%q): %v", id, err)
		}
		want := fmt.Sprintf("http://svc-%s.prod.svc.cluster.local:9000", id)
		assertURL(t, u, want)
	}
}

// ---- CachedResolver tests ---------------------------------------------------

// fakeClock is a manually-advanced mock clock for TTL tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func standardCacheConfig() CacheConfig {
	return CacheConfig{
		Size:        64,
		PositiveTTL: 60 * time.Second,
		NegativeTTL: 5 * time.Second,
	}
}

func makeCached(t *testing.T, lookup LookupFunc) (*CachedResolver, *fakeClock) {
	t.Helper()
	clk := newFakeClock(time.Unix(1_000_000, 0))
	r, err := NewCachedResolverWithClock(lookup, standardCacheConfig(), clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	return r, clk
}

func TestCachedResolver_CacheMiss_CallsLookup(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, id string) (*url.URL, error) {
		calls.Add(1)
		return mustURL(t, "http://upstream-"+id+":8000"), nil
	}
	r, _ := makeCached(t, lookup)

	u, err := r.Resolve("model-a")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u, "http://upstream-model-a:8000")
	if calls.Load() != 1 {
		t.Fatalf("expected 1 lookup call, got %d", calls.Load())
	}
}

func TestCachedResolver_CacheHit_SkipsLookup(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		calls.Add(1)
		return mustURL(t, "http://upstream:8000"), nil
	}
	r, _ := makeCached(t, lookup)

	for i := 0; i < 5; i++ {
		_, err := r.Resolve("model-b")
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 lookup call (cache hit), got %d", calls.Load())
	}
}

func TestCachedResolver_PositiveTTLExpiry(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		calls.Add(1)
		return mustURL(t, "http://upstream:8000"), nil
	}
	r, clk := makeCached(t, lookup)

	if _, err := r.Resolve("m"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("expected 1 call after first resolve")
	}

	// Advance past positive TTL.
	clk.Advance(61 * time.Second)

	if _, err := r.Resolve("m"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 lookup calls after TTL expiry, got %d", calls.Load())
	}
}

func TestCachedResolver_NegativeCache_NotFoundPropagates(t *testing.T) {
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		return nil, ErrNotFound
	}
	r, _ := makeCached(t, lookup)

	_, err := r.Resolve("gone")
	assertErrNotFound(t, err)
}

func TestCachedResolver_NegativeCache_ShortTTL(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		n := calls.Add(1)
		if n <= 2 {
			return nil, ErrNotFound
		}
		// Model was recreated; now returns a URL.
		return mustURL(t, "http://new-upstream:8000"), nil
	}
	r, clk := makeCached(t, lookup)

	// First call: not found, cached for NegativeTTL (5 s).
	if _, err := r.Resolve("reborn"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Second call within negative TTL: hits cache, no new lookup.
	if _, err := r.Resolve("reborn"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound (cached), got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 lookup (neg-cached), got %d", calls.Load())
	}

	// Advance past negative TTL (5 s + 1 ms).
	clk.Advance(6 * time.Second)

	// Now the negative cache is expired; lookup fires again. Still not found.
	if _, err := r.Resolve("reborn"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after negative TTL, got %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2nd lookup after neg-TTL expiry, got %d", calls.Load())
	}

	// Advance again so the second negative cache expires.
	clk.Advance(6 * time.Second)

	// Now the model is "back" — lookup returns a URL.
	u, err := r.Resolve("reborn")
	if err != nil {
		t.Fatalf("expected model to be found after recreation: %v", err)
	}
	assertURL(t, u, "http://new-upstream:8000")
}

func TestCachedResolver_TransientError_NotCachedLong(t *testing.T) {
	var calls atomic.Int32
	transientErr := errors.New("control-plane unavailable")
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		n := calls.Add(1)
		if n == 1 {
			return nil, transientErr
		}
		return mustURL(t, "http://upstream:8000"), nil
	}
	r, clk := makeCached(t, lookup)

	// First call: transient error → synthetic not-found, zero TTL (expired immediately).
	_, err := r.Resolve("flaky")
	if err == nil {
		t.Fatal("expected error on transient failure")
	}

	// Advance a tiny bit — the zero-TTL entry is already expired.
	clk.Advance(time.Millisecond)

	// Second call: lookup fires again and succeeds.
	u, err := r.Resolve("flaky")
	if err != nil {
		t.Fatalf("expected success on retry: %v", err)
	}
	assertURL(t, u, "http://upstream:8000")
	if calls.Load() != 2 {
		t.Fatalf("expected 2 lookup calls, got %d", calls.Load())
	}
}

func TestCachedResolver_Invalidate(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		calls.Add(1)
		return mustURL(t, "http://upstream:8000"), nil
	}
	r, _ := makeCached(t, lookup)

	if _, err := r.Resolve("x"); err != nil {
		t.Fatal(err)
	}
	r.Invalidate("x")
	if _, err := r.Resolve("x"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 lookups after Invalidate, got %d", calls.Load())
	}
}

func TestCachedResolver_ConcurrentResolve(t *testing.T) {
	var calls atomic.Int32
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		calls.Add(1)
		time.Sleep(5 * time.Millisecond) // simulate latency
		return mustURL(t, "http://upstream:8000"), nil
	}

	r, err := NewCachedResolverWithClock(lookup, standardCacheConfig(), time.Now)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 50
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = r.Resolve("concurrent-model")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, err)
		}
	}
	// Single-flight: all 50 goroutines should share ≤ 1 in-flight lookup.
	if calls.Load() > 2 {
		t.Fatalf("too many lookup calls during concurrent resolve: %d (expected ~1)", calls.Load())
	}
}

func TestCachedResolver_NilLookup(t *testing.T) {
	_, err := NewCachedResolver(nil, standardCacheConfig())
	if err == nil {
		t.Fatal("expected error for nil LookupFunc")
	}
}

func TestCachedResolver_InvalidConfig(t *testing.T) {
	lookup := func(_ context.Context, _ string) (*url.URL, error) { return nil, nil }

	if _, err := NewCachedResolver(lookup, CacheConfig{Size: 0, PositiveTTL: time.Second, NegativeTTL: time.Second}); err == nil {
		t.Fatal("expected error for Size=0")
	}
	if _, err := NewCachedResolver(lookup, CacheConfig{Size: 1, PositiveTTL: 0, NegativeTTL: time.Second}); err == nil {
		t.Fatal("expected error for PositiveTTL=0")
	}
	if _, err := NewCachedResolver(lookup, CacheConfig{Size: 1, PositiveTTL: time.Second, NegativeTTL: 0}); err == nil {
		t.Fatal("expected error for NegativeTTL=0")
	}
}

func TestCachedResolver_URLMutationSafe(t *testing.T) {
	// Use a manual parse so we don't need a testing.T inside the lookup.
	lookup := func(_ context.Context, _ string) (*url.URL, error) {
		u, _ := url.Parse("http://upstream:8000")
		return u, nil
	}
	r, err := NewCachedResolverWithClock(lookup, standardCacheConfig(), time.Now)
	if err != nil {
		t.Fatal(err)
	}

	u1, _ := r.Resolve("q")
	// Mutate the returned URL.
	u1.Host = "evil:9999"

	u2, _ := r.Resolve("q")
	// The cached copy must not have been mutated.
	if u2.Host == "evil:9999" {
		t.Fatal("cached URL was mutated by caller")
	}
}

// ---- ChainResolver tests ----------------------------------------------------

func TestChainResolver_FirstSucceeds(t *testing.T) {
	a := &fakeResolver{url: mustURL(t, "http://resolver-a:8000")}
	b := &fakeResolver{url: mustURL(t, "http://resolver-b:8000")}
	chain := ChainResolver{a, b}

	u, err := chain.Resolve("x")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u, "http://resolver-a:8000")
	if a.calls != 1 {
		t.Fatalf("expected a to be called once, got %d", a.calls)
	}
	if b.calls != 0 {
		t.Fatalf("expected b to be skipped, got %d calls", b.calls)
	}
}

func TestChainResolver_FirstNotFoundFallsThrough(t *testing.T) {
	a := &fakeResolver{err: ErrNotFound}
	b := &fakeResolver{url: mustURL(t, "http://resolver-b:8000")}
	chain := ChainResolver{a, b}

	u, err := chain.Resolve("x")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u, "http://resolver-b:8000")
}

func TestChainResolver_TransientErrorFallsThrough(t *testing.T) {
	// If the control-plane resolver returns a transient error (not ErrNotFound),
	// the chain should fall through to the convention fallback.
	a := &fakeResolver{err: errors.New("control-plane timeout")}
	b := &fakeResolver{url: mustURL(t, "http://convention:8000")}
	chain := ChainResolver{a, b}

	u, err := chain.Resolve("x")
	if err != nil {
		t.Fatalf("expected graceful degradation to convention resolver: %v", err)
	}
	assertURL(t, u, "http://convention:8000")
}

func TestChainResolver_AllFail_ReturnsLastError(t *testing.T) {
	a := &fakeResolver{err: ErrNotFound}
	b := &fakeResolver{err: ErrNotFound}
	chain := ChainResolver{a, b}

	_, err := chain.Resolve("x")
	assertErrNotFound(t, err)
}

func TestChainResolver_Empty(t *testing.T) {
	chain := ChainResolver{}
	_, err := chain.Resolve("x")
	assertErrNotFound(t, err)
}

// ---- CachedResolver wrapping ConventionResolver (composition test) ----------

func TestComposition_CachedOverConvention(t *testing.T) {
	// Demonstrate that a CachedResolver can wrap a ConventionResolver's Resolve
	// as its LookupFunc, providing caching on top of the zero-lookup path.
	conv, err := NewConventionResolver(ConventionConfig{
		Template: "http://model-{id}.ns.svc:8000",
	})
	if err != nil {
		t.Fatal(err)
	}

	lookup := LookupFunc(func(_ context.Context, id string) (*url.URL, error) {
		return conv.Resolve(id)
	})

	clk := newFakeClock(time.Unix(1_000_000, 0))
	cached, err := NewCachedResolverWithClock(lookup, standardCacheConfig(), clk.Now)
	if err != nil {
		t.Fatal(err)
	}

	u, err := cached.Resolve("gpt2")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u, "http://model-gpt2.ns.svc:8000")
}

func TestComposition_ChainCachedThenConvention(t *testing.T) {
	// The typical "cached control plane → convention fallback" chain.
	var lookupCalls atomic.Int32
	lookup := LookupFunc(func(_ context.Context, _ string) (*url.URL, error) {
		lookupCalls.Add(1)
		return nil, errors.New("control-plane is down")
	})
	cached, err := NewCachedResolver(lookup, standardCacheConfig())
	if err != nil {
		t.Fatal(err)
	}

	conv, err := NewConventionResolver(ConventionConfig{
		Template: "http://model-{id}.fallback.svc:8000",
	})
	if err != nil {
		t.Fatal(err)
	}

	chain := ChainResolver{cached, conv}

	u, err := chain.Resolve("phi3")
	if err != nil {
		t.Fatalf("expected fallback to convention: %v", err)
	}
	assertURL(t, u, "http://model-phi3.fallback.svc:8000")
}

// ---- Static resolver (existing) smoke test ----------------------------------

func TestStatic_Existing(t *testing.T) {
	fallback := mustURL(t, "http://fallback:9000")
	s := NewStatic(fallback)
	s.Set("known", mustURL(t, "http://known:8000"))

	u, err := s.Resolve("known")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u, "http://known:8000")

	u2, err := s.Resolve("unknown")
	if err != nil {
		t.Fatal(err)
	}
	assertURL(t, u2, "http://fallback:9000")

	s2 := NewStatic(nil)
	_, err = s2.Resolve("anything")
	assertErrNotFound(t, err)
}

// ---- fakeResolver helper ----------------------------------------------------

type fakeResolver struct {
	url   *url.URL
	err   error
	calls int
}

func (f *fakeResolver) Resolve(_ string) (*url.URL, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.url, nil
}
