package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// LookupFunc is the seam for a control-plane call that resolves a resource ID
// to its upstream URL.
//
// NOTE (verify-gate): The design requires confirmation that the auth-server
// already resolves model/deployment resources through the X-Saturn-Resource-Id
// path before this seam is wired to a real Atlas call. The exact API shape
// (endpoint, auth, response schema) is TBD pending that verification. Build
// the seam here; wire the real call once the contract is confirmed.
//
// LookupFunc must return ErrNotFound (or an error wrapping it) when the
// resource is unknown or torn down. Any other non-nil error is treated as a
// transient control-plane failure.
type LookupFunc func(ctx context.Context, resourceID string) (*url.URL, error)

// cacheEntry is a single TTL-tracked entry in the CachedResolver's LRU.
type cacheEntry struct {
	url       *url.URL // nil for a negative (not-found) entry
	notFound  bool
	expiresAt time.Time
}

func (e *cacheEntry) expired(now time.Time) bool {
	return now.After(e.expiresAt)
}

// CachedResolver wraps a LookupFunc with an LRU cache and per-entry TTLs so
// the control plane is not hammered on every request.
//
// Positive TTL: how long a found upstream is cached before re-validating.
// Negative TTL: how long a not-found result is cached. This MUST be short —
// a newly-created model must become reachable quickly — but non-zero to
// prevent a lookup storm when a model is torn down and many requests arrive.
//
// The clock is injectable (defaulting to time.Now) for deterministic tests.
//
// Concurrent Resolve calls for the same resource ID are safe; a single-flight
// guard (singleflight-style per-key mutex) prevents a thundering herd on cache
// miss: only one goroutine performs the lookup; others wait and share the result.
type CachedResolver struct {
	lookup   LookupFunc
	cfg      CacheConfig
	clock    func() time.Time
	cache    *lru.Cache[string, *cacheEntry]
	inflight sync.Map // map[string]*call
}

// call is a pending lookup for a single resource ID.
// done is closed when val is ready; readers wait on it with <-c.done.
type call struct {
	done chan struct{}
	val  *cacheEntry
}

// CacheConfig holds CachedResolver settings.
type CacheConfig struct {
	// Size is the maximum number of entries in the LRU cache. Required > 0.
	Size int

	// PositiveTTL is how long a found upstream URL is cached.
	PositiveTTL time.Duration

	// NegativeTTL is how long a not-found result is cached. Should be short
	// (e.g. 5–30 s) so newly-created models become reachable quickly.
	NegativeTTL time.Duration
}

// NewCachedResolver creates a CachedResolver wrapping the given LookupFunc.
func NewCachedResolver(lookup LookupFunc, cfg CacheConfig) (*CachedResolver, error) {
	return NewCachedResolverWithClock(lookup, cfg, time.Now)
}

// NewCachedResolverWithClock is like NewCachedResolver but accepts an
// injectable clock, enabling deterministic TTL tests.
func NewCachedResolverWithClock(lookup LookupFunc, cfg CacheConfig, clock func() time.Time) (*CachedResolver, error) {
	if lookup == nil {
		return nil, fmt.Errorf("registry: CachedResolver requires a non-nil LookupFunc")
	}
	if cfg.Size <= 0 {
		return nil, fmt.Errorf("registry: CachedResolver requires Size > 0")
	}
	if cfg.PositiveTTL <= 0 {
		return nil, fmt.Errorf("registry: CachedResolver requires PositiveTTL > 0")
	}
	if cfg.NegativeTTL <= 0 {
		return nil, fmt.Errorf("registry: CachedResolver requires NegativeTTL > 0")
	}
	cache, err := lru.New[string, *cacheEntry](cfg.Size)
	if err != nil {
		return nil, fmt.Errorf("registry: could not create LRU cache: %w", err)
	}
	return &CachedResolver{
		lookup: lookup,
		cfg:    cfg,
		clock:  clock,
		cache:  cache,
	}, nil
}

// Resolve returns the upstream URL for resourceID, using the cache when
// possible. On a cache miss it calls the LookupFunc (with single-flight
// deduplication), caches the result, and returns it.
func (r *CachedResolver) Resolve(resourceID string) (*url.URL, error) {
	now := r.clock()

	if entry, ok := r.cache.Get(resourceID); ok && !entry.expired(now) {
		if entry.notFound {
			return nil, ErrNotFound
		}
		return cloneURL(entry.url), nil
	}

	// Cache miss (or expired): single-flight the lookup so concurrent callers
	// share one in-flight request rather than all hammering the control plane.
	// We pre-initialize the call with a closed-channel signal so that waiters
	// block on <-c.done (channel close is a proper happens-before edge).
	newCall := &call{done: make(chan struct{})}
	actual, loaded := r.inflight.LoadOrStore(resourceID, newCall)
	c := actual.(*call)
	if loaded {
		// Another goroutine is already looking this up — wait for it.
		<-c.done
	} else {
		// We own the lookup.
		c.val = r.doLookup(resourceID)
		close(c.done) // signal all waiters; happens-before any <-c.done read
		r.inflight.Delete(resourceID)
	}

	entry := c.val
	if entry.notFound {
		return nil, ErrNotFound
	}
	return cloneURL(entry.url), nil
}

// doLookup performs the actual LookupFunc call and stores the result in the
// cache (positive or negative) before returning the entry.
func (r *CachedResolver) doLookup(resourceID string) *cacheEntry {
	now := r.clock()
	u, err := r.lookup(context.Background(), resourceID)
	var entry *cacheEntry
	switch {
	case err == nil:
		entry = &cacheEntry{
			url:       u,
			notFound:  false,
			expiresAt: now.Add(r.cfg.PositiveTTL),
		}
	case errors.Is(err, ErrNotFound):
		entry = &cacheEntry{
			notFound:  true,
			expiresAt: now.Add(r.cfg.NegativeTTL),
		}
	default:
		// Transient error: do NOT cache, return a synthetic not-found so the
		// caller gets a clean error. The next Resolve will retry.
		entry = &cacheEntry{notFound: true, expiresAt: now} // zero TTL → expired immediately
	}
	r.cache.Add(resourceID, entry)
	return entry
}

// Invalidate evicts the entry for resourceID from the cache, forcing the next
// Resolve to perform a fresh lookup.
func (r *CachedResolver) Invalidate(resourceID string) {
	r.cache.Remove(resourceID)
}

// cloneURL returns a shallow copy of u so callers cannot mutate the cached URL.
func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	dup := *u
	return &dup
}

// ChainResolver tries each resolver in order, returning the first successful
// result. If a resolver returns ErrNotFound, the chain continues. If a
// resolver returns any other error, the chain also continues (graceful
// degradation: a control-plane outage falls through to the naming-convention
// resolver, which needs no network). The final resolver's error is returned
// if all resolvers fail.
//
// Typical composition:
//
//	ChainResolver{cachedControlPlane, conventionFallback}
//
// This ensures:
//   - Happy path: control plane is authoritative.
//   - Degraded path: if the control plane is down, the naming-convention
//     guess is better than failing all traffic.
//   - Torn-down models: both resolvers must return ErrNotFound (or error) for
//     a 404 to propagate; a naming-convention URL that still resolves is used.
type ChainResolver []Resolver

// Resolve iterates the chain, returning the first successful URL.
func (c ChainResolver) Resolve(resourceID string) (*url.URL, error) {
	var lastErr error
	for _, r := range c {
		u, err := r.Resolve(resourceID)
		if err == nil {
			return u, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrNotFound
	}
	return nil, lastErr
}

// Config is the top-level registry configuration; embed or reference from the
// global config.Settings if desired, or construct directly.
type Config struct {
	// Strategy selects the active resolver: "static", "convention", "cached",
	// or "chain" (cached → convention fallback).
	Strategy string `yaml:"strategy"`

	// Convention holds settings for the ConventionResolver.
	Convention ConventionConfig `yaml:"convention"`

	// Cache holds settings for the CachedResolver.
	Cache CacheConfig `yaml:"cache"`
}
