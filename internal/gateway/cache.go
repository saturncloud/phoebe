package gateway

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Cache TTLs. Positive entries ride the (slow-moving) tf_model lifecycle — a
// model created/deleted mid-TTL is visible within 30s, which matches the
// create/teardown latency of the serving graph itself. Negative entries are
// deliberately much shorter: a just-created model must not keep 404ing for
// long after Atlas commits its row.
const (
	DefaultPositiveTTL = 30 * time.Second
	DefaultNegativeTTL = 5 * time.Second
)

// maxCacheEntries bounds the cache map. The key contains the CLIENT-CHOSEN
// model string, so an attacker holding any valid gateway credential could
// otherwise grow the map without bound by streaming unique unknown names
// (each minting a negative entry). At the cap, expired entries are pruned;
// if the cache is still full the new entry is simply not stored — resolution
// stays correct (the DB answered), only uncached.
const maxCacheEntries = 4096

type cacheKey struct{ org, model string }

type cacheEntry struct {
	res      Resolution
	notFound bool // caches ErrNotFound (never other errors)
	expires  time.Time
}

// Cache is a TTL cache in front of a Resolver, so the per-request hot path
// does not hit Postgres. Semantics:
//
//   - HIT (positive): the cached Resolution, no inner call.
//   - HIT (negative): ErrNotFound, no inner call — a burst of requests for an
//     unknown model costs one DB query per negativeTTL, not one per request.
//   - MISS: one inner call; the result (found or ErrNotFound) is cached.
//   - Inner ERROR (DB down): returned to the caller and NEVER cached — the
//     next request retries the DB immediately rather than pinning an outage
//     for a TTL. The caller 503s per request; correctness never degrades to
//     serving unattributed.
//
// Concurrent misses for one key may each query the DB (no singleflight):
// duplicate reads of one indexed row are cheaper than the coordination, and
// the window is one TTL.
type Cache struct {
	inner       Resolver
	positiveTTL time.Duration
	negativeTTL time.Duration

	// now is the clock, a field so tests can drive TTL expiry deterministically.
	now func() time.Time

	mu      sync.Mutex
	entries map[cacheKey]cacheEntry
}

// NewCache wraps inner with a TTL cache. Non-positive TTLs take the defaults.
func NewCache(inner Resolver, positiveTTL, negativeTTL time.Duration) *Cache {
	if positiveTTL <= 0 {
		positiveTTL = DefaultPositiveTTL
	}
	if negativeTTL <= 0 {
		negativeTTL = DefaultNegativeTTL
	}
	return &Cache{
		inner:       inner,
		positiveTTL: positiveTTL,
		negativeTTL: negativeTTL,
		now:         time.Now,
		entries:     map[cacheKey]cacheEntry{},
	}
}

// Resolve implements Resolver with the caching semantics above.
func (c *Cache) Resolve(ctx context.Context, orgID, model string) (Resolution, error) {
	k := cacheKey{org: orgID, model: model}
	now := c.now()

	c.mu.Lock()
	if e, ok := c.entries[k]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		if e.notFound {
			return Resolution{}, ErrNotFound
		}
		return e.res, nil
	}
	c.mu.Unlock()

	res, err := c.inner.Resolve(ctx, orgID, model)
	switch {
	case err == nil:
		c.store(k, cacheEntry{res: res, expires: now.Add(c.positiveTTL)})
		return res, nil
	case errors.Is(err, ErrNotFound):
		c.store(k, cacheEntry{notFound: true, expires: now.Add(c.negativeTTL)})
		return Resolution{}, ErrNotFound
	default:
		// DB/lookup failure: surface it, cache nothing (see the type comment).
		return Resolution{}, err
	}
}

// store inserts an entry, enforcing maxCacheEntries (prune expired first; if
// still full, skip storing — never evict live entries at random).
func (c *Cache) store(k cacheKey, e cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= maxCacheEntries {
		now := c.now()
		for key, old := range c.entries {
			if !now.Before(old.expires) {
				delete(c.entries, key)
			}
		}
		if len(c.entries) >= maxCacheEntries {
			return
		}
	}
	c.entries[k] = e
}
