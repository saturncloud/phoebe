package admission

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
)

// groupSpendCacheTTL is how long a group's monthly-spend verdict is reused
// before re-reading Postgres. Ruling-scale: ~60 seconds per group (the spend
// sum moves hourly, when the rater writes a window, so a tighter TTL would
// only add load). A constant, not a config knob: the check is deliberately a
// coarse guardrail, and an operator-tunable TTL buys nothing the hourly
// rating cadence doesn't already bound.
const groupSpendCacheTTL = time.Minute

// groupSpendNegativeCacheTTL is how long a fail-open verdict recorded after a
// store error is reused before re-reading Postgres. Short on purpose: the
// negative entry only absorbs the re-query storm while the store is down —
// past it the check re-probes, so recovery is seen within seconds rather than
// after a full verdict TTL.
const groupSpendNegativeCacheTTL = 10 * time.Second

// groupSpendQueryBudget bounds one spend-check read on the request path. The
// check runs synchronously before the Valkey reservation, so a slow or
// blackholed Postgres must not hold the request: past this budget the read is
// abandoned and the check fails open like any other store error. It sits well
// inside admitOperationBudget so the reservation keeps its own time.
const groupSpendQueryBudget = 250 * time.Millisecond

// groupSpendStarvedFloor is the remaining shared budget below which a read
// that times out is treated as starved by the earlier reads in the same
// request rather than as a slow store. A read that starts with this little
// time cannot finish against a healthy Postgres either, so its timeout says
// nothing about the store and is neither logged nor negative-cached.
const groupSpendStarvedFloor = 25 * time.Millisecond

// groupSpendConnectTimeout bounds establishing one pool connection, so a
// Postgres that drops packets cannot pin a dial (and with it one of the four
// pool slots) forever.
const groupSpendConnectTimeout = 2 * time.Second

// GroupSpendStore is the admission package's seam onto the group spend the
// monthly cap compares against. It is an interface so admission can be tested
// against a fake and the SQL tested in isolation via sqlmock, mirroring the
// drainer/rater store pattern.
type GroupSpendStore interface {
	// GroupSpendExhausted reports whether the group's spend in the CURRENT
	// CALENDAR MONTH has reached the cap: SUM(cost) over group_usage for
	// window_start >= the start of the current UTC month, compared in Postgres
	// (NUMERIC, never a Go number). cap is a plain decimal NUMERIC(20,9)
	// string straight from the trusted envelope. The verdict — not the spend
	// sum — crosses this seam, so no money value becomes a Go number.
	GroupSpendExhausted(ctx context.Context, groupID, spendCap string) (bool, error)
}

// PostgresSpendStore answers the monthly spend check against group_usage
// (migration 0008), in phoebe's own Postgres — the same DSN convention
// (DATABASE_URL) as the drainer and the rater.
type PostgresSpendStore struct {
	db *sql.DB
}

// OpenPostgresSpendStore opens the spend-check pool. Failure modes are the
// caller's call: cmd/interceptor logs loudly and runs WITHOUT spend caps
// (group rate limits still enforce from the envelope) rather than refusing to
// serve — the 2026-09-24 posture: quotas are permissible, they never take
// inference down.
func OpenPostgresSpendStore(ctx context.Context, databaseURL string) (*PostgresSpendStore, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("admission: spend check: DATABASE_URL is empty (group_usage lives in phoebe's Postgres; the spend cap cannot be checked without it)")
	}
	// The pgx stdlib driver, opened from a parsed config so the connect
	// timeout can be set — the same driver/DSN convention as the drainer and
	// the rater. The spend check reads Postgres (group_usage, written by the
	// rater), NOT the Valkey admission store, so the Valkey-outage ruling's
	// store is not in this path.
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("admission: spend check: parse DATABASE_URL: %w", err)
	}
	// A connect_timeout given in the DSN wins; otherwise dials are bounded
	// here rather than left to the kernel's TCP timeout.
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = groupSpendConnectTimeout
	}
	db := stdlib.OpenDB(*cfg)
	// The hot path is served from the in-process verdict cache: the pool sees
	// at most one query per (group, cap) per TTL, so a handful of connections
	// is generous.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	s := &PostgresSpendStore{db: db}
	if err := s.db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("admission: spend check: postgres ping: %w", err)
	}
	return s, nil
}

// NewPostgresSpendStore wraps an existing *sql.DB (tests / caller-owned pools).
func NewPostgresSpendStore(db *sql.DB) *PostgresSpendStore { return &PostgresSpendStore{db: db} }

func (s *PostgresSpendStore) Close() error { return s.db.Close() }

// groupSpendExhaustedSQL compares the group's month-to-date attribution spend
// against the cap in ONE round-trip, in Postgres, so money never becomes a Go
// number. The month boundary is computed in UTC explicitly: the rater fills
// window_start from UTC hour buckets, so a boundary taken in the session's
// TimeZone would shift the month by the session's UTC offset. An empty
// group_usage month (no rating yet, or a group with no
// attributed usage) sums to NULL → COALESCE to 0 → the cap is unreached until
// rated spend says otherwise.
const groupSpendExhaustedSQL = `
SELECT COALESCE(SUM(cost), 0) >= $2::numeric
FROM group_usage
WHERE group_id = $1
  AND window_start >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'`

func (s *PostgresSpendStore) GroupSpendExhausted(ctx context.Context, groupID, spendCap string) (bool, error) {
	var exhausted bool
	if err := s.db.QueryRowContext(ctx, groupSpendExhaustedSQL, groupID, spendCap).Scan(&exhausted); err != nil {
		return false, fmt.Errorf("admission: spend check: group %s: %w", groupID, err)
	}
	return exhausted, nil
}

// spendVerdictCache is the per-(group, cap) verdict cache: one entry per
// distinct cap a group has been checked against, refreshed after
// groupSpendCacheTTL. The TTL is measured from the verdict's capture, so a
// hot group re-reads Postgres at most once per TTL; a quiet group's entry is
// simply stale on its next request. Unbounded growth is not a hazard: the
// key set is bounded by the distinct (group, cap) pairs actually stamped on
// arriving envelopes (at most 16 groups per request).
type spendVerdictCache struct {
	mu      sync.Mutex
	entries map[string]spendVerdictEntry
	// now exists so a test can simulate the passage of time; nil selects
	// time.Now (production).
	now func() time.Time
}

type spendVerdictEntry struct {
	exhausted bool
	// failed marks a fail-open entry recorded after a store error: it admits
	// without querying and without waiting, its bypass already logged once
	// (throttled) by the request that took the error. It expires after the
	// short groupSpendNegativeCacheTTL and is not subject to the UTC month
	// rollover — a fail-open verdict never denies, so letting it lapse a few
	// seconds into a new month is harmless.
	failed bool
	at     time.Time
}

func newSpendVerdictCache() *spendVerdictCache {
	return &spendVerdictCache{entries: map[string]spendVerdictEntry{}}
}

// lookup returns the cached verdict for (groupID, cap) and whether it is
// fresh enough to trust. An entry expires at the earlier of its TTL and the
// next UTC month boundary after its capture: the cap counts calendar-month
// spend, so a verdict taken in the old month says nothing about the new one.
// The negative (store-error) entries use the shorter groupSpendNegativeCacheTTL
// and skip the month check — see spendVerdictEntry.failed.
func (c *spendVerdictCache) lookup(groupID, spendCap string) (spendVerdictEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[groupID+"\x00"+spendCap]
	if !ok {
		return spendVerdictEntry{}, false
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	t := now()
	ttl := groupSpendCacheTTL
	if e.failed {
		ttl = groupSpendNegativeCacheTTL
	}
	if t.Sub(e.at) >= ttl || (!e.failed && !t.Before(nextUTCMonth(e.at))) {
		return spendVerdictEntry{}, false
	}
	return e, true
}

// store records a fresh verdict for (groupID, cap).
func (c *spendVerdictCache) store(groupID, spendCap string, exhausted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	c.entries[groupID+"\x00"+spendCap] = spendVerdictEntry{exhausted: exhausted, at: now()}
}

// storeFailure records a fail-open entry for (groupID, cap) after a store
// error: for groupSpendNegativeCacheTTL the check admits without querying and
// without waiting, then re-probes the store.
func (c *spendVerdictCache) storeFailure(groupID, spendCap string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	c.entries[groupID+"\x00"+spendCap] = spendVerdictEntry{failed: true, at: now()}
}

// nextUTCMonth returns the first instant of the UTC calendar month after t's.
func nextUTCMonth(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

// spendFailureLog throttles the fail-open spend-check ERROR lines: the first
// bypass of an incident logs immediately, then at most one line per
// spendFailureLogInterval while the incident persists, carrying the count of
// suppressed occurrences. Without it a Postgres outage would emit one ERROR
// per admitted request carrying a group spend cap — flooding the log exactly
// when the check is down.
const spendFailureLogInterval = time.Minute

type spendFailureLog struct {
	mu         sync.Mutex
	last       time.Time
	suppressed int
	// now is the clock; nil means time.Now. Tests inject a fixed clock to
	// step across spendFailureLogInterval without sleeping.
	now func() time.Time
}

func (l *spendFailureLog) logf(log *logging.Logger, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	clock := time.Now
	if l.now != nil {
		clock = l.now
	}
	now := clock()
	if now.Sub(l.last) < spendFailureLogInterval {
		l.suppressed++
		return
	}
	l.last = now
	msg := fmt.Sprintf(format, args...)
	if l.suppressed > 0 {
		log.Error.Printf("%s (+%d similar suppressed)", msg, l.suppressed)
		l.suppressed = 0
		return
	}
	log.Error.Printf("%s", msg)
}

// spendFlight collapses concurrent in-flight spend-check reads per (group,
// cap): while one request reads Postgres for a key, every other request on
// the same key waits for that single read instead of issuing its own, so a
// blackholed store with N concurrent capped requests makes one query, not N.
// golang.org/x/sync is only an indirect dependency here, so this is a small
// hand-rolled in-flight map (a mutex and one call struct per key).
type spendFlight struct {
	mu sync.Mutex
	in map[string]*spendFlightCall
	// joined, when set, is called each time a caller joins an in-flight read
	// as a waiter. It exists so a test can release the read only after a
	// waiter has joined it; nil in production.
	joined func(key string)
}

type spendFlightCall struct {
	done   chan struct{}
	result spendFlightResult
}

type spendFlightResult struct {
	exhausted bool
	err       error
}

func newSpendFlight() *spendFlight {
	return &spendFlight{in: map[string]*spendFlightCall{}}
}

// do runs fn once per key while any caller is in flight and returns its
// result to every caller: concurrent waiters take the leader's verdict, or
// its error (the fail-open path), without re-running the query. The leader
// reports leader=true so exactly one caller records and logs the outcome.
// A waiter stops waiting when its own ctx is done (its share of the spend
// query budget ran out, or its client left) and gets ctx's error, which the
// caller treats as fail-open; the leader's read keeps running for the others.
func (f *spendFlight) do(ctx context.Context, key string, fn func() (bool, error)) (spendFlightResult, bool) {
	f.mu.Lock()
	if call, ok := f.in[key]; ok {
		joined := f.joined
		f.mu.Unlock()
		if joined != nil {
			joined(key)
		}
		select {
		case <-call.done:
			return call.result, false
		case <-ctx.Done():
			return spendFlightResult{err: ctx.Err()}, false
		}
	}
	call := &spendFlightCall{done: make(chan struct{})}
	f.in[key] = call
	f.mu.Unlock()

	call.result.exhausted, call.result.err = fn()
	close(call.done)

	f.mu.Lock()
	delete(f.in, key)
	f.mu.Unlock()
	return call.result, true
}
