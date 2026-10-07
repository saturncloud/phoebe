package admission

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	// pgx stdlib driver registers itself as "pgx" with database/sql — the same
	// driver/DSN convention as the drainer and the rater. The spend check reads
	// Postgres (group_usage, written by the rater), NOT the Valkey admission
	// store, so the Valkey-outage ruling's store is not in this path.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
)

// groupSpendCacheTTL is how long a group's monthly-spend verdict is reused
// before re-reading Postgres. Ruling-scale: ~60 seconds per group (the spend
// sum moves hourly, when the rater writes a window, so a tighter TTL would
// only add load). A constant, not a config knob: the check is deliberately a
// coarse guardrail, and an operator-tunable TTL buys nothing the hourly
// rating cadence doesn't already bound.
const groupSpendCacheTTL = time.Minute

// GroupSpendStore is the admission package's seam onto the group spend the
// monthly cap compares against. It is an interface so admission can be tested
// against a fake and the SQL tested in isolation via sqlmock, mirroring the
// drainer/rater store pattern.
type GroupSpendStore interface {
	// GroupSpendExhausted reports whether the group's spend in the CURRENT
	// CALENDAR MONTH has reached the cap: SUM(cost) over group_usage for
	// window_start >= date_trunc('month', now()), compared in Postgres
	// (NUMERIC, never a Go number). cap is a plain decimal NUMERIC(20,9)
	// string straight from the trusted envelope. The verdict — not the spend
	// sum — crosses this seam, so no money value becomes a Go number.
	GroupSpendExhausted(ctx context.Context, groupID, cap string) (bool, error)
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
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("admission: spend check: open postgres: %w", err)
	}
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
// number. An empty group_usage month (no rating yet, or a group with no
// attributed usage) sums to NULL → COALESCE to 0 → the cap is unreached until
// rated spend says otherwise.
const groupSpendExhaustedSQL = `
SELECT COALESCE(SUM(cost), 0) >= $2::numeric
FROM group_usage
WHERE group_id = $1
  AND window_start >= date_trunc('month', now())`

func (s *PostgresSpendStore) GroupSpendExhausted(ctx context.Context, groupID, cap string) (bool, error) {
	var exhausted bool
	if err := s.db.QueryRowContext(ctx, groupSpendExhaustedSQL, groupID, cap).Scan(&exhausted); err != nil {
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
	at        time.Time
}

func newSpendVerdictCache() *spendVerdictCache {
	return &spendVerdictCache{entries: map[string]spendVerdictEntry{}}
}

// lookup returns the cached verdict for (groupID, cap) and whether it is
// fresh enough to trust.
func (c *spendVerdictCache) lookup(groupID, cap string) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[groupID+"\x00"+cap]
	if !ok {
		return false, false
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	if now().Sub(e.at) >= groupSpendCacheTTL {
		return false, false
	}
	return e.exhausted, true
}

// store records a fresh verdict for (groupID, cap).
func (c *spendVerdictCache) store(groupID, cap string, exhausted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	c.entries[groupID+"\x00"+cap] = spendVerdictEntry{exhausted: exhausted, at: now()}
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
}

func (l *spendFailureLog) logf(log *logging.Logger, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
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
