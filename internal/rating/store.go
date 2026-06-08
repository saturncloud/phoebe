package rating

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	// pgx stdlib driver registers itself as "pgx" with database/sql. Same choice
	// as internal/drain: standard database/sql so the store is a thin, mockable
	// seam (sqlmock) and pool tuning is the familiar API.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Rollup is one (auth_id, model, hour) cost rollup, the unit written to
// rated_usage. The aggregator produces these; the Store upserts them.
type Rollup struct {
	AuthID               string
	Model                string
	WindowStart          time.Time // truncated to the hour (UTC)
	WindowEnd            time.Time // WindowStart + 1h
	PromptTokens         int64
	CachedTokens         int64
	CompletionTokens     int64
	BillablePromptTokens int64
	CostMicroUSD         int64
	EventCount           int
}

// Store is rating's data seam. It is an interface so the rating loop can be
// unit-tested against a fake and the Postgres SQL tested in isolation via
// sqlmock. Mirrors internal/drain.Store. The contract:
//
//   - LoadPrices returns the WHOLE price book (all models, all effective rows).
//     Rating snapshots it once per run.
//   - ReadWindow returns the billing_event rows whose rating instant (event_ts,
//     or created_at when event_ts is null) falls in [start, end). Half-open so
//     adjacent windows never double-count a boundary event.
//   - UpsertRollups writes the rollups idempotently: ON CONFLICT on the
//     (auth_id, model, window_start) unique key it REPLACES the row, so a re-run
//     of the same window reconciles rather than doubles.
type Store interface {
	LoadPrices(ctx context.Context) ([]Price, error)
	ReadWindow(ctx context.Context, start, end time.Time) ([]RatedEvent, error)
	UpsertRollups(ctx context.Context, rollups []Rollup) error
	Ping(ctx context.Context) error
	Close() error
}

// PostgresStore reads billing_event + model_price and writes rated_usage in the
// shared Atlas Postgres. Like the drainer it does NOT run migrations — it assumes
// the tables exist (owned by the Atlas Alembic chain; see migrations/README.md).
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres opens a *sql.DB against the DSN using the pgx stdlib driver,
// applies pool settings, and Pings once so a bad DSN fails fast at job start.
func OpenPostgres(ctx context.Context, cfg Config) (*PostgresStore, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("rating: DATABASE_URL is empty (Postgres holds billing_event and the price book; the rater cannot run without it)")
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("rating: open postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	s := &PostgresStore{db: db}
	if err := s.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rating: postgres ping: %w", err)
	}
	return s, nil
}

// NewPostgresStore wraps an existing *sql.DB. Used by tests (sqlmock) and callers
// owning the pool lifecycle.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *PostgresStore) Close() error                   { return s.db.Close() }

// LoadPrices reads the entire model_price table.
func (s *PostgresStore) LoadPrices(ctx context.Context) ([]Price, error) {
	const q = `SELECT model, prompt_price_micro, cached_price_micro, completion_price_micro, effective_from, effective_to FROM model_price`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("rating: load prices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Price
	for rows.Next() {
		var p Price
		var effectiveTo sql.NullTime
		if err := rows.Scan(
			&p.Model,
			&p.PromptPriceMicro,
			&p.CachedPriceMicro,
			&p.CompletionPriceMicro,
			&p.EffectiveFrom,
			&effectiveTo,
		); err != nil {
			return nil, fmt.Errorf("rating: scan price: %w", err)
		}
		if effectiveTo.Valid {
			p.EffectiveTo = effectiveTo.Time
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rating: iterate prices: %w", err)
	}
	return out, nil
}

// ReadWindow reads billing_event rows whose rating instant is in [start, end).
//
// The rating instant is COALESCE(event_ts, created_at): event_ts is the
// interceptor's stamp (the true moment of the request); created_at is the drain
// write time, used only when event_ts is null. We both filter and rate on this
// coalesced instant so window membership and price selection agree.
//
// auth_id and model are NOT NULL on a real metering row, but billing_event leaves
// them nullable; a row with a NULL auth_id or model cannot be attributed/priced,
// so we exclude it here (it is surfaced as a drained-but-unattributable record
// elsewhere, not silently rated). This keeps rating's input well-formed.
func (s *PostgresStore) ReadWindow(ctx context.Context, start, end time.Time) ([]RatedEvent, error) {
	const q = `
		SELECT auth_id, model, prompt_tokens, cached_tokens, completion_tokens, aborted,
		       COALESCE(event_ts, created_at) AS rating_ts
		FROM billing_event
		WHERE COALESCE(event_ts, created_at) >= $1
		  AND COALESCE(event_ts, created_at) <  $2
		  AND auth_id IS NOT NULL
		  AND model   IS NOT NULL`
	rows, err := s.db.QueryContext(ctx, q, start, end)
	if err != nil {
		return nil, fmt.Errorf("rating: read window [%s,%s): %w", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}
	defer func() { _ = rows.Close() }()

	var out []RatedEvent
	for rows.Next() {
		var e RatedEvent
		if err := rows.Scan(
			&e.AuthID,
			&e.Model,
			&e.PromptTokens,
			&e.CachedTokens,
			&e.CompletionTokens,
			&e.Aborted,
			&e.At,
		); err != nil {
			return nil, fmt.Errorf("rating: scan event: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rating: iterate events: %w", err)
	}
	return out, nil
}

// upsertColumns is the column order shared by the INSERT and the per-row value
// binding (same single-source-of-truth pattern as drain.upsertColumns).
var upsertColumns = []string{
	"id",
	"auth_id",
	"model",
	"window_start",
	"window_end",
	"prompt_tokens",
	"cached_tokens",
	"completion_tokens",
	"billable_prompt_tokens",
	"cost_micro_usd",
	"event_count",
}

const colsPerRow = 11 // len(upsertColumns); rated_at is DB-defaulted.

// UpsertRollups writes the rollups in a single transaction with a multi-row
// INSERT ... ON CONFLICT (auth_id, model, window_start) DO UPDATE.
//
// DO UPDATE (not DO NOTHING) is the IDEMPOTENCY mechanism for re-runs: re-rating
// a window must RECONCILE, not duplicate and not stale-retain. The aggregator
// recomputes each rollup's totals from the current billing_event rows, so on a
// re-run we REPLACE the stored sums/cost with the freshly computed ones. If a
// late event landed in the window since the last run, the re-run picks it up and
// the rollup converges to the correct total. Running the same window twice with
// no new events yields byte-identical rows (no doubling) — the core idempotency
// invariant.
//
// The whole batch is one transaction so a window's rollups are all-or-nothing.
func (s *PostgresStore) UpsertRollups(ctx context.Context, rollups []Rollup) error {
	if len(rollups) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(rollups))
	args := make([]any, 0, len(rollups)*colsPerRow)
	p := 1
	for i := range rollups {
		marks := make([]string, colsPerRow)
		for c := 0; c < colsPerRow; c++ {
			marks[c] = fmt.Sprintf("$%d", p)
			p++
		}
		placeholders = append(placeholders, "("+strings.Join(marks, ",")+")")
		id, err := newID()
		if err != nil {
			return fmt.Errorf("rating: generate rollup id: %w", err)
		}
		args = append(args, rollupArgs(id, rollups[i])...)
	}

	stmt := "INSERT INTO rated_usage (" +
		strings.Join(upsertColumns, ", ") +
		") VALUES " + strings.Join(placeholders, ", ") +
		" ON CONFLICT (auth_id, model, window_start) DO UPDATE SET " +
		"window_end = EXCLUDED.window_end, " +
		"prompt_tokens = EXCLUDED.prompt_tokens, " +
		"cached_tokens = EXCLUDED.cached_tokens, " +
		"completion_tokens = EXCLUDED.completion_tokens, " +
		"billable_prompt_tokens = EXCLUDED.billable_prompt_tokens, " +
		"cost_micro_usd = EXCLUDED.cost_micro_usd, " +
		"event_count = EXCLUDED.event_count, " +
		"rated_at = now()"

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("rating: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("rating: upsert %d rollups: %w", len(rollups), err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("rating: commit %d rollups: %w", len(rollups), err)
	}
	return nil
}

// rollupArgs maps a Rollup to its column values in upsertColumns order. The
// surrogate id is only used on INSERT; on CONFLICT the existing row's id is kept
// (the natural key, not id, identifies the row).
func rollupArgs(id string, r Rollup) []any {
	return []any{
		id,
		r.AuthID,
		r.Model,
		r.WindowStart.UTC(),
		r.WindowEnd.UTC(),
		r.PromptTokens,
		r.CachedTokens,
		r.CompletionTokens,
		r.BillablePromptTokens,
		r.CostMicroUSD,
		r.EventCount,
	}
}

// newID returns a 32-char hex id, matching Atlas's id convention for the table.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
