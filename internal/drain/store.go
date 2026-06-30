package drain

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// pgx stdlib driver registers itself as "pgx" with database/sql. We use the
	// standard library sql package (not the native pgx pool) so the store is a
	// thin, mockable seam and so connection-pool tuning is the familiar
	// database/sql API.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/metering"
)

// Store is the durable sink for metering events. It is an interface so the
// drain loop can be unit-tested against a fake, and so the Postgres SQL can be
// tested in isolation via sqlmock. The contract is:
//
//   - Upsert is IDEMPOTENT on request_id: writing the same event (same
//     request_id) twice produces exactly one row and never errors. This is the
//     load-bearing invariant that makes at-least-once delivery safe.
//   - Upsert is all-or-nothing per call: it either commits every event in the
//     batch or returns an error having committed none, so the caller can
//     withhold the XACK and let the whole batch redeliver.
type Store interface {
	Upsert(ctx context.Context, events []metering.Event) error
	// Ping verifies connectivity (pool_pre_ping equivalent on startup).
	Ping(ctx context.Context) error
	Close() error
}

// PostgresStore writes metering events to the billing_event table. It assumes
// the table already exists — the drainer does NOT run migrations (the schema is
// owned by the shared Atlas Alembic chain; see migrations/README.md). Upsert
// surfaces a clear, semantic error if the table is missing.
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres opens a *sql.DB against the given DSN using the pgx stdlib
// driver, applies the pool settings, and Pings once (pool_pre_ping equivalent)
// so a misconfigured DSN fails fast at startup rather than on first event.
func OpenPostgres(ctx context.Context, cfg Config) (*PostgresStore, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("drain: DATABASE_URL is empty (Postgres is the system of record; the drainer cannot run without it)")
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("drain: open postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	s := &PostgresStore{db: db}
	if err := s.Ping(ctx); err != nil {
		// Close the half-open pool so we don't leak it on a failed startup.
		_ = db.Close()
		return nil, fmt.Errorf("drain: postgres ping: %w", err)
	}
	return s, nil
}

// NewPostgresStore wraps an existing *sql.DB. Used by tests (sqlmock) and by
// callers that want to own the pool's lifecycle.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// upsertColumns is the column order shared by the INSERT statement and the
// per-row value binding. Keeping it in one place keeps the two in lockstep.
var upsertColumns = []string{
	"request_id",
	"auth_id",
	"user_id",
	"group_id",
	"resource_id",
	"resource_type",
	"org_id",
	"model",
	"base_model",
	"adapter",
	"prompt_tokens",
	"cached_tokens",
	"completion_tokens",
	"finish_reason",
	"gpu_type",
	"aborted",
	"event_ts",
}

const colsPerRow = 17 // len(upsertColumns); created_at is DB-defaulted.

// Upsert writes a batch of events in a single transaction with a multi-row
// INSERT ... ON CONFLICT (request_id) DO NOTHING.
//
// DO NOTHING (not DO UPDATE) is deliberate: a metering Event is immutable once
// emitted (one record per request, keyed by request_id). A redelivery carries
// the SAME data, so there is nothing to update — the first write wins and the
// duplicate is a no-op. This is the cheapest correct idempotency policy and
// avoids gratuitous row churn / WAL writes on every redelivery.
//
// The whole batch is one transaction so the caller's "XACK only after commit"
// contract holds at batch granularity: either all rows are durable (ACK the
// batch) or none are (don't ACK; redeliver).
func (s *PostgresStore) Upsert(ctx context.Context, events []metering.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Build a parameterised multi-row VALUES clause: ($1,...,$15),($16,...),...
	// Parameterised (never string-interpolated) to avoid any injection via the
	// engine-supplied request_id / model strings.
	placeholders := make([]string, 0, len(events))
	args := make([]any, 0, len(events)*colsPerRow)
	p := 1
	for i := range events {
		marks := make([]string, colsPerRow)
		for c := 0; c < colsPerRow; c++ {
			marks[c] = fmt.Sprintf("$%d", p)
			p++
		}
		placeholders = append(placeholders, "("+strings.Join(marks, ",")+")")
		args = append(args, eventArgs(events[i])...)
	}

	stmt := "INSERT INTO billing_event (" +
		strings.Join(upsertColumns, ", ") +
		") VALUES " + strings.Join(placeholders, ", ") +
		" ON CONFLICT (request_id) DO NOTHING"

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("drain: begin tx: %w", err)
	}
	// Rollback is a no-op after a successful Commit; safe to always defer.
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("drain: upsert %d events: %w", len(events), err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("drain: commit %d events: %w", len(events), err)
	}
	return nil
}

// eventArgs maps an Event to its column values in upsertColumns order.
//
// Identity columns are emitted as NULL (not "") when empty so the DB reflects
// "absent" rather than "empty string" — billing queries group by auth_id and a
// stray "" would be a spurious bucket. Token counts are NOT NULL with DB
// defaults but we always supply them. event_ts is derived from the emitter's
// TimestampUnixMs; created_at is left to the DB default (now() == drain time).
func eventArgs(e metering.Event) []any {
	var eventTS any
	if e.TimestampUnixMs > 0 {
		eventTS = time.UnixMilli(e.TimestampUnixMs).UTC()
	}
	return []any{
		e.RequestID,
		nullStr(e.AuthID),
		nullStr(e.UserID),
		nullStr(e.GroupID),
		nullStr(e.ResourceID),
		nullStr(e.ResourceType),
		// OrgID is "" when Atlas isn't injecting X-Saturn-Org-Id yet (producer-rollout
		// gap). nullStr so it stores NULL, not '' — the rater/push fail-closed predicate
		// is `org_id IS NULL` (held + screamed at push), and a stored '' would dodge it
		// and be pushed as an empty-org rollup (billed to a guessed/blank org).
		nullStr(e.OrgID),
		// Model is "" when the upstream never reported one (capture gap), so it
		// goes through nullStr like every other identity column: the rater's
		// unattributable predicate is `model_id IS NULL`, and a stored ''
		// would dodge it and be misreported as UNPRICED (wrong runbook —
		// "backfill prices" instead of "fix the capture gap").
		nullStr(e.Model),
		// BaseModel is "" for a base model (the common case) and the HF base id for
		// a fine-tune. nullStr so a base model stores NULL, not '' — the rater's
		// derived-price join keys on a non-null base_model, and a stray '' could
		// only ever miss the join (NULL = NULL is never true), so this is belt-and
		// -braces for a clean column either way.
		nullStr(e.BaseModel),
		nullStr(e.Adapter),
		e.PromptTokens,
		e.CachedTokens,
		e.CompletionTokens,
		nullStr(e.FinishReason),
		nullStr(e.GPUType),
		e.Aborted,
		eventTS,
	}
}

// nullStr returns a driver NULL for "" and the string otherwise.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
