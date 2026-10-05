// Package prune implements the RULED retention policy for phoebe's evidence
// tables (rulings.md 2026-09-30, queue #5 + follow-ons 1-3):
//
//   - billing_event (the raw evidence ledger) is pruned on created_at at a
//     default horizon of 30 days, configurable per install, HARD FLOOR 7 days
//     — pruning inside the rater's re-rate reach (rateTrailingHours, default
//     24h, plus drain lag and operator backfills) would let a later re-rate
//     reconcile-DELETE previously billed rated_usage rows. ONE horizon for
//     every row class: withheld/invalid rows prune on the same horizon as
//     everything — the pruner deliberately does NOT reimplement the rater's
//     withholding logic (a second copy of money logic that can silently
//     diverge). The durable anomaly record is the rater's hourly counts/logs.
//   - io_log is pruned on created_at at its OWN period, default 7 days
//     (configurable; floor 1 day) — the io_log migration (0003) promised a
//     retention job that did not exist until this one.
//   - Archive-before-delete is OUT OF SCOPE by ruling: the manager's money
//     rollup outlives the trailing window and the WAL keeps recent evidence.
//
// Shape (A) by ruling: batched DELETEs keyed on created_at. NO schema change,
// NO partition, NO table rebuild — the table, the PK, and the drainer's
// ON CONFLICT (request_id) dedup stay untouched. rated_usage (money) is never
// referenced here at all.
package prune

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Table identifies a prunable table. It is a closed enum, not a free string:
// the table name is baked into each query at compile time, so no caller can
// ever interpolate an identifier.
type Table int

const (
	BillingEvent Table = iota
	IoLog
)

func (t Table) Name() string {
	switch t {
	case BillingEvent:
		return "billing_event"
	case IoLog:
		return "io_log"
	}
	return fmt.Sprintf("unknown(%d)", int(t))
}

func (t Table) minRetentionDays() int {
	switch t {
	case BillingEvent:
		// Hard floor from the ruling: must stay strictly beyond the rater's
		// routine re-rate reach (24h trailing window + drain lag + skew).
		return MinBillingEventRetentionDays
	case IoLog:
		return MinIoLogRetentionDays
	}
	return 1
}

// maxRetentionDays mirrors the Config.Validate ceilings per table (the unknown
// table falls back to the most conservative bound, io_log's) so a caller that
// bypasses Config (Store.Prune is also called directly) cannot overflow
// time.Duration(retentionDays)*24h into a future cutoff that wipes the table.
func (t Table) maxRetentionDays() int {
	switch t {
	case BillingEvent:
		return MaxBillingEventRetentionDays
	case IoLog:
		return MaxIoLogRetentionDays
	}
	return MaxIoLogRetentionDays
}

const (
	// DefaultBillingEventRetentionDays is the ruled default horizon for
	// billing_event: 30 days, configurable per install.
	DefaultBillingEventRetentionDays = 30
	// MinBillingEventRetentionDays is the ruled hard floor (7 days) vs the
	// 24h re-rate reach. Anything below risks reconcile-deleting billed money.
	MinBillingEventRetentionDays = 7
	// DefaultIoLogRetentionDays is the ruled default for io_log's OWN period
	// (distinct from billing_event's 30): 7 days.
	DefaultIoLogRetentionDays = 7
	// MinIoLogRetentionDays keeps io_log pruning from being a table wipe.
	MinIoLogRetentionDays = 1
	// DefaultBatchSize is how many rows one DELETE statement removes. Batches
	// bound statement time, lock hold, and WAL per statement; the loop repeats
	// until a batch deletes nothing.
	DefaultBatchSize = 5000
)

// Config is the prune policy. Zero values mean "use the ruled default".
type Config struct {
	// BillingEventRetentionDays: billing_event horizon. Default 30, floor 7.
	BillingEventRetentionDays int
	// IoLogRetentionDays: io_log horizon, its OWN period. Default 7, floor 1.
	IoLogRetentionDays int
	// BatchSize: rows per DELETE statement. Default 5000; must be >= 1.
	BatchSize int
}

// WithDefaults fills zero fields with the ruled defaults.
func (c Config) WithDefaults() Config {
	if c.BillingEventRetentionDays == 0 {
		c.BillingEventRetentionDays = DefaultBillingEventRetentionDays
	}
	if c.IoLogRetentionDays == 0 {
		c.IoLogRetentionDays = DefaultIoLogRetentionDays
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	return c
}

// MaxBillingEventRetentionDays bounds the billing_event horizon at 100 years:
// time.Duration(retentionDays)*24h overflows int64 nanoseconds past ~106752
// days and wraps the cutoff into the FUTURE, making created_at < cutoff match
// the WHOLE table — one run would wipe billing_event silently at exit 0. The
// bound sits far below the overflow point and past any real retention ask.
const MaxBillingEventRetentionDays = 36500

// MaxIoLogRetentionDays bounds the io_log horizon at 10 years, same overflow
// rationale as MaxBillingEventRetentionDays.
const MaxIoLogRetentionDays = 3650

// Validate enforces the ruled floors. A horizon below the floor is a config
// error, not a silently clamped value: silently pruning LESS than asked would
// leave an operator believing evidence is gone when it is not. The upper
// bounds are a hard-stop guard against the duration overflow that turns a
// typo'd horizon into a full-table wipe.
func (c Config) Validate() error {
	if c.BillingEventRetentionDays < MinBillingEventRetentionDays {
		return fmt.Errorf("billingEvent retention %dd below the hard floor %dd: pruning inside the rater's re-rate reach can reconcile-delete billed money", c.BillingEventRetentionDays, MinBillingEventRetentionDays)
	}
	if c.BillingEventRetentionDays > MaxBillingEventRetentionDays {
		return fmt.Errorf("billingEvent retention %dd above the maximum %dd: the horizon would overflow the cutoff computation and wipe the table", c.BillingEventRetentionDays, MaxBillingEventRetentionDays)
	}
	if c.IoLogRetentionDays < MinIoLogRetentionDays {
		return fmt.Errorf("ioLog retention %dd below the floor %dd", c.IoLogRetentionDays, MinIoLogRetentionDays)
	}
	if c.IoLogRetentionDays > MaxIoLogRetentionDays {
		return fmt.Errorf("ioLog retention %dd above the maximum %dd: the horizon would overflow the cutoff computation and wipe the table", c.IoLogRetentionDays, MaxIoLogRetentionDays)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("batchSize must be >= 1, got %d", c.BatchSize)
	}
	return nil
}

// Result reports one table's prune outcome for logging and tests.
type Result struct {
	Table       Table
	Cutoff      time.Time // rows older than this were eligible
	RowsDeleted int64
}

// Store prunes evidence tables through a database/sql pool (pgx stdlib
// driver, same seam as internal/drain and internal/rating).
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// pruneQuery is one batched DELETE. The inner SELECT hits the created_at
// index (billing_event_created_at_ix / io_log_created_at_ix — io_log's 0003
// migration created its index explicitly for this job); ORDER BY + LIMIT walk
// the index in order so each DELETE touches a compact set of heap pages.
// ctid targeting avoids taking row locks on rows outside the batch. The query
// text is per-table at the call site (switch), never interpolated. It is a
// package-level function (not inline in deleteBatch) so the integration test
// can EXPLAIN the EXACT statement the store runs — a hand-copied query in a
// test could drift from the real one and green-light a regression.
func pruneQuery(table Table) string {
	switch table {
	case BillingEvent:
		return `DELETE FROM billing_event WHERE ctid = ANY(ARRAY(
			SELECT ctid FROM billing_event WHERE created_at < $1 ORDER BY created_at LIMIT $2))`
	case IoLog:
		return `DELETE FROM io_log WHERE ctid = ANY(ARRAY(
			SELECT ctid FROM io_log WHERE created_at < $1 ORDER BY created_at LIMIT $2))`
	}
	return ""
}

func deleteBatch(ctx context.Context, db *sql.DB, table Table, cutoff time.Time, batchSize int) (int64, error) {
	query := pruneQuery(table)
	if query == "" {
		return 0, fmt.Errorf("unknown table %d", int(table))
	}
	res, err := db.ExecContext(ctx, query, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Prune deletes rows of table older than retentionDays (measured from now,
// truncated to the second so a re-run in the same second is a true no-op) and
// returns the total deleted. It loops in batches until a batch deletes
// nothing, so a large backlog is drained in bounded statements. It only ever
// DELETEs; it never touches rated_usage or any other table.
//
// retentionDays is explicit at this seam: zero or negative is an error here;
// prune.Config.WithDefaults is the layer that maps an unset (zero) config
// value to the ruled default.
func (s *Store) Prune(ctx context.Context, table Table, retentionDays int, batchSize int) (Result, error) {
	if batchSize < 1 {
		return Result{}, fmt.Errorf("batchSize must be >= 1, got %d", batchSize)
	}
	if retentionDays < table.minRetentionDays() {
		return Result{}, fmt.Errorf("%s retention %dd below floor %dd", table.Name(), retentionDays, table.minRetentionDays())
	}
	if retentionDays > table.maxRetentionDays() {
		return Result{}, fmt.Errorf("%s retention %dd above maximum %dd", table.Name(), retentionDays, table.maxRetentionDays())
	}
	cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour).Truncate(time.Second)
	total := int64(0)
	for {
		n, err := deleteBatch(ctx, s.db, table, cutoff, batchSize)
		if err != nil {
			return Result{Table: table, Cutoff: cutoff, RowsDeleted: total}, fmt.Errorf("prune %s batch: %w", table.Name(), err)
		}
		total += n
		if n == 0 {
			break
		}
	}
	return Result{Table: table, Cutoff: cutoff, RowsDeleted: total}, nil
}
