//go:build integration

package prune

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/saturncloud/phoebe/migrations"
)

// btreeGistAdvisoryLock serializes CREATE EXTENSION IF NOT EXISTS btree_gist
// between parallel test binaries: IF NOT EXISTS is not atomic (two concurrent
// runs both see "absent" and one fails on pg_extension_name_index with 23505 —
// reproduced against a fresh database). 727301 is a phoebe-test-specific
// constant, session-held on this harness's own admin connection.
const btreeGistAdvisoryLock = 727301

// newPruneHarness creates an isolated schema — named per test PROCESS so
// concurrent runs against a shared database stop stomping each other — and
// applies the REAL migrations (all of them, like the e2e harness — the pruner
// must run against the true table shapes, not an inline copy), then returns a
// pool pinned to the schema via search_path in the DSN.
func newPruneHarness(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune integration test")
	}
	schema := fmt.Sprintf("phoebe_prune_it_%d", os.Getpid())

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	mustExec(t, admin, fmt.Sprintf("SELECT pg_advisory_lock(%d)", btreeGistAdvisoryLock))
	t.Cleanup(func() {
		_, _ = admin.Exec(fmt.Sprintf("SELECT pg_advisory_unlock(%d)", btreeGistAdvisoryLock))
	})
	mustExec(t, admin, "CREATE EXTENSION IF NOT EXISTS btree_gist")
	mustExec(t, admin, fmt.Sprintf("SELECT pg_advisory_unlock(%d)", btreeGistAdvisoryLock))
	mustExec(t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	mustExec(t, admin, "CREATE SCHEMA "+schema)

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	applyAllMigrations(t, db)
	return db
}

// applyAllMigrations applies every *.up.sql from the embedded migrations.FS
// (the same source cmd/migrate applies), ordered by filename. Discovering the
// list from the FS — not a hand-maintained literal — means a future migration
// cannot leave this harness stale while green.
func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("read migrations FS: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		mustExec(t, db, string(b))
	}
}

func mustExec(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatalf("exec failed: %v\nstatement: %s", err, stmt)
	}
}

// insertBillingEvent inserts one row with an explicit age (negative d means
// created_at = now+d·24h in the past) and the anomaly-flavored fields set so
// the one-horizon behavior is pinned: withheld-style rows (usage_found=false,
// odd status_code) must prune on exactly the same predicate as clean rows.
func insertBillingEvent(t *testing.T, db *sql.DB, requestID string, ageDays int, usageFound bool, statusCode int) {
	t.Helper()
	createdAt := time.Now().Add(time.Duration(ageDays) * 24 * time.Hour)
	_, err := db.Exec(`INSERT INTO billing_event
		(request_id, auth_id, resource_id, model, prompt_tokens, usage_found, status_code, created_at)
		VALUES ($1, 'auth', 'res', 'model', 1, $2, $3, $4)`,
		requestID, usageFound, statusCode, createdAt)
	if err != nil {
		t.Fatalf("insert billing_event %s: %v", requestID, err)
	}
}

func insertIoLog(t *testing.T, db *sql.DB, requestID string, ageDays int) {
	t.Helper()
	createdAt := time.Now().Add(time.Duration(ageDays) * 24 * time.Hour)
	_, err := db.Exec(`INSERT INTO io_log (request_id, model, created_at) VALUES ($1, 'model', $2)`,
		requestID, createdAt)
	if err != nil {
		t.Fatalf("insert io_log %s: %v", requestID, err)
	}
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func countWhere(t *testing.T, db *sql.DB, table, where string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table + " WHERE " + where).Scan(&n); err != nil {
		t.Fatalf("count %s where %s: %v", table, where, err)
	}
	return n
}

func TestIntegration_PrunesOnlyOlderThanCutoff(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)

	// 40 days old: prunable at the 30-day default. Includes the
	// withheld/invalid-flavored rows — ONE horizon for every row class.
	insertBillingEvent(t, db, "old-clean", -40, true, 200)
	insertBillingEvent(t, db, "old-withheld-usage", -40, false, 200)
	insertBillingEvent(t, db, "old-weird-status", -40, true, 418)
	// 10 days old and fresh: inside the horizon, must survive.
	insertBillingEvent(t, db, "mid", -10, true, 200)
	insertBillingEvent(t, db, "fresh", 0, true, 200)

	res, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 100)
	if err != nil {
		t.Fatalf("prune billing_event: %v", err)
	}
	if res.RowsDeleted != 3 {
		t.Fatalf("RowsDeleted = %d, want 3 (the three 40-day-old rows)", res.RowsDeleted)
	}
	if got := countRows(t, db, "billing_event"); got != 2 {
		t.Fatalf("surviving rows = %d, want 2 (mid + fresh)", got)
	}
}

func TestIntegration_BatchLoopDrainsBacklog(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)

	const total = 12
	for i := 0; i < total; i++ {
		insertBillingEvent(t, db, fmt.Sprintf("backlog-%02d", i), -40, true, 200)
	}
	res, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 5)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.RowsDeleted != total {
		t.Fatalf("RowsDeleted = %d, want %d across batched loops", res.RowsDeleted, total)
	}
	if got := countRows(t, db, "billing_event"); got != 0 {
		t.Fatalf("billing_event has %d rows left, want 0", got)
	}
}

func TestIntegration_HardFloorRefusesBelowReRateReach(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)
	insertBillingEvent(t, db, "old", -40, true, 200)

	_, err := s.Prune(context.Background(), BillingEvent, MinBillingEventRetentionDays-1, 100)
	if err == nil || !strings.Contains(err.Error(), "floor") {
		t.Fatalf("expected floor refusal, got %v", err)
	}
	if got := countRows(t, db, "billing_event"); got != 1 {
		t.Fatalf("row must survive a refused prune; count = %d", got)
	}
}

func TestIntegration_RerunIsANoOp(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)
	insertBillingEvent(t, db, "old", -40, true, 200)

	first, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 100)
	if err != nil {
		t.Fatalf("first prune: %v", err)
	}
	if first.RowsDeleted != 1 {
		t.Fatalf("first prune deleted %d, want 1", first.RowsDeleted)
	}
	second, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 100)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if second.RowsDeleted != 0 {
		t.Fatalf("second prune deleted %d, want 0 (idempotent)", second.RowsDeleted)
	}
}

func TestIntegration_IoLogPrunedAtOwnPeriod(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)

	insertIoLog(t, db, "iol-old", -40)
	insertIoLog(t, db, "iol-mid", -5)
	insertIoLog(t, db, "iol-fresh", 0)

	// io_log's OWN period (default 7d) — NOT billing_event's 30d: the -5d
	// row is inside io_log's 7-day horizon and must survive.
	res, err := s.Prune(context.Background(), IoLog, DefaultIoLogRetentionDays, 100)
	if err != nil {
		t.Fatalf("prune io_log: %v", err)
	}
	if res.RowsDeleted != 1 {
		t.Fatalf("RowsDeleted = %d, want 1 (only the 40-day-old row)", res.RowsDeleted)
	}
	if got := countRows(t, db, "io_log"); got != 2 {
		t.Fatalf("surviving io_log rows = %d, want 2", got)
	}
}

// TestIntegration_BoundaryCutoffIsStrictlyOlder pins the predicate's strict
// inequality at the exact cutoff: rows at cutoff-2s must delete, rows at
// cutoff+2s must survive. The cutoff is computed with the store's own
// expression BEFORE inserting so the boundary rows are pinned relative to it;
// second-truncation makes an exact-cutoff row racy in tests (the insert and
// the prune can truncate now to different seconds, flipping which side of the
// predicate an exact-cutoff row lands on), so the ±2s margin pins the
// strictly-older semantics deterministically.
func TestIntegration_BoundaryCutoffIsStrictlyOlder(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)

	cutoff := time.Now().Add(-time.Duration(DefaultBillingEventRetentionDays) * 24 * time.Hour).Truncate(time.Second)
	mustExec(t, db, `INSERT INTO billing_event
		(request_id, auth_id, resource_id, model, prompt_tokens, usage_found, status_code, created_at)
		VALUES ('boundary-old', 'auth', 'res', 'model', 1, true, 200, $1)`, cutoff.Add(-2*time.Second))
	mustExec(t, db, `INSERT INTO billing_event
		(request_id, auth_id, resource_id, model, prompt_tokens, usage_found, status_code, created_at)
		VALUES ('boundary-young', 'auth', 'res', 'model', 1, true, 200, $1)`, cutoff.Add(2*time.Second))

	res, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 100)
	if err != nil {
		t.Fatalf("prune billing_event: %v", err)
	}
	if res.RowsDeleted != 1 {
		t.Fatalf("RowsDeleted = %d, want 1 (only the strictly-older row)", res.RowsDeleted)
	}
	if got := countWhere(t, db, "billing_event", "request_id = 'boundary-old'"); got != 0 {
		t.Fatalf("cutoff-2s row must be deleted; %d left", got)
	}
	if got := countWhere(t, db, "billing_event", "request_id = 'boundary-young'"); got != 1 {
		t.Fatalf("cutoff+2s row must survive; %d left", got)
	}
}

// TestIntegration_ConcurrentInsertsAreNeverDeleted pins the ctid-snapshot
// safety under real concurrency: rows inserted while the prune loop is
// mid-flight are not in any ctid batch the DELETE targets (and the predicate
// excludes them anyway), so every fresh row survives, the loop still drains
// the full old backlog, and a re-run converges to zero. The tiny sleeps widen
// the overlap window so the inserts genuinely interleave with the loop; the
// assertions hold whichever way the scheduler interleaves them.
func TestIntegration_ConcurrentInsertsAreNeverDeleted(t *testing.T) {
	db := newPruneHarness(t)
	s := NewStore(db)

	const oldRows = 2000
	for i := 0; i < oldRows; i++ {
		insertBillingEvent(t, db, fmt.Sprintf("old-%04d", i), -40, true, 200)
	}

	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 50)
		done <- outcome{res, err}
	}()

	const freshRows = 50
	for i := 0; i < freshRows; i++ {
		insertBillingEvent(t, db, fmt.Sprintf("fresh-%02d", i), 0, true, 200)
		if i%5 == 4 {
			time.Sleep(time.Millisecond)
		}
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("prune: %v", got.err)
	}
	if got.res.RowsDeleted != oldRows {
		t.Fatalf("RowsDeleted = %d, want %d (the whole pre-existing backlog)", got.res.RowsDeleted, oldRows)
	}
	if n := countWhere(t, db, "billing_event", "request_id LIKE 'fresh-%'"); n != freshRows {
		t.Fatalf("fresh rows mid-loop = %d, want %d (none may be deleted)", n, freshRows)
	}

	second, err := s.Prune(context.Background(), BillingEvent, DefaultBillingEventRetentionDays, 50)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if second.RowsDeleted != 0 {
		t.Fatalf("second prune deleted %d, want 0 (converged)", second.RowsDeleted)
	}
}

// TestIntegration_PredicateUsesTheCreatedAtIndex pins the operational point of
// shape (A): as billing_event grows, the prune statement must keep hitting
// billing_event_created_at_ix (io_log's 0003 index exists explicitly for this
// job). A seq-scan regression here is how a daily CronJob becomes an I/O
// incident on a 10Gi PVC. The planner only prefers the index once the table is
// non-trivial, so each table is seeded past the tipping point and ANALYZEd —
// an EXPLAIN against an empty table would seq-scan legitimately and prove
// nothing. The EXPLAIN runs the EXACT deleteBatch statement (same pruneQuery
// switch), not a hand-copied approximation that could drift from the real one.
func TestIntegration_PredicateUsesTheCreatedAtIndex(t *testing.T) {
	db := newPruneHarness(t)
	cutoff := time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Second)

	for _, table := range []Table{BillingEvent, IoLog} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO %s (request_id, model, created_at)
			SELECT 'explain-'||g, 'model', now() - interval '40 days' FROM generate_series(1, 20000) g`, table.Name()))
		mustExec(t, db, "ANALYZE "+table.Name())

		rows, err := db.Query(`EXPLAIN (FORMAT TEXT) `+pruneQuery(table), cutoff, 5000)
		if err != nil {
			t.Fatalf("explain %s: %v", table.Name(), err)
		}
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("scan explain: %v", err)
			}
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatalf("explain rows: %v", rows.Err())
		}

		index := table.Name() + "_created_at_ix"
		if !strings.Contains(plan.String(), index) {
			t.Fatalf("%s statement does not use %s:\n%s", table.Name(), index, plan.String())
		}
		if !strings.Contains(plan.String(), "Tid Scan") {
			t.Fatalf("%s statement lost the ctid-targeted Tid Scan:\n%s", table.Name(), plan.String())
		}
		if strings.Contains(plan.String(), "Seq Scan on "+table.Name()) {
			t.Fatalf("%s statement seq-scans:\n%s", table.Name(), plan.String())
		}
	}
}
