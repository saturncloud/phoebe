//go:build integration

package prune

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/saturncloud/phoebe/migrations"
)

// newPruneHarness creates an isolated schema and applies the REAL migrations
// (all of them, like the e2e harness — the pruner must run against the true
// table shapes, not an inline copy), then returns a pool pinned to the schema
// via search_path in the DSN.
func newPruneHarness(t *testing.T, schema string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune integration test")
	}

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	mustExec(t, admin, "CREATE EXTENSION IF NOT EXISTS btree_gist")
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

	for _, m := range []string{
		"0001_billing_event.up.sql",
		"0002_rating.up.sql",
		"0003_io_log.up.sql",
		"0004_billing_event_serving_mode.up.sql",
		"0005_invoice_grade_attempts.up.sql",
		"0006_rollup_grain.up.sql",
		"0007_serving_mode_explicit.up.sql",
	} {
		b, err := migrations.FS.ReadFile(m)
		if err != nil {
			t.Fatalf("read migration %s: %v", m, err)
		}
		mustExec(t, db, string(b))
	}
	return db
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
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

func TestIntegration_PrunesOnlyOlderThanCutoff(t *testing.T) {
	db := newPruneHarness(t, "phoebe_prune_it")
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
	db := newPruneHarness(t, "phoebe_prune_it")
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
	db := newPruneHarness(t, "phoebe_prune_it")
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
	db := newPruneHarness(t, "phoebe_prune_it")
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
	db := newPruneHarness(t, "phoebe_prune_it")
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

// TestIntegration_PredicateUsesTheCreatedAtIndex pins the operational point of
// shape (A): as billing_event grows, the prune predicate must keep hitting
// billing_event_created_at_ix (io_log's 0003 index exists explicitly for this
// job). A seq-scan regression here is how a daily CronJob becomes an I/O
// incident on a 10Gi PVC. The planner only prefers the index once the table is
// non-trivial, so each table is seeded past the tipping point and ANALYZEd —
// an EXPLAIN against an empty table would seq-scan legitimately and prove
// nothing.
func TestIntegration_PredicateUsesTheCreatedAtIndex(t *testing.T) {
	db := newPruneHarness(t, "phoebe_prune_it")
	cutoff := time.Now().Add(-30 * 24 * time.Hour)

	for _, tc := range []struct {
		table string
		index string
	}{
		{"billing_event", "billing_event_created_at_ix"},
		{"io_log", "io_log_created_at_ix"},
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO %s (request_id, model, created_at)
			SELECT 'explain-'||g, 'model', now() - interval '40 days' FROM generate_series(1, 20000) g`, tc.table))
		mustExec(t, db, "ANALYZE "+tc.table)

		rows, err := db.Query(`EXPLAIN (FORMAT TEXT) SELECT ctid FROM `+tc.table+` WHERE created_at < $1 ORDER BY created_at LIMIT 5000`, cutoff)
		if err != nil {
			t.Fatalf("explain %s: %v", tc.table, err)
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
		if !strings.Contains(plan.String(), tc.index) {
			t.Fatalf("%s predicate does not use %s:\n%s", tc.table, tc.index, plan.String())
		}
	}
}
