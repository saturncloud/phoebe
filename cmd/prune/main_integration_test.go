//go:build integration

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/prune"
	"github.com/saturncloud/phoebe/migrations"
)

// newCmdPruneHarness creates an isolated schema — named per test PROCESS so
// concurrent runs against a shared database stop stomping each other — with
// ALL production migrations applied (discovered from the embedded migrations.FS,
// ordered by filename; a hand-maintained list could go stale while green), and
// returns a pool pinned to it plus the schema-pinned DSN for handing to run()
// as DATABASE_URL.
//
// No migration requires btree_gist (verified: all 7 apply on a database
// without it); internal/e2e creates it for the rating tests' GiST constraints.
// The prune harnesses stay out of the catalog race by not creating it.
func newCmdPruneHarness(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune cmd integration test")
	}
	schema := fmt.Sprintf("phoebe_prune_cmd_it_%d", os.Getpid())

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	mustExecCmd(t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	mustExecCmd(t, admin, "CREATE SCHEMA "+schema)

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pinned := dsn + sep + "search_path=" + schema
	db, err := sql.Open("pgx", pinned)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	applyCmdMigrations(t, db)
	return db, pinned
}

func applyCmdMigrations(t *testing.T, db *sql.DB) {
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
		mustExecCmd(t, db, string(b))
	}
}

func mustExecCmd(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatalf("exec failed: %v\nstatement: %s", err, stmt)
	}
}

func countCmd(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestIntegration_CmdPrune_ContinuesPastFailedTable pins the job-level failure
// contract: a table whose prune fails must not stop the other table from being
// pruned (exit 1, but io_log's work stands), and the log must NAME the failed
// table so a CronJob alert tells the operator what broke. A missing table is a
// plain failure — it must NOT be misattributed to the per-table timeout (the
// ctx.Err check must happen before cancel, or cancel marks every failure as a
// timeout).
func TestIntegration_CmdPrune_ContinuesPastFailedTable(t *testing.T) {
	db, pinned := newCmdPruneHarness(t)

	// billing_event missing: its prune fails with relation-does-not-exist;
	// io_log's must still run to completion. CASCADE also drops the
	// billing_reconciliation_hourly view that depends on the table.
	mustExecCmd(t, db, "DROP TABLE billing_event CASCADE")
	mustExecCmd(t, db, `INSERT INTO io_log (request_id, model, created_at) VALUES ('cmd-old', 'model', $1)`,
		time.Now().Add(-10*24*time.Hour))

	configPath := writeConfig(t, "billingEventRetentionDays: 30\nioLogRetentionDays: 7\nbatchSize: 100\n")
	env := func(string) string { return pinned }

	code := exitOK
	out := captureOutput(t, func() {
		code = run(configPath, env)
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want %d (billing_event failed)", code, exitFatal)
	}
	if got := countCmd(t, db, "io_log"); got != 0 {
		t.Fatalf("io_log rows = %d, want 0: the job must continue past the failed billing_event table", got)
	}
	if !strings.Contains(out, "prune: billing_event:") {
		t.Fatalf("log must name billing_event as the failed table:\n%s", out)
	}
	if strings.Contains(out, "per-table timeout") {
		t.Fatalf("a missing table is a plain failure, not a timeout:\n%s", out)
	}
}

// TestIntegration_StorePruneWithExpiredContextWrapsDeadline proves the
// positive half of the timeout attribution: against live Postgres, a Prune
// whose context has already hit its deadline fails with an error that
// errors.Is(context.DeadlineExceeded) matches through the store's %w wrap —
// exactly the signal timeoutForm keys on. (Driving run()'s whole loop to a
// real 30-minute deadline is not testable in a suite, so the log branch is
// pinned by the timeoutForm unit test and this pins the store half.)
func TestIntegration_StorePruneWithExpiredContextWrapsDeadline(t *testing.T) {
	db, _ := newCmdPruneHarness(t)
	mustExecCmd(t, db, `INSERT INTO io_log (request_id, model, created_at) VALUES ('deadline-old', 'model', $1)`,
		time.Now().Add(-40*24*time.Hour))

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := prune.NewStore(db).Prune(ctx, prune.IoLog, prune.DefaultIoLogRetentionDays, 100)
	if err == nil {
		t.Fatal("expected an error pruning with an expired context, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired-context prune error = %v, want it to wrap context.DeadlineExceeded", err)
	}
}
