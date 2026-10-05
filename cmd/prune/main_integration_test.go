//go:build integration

package main

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/migrations"
)

// newCmdPruneHarness creates an isolated schema with ALL production migrations
// applied (discovered from the embedded migrations.FS, ordered by filename —
// a hand-maintained list could go stale while green) and returns a pool pinned
// to it plus the schema-pinned DSN for handing to run() as DATABASE_URL.
func newCmdPruneHarness(t *testing.T, schema string) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune cmd integration test")
	}

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	mustExecCmd(t, admin, "CREATE EXTENSION IF NOT EXISTS btree_gist")
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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prune.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// captureOutput redirects os.Stdout and os.Stderr — where logging.New writes —
// for the duration of f, returning everything printed. run() builds its logger
// after the redirect, so the capture sees the job's real output.
func captureOutput(t *testing.T, f func()) string {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = w, w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	_ = w.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	return <-done
}

// TestIntegration_CmdPrune_ContinuesPastFailedTable pins the job-level failure
// contract: a table whose prune fails must not stop the other table from being
// pruned (exit 1, but io_log's work stands), and the log must NAME the failed
// table so a CronJob alert tells the operator what broke.
func TestIntegration_CmdPrune_ContinuesPastFailedTable(t *testing.T) {
	db, pinned := newCmdPruneHarness(t, "phoebe_prune_cmd_it")

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
}

// TestIntegration_CmdPrune_MissingConfigAndNoDatabaseURL pins the env-only
// failure mode: a missing settings file is tolerated (INFO + ruled defaults),
// and with no DATABASE_URL the job then refuses cleanly with exit 1 — no
// panic, no partial work.
func TestIntegration_CmdPrune_MissingConfigAndNoDatabaseURL(t *testing.T) {
	if os.Getenv("PHOEBE_TEST_DATABASE_URL") == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune cmd integration test")
	}
	env := func(string) string { return "" }

	code := exitOK
	out := captureOutput(t, func() {
		code = run(filepath.Join(t.TempDir(), "does-not-exist.yaml"), env)
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want %d", code, exitFatal)
	}
	if !strings.Contains(out, "DATABASE_URL") {
		t.Fatalf("expected a clean DATABASE_URL error, got:\n%s", out)
	}
}

// TestIntegration_CmdPrune_BelowFloorConfigRefusedBeforeDB pins the order of
// checks: a below-floor config is refused at validation, BEFORE the database
// is consulted — env records whether the job asked for DATABASE_URL at all.
func TestIntegration_CmdPrune_BelowFloorConfigRefusedBeforeDB(t *testing.T) {
	if os.Getenv("PHOEBE_TEST_DATABASE_URL") == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping prune cmd integration test")
	}
	configPath := writeConfig(t, "billingEventRetentionDays: 3\n")
	consulted := false
	env := func(string) string {
		consulted = true
		return ""
	}

	code := exitOK
	out := captureOutput(t, func() {
		code = run(configPath, env)
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want %d", code, exitFatal)
	}
	if consulted {
		t.Fatal("below-floor config must be refused before the job consults the database")
	}
	if !strings.Contains(out, "hard floor") {
		t.Fatalf("expected the hard-floor refusal in the log, got:\n%s", out)
	}
}
