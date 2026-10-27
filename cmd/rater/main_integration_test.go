//go:build integration

package main

import (
	"bytes"
	"database/sql"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/migrations"
)

// applyMigrations creates schema sch and applies every embedded up migration
// in version order, the way cmd/migrate does.
func applyMigrations(t *testing.T, dsn, sch string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("exec %.60q: %v", q, err)
		}
	}
	mustExec("DROP SCHEMA IF EXISTS " + sch + " CASCADE")
	mustExec("CREATE SCHEMA " + sch)
	t.Cleanup(func() {
		if db, err := sql.Open("pgx", dsn); err == nil {
			_, _ = db.Exec("DROP SCHEMA IF EXISTS " + sch + " CASCADE")
			_ = db.Close()
		}
	})
	mustExec("SET search_path TO " + sch)
	names, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	for _, n := range names {
		b, _ := migrations.FS.ReadFile(n)
		mustExec(string(b))
	}
}

// Hugo approved removing SATURN_TOKEN and managerURL from the rater
// (2026-10-27). The rater must run to completion with neither: it makes no
// manager call. An old ConfigMap that still carries managerURL gets exactly
// one warning, never a failure.
func TestRater_RunsWithNoSaturnTokenAndNoManagerURL(t *testing.T) {
	base := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
	const sch = "phoebe_rater_cmd_it"
	applyMigrations(t, base, sch)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	t.Setenv("DATABASE_URL", base+sep+"search_path="+sch)
	t.Setenv("SATURN_TOKEN", "")
	os.Unsetenv("SATURN_TOKEN")

	db, err := sql.Open("pgx", base+sep+"search_path="+sch)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	if _, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model,
		user_id, serving_mode, prompt_tokens, completion_tokens, usage_found, status_code, event_ts)
		VALUES ('r1','a','res','org','m','B','u','dedicated',10,2,true,200,$1)`, hour.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	for name, settings := range map[string]string{
		"no managerURL":                      "rateTrailingHours: 6\n",
		"old ConfigMap still has managerURL": "rateTrailingHours: 6\nmanagerURL: \"https://manager.example\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rater.yaml")
			if err := os.WriteFile(path, []byte(settings), 0o600); err != nil {
				t.Fatal(err)
			}
			var warn bytes.Buffer
			l := logging.New(logging.INFO)
			l.Warn = log.New(&warn, "", 0)
			if code := runWith([]string{"-f", path}, l); code != exitOK {
				t.Fatalf("exit %d, want 0 with no SATURN_TOKEN and no manager", code)
			}
			n := strings.Count(warn.String(), "ignoring managerURL")
			if strings.Contains(settings, "managerURL") && n != 1 {
				t.Fatalf("%d managerURL warnings, want exactly 1: %q", n, warn.String())
			}
			if !strings.Contains(settings, "managerURL") && n != 0 {
				t.Fatalf("unexpected managerURL warning: %q", warn.String())
			}
			var rows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM rated_usage WHERE window_start = $1`, hour).Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("rated_usage rows %d (err %v), want the hour aggregated", rows, err)
			}
		})
	}
}
