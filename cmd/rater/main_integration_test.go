//go:build integration

package main

import (
	"bytes"
	"context"
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
	"github.com/saturncloud/phoebe/internal/prune"
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

// Parked usage is never dropped after the 24-hour window. A conflict parked
// three days ago (inside the automatic re-check reach) and one parked ten days
// ago (beyond it) are both still listed on a routine run today, the run exits
// exitParked (4), and `-list-parked` prints both with their token counts.
func TestRater_ParkedUsageSurvivesPast24HoursAndIsListed(t *testing.T) {
	base := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
	const sch = "phoebe_rater_parked_it"
	applyMigrations(t, base, sch)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	dsn := base + sep + "search_path=" + sch
	t.Setenv("DATABASE_URL", dsn)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Hour)
	for i, h := range []time.Time{now.Add(-72 * time.Hour), now.Add(-240 * time.Hour)} {
		for j, org := range []string{"org-1", "org-2"} {
			if _, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model,
				user_id, serving_mode, prompt_tokens, completion_tokens, usage_found, status_code, event_ts)
				VALUES ($1,'a','res','`+org+`','m','B','u','dedicated',100,20,true,200,$2)`,
				"p"+string(rune('0'+i))+string(rune('0'+j)), h.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		// The run that covered that hour at the time parked it.
		path := filepath.Join(t.TempDir(), "rater.yaml")
		_ = os.WriteFile(path, []byte("rateTrailingHours: 1\n"), 0o600)
		if code := runWith([]string{"-f", path, "-since", h.Format(time.RFC3339), "-until", h.Add(time.Hour).Format(time.RFC3339)},
			logging.New(logging.ERROR)); code != exitParked {
			t.Fatalf("parking run for %s: exit %d, want exitParked", h, code)
		}
	}

	path := filepath.Join(t.TempDir(), "rater.yaml")
	_ = os.WriteFile(path, []byte("rateTrailingHours: 24\n"), 0o600)
	var errLog bytes.Buffer
	l := logging.New(logging.INFO)
	l.Error = log.New(&errLog, "", 0)
	if code := runWith([]string{"-f", path}, l); code != exitParked {
		t.Fatalf("routine run today: exit %d, want exitParked (%d)", code, exitParked)
	}
	if n := strings.Count(errLog.String(), "PARKED (unbilled) usage"); n != 2 {
		t.Fatalf("%d PARKED lines, want 2 (one per parked identity-hour):\n%s", n, errLog.String())
	}
	if !strings.Contains(errLog.String(), "reason=ambiguous_org events=2 prompt=200") {
		t.Fatalf("parked line lacks the reason and token counts:\n%s", errLog.String())
	}

	var unresolved int
	if err := db.QueryRow(`SELECT COUNT(*) FROM parked_usage WHERE resolved_at IS NULL`).Scan(&unresolved); err != nil || unresolved != 2 {
		t.Fatalf("%d unresolved parked rows (err %v), want 2", unresolved, err)
	}

	// The operator listing.
	r, w, _ := os.Pipe()
	stdout := os.Stdout
	os.Stdout = w
	code := runWith([]string{"-f", path, "-list-parked"}, logging.New(logging.ERROR))
	_ = w.Close()
	os.Stdout = stdout
	var out bytes.Buffer
	_, _ = out.ReadFrom(r)
	if code != exitParked || !strings.Contains(out.String(), "2 parked identity-hour(s)") ||
		!strings.Contains(out.String(), now.Add(-240*time.Hour).Format(time.RFC3339)) {
		t.Fatalf("-list-parked exit %d output:\n%s", code, out.String())
	}
}

// Hugo, 2026-10-27: the 30-day billing_event horizon has no exception for
// parked usage. A parked hour 40 days old, after the REAL pruner has run, is
// listed with the pruned marker (in -list-parked and in the run's ERROR line),
// keeps its counts, and is never re-aggregated: an explicit window over it is
// refused, the clean rollup of that hour is untouched, the parked row stays
// unresolved.
func TestRater_ParkedRowOlderThanThePruneHorizonIsListedAsPrunedAndNeverReaggregated(t *testing.T) {
	base := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
	const sch = "phoebe_rater_pruned_it"
	applyMigrations(t, base, sch)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	dsn := base + sep + "search_path=" + sch
	t.Setenv("DATABASE_URL", dsn)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	old := time.Now().UTC().Truncate(time.Hour).Add(-40 * 24 * time.Hour)
	for i, ev := range []struct{ res, org string }{{"res-clean", "org-1"}, {"res-x", "org-1"}, {"res-x", "org-2"}} {
		at := old.Add(time.Duration(i+1) * time.Minute)
		if _, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model,
			user_id, serving_mode, prompt_tokens, completion_tokens, usage_found, status_code, event_ts, created_at)
			VALUES ($1,'a',$2,$3,'m','B','u','dedicated',100,20,true,200,$4,$4)`,
			"old-"+string(rune('0'+i)), ev.res, ev.org, at); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "rater.yaml")
	_ = os.WriteFile(path, []byte("rateTrailingHours: 24\n"), 0o600)
	explicit := []string{"-f", path, "-since", old.Format(time.RFC3339), "-until", old.Add(time.Hour).Format(time.RFC3339)}
	if code := runWith(explicit, logging.New(logging.ERROR)); code != exitParked {
		t.Fatalf("the run 40 days ago: exit %d, want exitParked", code)
	}

	// The real pruner at the ruled default horizon.
	if res, err := prune.NewStore(db).Prune(context.Background(), prune.BillingEvent,
		prune.DefaultBillingEventRetentionDays, prune.DefaultBatchSize); err != nil || res.RowsDeleted != 3 {
		t.Fatalf("prune: %+v err %v, want the 3 old events deleted", res, err)
	}

	// Routine run today: still parked, ERROR line carries the marker.
	var errLog bytes.Buffer
	l := logging.New(logging.INFO)
	l.Error = log.New(&errLog, "", 0)
	if code := runWith([]string{"-f", path}, l); code != exitParked {
		t.Fatalf("routine run: exit %d, want exitParked", code)
	}
	if !strings.Contains(errLog.String(), "raw_events: pruned — settle manually via manager credit") ||
		!strings.Contains(errLog.String(), "events=2 prompt=200") {
		t.Fatalf("ERROR line lacks the marker or the counts:\n%s", errLog.String())
	}

	// -list-parked marks it.
	r, w, _ := os.Pipe()
	stdout := os.Stdout
	os.Stdout = w
	code := runWith([]string{"-f", path, "-list-parked"}, logging.New(logging.ERROR))
	_ = w.Close()
	os.Stdout = stdout
	var out bytes.Buffer
	_, _ = out.ReadFrom(r)
	if code != exitParked || !strings.Contains(out.String(), "RAW_EVENTS") ||
		!strings.Contains(out.String(), "raw_events: pruned — settle manually via manager credit") {
		t.Fatalf("-list-parked exit %d output:\n%s", code, out.String())
	}

	// Never re-aggregated: an explicit run over the hour is refused, and nothing changes.
	if code := runWith(explicit, logging.New(logging.ERROR)); code != exitFatal {
		t.Fatalf("explicit re-run over the pruned parked hour: exit %d, want exitFatal (refused)", code)
	}
	var clean, unresolved int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rated_usage WHERE window_start = $1 AND resource_id = 'res-clean'`, old).Scan(&clean); err != nil || clean != 1 {
		t.Fatalf("clean rollup rows %d (err %v), want 1 untouched", clean, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM parked_usage WHERE resolved_at IS NULL AND event_count = 2`).Scan(&unresolved); err != nil || unresolved != 1 {
		t.Fatalf("unresolved parked rows %d (err %v), want 1 with its counts", unresolved, err)
	}
}
