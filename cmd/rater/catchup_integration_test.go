//go:build integration

package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// catchUpDB applies every migration to a fresh schema, points DATABASE_URL
// at it, and returns the database and a routine settings file.
func catchUpDB(t *testing.T, sch string) (*sql.DB, string) {
	t.Helper()
	base := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
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
	t.Cleanup(func() { _ = db.Close() })
	path := filepath.Join(t.TempDir(), "rater.yaml")
	if err := os.WriteFile(path, []byte("rateTrailingHours: 24\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return db, path
}

var catchUpSeq int

// seedHour writes n clean, billable events in hour h.
func seedHour(t *testing.T, db *sql.DB, h time.Time, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		catchUpSeq++
		at := h.Add(time.Duration(i+1) * time.Minute)
		if _, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model,
			user_id, serving_mode, prompt_tokens, cached_tokens, completion_tokens, usage_found, status_code, event_ts, created_at)
			VALUES ($1,'a','res','org-1','m','B','u','dedicated',100,10,20,true,200,$2,$2)`,
			fmt.Sprintf("cu-%d", catchUpSeq), at); err != nil {
			t.Fatal(err)
		}
	}
}

// ratedHours maps each rated hour to its total completion tokens.
func ratedHours(t *testing.T, db *sql.DB) map[time.Time]int64 {
	t.Helper()
	rows, err := db.Query(`SELECT window_start, SUM(completion_tokens)::bigint FROM rated_usage GROUP BY window_start`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[time.Time]int64{}
	for rows.Next() {
		var h time.Time
		var n int64
		if err := rows.Scan(&h, &n); err != nil {
			t.Fatal(err)
		}
		out[h.UTC()] = n
	}
	return out
}

// ratedSnapshot is every rated_usage row with every column but rated_at, in
// a stable order.
func ratedSnapshot(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT concat_ws('|', id, auth_id, owner_type, owner_id, resource_id, org_id, model_id,
		serving_mode, graph_k8s_name, topology_revision, region, window_start, window_end, prompt_tokens, cached_tokens,
		completion_tokens, billable_prompt_tokens, event_count, base_model, adapter)
		FROM rated_usage ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func catchUpCompleted(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var done bool
	err := db.QueryRow(`SELECT catchup_completed_at IS NOT NULL FROM rater_state`).Scan(&done)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return done
}

func routineRun(t *testing.T, path string) int {
	t.Helper()
	return runWith([]string{"-f", path}, logging.New(logging.ERROR))
}

// An install that ran with the rater suspended has billing_event hours, older
// than the trailing window, that no run ever rated. The first routine run
// rates every one of them (and the trailing window), then marks the catch-up
// complete.
func TestRater_FirstRunCatchUpRatesEveryNeverRatedHour(t *testing.T) {
	db, path := catchUpDB(t, "phoebe_rater_catchup_all_it")
	now := time.Now().UTC().Truncate(time.Hour)
	old := []time.Time{now.Add(-29 * 24 * time.Hour), now.Add(-500 * time.Hour), now.Add(-100 * time.Hour), now.Add(-25 * time.Hour)}
	for _, h := range old {
		seedHour(t, db, h, 2)
	}
	recent := now.Add(-2 * time.Hour)
	seedHour(t, db, recent, 1)

	if code := routineRun(t, path); code != exitOK {
		t.Fatalf("first routine run: exit %d, want exitOK", code)
	}
	got := ratedHours(t, db)
	for _, h := range append(old, recent) {
		want := int64(40)
		if h.Equal(recent) {
			want = 20
		}
		if got[h] != want {
			t.Fatalf("hour %s: %d completion tokens rated, want %d (rated: %v)", h, got[h], want, got)
		}
	}
	if len(got) != len(old)+1 {
		t.Fatalf("rated hours %v, want exactly the %d seeded", got, len(old)+1)
	}
	if !catchUpCompleted(t, db) {
		t.Fatal("the catch-up rated everything but did not mark itself complete")
	}
}

// The catch-up rates at most catchUpHoursPerRun hours per run, oldest first,
// and finishes over several runs, marking itself complete on the last.
func TestRater_CatchUpRespectsThePerRunCapAndFinishesOverSeveralRuns(t *testing.T) {
	db, path := catchUpDB(t, "phoebe_rater_catchup_cap_it")
	prev := catchUpHoursPerRun
	catchUpHoursPerRun = 2
	t.Cleanup(func() { catchUpHoursPerRun = prev })

	now := time.Now().UTC().Truncate(time.Hour)
	var old []time.Time // oldest first
	for i := 5; i >= 1; i-- {
		h := now.Add(-time.Duration(24+10*i) * time.Hour)
		old = append(old, h)
		seedHour(t, db, h, 1)
	}
	for run, wantRated := range []int{2, 4, 5} {
		if code := routineRun(t, path); code != exitOK {
			t.Fatalf("run %d: exit %d, want exitOK", run+1, code)
		}
		got := ratedHours(t, db)
		if len(got) != wantRated {
			t.Fatalf("run %d: %d hours rated, want %d", run+1, len(got), wantRated)
		}
		for i, h := range old {
			if _, ok := got[h]; ok != (i < wantRated) {
				t.Fatalf("run %d: hour %d (%s) rated=%v; want the oldest %d rated first", run+1, i, h, ok, wantRated)
			}
		}
		if done := catchUpCompleted(t, db); done != (run == 2) {
			t.Fatalf("run %d: catch-up complete=%v, want %v", run+1, done, run == 2)
		}
	}
}

// Running the catch-up again, from scratch or by re-rating its hours with an
// explicit window, yields exactly the same rated_usage rows: no duplicates,
// no changed quantities, no changed ids.
func TestRater_CatchUpIsIdempotent(t *testing.T) {
	db, path := catchUpDB(t, "phoebe_rater_catchup_idem_it")
	now := time.Now().UTC().Truncate(time.Hour)
	old := []time.Time{now.Add(-300 * time.Hour), now.Add(-200 * time.Hour), now.Add(-48 * time.Hour)}
	for _, h := range old {
		seedHour(t, db, h, 3)
	}
	if code := routineRun(t, path); code != exitOK {
		t.Fatalf("first run: exit %d", code)
	}
	first := ratedSnapshot(t, db)
	if len(first) != len(old) {
		t.Fatalf("%d rated rows after the catch-up, want %d", len(first), len(old))
	}

	// The marker lost (a restored database, a migration re-applied): the
	// catch-up starts over.
	if _, err := db.Exec(`DELETE FROM rater_state`); err != nil {
		t.Fatal(err)
	}
	if code := routineRun(t, path); code != exitOK {
		t.Fatalf("catch-up from scratch: exit %d", code)
	}
	if again := ratedSnapshot(t, db); strings.Join(again, "\n") != strings.Join(first, "\n") {
		t.Fatalf("rated_usage changed when the catch-up ran again:\nfirst:\n%s\nagain:\n%s", strings.Join(first, "\n"), strings.Join(again, "\n"))
	}

	// Re-rating the caught-up hours themselves converges to the same rows.
	for _, h := range old {
		if code := runWith([]string{"-f", path, "-since", h.Format(time.RFC3339), "-until", h.Add(time.Hour).Format(time.RFC3339)},
			logging.New(logging.ERROR)); code != exitOK {
			t.Fatalf("explicit re-rate of %s: exit %d", h, code)
		}
	}
	if again := ratedSnapshot(t, db); strings.Join(again, "\n") != strings.Join(first, "\n") {
		t.Fatalf("re-rating the caught-up hours changed rated_usage:\nfirst:\n%s\nagain:\n%s", strings.Join(first, "\n"), strings.Join(again, "\n"))
	}
}

// The catch-up never rewrites an hour that already has rollups: after the
// pruner (or anything else) removed some of its events, re-aggregating it
// would replace the rollup with a partial sum, which token-push would send.
func TestRater_CatchUpNeverRewritesAnHourThatWasAlreadyRated(t *testing.T) {
	db, path := catchUpDB(t, "phoebe_rater_catchup_rated_it")
	now := time.Now().UTC().Truncate(time.Hour)
	rated := now.Add(-400 * time.Hour)
	seedHour(t, db, rated, 4)
	if code := runWith([]string{"-f", path, "-since", rated.Format(time.RFC3339), "-until", rated.Add(time.Hour).Format(time.RFC3339)},
		logging.New(logging.ERROR)); code != exitOK {
		t.Fatalf("the run that rated the hour at the time: exit %d", code)
	}
	before := ratedSnapshot(t, db)
	// Half of the hour's events are gone.
	if _, err := db.Exec(`DELETE FROM billing_event WHERE request_id IN (SELECT request_id FROM billing_event ORDER BY event_ts LIMIT 2)`); err != nil {
		t.Fatal(err)
	}
	if code := routineRun(t, path); code != exitOK {
		t.Fatalf("routine run: exit %d", code)
	}
	if after := ratedSnapshot(t, db); strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("the catch-up rewrote an hour that was already rated:\nbefore:\n%s\nafter:\n%s", strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	if !catchUpCompleted(t, db) {
		t.Fatal("catch-up not complete")
	}
}

// Once the catch-up is complete, a routine run rates only its trailing
// window: an older never-rated hour is left alone.
func TestRater_AfterTheCatchUpMarkerOnlyTheTrailingWindowIsRated(t *testing.T) {
	db, path := catchUpDB(t, "phoebe_rater_catchup_done_it")
	if code := routineRun(t, path); code != exitOK || !catchUpCompleted(t, db) {
		t.Fatalf("first run on an empty install: exit %d, complete %v; want exitOK and the catch-up complete", code, catchUpCompleted(t, db))
	}
	now := time.Now().UTC().Truncate(time.Hour)
	old, recent := now.Add(-100*time.Hour), now.Add(-3*time.Hour)
	seedHour(t, db, old, 1)
	seedHour(t, db, recent, 1)
	if code := routineRun(t, path); code != exitOK {
		t.Fatalf("routine run: exit %d", code)
	}
	got := ratedHours(t, db)
	if _, ok := got[old]; ok {
		t.Fatalf("hour %s outside the trailing window was rated after the catch-up completed: %v", old, got)
	}
	if got[recent] != 20 || len(got) != 1 {
		t.Fatalf("rated %v, want only the trailing-window hour %s", got, recent)
	}
}
