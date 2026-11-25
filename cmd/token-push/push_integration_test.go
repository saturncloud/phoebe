//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
	"github.com/saturncloud/phoebe/internal/rating"
	"github.com/saturncloud/phoebe/migrations"
)

// itDB creates schema sch with every embedded up migration applied and returns
// a pool whose connections use it.
func itDB(t *testing.T, sch string) *sql.DB {
	t.Helper()
	base := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for _, q := range []string{"DROP SCHEMA IF EXISTS " + sch + " CASCADE", "CREATE SCHEMA " + sch} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", base+sep+"search_path="+sch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if a, err := sql.Open("pgx", base); err == nil {
			_, _ = a.Exec("DROP SCHEMA IF EXISTS " + sch + " CASCADE")
			_ = a.Close()
		}
	})
	names, _ := fs.Glob(migrations.FS, "*.up.sql")
	sort.Strings(names)
	for _, n := range names {
		b, _ := migrations.FS.ReadFile(n)
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("migration %s: %v", n, err)
		}
	}
	return db
}

const (
	itOrg1 = "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
	itOrg2 = "9f8e7d6c5b4a39281706f5e4d3c2b1a0"
	itUser = "1f2e3d4c5b6a79880716a5b4c3d2e1f0"
	itAuth = "5f60718293a4b5c6d7e8f90a1b2c3d4e"
	itRes1 = "3d4e5f60718293a4b5c6d7e8f90a1b2c"
	itRes2 = "4e5f60718293a4b5c6d7e8f90a1b2c3d"
)

var itSeq int

func event(t *testing.T, db *sql.DB, at time.Time, resource, org string) {
	t.Helper()
	itSeq++
	var orgArg any
	if org != "" {
		orgArg = org
	}
	if _, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model,
		user_id, serving_mode, prompt_tokens, cached_tokens, completion_tokens, usage_found, status_code, event_ts)
		VALUES ($1,$2,$3,$4,'qwen-ep','Qwen/Qwen2.5-32B-Instruct',$5,'dedicated',100,10,20,true,200,$6)`,
		fmt.Sprintf("it-%d", itSeq),
		itAuth, resource, orgArg, itUser, at); err != nil {
		t.Fatal(err)
	}
}

func rate(t *testing.T, db *sql.DB, start, end time.Time) rating.Result {
	t.Helper()
	res, err := rating.New(rating.NewPostgresStore(db), logging.New(logging.ERROR)).Run(context.Background(), start, end, false)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func push(t *testing.T, db *sql.DB, m *recordingManager, hours ...time.Time) int {
	t.Helper()
	return (&pusher{store: pgStore{db: db}, log: logging.New(logging.ERROR), client: m.client(t)}).
		pushWindows(context.Background(), hours, time.Now())
}

func lastRequest(t *testing.T, m *recordingManager) meteredusage.Request {
	t.Helper()
	return sentRequest(t, m.bodies[len(m.bodies)-1])
}

// A conflict in one identity (two orgs for res-2) parks res-2 only; res-1's
// record for the same hour is sent on time.
func TestIntegration_AConflictInOneIdentityStillSendsEveryCleanRecordOfTheHour(t *testing.T) {
	db := itDB(t, "phoebe_push_conflict_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	event(t, db, h.Add(2*time.Minute), itRes2, itOrg1)
	event(t, db, h.Add(3*time.Minute), itRes2, itOrg2)

	res := rate(t, db, h, h.Add(time.Hour))
	if res.AmbiguousOrgEvents != 2 || !res.HasParked() || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want res-2 parked and res-1 written", res.AggregateResult)
	}
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	req := lastRequest(t, m)
	if len(req.Records) != 1 || req.Records[0].ResourceID != itRes1 || len(req.Held) != 0 {
		t.Fatalf("records %+v held %v, want only res-1 sent and nothing held", req.Records, req.Held)
	}
}

// res-2 was clean and sent; a late event makes it conflict. The next push sends
// res-1 and lists res-2's earlier usage_id in held, every time, until resolved.
func TestIntegration_AnAlreadySentConflictedRecordIsHeld(t *testing.T) {
	db := itDB(t, "phoebe_push_held_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	event(t, db, h.Add(2*time.Minute), itRes2, itOrg1)
	rate(t, db, h, h.Add(time.Hour))
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	var res2ID string
	for _, r := range lastRequest(t, m).Records {
		if r.ResourceID == itRes2 {
			res2ID = r.UsageID
		}
	}

	event(t, db, h.Add(30*time.Minute), itRes2, itOrg2) // late, conflicting
	if res := rate(t, db, h, h.Add(time.Hour)); res.ParkedDeletions != 1 {
		t.Fatalf("%+v, want res-2's row moved to parked", res.AggregateResult)
	}
	for i := 0; i < 2; i++ {
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("push %d: exit %d", i, code)
		}
		req := lastRequest(t, m)
		if len(req.Records) != 1 || req.Records[0].ResourceID != itRes1 || len(req.Held) != 1 || req.Held[0] != res2ID {
			t.Fatalf("push %d: records %d held %v, want res-1 sent and held [%s]", i, len(req.Records), req.Held, res2ID)
		}
	}
}

// A parked hour three days old (outside the 24h windows) is resolved when the
// data is fixed: the rater re-checks it on its own and token-push sends it once.
func TestIntegration_ResolvingTheConflictSendsTheParkedHour(t *testing.T) {
	db := itDB(t, "phoebe_push_resolve_it")
	now := time.Now().UTC().Truncate(time.Hour)
	old := now.Add(-72 * time.Hour)
	event(t, db, old.Add(time.Minute), itRes2, itOrg1)
	event(t, db, old.Add(2*time.Minute), itRes2, itOrg2)
	rate(t, db, old, old.Add(time.Hour)) // the run three days ago parked it

	window := now.Add(-24 * time.Hour)
	if res := rate(t, db, window, now); len(res.Parked) != 1 || res.RescannedParkedHours != 1 {
		t.Fatalf("%+v parked=%d, want the old hour re-checked and still parked", res.AggregateResult, len(res.Parked))
	}
	m := &recordingManager{}
	push(t, db, m, window)
	if len(m.bodies) != 1 {
		t.Fatalf("sent %d hours, want only the window hour while the old hour is parked", len(m.bodies))
	}

	// An operator fixes the data: the second org was wrong.
	if _, err := db.Exec(`UPDATE billing_event SET org_id = $1 WHERE resource_id = $2`, itOrg1, itRes2); err != nil {
		t.Fatal(err)
	}
	res := rate(t, db, window, now)
	if res.ParkedResolved != 1 || res.HasParked() {
		t.Fatalf("%+v parked=%d, want the old hour resolved", res.AggregateResult, len(res.Parked))
	}
	push(t, db, m, window)
	var oldReq *meteredusage.Request
	for _, b := range m.bodies[1:] {
		r := sentRequest(t, b)
		if r.WindowStart == meteredusage.FormatWindowStart(old) {
			oldReq = &r
		}
	}
	if oldReq == nil || len(oldReq.Records) != 1 || oldReq.Records[0].Quantities.RequestCount != 2 || oldReq.Records[0].OrgID != itOrg1 {
		t.Fatalf("old hour request %+v, want res-2 sent with both requests under org-1", oldReq)
	}
	// Sent once: the next run does not re-send it.
	n := len(m.bodies)
	push(t, db, m, window)
	if len(m.bodies) != n+1 {
		t.Fatalf("the resolved hour was sent again (%d requests, want %d)", len(m.bodies), n+1)
	}
}
