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

// push runs token-push as the CronJob does (a scheduled run, no -since/-until)
// over the given hours.
func push(t *testing.T, db *sql.DB, m *recordingManager, hours ...time.Time) int {
	t.Helper()
	return pushRun(t, db, m, false, hours...)
}

// pushExplicit runs token-push as an operator's -since/-until run.
func pushExplicit(t *testing.T, db *sql.DB, m *recordingManager, hours ...time.Time) int {
	t.Helper()
	return pushRun(t, db, m, true, hours...)
}

func pushRun(t *testing.T, db *sql.DB, m *recordingManager, explicit bool, hours ...time.Time) int {
	t.Helper()
	return (&pusher{store: pgStore{db: db}, log: logging.New(logging.ERROR), client: m.client(t)}).
		pushWindows(context.Background(), hours, time.Now(), explicit)
}

func autoFloor(t *testing.T, db *sql.DB) *time.Time {
	t.Helper()
	var f time.Time
	switch err := db.QueryRow(`SELECT auto_floor FROM token_push_state`).Scan(&f); {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		t.Fatal(err)
	}
	f = f.UTC()
	return &f
}

func windowStarts(t *testing.T, m *recordingManager) []string {
	t.Helper()
	var out []string
	for _, b := range m.bodies {
		out = append(out, sentRequest(t, b).WindowStart)
	}
	return out
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
	event(t, db, window.Add(time.Minute), itRes1, itOrg1) // the window hour has usage, so it is sent
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

// Missed hours are re-sent; usage from before the install was connected is not.
// h0 has usage but comes before the first hour a scheduled run had accepted
// (the install was not connected yet), h1 is that first accepted hour, h2 has
// usage and its push failed (the manager answered 503 until the retries ran
// out), h3 is the current window.
func TestIntegration_AnHourNeverAcceptedAfterTheFirstPushIsReSentButNotEarlierUsage(t *testing.T) {
	db := itDB(t, "phoebe_push_unsent_it")
	now := time.Now().UTC().Truncate(time.Hour)
	h0, h1, h2, h3 := now.Add(-100*time.Hour), now.Add(-80*time.Hour), now.Add(-60*time.Hour), now.Add(-time.Hour)
	for _, h := range []time.Time{h0, h1, h2} {
		event(t, db, h.Add(time.Minute), itRes1, itOrg1)
		rate(t, db, h, h.Add(time.Hour))
	}
	store := pgStore{db: db}
	if got, err := store.UnsentHours(context.Background(), now, 10); err != nil || len(got) != 0 {
		t.Fatalf("before any accepted push: unsent %v err %v, want none", got, err)
	}
	if ever, err := store.EverPushed(context.Background()); err != nil || ever {
		t.Fatalf("EverPushed = %v err %v before any push, want false", ever, err)
	}

	m := &recordingManager{}
	if code := push(t, db, m, h1); code != exitOK { // first hour a scheduled run had accepted
		t.Fatalf("exit %d", code)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(h1) {
		t.Fatalf("auto_floor %v, want %s", f, h1)
	}
	down := &recordingManager{statuses: []int{503}}
	if code := push(t, db, down, h2); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal (503 outlasted the retries)", code)
	}
	got, err := store.UnsentHours(context.Background(), now, 10)
	if err != nil || len(got) != 1 || !got[0].Equal(h2) {
		t.Fatalf("unsent %v err %v, want [%s] (h0 predates the first push)", got, err, h2)
	}
	if n, oldest, err := store.PreConnectionHours(context.Background()); err != nil || n != 1 || !oldest.Equal(h0) {
		t.Fatalf("PreConnectionHours = %d, %s, %v; want 1 hour, oldest %s (h0)", n, oldest, err, h0)
	}

	event(t, db, h3.Add(time.Minute), itRes1, itOrg1)
	rate(t, db, h3, h3.Add(time.Hour))
	m2 := &recordingManager{}
	if code := push(t, db, m2, h3); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	sent := windowStarts(t, m2)
	// The window first, then the missed hour.
	want := []string{meteredusage.FormatWindowStart(h3), meteredusage.FormatWindowStart(h2)}
	if strings.Join(sent, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v", sent, want)
	}
	if got, _ := store.UnsentHours(context.Background(), now, 10); len(got) != 0 {
		t.Fatalf("unsent after re-send %v, want none", got)
	}
}

// An operator's -since/-until backfill sends old hours, but must not lower the
// automatic re-send floor: if it did, every later scheduled run would send, on
// its own, the pre-connection usage between the backfilled hour and the floor.
func TestExplicitBackfillDoesNotLowerAutoResendFloor(t *testing.T) {
	db := itDB(t, "phoebe_push_floor_it")
	now := time.Now().UTC().Truncate(time.Hour)
	hBackfill, hBetween, hFirst := now.Add(-60*time.Hour), now.Add(-50*time.Hour), now.Add(-10*time.Hour)
	for _, h := range []time.Time{hBackfill, hBetween, hFirst} {
		event(t, db, h.Add(time.Minute), itRes1, itOrg1)
		rate(t, db, h, h.Add(time.Hour))
	}
	store := pgStore{db: db}

	// An explicit run before any scheduled one sets no floor.
	m := &recordingManager{}
	if code := pushExplicit(t, db, m, hBackfill); code != exitOK {
		t.Fatalf("explicit run: exit %d", code)
	}
	if f := autoFloor(t, db); f != nil {
		t.Fatalf("an explicit run set auto_floor to %s", f)
	}
	if got, err := store.UnsentHours(context.Background(), now, 10); err != nil || len(got) != 0 {
		t.Fatalf("unsent %v err %v after only an explicit run, want none", got, err)
	}

	// The first scheduled run sets the floor to its first accepted hour.
	if code := push(t, db, m, hFirst); code != exitOK {
		t.Fatalf("scheduled run: exit %d", code)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(hFirst) {
		t.Fatalf("auto_floor %v, want %s", f, hFirst)
	}

	// Another explicit backfill of an older hour leaves the floor where it is.
	if code := pushExplicit(t, db, m, hBackfill); code != exitOK {
		t.Fatalf("second explicit run: exit %d", code)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(hFirst) {
		t.Fatalf("auto_floor %v after a backfill, want %s still", f, hFirst)
	}

	// hBetween is pre-connection usage: a scheduled run never sends it.
	if got, err := store.UnsentHours(context.Background(), now, 10); err != nil || len(got) != 0 {
		t.Fatalf("unsent %v err %v, want none (hBetween is before the floor)", got, err)
	}
	m2 := &recordingManager{}
	if code := push(t, db, m2, hFirst); code != exitOK {
		t.Fatalf("later scheduled run: exit %d", code)
	}
	if sent := windowStarts(t, m2); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(hFirst) {
		t.Fatalf("later scheduled run sent %v, want only %s", sent, meteredusage.FormatWindowStart(hFirst))
	}
	// It is reported instead; the backfilled hour is not, it was accepted.
	if n, oldest, err := store.PreConnectionHours(context.Background()); err != nil || n != 1 || !oldest.Equal(hBetween) {
		t.Fatalf("PreConnectionHours = %d, %s, %v; want 1, %s", n, oldest, err, hBetween)
	}
}

// 0013 backfills the floor from pushed_hour on an install that already pushed,
// so the floor does not move on upgrade.
func TestIntegration_Migration0013BackfillsTheFloorFromPushedHour(t *testing.T) {
	db := itDB(t, "phoebe_push_m0013_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Hour)
	down, _ := migrations.FS.ReadFile("0013_token_push_state.down.sql")
	up, _ := migrations.FS.ReadFile("0013_token_push_state.up.sql")
	if _, err := db.Exec(string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now()), ($2, now())`, h, h.Add(5*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(up)); err != nil {
		t.Fatal(err)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(h) {
		t.Fatalf("auto_floor %v after 0013, want %s (min pushed_hour)", f, h)
	}
	if _, err := db.Exec(`INSERT INTO token_push_state (singleton, auto_floor) VALUES (false, now())`); err == nil {
		t.Fatal("token_push_state accepted a second row; it must hold one")
	}
}

// The unsent-hours list is capped and oldest first: with 200 never-accepted
// hours after the floor, one call returns exactly the 168 oldest, in order.
func TestIntegration_UnsentHoursIsCappedAndOldestFirst(t *testing.T) {
	db := itDB(t, "phoebe_push_unsent_cap_it")
	now := time.Now().UTC().Truncate(time.Hour)
	floor := now.Add(-210 * time.Hour)
	if _, err := db.Exec(`INSERT INTO token_push_state (auto_floor) VALUES ($1)`, floor); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now())`, floor); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 200; i++ {
		event(t, db, floor.Add(time.Duration(i)*time.Hour+time.Minute), itRes1, itOrg1)
	}
	rate(t, db, floor, now)
	got, err := pgStore{db: db}.UnsentHours(context.Background(), now, maxUnsentHoursPerRun)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 168 {
		t.Fatalf("%d unsent hours, want 168 (the cap)", len(got))
	}
	for i, h := range got {
		if want := floor.Add(time.Duration(i+1) * time.Hour); !h.Equal(want) {
			t.Fatalf("unsent[%d] = %s, want %s (oldest first)", i, h, want)
		}
	}
}
