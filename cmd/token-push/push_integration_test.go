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

// Missed hours are re-sent, and usage from before the install was connected
// is back-filled after them. h0 has usage but comes before the first hour a
// scheduled run had accepted (the install was not connected yet), h1 is that
// first accepted hour, h2 has usage and its push failed (the manager answered
// 503 until the retries ran out), h3 is the current window.
func TestIntegration_AnHourNeverAcceptedAfterTheFirstPushIsReSentThenEarlierUsage(t *testing.T) {
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
	// The window first, then the missed hour, then the back-fill.
	want := []string{meteredusage.FormatWindowStart(h3), meteredusage.FormatWindowStart(h2), meteredusage.FormatWindowStart(h0)}
	if strings.Join(sent, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v", sent, want)
	}
	if got, _ := store.UnsentHours(context.Background(), now, 10); len(got) != 0 {
		t.Fatalf("unsent after re-send %v, want none", got)
	}
	if n, _, err := store.PreConnectionHours(context.Background()); err != nil || n != 0 {
		t.Fatalf("PreConnectionHours = %d, %v after the back-fill, want 0", n, err)
	}
}

// An operator's -since/-until backfill sends old hours, but must not lower the
// automatic re-send floor. The floor is the first hour a scheduled run had
// accepted: missed hours after it go first, the back-fill of the usage before
// it shares what is left of the per-run cap. An explicit run moving it would
// turn pre-connection usage into missed hours ahead of real outages.
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

	// hBetween is pre-connection usage: not a missed hour after the floor but
	// the back-fill's; the explicitly sent hBackfill was accepted and is not.
	if got, err := store.UnsentHours(context.Background(), now, 10); err != nil || len(got) != 0 {
		t.Fatalf("unsent %v err %v, want none (hBetween is before the floor)", got, err)
	}
	if got, err := store.BackfillHours(context.Background(), 10); err != nil || len(got) != 1 || !got[0].Equal(hBetween) {
		t.Fatalf("back-fill %v err %v, want [%s]", got, err, hBetween)
	}

	// Another explicit backfill of an older hour leaves the floor where it is.
	if code := pushExplicit(t, db, m, hBackfill); code != exitOK {
		t.Fatalf("second explicit run: exit %d", code)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(hFirst) {
		t.Fatalf("auto_floor %v after a backfill, want %s still", f, hFirst)
	}
	// That run also sent the pending back-fill hour (every run does); nothing
	// before the floor is left, and the later scheduled run sends the window.
	if n, _, err := store.PreConnectionHours(context.Background()); err != nil || n != 0 {
		t.Fatalf("PreConnectionHours = %d, %v; want 0", n, err)
	}
	m2 := &recordingManager{}
	if code := push(t, db, m2, hFirst); code != exitOK {
		t.Fatalf("later scheduled run: exit %d", code)
	}
	if sent := windowStarts(t, m2); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(hFirst) {
		t.Fatalf("later scheduled run sent %v, want only %s", sent, meteredusage.FormatWindowStart(hFirst))
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

func refusedCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM refused_hour`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 170 hours the manager refused for their content (400) and one newer hour
// whose push was never accepted: the newer hour is sent in the next run. The
// refused hours are parked, so they are not in the unsent list and cannot use
// up the 168-hour cap, and none is sent again while its usage is unchanged.
func TestIntegration_RefusedHoursDoNotStarveNewerMissedHours(t *testing.T) {
	db := itDB(t, "phoebe_push_refused_starve_it")
	now := time.Now().UTC().Truncate(time.Hour)
	floor := now.Add(-200 * time.Hour)
	if _, err := db.Exec(`INSERT INTO token_push_state (auto_floor) VALUES ($1)`, floor); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now())`, floor); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 170; i++ {
		event(t, db, floor.Add(time.Duration(i)*time.Hour+time.Minute), itRes1, itOrg1)
	}
	rate(t, db, floor, now)

	refusing := &recordingManager{statuses: []int{400}}
	for run := 0; run < 2; run++ { // 168 hours, then the last 2
		if code := push(t, db, refusing); code != exitRejected {
			t.Fatalf("refusing run %d: exit %d, want exitRejected", run, code)
		}
	}
	if len(refusing.bodies) != 170 || refusedCount(t, db) != 170 {
		t.Fatalf("%d requests, %d parked; want each of the 170 hours sent once and parked", len(refusing.bodies), refusedCount(t, db))
	}

	newer := floor.Add(180 * time.Hour)
	event(t, db, newer.Add(time.Minute), itRes1, itOrg1)
	rate(t, db, newer, newer.Add(time.Hour))
	got, err := pgStore{db: db}.UnsentHours(context.Background(), now, maxUnsentHoursPerRun)
	if err != nil || len(got) != 1 || !got[0].Equal(newer) {
		t.Fatalf("unsent %v err %v, want only [%s]", got, err, newer)
	}
	var runStart time.Time
	if err := db.QueryRow(`SELECT clock_timestamp()`).Scan(&runStart); err != nil {
		t.Fatal(err)
	}
	m := &recordingManager{}
	if code := push(t, db, m); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if sent := windowStarts(t, m); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(newer) {
		t.Fatalf("sent %v, want only the newer missed hour %s", sent, meteredusage.FormatWindowStart(newer))
	}
	var checked int
	if err := db.QueryRow(`SELECT count(*) FROM refused_hour WHERE checked_at >= $1`, runStart).Scan(&checked); err != nil {
		t.Fatal(err)
	}
	if checked != maxParkedRechecksPerRun {
		t.Fatalf("%d parked hours re-checked, want %d (bounded per run)", checked, maxParkedRechecksPerRun)
	}
}

// The refused_hour row records the refusal (status, body, hash of the sent
// body, count, first and last refusal); a changed payload refused again
// updates it; an accepted push of the hour deletes it.
func TestIntegration_RefusedHourRecordsTheRefusalAndAcceptanceUnparksIt(t *testing.T) {
	db := itDB(t, "phoebe_push_refused_row_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	rate(t, db, h, h.Add(time.Hour))

	refusing := &recordingManager{statuses: []int{400}}
	if code := push(t, db, refusing, h); code != exitRejected {
		t.Fatalf("exit %d", code)
	}
	var status, refusals int
	var message, sha string
	var first, last time.Time
	row := `SELECT status, message, payload_sha256, refusals, first_refused_at, last_refused_at FROM refused_hour WHERE window_start = $1`
	if err := db.QueryRow(row, h).Scan(&status, &message, &sha, &refusals, &first, &last); err != nil {
		t.Fatal(err)
	}
	if status != 400 || !strings.Contains(message, `"path":"records[0]"`) || refusals != 1 || sha != bodySHA(refusing.bodies[0]) {
		t.Fatalf("row status=%d message=%q refusals=%d sha=%s, want 400, the body, 1, %s", status, message, refusals, sha, bodySHA(refusing.bodies[0]))
	}
	if code := push(t, db, refusing, h); code != exitOK || len(refusing.bodies) != 1 {
		t.Fatalf("unchanged: exit %d, %d requests; want 0 and 1", code, len(refusing.bodies))
	}

	event(t, db, h.Add(2*time.Minute), itRes1, itOrg1) // a late event changes the hour
	rate(t, db, h, h.Add(time.Hour))
	if code := push(t, db, refusing, h); code != exitRejected || len(refusing.bodies) != 2 {
		t.Fatalf("changed: exit %d, %d requests; want 3 and 2", code, len(refusing.bodies))
	}
	var first2 time.Time
	if err := db.QueryRow(row, h).Scan(&status, &message, &sha, &refusals, &first2, &last); err != nil {
		t.Fatal(err)
	}
	if refusals != 2 || !first2.Equal(first) || sha != bodySHA(refusing.bodies[1]) {
		t.Fatalf("refusals=%d first %s (was %s) sha=%s; want 2, first kept, the second body's hash", refusals, first2, first, sha)
	}

	event(t, db, h.Add(3*time.Minute), itRes1, itOrg1)
	rate(t, db, h, h.Add(time.Hour))
	if code := push(t, db, &recordingManager{}, h); code != exitOK {
		t.Fatalf("accepted: exit %d", code)
	}
	if n := refusedCount(t, db); n != 0 {
		t.Fatalf("%d refused_hour rows after the hour was accepted, want 0", n)
	}
}

// The back-fill query: nothing before the first scheduled acceptance sets the
// floor; then every hour before it with usage that was never accepted (an
// hour an operator's -since/-until run sent is skipped) and is not parked,
// oldest first; the scheduled runs send them after the window, once.
func TestIntegration_BackfillSendsPreConnectionUsageOnceSkippingAcceptedAndParkedHours(t *testing.T) {
	db := itDB(t, "phoebe_push_backfill_it")
	now := time.Now().UTC().Truncate(time.Hour)
	p1, p2, p3, p4, hc := now.Add(-500*time.Hour), now.Add(-400*time.Hour), now.Add(-300*time.Hour), now.Add(-200*time.Hour), now.Add(-2*time.Hour)
	for _, h := range []time.Time{p1, p2, p3, p4, hc} {
		event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	}
	rate(t, db, p1, now)
	store := pgStore{db: db}
	ctx := context.Background()

	if got, err := store.BackfillHours(ctx, 10); err != nil || len(got) != 0 {
		t.Fatalf("before any scheduled acceptance: back-fill %v err %v, want none", got, err)
	}
	m := &recordingManager{}
	if code := pushExplicit(t, db, m, p2); code != exitOK { // an operator sent p2
		t.Fatalf("explicit run: exit %d", code)
	}
	if code := push(t, db, m, hc); code != exitOK { // the first scheduled acceptance
		t.Fatalf("first scheduled run: exit %d", code)
	}
	if f := autoFloor(t, db); f == nil || !f.Equal(hc) {
		t.Fatalf("auto_floor %v, want %s", f, hc)
	}
	if err := store.RecordRefused(ctx, p4, 400, `{"error":"x","path":"records[0]"}`, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	got, err := store.BackfillHours(ctx, 10)
	if err != nil || len(got) != 2 || !got[0].Equal(p1) || !got[1].Equal(p3) {
		t.Fatalf("back-fill %v err %v, want [%s %s] (p2 accepted, p4 parked)", got, err, p1, p3)
	}
	if got, _ := store.BackfillHours(ctx, 1); len(got) != 1 || !got[0].Equal(p1) {
		t.Fatalf("back-fill with limit 1 %v, want [%s]", got, p1)
	}
	if n, oldest, err := store.PreConnectionHours(ctx); err != nil || n != 2 || !oldest.Equal(p1) {
		t.Fatalf("PreConnectionHours = %d, %s, %v; want 2, %s", n, oldest, err, p1)
	}

	m2 := &recordingManager{}
	if code := push(t, db, m2, hc); code != exitOK {
		t.Fatalf("back-fill run: exit %d", code)
	}
	// p4 is parked with a hash that no longer matches its payload, so the
	// bounded re-check sends it last.
	want := []string{meteredusage.FormatWindowStart(hc), meteredusage.FormatWindowStart(p1), meteredusage.FormatWindowStart(p3), meteredusage.FormatWindowStart(p4)}
	if sent := windowStarts(t, m2); strings.Join(sent, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v", sent, want)
	}
	if n, _, err := store.PreConnectionHours(ctx); err != nil || n != 0 {
		t.Fatalf("PreConnectionHours = %d, %v after the back-fill, want 0", n, err)
	}
	m3 := &recordingManager{}
	if code := push(t, db, m3, hc); code != exitOK {
		t.Fatalf("next run: exit %d", code)
	}
	if sent := windowStarts(t, m3); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(hc) {
		t.Fatalf("next run sent %v, want only %s", sent, meteredusage.FormatWindowStart(hc))
	}
	if f := autoFloor(t, db); !f.Equal(hc) {
		t.Fatalf("auto_floor moved to %s", f)
	}
}

func deferredCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deferred_hour`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A 409 from the manager to an hour that lists video defers the hour
// (deferred_hour: status, body, count, first and last refusal) instead of
// stopping the run. The deferred hour is not in the unsent list (it does not
// use up that cap) and is re-sent by every scheduled run, outside the window
// too, until accepted; only the run that deferred it exits 3 (issue #95,
// option B). Acceptance deletes the row. Parking the hour also deletes it, and
// deferring a parked hour unparks it.
func TestIntegration_DeferredHourIsResentEveryRunAndAcceptanceUndefersIt(t *testing.T) {
	db := itDB(t, "phoebe_push_deferred_it")
	ctx := context.Background()
	store := pgStore{db: db}
	now := time.Now().UTC().Truncate(time.Hour)
	floor := now.Add(-10 * time.Hour)
	if _, err := db.Exec(`INSERT INTO token_push_state (auto_floor) VALUES ($1)`, floor); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now())`, floor); err != nil {
		t.Fatal(err)
	}
	h, other := floor.Add(2*time.Hour), floor.Add(3*time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	event(t, db, other.Add(time.Minute), itRes1, itOrg1)
	rate(t, db, floor, now)

	legacy := &recordingManager{statuses: []int{409, 200}, errBody: `{"error": "install billing path is legacy"}`}
	if code := push(t, db, legacy, h, other); code != exitRejected {
		t.Fatalf("exit %d, want exitRejected", code)
	}
	if sent := windowStarts(t, legacy); len(sent) != 2 {
		t.Fatalf("sent %v, want both hours (the 409 does not stop the run)", sent)
	}
	var status, refusals int
	var message string
	var first, last time.Time
	row := `SELECT status, message, refusals, first_deferred_at, last_deferred_at FROM deferred_hour WHERE window_start = $1`
	if err := db.QueryRow(row, h).Scan(&status, &message, &refusals, &first, &last); err != nil {
		t.Fatal(err)
	}
	if status != 409 || message != `{"error": "install billing path is legacy"}` || refusals != 1 {
		t.Fatalf("row status=%d message=%q refusals=%d, want 409, the body, 1", status, message, refusals)
	}
	if refusedCount(t, db) != 0 || deferredCount(t, db) != 1 {
		t.Fatalf("%d parked, %d deferred; want 0 and 1", refusedCount(t, db), deferredCount(t, db))
	}
	if unsent, err := store.UnsentHours(ctx, now, maxUnsentHoursPerRun); err != nil || len(unsent) != 0 {
		t.Fatalf("unsent %v err %v, want none (the deferred hour is listed on its own)", unsent, err)
	}

	still := &recordingManager{statuses: []int{409}, errBody: `{"error": "install billing path is legacy"}`}
	if code := push(t, db, still); code != exitOK {
		t.Fatalf("second run: exit %d, want exitOK (only the run that defers an hour exits 3)", code)
	}
	if sent := windowStarts(t, still); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(h) {
		t.Fatalf("second run sent %v, want only the deferred hour", sent)
	}
	var first2 time.Time
	if err := db.QueryRow(row, h).Scan(&status, &message, &refusals, &first2, &last); err != nil {
		t.Fatal(err)
	}
	if refusals != 2 || !first2.Equal(first) {
		t.Fatalf("refusals=%d first %s (was %s); want 2 and first kept", refusals, first2, first)
	}

	upgraded := &recordingManager{}
	if code := push(t, db, upgraded); code != exitOK {
		t.Fatalf("upgraded run: exit %d", code)
	}
	if sent := windowStarts(t, upgraded); len(sent) != 1 || sent[0] != meteredusage.FormatWindowStart(h) {
		t.Fatalf("upgraded run sent %v, want the deferred hour", sent)
	}
	if !bytesEqual(upgraded.bodies[0], still.bodies[0]) {
		t.Fatal("the accepted request differs from the refused one")
	}
	if deferredCount(t, db) != 0 {
		t.Fatal("the accepted hour is still deferred")
	}
	if code := push(t, db, upgraded); code != exitOK || len(upgraded.bodies) != 1 {
		t.Fatalf("after acceptance: exit %d, %d requests; want 0 and no new request", code, len(upgraded.bodies))
	}

	// Deferred and parked never hold the same hour.
	if first, err := store.RecordDeferred(ctx, other, 409, "x"); err != nil || !first {
		t.Fatalf("first %v err %v, want the first deferral", first, err)
	}
	if first, err := store.RecordDeferred(ctx, other, 409, "x"); err != nil || first {
		t.Fatalf("first %v err %v, want a repeat refusal (not first)", first, err)
	}
	if err := store.RecordRefused(ctx, other, 400, `{"error":"x","path":"records[0]"}`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if refusedCount(t, db) != 1 || deferredCount(t, db) != 0 {
		t.Fatalf("after parking: %d parked, %d deferred; want 1 and 0", refusedCount(t, db), deferredCount(t, db))
	}
	// Parking removed the deferral, so deferring it again is a first deferral.
	if first, err := store.RecordDeferred(ctx, other, 409, "x"); err != nil || !first {
		t.Fatalf("first %v err %v, want a first deferral after the park", first, err)
	}
	if refusedCount(t, db) != 0 || deferredCount(t, db) != 1 {
		t.Fatalf("after deferring: %d parked, %d deferred; want 0 and 1", refusedCount(t, db), deferredCount(t, db))
	}
	if n, oldest, err := store.DeferredSummary(ctx); err != nil || n != 1 || !oldest.Equal(other) {
		t.Fatalf("summary %d %s %v, want 1 deferred hour, oldest %s", n, oldest, err, other)
	}
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

// Deferred hours are retried under their own cap, so the queries for
// never-accepted hours leave them out: BackfillHours (before the floor),
// PreConnectionHours and UnsentHours (after it) list none of them.
func TestIntegration_DeferredHoursAreLeftOutOfBackfillAndUnsentHours(t *testing.T) {
	db := itDB(t, "phoebe_push_deferred_backfill_it")
	ctx := context.Background()
	store := pgStore{db: db}
	now := time.Now().UTC().Truncate(time.Hour)
	floor := now.Add(-5 * time.Hour)
	before1, before2 := floor.Add(-3*time.Hour), floor.Add(-2*time.Hour)
	after := floor.Add(time.Hour)
	for _, h := range []time.Time{before1, before2, after} {
		event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	}
	rate(t, db, before1, now)
	if _, err := db.Exec(`INSERT INTO token_push_state (auto_floor) VALUES ($1)`, floor); err != nil {
		t.Fatal(err)
	}
	for _, h := range []time.Time{before1, after} {
		if _, err := store.RecordDeferred(ctx, h, 409, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := store.BackfillHours(ctx, maxUnsentHoursPerRun); err != nil || len(got) != 1 || !got[0].Equal(before2) {
		t.Fatalf("BackfillHours %v (err %v), want only %s (the deferred hour is left out)", got, err, before2)
	}
	if n, oldest, err := store.PreConnectionHours(ctx); err != nil || n != 1 || !oldest.Equal(before2) {
		t.Fatalf("PreConnectionHours %d %s (err %v), want 1, %s", n, oldest, err, before2)
	}
	if got, err := store.UnsentHours(ctx, now, maxUnsentHoursPerRun); err != nil || len(got) != 0 {
		t.Fatalf("UnsentHours %v (err %v), want none (the hour after the floor is deferred)", got, err)
	}
}

// The case deferral exists for, end to end on a real database: an hour the
// manager accepted with token records only, refused (409) once a video job is
// rated into it, is deferred and re-sent by scheduled runs after it has left
// the trailing window, until the upgraded manager accepts it. The manager's
// token records for it stay recorded throughout, and the refused attempt's
// video intent rows are removed.
func TestIntegration_AnAcceptedHourDeferredOnceVideoArrivesIsResentAfterLeavingTheWindow(t *testing.T) {
	db := itDB(t, "phoebe_push_deferred_window_it")
	now := time.Now().UTC().Truncate(time.Hour)
	h := now.Add(-3 * time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	rate(t, db, h, now)
	if code := push(t, db, &recordingManager{}, h); code != exitOK {
		t.Fatalf("token-only push: exit %d", code)
	}
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	rate(t, db, h, now)
	legacy := &recordingManager{statuses: []int{409}, errBody: `{"error": "install billing path is legacy"}`}
	if code := push(t, db, legacy, h); code != exitRejected {
		t.Fatalf("push with video: exit %d, want exitRejected", code)
	}
	if deferredCount(t, db) != 1 {
		t.Fatal("the refused hour was not deferred")
	}
	kinds := func() string {
		var k sql.NullString
		if err := db.QueryRow(`SELECT string_agg(kind, ',' ORDER BY kind) FROM pushed_usage WHERE window_start = $1`, h).Scan(&k); err != nil {
			t.Fatal(err)
		}
		return k.String
	}
	if got := kinds(); got != "token" {
		t.Fatalf("pushed_usage kinds %q after the refusal, want token only (the accepted token record kept, the video intent row removed)", got)
	}
	// The trailing window has moved past h: the scheduled run's window is a
	// later, empty hour.
	later := now.Add(-time.Hour)
	if code := push(t, db, legacy, later); code != exitOK {
		t.Fatalf("retry with the old manager: exit %d, want exitOK", code)
	}
	if sent := windowStarts(t, legacy); len(sent) != 2 || sent[1] != meteredusage.FormatWindowStart(h) {
		t.Fatalf("sent %v, want the deferred hour re-sent outside the window", sent)
	}
	upgraded := &recordingManager{}
	if code := push(t, db, upgraded, later); code != exitOK {
		t.Fatalf("upgraded: exit %d", code)
	}
	if req := lastRequest(t, upgraded); req.WindowStart != meteredusage.FormatWindowStart(h) || len(req.VideoRecords) != 1 || len(req.Records) != 1 {
		t.Fatalf("accepted request %s with %d token and %d video records, want %s with 1 and 1", req.WindowStart, len(req.Records), len(req.VideoRecords), meteredusage.FormatWindowStart(h))
	}
	if deferredCount(t, db) != 0 || kinds() != "token,video" {
		t.Fatalf("%d deferred, pushed_usage kinds %q; want 0 and token,video", deferredCount(t, db), kinds())
	}
}

// TouchDeferred moves an hour to the back of the retry order, and ignores
// hours that are not deferred.
func TestIntegration_TouchDeferredMovesAnHourToTheBackOfTheRetryOrder(t *testing.T) {
	db := itDB(t, "phoebe_push_deferred_touch_it")
	ctx := context.Background()
	store := pgStore{db: db}
	base := time.Now().UTC().Truncate(time.Hour).Add(-10 * time.Hour)
	a, b, c := base, base.Add(time.Hour), base.Add(2*time.Hour)
	for _, h := range []time.Time{a, b} {
		if _, err := store.RecordDeferred(ctx, h, 409, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.TouchDeferred(ctx, []time.Time{a, c}); err != nil {
		t.Fatal(err)
	}
	got, err := store.DeferredHours(ctx, 10)
	if err != nil || len(got) != 2 || !got[0].Equal(b) || !got[1].Equal(a) {
		t.Fatalf("DeferredHours %v (err %v), want [%s %s]", got, err, b, a)
	}
}

// A response body longer than the stored limit, with a multi-byte character
// across the cut, a NUL and an invalid byte, is stored when the hour is
// parked or deferred (Postgres would refuse the raw cut with SQLSTATE 22021).
func TestIntegration_ALongMultiByteRefusalBodyIsStored(t *testing.T) {
	db := itDB(t, "phoebe_push_message_it")
	ctx := context.Background()
	store := pgStore{db: db}
	h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	body := "\x00\xff" + strings.Repeat("a", maxRefusedMessage-4) + "é" + "tail"
	if err := store.RecordRefused(ctx, h, 400, body, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("park: %v", err)
	}
	if _, err := store.RecordDeferred(ctx, h, 409, body); err != nil {
		t.Fatalf("defer: %v", err)
	}
	var msg string
	if err := db.QueryRow(`SELECT message FROM deferred_hour WHERE window_start = $1`, h).Scan(&msg); err != nil {
		t.Fatal(err)
	}
	if len(msg) > maxRefusedMessage || !strings.HasPrefix(msg, "�aaa") {
		t.Fatalf("stored %d bytes starting %q", len(msg), msg[:8])
	}
}

// RecordIntent reports the record ids it added; ForgetIntent removes exactly
// those, never a row that was there before.
func TestIntegration_ForgetIntentRemovesOnlyTheRowsTheAttemptAdded(t *testing.T) {
	db := itDB(t, "phoebe_push_forget_it")
	ctx := context.Background()
	store := pgStore{db: db}
	h := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	row := func(id string, held bool) pushedRow {
		return pushedRow{UsageID: strings.Repeat(id, 64), Kind: meteredusage.KindToken, held: held,
			identity: identity{AuthID: itAuth, OwnerType: "user", OwnerID: itUser, ResourceID: itRes1, ModelID: "m", ServingMode: "dedicated"}}
	}
	if _, err := store.RecordIntent(ctx, h, []pushedRow{row("a", false)}); err != nil {
		t.Fatal(err)
	}
	added, err := store.RecordIntent(ctx, h, []pushedRow{row("a", false), row("b", false), row("c", true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || added[0] != strings.Repeat("b", 64) {
		t.Fatalf("added %v, want only the new record id (not the earlier one, not the held one)", added)
	}
	if err := store.ForgetIntent(ctx, h, added); err != nil {
		t.Fatal(err)
	}
	var ids string
	if err := db.QueryRow(`SELECT string_agg(left(usage_id, 1), ',' ORDER BY usage_id) FROM pushed_usage WHERE window_start = $1`, h).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if ids != "a,c" {
		t.Fatalf("pushed_usage %q, want a,c", ids)
	}
}

// A second run waits for the first one's run lock and goes ahead once it is
// released; the lock is per schema.
func TestIntegration_RunLockMakesASecondRunWaitForTheFirst(t *testing.T) {
	db := itDB(t, "phoebe_push_lock_it")
	ctx := context.Background()
	store := pgStore{db: db}
	release, err := store.lockRun(ctx, func() { t.Error("the first run waited") })
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{}, 1)
	got := make(chan func(), 1)
	go func() {
		r, err := store.lockRun(ctx, func() { waited <- struct{}{} })
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		t.Fatal("the second run did not report waiting")
	}
	select {
	case <-got:
		t.Fatal("the second run took the lock while the first held it")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case r := <-got:
		r()
	case <-time.After(10 * time.Second):
		t.Fatal("the second run did not get the lock after the first released it")
	}
	other := pgStore{db: itDB(t, "phoebe_push_lock_other_it")}
	r1, err := store.lockRun(ctx, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	r2, err := other.lockRun(ctx, func() { t.Error("a run in another schema waited") })
	if err != nil {
		t.Fatal(err)
	}
	r2()
}
