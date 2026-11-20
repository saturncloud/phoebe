package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

func bodySHA(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// parkAsBuilt records hour h in refused_hour with the hash of the payload
// token-push builds for it now, as an earlier run's 400 would have.
func parkAsBuilt(t *testing.T, f *fakeStore, h time.Time) {
	t.Helper()
	plan, err := newPusher(f, nil).buildHour(context.Background(), h, false)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := payloadSHA256(plan.req)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.RecordRefused(context.Background(), h, 400, `{"error":"earlier","path":"records[0]"}`, sum); err != nil {
		t.Fatal(err)
	}
}

func capturingPusher(f *fakeStore, c poster) (*pusher, *bytes.Buffer) {
	var buf bytes.Buffer
	p := newPusher(f, c)
	p.log = logging.New(logging.INFO)
	p.log.Info.SetOutput(&buf)
	p.log.Warn.SetOutput(&buf)
	p.log.Error.SetOutput(&buf)
	return p, &buf
}

// A 400 with the manager's validation body will be given again for the same
// payload. The hour is parked (status, message, hash of the refused body, one
// refusal), logged at ERROR, and the run exits 3. The next run rebuilds it,
// finds the payload unchanged, does not send it and exits 0.
func TestPushWindows_PermanentRefusalParksTheHourAndIsNotResent(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{statuses: []int{400, 200}}
	p, logs := capturingPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("first run: exit %d, want exitRejected (%d)", code, exitRejected)
	}
	r := f.refused[win]
	if r == nil {
		t.Fatal("the refused hour was not parked")
	}
	if r.status != 400 || !strings.Contains(r.message, `"path":"records[0]"`) || r.refusals != 1 || r.sha != bodySHA(m.bodies[0]) {
		t.Fatalf("parked row %+v, want status 400, the manager's body, 1 refusal and the hash of the sent body", *r)
	}
	if !strings.Contains(logs.String(), "ERROR: ") || !strings.Contains(logs.String(), "PARKED "+meteredusage.FormatWindowStart(win)) {
		t.Fatalf("no ERROR line for the parked hour:\n%s", logs.String())
	}
	if f.accepted[win] != 0 {
		t.Fatal("a refused hour was recorded as accepted")
	}

	logs.Reset()
	for i := 0; i < 2; i++ {
		if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
			t.Fatalf("later run %d: exit %d, want exitOK (an already-parked hour must not keep failing)", i, code)
		}
	}
	if len(m.bodies) != 1 {
		t.Fatalf("%d requests, want 1: a parked hour with unchanged usage is not re-sent", len(m.bodies))
	}
	if f.refused[win].refusals != 1 {
		t.Fatalf("refusals %d, want 1", f.refused[win].refusals)
	}
	if n := strings.Count(logs.String(), "1 hour(s) are parked (0 newly this run)"); n != 2 {
		t.Fatalf("parked-hours line logged %d times over 2 runs, want once per run:\n%s", n, logs.String())
	}
}

// 170 parked hours and one newer missed hour: the missed hour is sent in this
// run. Parked hours are not in the unsent list, so they cannot use up the
// 168-hour cap; a bounded number of them are rebuilt and none is re-sent.
func TestPushWindows_ParkedHoursDoNotCountAgainstTheCap(t *testing.T) {
	f := fixtureStore()
	f.everPushed = true
	floorHour := win.Add(-300 * time.Hour)
	f.autoFloor = &floorHour
	var parked []time.Time
	for i := 1; i <= 170; i++ {
		h := floorHour.Add(time.Duration(i) * time.Hour)
		f.rollups[h] = fixtureRollups()
		parkAsBuilt(t, f, h)
		parked = append(parked, h)
	}
	missed := floorHour.Add(200 * time.Hour)
	f.rollups[missed] = fixtureRollups()
	f.unsent = append(append([]time.Time(nil), parked...), missed) // the store's candidates, oldest first
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	got := windowStartsOf(t, m)
	want := []string{meteredusage.FormatWindowStart(win), meteredusage.FormatWindowStart(missed)}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v (the window, then the missed hour; no parked hour)", got, want)
	}
	if f.accepted[missed] != 1 {
		t.Fatal("the missed hour was not recorded as accepted")
	}
	if len(f.refused) != 170 {
		t.Fatalf("%d parked hours, want all 170 still parked", len(f.refused))
	}
	// The re-check is bounded: 168 rebuilt and marked checked this run; the
	// two left are the first re-checked next run.
	rechecked := 0
	for _, r := range f.refused {
		if r.checked > 170 {
			rechecked++
		}
	}
	if rechecked != maxParkedRechecksPerRun {
		t.Fatalf("%d parked hours re-checked, want %d", rechecked, maxParkedRechecksPerRun)
	}
	next, _ := f.RefusedHours(context.Background())
	if !next[0].Hour.Equal(parked[168]) || !next[1].Hour.Equal(parked[169]) {
		t.Fatalf("next re-check starts at %s, %s; want the two not checked this run", next[0].Hour, next[1].Hour)
	}
}

func windowStartsOf(t *testing.T, m *recordingManager) []string {
	t.Helper()
	var out []string
	for _, b := range m.bodies {
		out = append(out, sentRequest(t, b).WindowStart)
	}
	return out
}

// 5xx, 429 and network failures are transient: the hour is not parked, the
// run fails, and the next run sends it again.
func TestPushWindows_TransientErrorStillRetries(t *testing.T) {
	for _, status := range []int{503, 429} {
		f := fixtureStore()
		down := &recordingManager{statuses: []int{status}}
		if code := newPusher(f, down.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
			t.Fatalf("%d: exit %d, want exitFatal", status, code)
		}
		if len(down.bodies) != meteredusage.DefaultMaxAttempts {
			t.Fatalf("%d: %d attempts, want %d (retried)", status, len(down.bodies), meteredusage.DefaultMaxAttempts)
		}
		if len(f.refused) != 0 {
			t.Fatalf("%d: a transient failure parked the hour", status)
		}
		up := &recordingManager{}
		if code := newPusher(f, up.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK || f.accepted[win] != 1 {
			t.Fatalf("%d: next run exit %d, accepted %d; want the hour sent and accepted", status, code, f.accepted[win])
		}
	}

	// Network: nothing listens at the manager URL.
	f := fixtureStore()
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	c := &meteredusage.Client{BaseURL: gone.URL, Token: "tok", HTTP: &http.Client{Timeout: time.Second},
		Sleep: func(context.Context, time.Duration) error { return nil }}
	if code := newPusher(f, c).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
		t.Fatalf("network: exit %d, want exitFatal", code)
	}
	if len(f.refused) != 0 {
		t.Fatal("network: a transient failure parked the hour")
	}
}

// A parked hour whose usage changes (a late event re-rated into it) is sent
// again: in the trailing window, and outside it through the bounded re-check.
// Accepted, it is no longer parked.
func TestPushWindows_ParkedHourBecomesEligibleWhenItsDataChanges(t *testing.T) {
	old := win.Add(-200 * time.Hour)
	f := fixtureStore()
	f.rollups[old] = fixtureRollups()
	parkAsBuilt(t, f, win)
	parkAsBuilt(t, f, old)

	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK || len(m.bodies) != 0 {
		t.Fatalf("unchanged: exit %d, %d requests; want 0 and none", code, len(m.bodies))
	}

	f.rollups[win][0].Prompt++
	f.rollups[old][2].Completion++
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("changed: exit %d", code)
	}
	got := windowStartsOf(t, m)
	want := []string{meteredusage.FormatWindowStart(win), meteredusage.FormatWindowStart(old)}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v", got, want)
	}
	if len(f.refused) != 0 || f.accepted[win] != 1 || f.accepted[old] != 1 {
		t.Fatalf("parked %d, accepted win=%d old=%d; want both unparked and accepted", len(f.refused), f.accepted[win], f.accepted[old])
	}
}

// A changed payload that is refused again is parked again with the new hash
// and counts as newly parked (exit 3), once.
func TestPushWindows_ChangedParkedHourRefusedAgainIsReparkedOnce(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{statuses: []int{400}}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("exit %d", code)
	}
	f.rollups[win][0].Prompt++
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("changed and refused again: exit %d, want exitRejected", code)
	}
	if r := f.refused[win]; r.refusals != 2 || r.sha != bodySHA(m.bodies[1]) {
		t.Fatalf("parked row %+v, want 2 refusals and the hash of the second body", *r)
	}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK || len(m.bodies) != 2 {
		t.Fatalf("unchanged after re-park: exit %d, %d requests; want 0 and 2", code, len(m.bodies))
	}
}

// The recovery path: an operator's -since/-until run sends every hour of its
// range, parked or not.
func TestPushWindows_ExplicitRunSendsAParkedHour(t *testing.T) {
	f := fixtureStore()
	parkAsBuilt(t, f, win)
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, true); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if len(m.bodies) != 1 || f.refused[win] != nil {
		t.Fatalf("%d requests, parked %v; want the hour sent and unparked", len(m.bodies), f.refused[win])
	}
}

// Only the manager's validation answer parks an hour. A 400 without its
// {"error","path"} body (a proxy in front of the manager, say) may be about
// the install, and parking would stop the hour from ever being re-sent.
func TestPushWindows_A400WithoutTheManagersValidationBodyIsNotParked(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{statuses: []int{400}, errBody: "<html>400 Bad Request</html>"}
	p := newPusher(f, m.client(t))
	for i := 0; i < 2; i++ {
		if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
			t.Fatalf("run %d: exit %d, want exitRejected", i, code)
		}
	}
	if len(f.refused) != 0 || len(m.bodies) != 2 {
		t.Fatalf("parked %d, %d requests; want nothing parked and the hour sent each run", len(f.refused), len(m.bodies))
	}
}

// 401 and 403 are about the install, not the hour: nothing is parked or
// deferred, the run exits 3 and stops sending (every other hour would get the
// same answer), and the next run sends the hours again.
func TestPushWindows_WholeInstallRefusalParksNothingAndStopsTheRun(t *testing.T) {
	for _, status := range []int{401, 403} {
		f := fixtureStore()
		other := win.Add(-time.Hour)
		f.rollups[other] = fixtureRollups()
		m := &recordingManager{statuses: []int{status}}
		p := newPusher(f, m.client(t))
		for i := 0; i < 2; i++ {
			if code := p.pushWindows(context.Background(), []time.Time{other, win}, now, false); code != exitRejected {
				t.Fatalf("%d run %d: exit %d, want exitRejected", status, i, code)
			}
		}
		if len(f.refused) != 0 || len(f.deferred) != 0 {
			t.Fatalf("%d: parked %d and deferred %d hours, want none", status, len(f.refused), len(f.deferred))
		}
		if len(m.bodies) != 2 {
			t.Fatalf("%d: %d requests over 2 runs, want 2 (each run stops after the first refusal)", status, len(m.bodies))
		}
	}
}

// An hour whose request breaks a request-level wire rule (two rollups with
// one usage_id) is never sent. It is parked with status 0 and exits 2 in the
// run that parks it; later runs with the same usage do not fail.
func TestPushWindows_LocalWireRuleFailureParksTheHourOnce(t *testing.T) {
	f := newFake()
	r := fixtureRollups()[0]
	dup := r
	dup.ID = "rid-dup"
	f.rollups[win] = []rollupRow{r, dup}
	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld (%d)", code, exitWithheld)
	}
	if rr := f.refused[win]; rr == nil || rr.status != 0 || rr.message == "" {
		t.Fatalf("parked row %+v, want status 0 with the validation error", rr)
	}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("second run: exit %d, want exitOK", code)
	}
	if len(m.bodies) != 0 {
		t.Fatalf("%d requests, want none", len(m.bodies))
	}
}

// A 400 whose validation path is `kinds` is a manager that does not know a
// kind phoebe lists (a manager older than the video kind). It is about the
// hour's kinds, not its records and not the whole install: the hour is
// deferred, not parked (parking would also hold the hour's token records and
// never re-send them after the manager is upgraded, since the payload does
// not change), the other hours are still sent, the run exits 3, and the next
// run sends the deferred hour again.
func TestPushWindows_ARefusedKindDefersTheHourAndSendsTheOthers(t *testing.T) {
	for _, path := range []string{"kinds[1]", "kinds"} {
		f := videoStore()
		other := win.Add(-time.Hour)
		f.rollups[other] = fixtureRollups()
		f.videoRollups[other] = f.videoRollups[win]
		m := &recordingManager{statuses: []int{400}, errBody: `{"error":"unknown kind video","path":"` + path + `"}`}
		p := newPusher(f, m.client(t))
		for i := 0; i < 2; i++ {
			if code := p.pushWindows(context.Background(), []time.Time{other, win}, now, false); code != exitRejected {
				t.Fatalf("%s run %d: exit %d, want exitRejected", path, i, code)
			}
		}
		if len(f.refused) != 0 {
			t.Fatalf("%s: parked %d hours, want none", path, len(f.refused))
		}
		for _, h := range []time.Time{other, win} {
			if d := f.deferred[h]; d == nil || d.status != 400 || d.refusals != 2 {
				t.Fatalf("%s: hour %s deferred row %+v, want status 400 and 2 refusals", path, h, d)
			}
		}
		if len(m.bodies) != 4 {
			t.Fatalf("%s: %d requests over 2 runs, want 4 (a deferred hour does not stop the run)", path, len(m.bodies))
		}
	}
	// A validation answer about a record still parks the hour.
	if !refusalParksTheHour(&meteredusage.RejectedError{Status: 400, Body: `{"error":"x","path":"records[3].kind"}`}) {
		t.Fatal("a refusal of records[3].kind must park the hour")
	}
}
