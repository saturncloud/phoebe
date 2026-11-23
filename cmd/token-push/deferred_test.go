package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
)

// legacyManager is a stub of the production manager uk2 pushed to on
// 2026-11-20 (saturn-aws-manager prod-2026.11.17, #333 without #334): from an
// install on the legacy billing path it accepts a push listing only token and
// answers 409 {"error": "install billing path is legacy"} to one that lists
// video, storing nothing. With upgraded set it accepts both (#334).
type legacyManager struct {
	mu       sync.Mutex
	upgraded bool
	bodies   [][]byte
	refused  []string // window_start of every 409
	accepted []string // window_start of every 200
}

func (m *legacyManager) client(t *testing.T) *meteredusage.Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req meteredusage.Request
		_ = json.Unmarshal(body, &req)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.bodies = append(m.bodies, body)
		for _, k := range req.Kinds {
			if k != meteredusage.KindToken && !m.upgraded {
				m.refused = append(m.refused, req.WindowStart)
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error": "install billing path is legacy"}`))
				return
			}
		}
		m.accepted = append(m.accepted, req.WindowStart)
		_ = json.NewEncoder(w).Encode(meteredusage.Response{Accepted: int64(req.RecordCount()), WindowStart: req.WindowStart, Kinds: req.Kinds})
	}))
	t.Cleanup(srv.Close)
	return &meteredusage.Client{
		BaseURL: srv.URL + "/", Token: "tok-123", HTTP: &http.Client{Timeout: 5 * time.Second},
		Sleep: func(context.Context, time.Duration) error { return nil },
	}
}

// uk2Store is uk2's state at 22:15Z on 2026-11-20 in miniature: token usage in
// three hours, and video usage (the QA video job) in the middle one only.
func uk2Store() (f *fakeStore, before, videoHour, after time.Time) {
	f = videoStore() // token and video usage in win
	before, videoHour, after = win.Add(-time.Hour), win, win.Add(time.Hour)
	f.rollups[before] = fixtureRollups()
	f.rollups[after] = fixtureRollups()
	return f, before, videoHour, after
}

// The uk2 QA failure (issue #94): the manager answered 409 to the hour with
// the video job and token-push stopped, so no later hour reached the manager.
// Now the 409 defers only that hour: the hours before and after it are sent
// and accepted, the refused hour is recorded in deferred_hour with the
// manager's answer, nothing is parked, and the run exits 3 with an ERROR line
// naming the hour and the manager change it needs.
func TestPushWindows_A409OnTheVideoHourDefersOnlyThatHourAndSendsTheOthers(t *testing.T) {
	f, before, videoHour, after := uk2Store()
	m := &legacyManager{}
	p, logs := capturingPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{before, videoHour, after}, now, false); code != exitRejected {
		t.Fatalf("exit %d, want exitRejected (%d)", code, exitRejected)
	}
	if f.accepted[before] != 1 || f.accepted[after] != 1 {
		t.Fatalf("accepted %v, want the hours before and after the refused one accepted", f.accepted)
	}
	if f.accepted[videoHour] != 0 {
		t.Fatal("the refused hour was recorded as accepted")
	}
	d := f.deferred[videoHour]
	if d == nil || d.status != http.StatusConflict || d.message != `{"error": "install billing path is legacy"}` || d.refusals != 1 {
		t.Fatalf("deferred row %+v, want status 409, the manager's body and 1 refusal", d)
	}
	if len(f.deferred) != 1 || len(f.refused) != 0 {
		t.Fatalf("%d deferred and %d parked hours, want exactly the video hour deferred and nothing parked", len(f.deferred), len(f.refused))
	}
	out := logs.String()
	if !strings.Contains(out, "DEFERRED "+meteredusage.FormatWindowStart(videoHour)) || !strings.Contains(out, "saturn-aws-manager#334") {
		t.Fatalf("no ERROR line naming the deferred hour and the manager change:\n%s", out)
	}
	if !strings.Contains(out, "1 hour(s) are deferred (1 refused this run, 1 newly)") || !strings.Contains(out, "report it to Saturn support") {
		t.Fatalf("no WARN summary of deferred hours:\n%s", out)
	}
}

// A deferred hour is re-sent by every run, also once it has left the trailing
// window, until the manager accepts it. Only the run that deferred it exits 3
// (issue #95, option B): later runs that are refused again exit 0, log no
// ERROR for it and log the WARN summary. The accepted request is byte for
// byte the refused one (one snapshot of the hour, replacing what the manager
// held), so nothing is counted twice; acceptance removes the deferral and the
// run exits 0.
func TestPushWindows_ADeferredHourIsResentEveryRunUntilTheUpgradedManagerAcceptsIt(t *testing.T) {
	f, before, videoHour, after := uk2Store()
	m := &legacyManager{}
	p, logs := capturingPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{before, videoHour, after}, now, false); code != exitRejected {
		t.Fatalf("first run: exit %d", code)
	}
	refusedBody := m.bodies[1]
	// Later runs: the window has moved past every one of these hours.
	later := after.Add(time.Hour)
	for i := 0; i < 2; i++ {
		logs.Reset()
		if code := p.pushWindows(context.Background(), []time.Time{later}, now, false); code != exitOK {
			t.Fatalf("run %d with the old manager: exit %d, want exitOK (only the run that defers an hour exits 3)", i, code)
		}
		out := logs.String()
		if strings.Contains(out, "ERROR") || !strings.Contains(out, "1 hour(s) are deferred (1 refused this run, 0 newly)") {
			t.Fatalf("run %d: want no ERROR line and the WARN summary:\n%s", i, out)
		}
	}
	if d := f.deferred[videoHour]; d == nil || d.refusals != 3 {
		t.Fatalf("deferred row %+v, want 3 refusals (the hour re-sent by every run)", d)
	}
	m.upgraded = true
	if code := p.pushWindows(context.Background(), []time.Time{later}, now, false); code != exitOK {
		t.Fatalf("run with the upgraded manager: exit %d, want exitOK", code)
	}
	if f.accepted[videoHour] != 1 || f.deferred[videoHour] != nil {
		t.Fatalf("accepted %d, deferred %+v; want the hour accepted once and no longer deferred", f.accepted[videoHour], f.deferred[videoHour])
	}
	last := m.bodies[len(m.bodies)-1]
	if !bytes.Equal(last, refusedBody) {
		t.Fatalf("the accepted request differs from the refused one:\n%s\n%s", refusedBody, last)
	}
	if got := countKinds(f.pushed[videoHour]); got[meteredusage.KindToken] != 4 || got[meteredusage.KindVideo] != 2 {
		t.Fatalf("pushed_usage for the hour %v, want its 4 token and 2 video records", got)
	}
	// Accepted: the next run does not send it again.
	n := len(m.bodies)
	if code := p.pushWindows(context.Background(), []time.Time{later}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	// The window hour has no usage and was never sent, so it is skipped: no request.
	if len(m.bodies) != n || f.accepted[videoHour] != 1 {
		t.Fatalf("%d requests, hour accepted %d times; want no request (the accepted hour is not re-sent)", len(m.bodies)-n, f.accepted[videoHour])
	}
}

// An hour the manager already accepted with token records only, refused once
// video is rated into it, is neither an unsent hour (it has a pushed_hour row)
// nor parked. Before deferral it was re-sent only while it was in the trailing
// window, so its video would never have reached the manager. Deferred, it is
// re-sent after it leaves the window, and the manager keeps its token records
// meanwhile (a refused request stores nothing).
func TestPushWindows_AnAcceptedHourRefusedOnceVideoArrivesIsDeferredNotDropped(t *testing.T) {
	f := fixtureStore()
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("token-only run: exit %d", code)
	}
	// The rater rates a late video job into the same hour.
	v := videoStore()
	f.hasVideo = true
	f.videoRollups[win] = v.videoRollups[win]
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("run with video: exit %d, want exitRejected", code)
	}
	if f.deferred[win] == nil {
		t.Fatal("the refused hour was not deferred")
	}
	if got := countKinds(f.pushed[win]); got[meteredusage.KindToken] != 4 {
		t.Fatalf("pushed_usage %v: the token records the manager accepted must stay recorded", got)
	}
	m.upgraded = true
	if code := p.pushWindows(context.Background(), []time.Time{win.Add(2 * time.Hour)}, now, false); code != exitOK {
		t.Fatalf("run outside the window: exit %d", code)
	}
	if f.accepted[win] != 2 || f.deferred[win] != nil {
		t.Fatalf("accepted %d times, deferred %+v; want the video hour accepted after leaving the window", f.accepted[win], f.deferred[win])
	}
	if got := countKinds(f.pushed[win]); got[meteredusage.KindVideo] != 2 {
		t.Fatalf("pushed_usage %v, want the hour's 2 video records", got)
	}
}

// A deferred hour is retried under its own cap, least recently refused first,
// and never takes room from the never-accepted hours.
func TestPushWindows_DeferredHoursAreRetriedUnderTheirOwnCap(t *testing.T) {
	f := videoStore()
	total := maxDeferredRetriesPerRun + 5
	for i := 0; i < total; i++ {
		h := win.Add(-time.Duration(i+1) * time.Hour)
		f.rollups[h] = fixtureRollups()
		f.videoRollups[h] = f.videoRollups[win]
		if _, err := f.RecordDeferred(context.Background(), h, 409, "x"); err != nil {
			t.Fatal(err)
		}
	}
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	p.pushWindows(context.Background(), []time.Time{win}, now, false)
	if f.unsentLimit != maxUnsentHoursPerRun {
		t.Fatalf("unsent cap %d, want %d (deferred hours do not use it up)", f.unsentLimit, maxUnsentHoursPerRun)
	}
	if got := len(m.refused); got != maxDeferredRetriesPerRun+1 {
		t.Fatalf("%d hours sent and refused, want the window hour plus %d deferred hours", got, maxDeferredRetriesPerRun)
	}
	// The 5 hours this run left out are now the least recently refused, so the
	// next run sends them first.
	next, _ := f.DeferredHours(context.Background(), 5)
	for _, h := range next {
		if f.deferred[h].refusals != 1 {
			t.Fatalf("hour %s (refused %d times) comes before the hours the last run left out", h, f.deferred[h].refusals)
		}
	}
}

// Deferred and parked never hold the same hour. A deferred hour the manager
// then refuses for its content (a 400 validation answer about a record) is
// parked and no longer deferred; a parked hour whose changed payload is
// refused for its kinds is deferred and no longer parked.
func TestPushWindows_AnHourIsDeferredOrParkedNeverBoth(t *testing.T) {
	f := videoStore()
	if _, err := f.RecordDeferred(context.Background(), win, 409, "x"); err != nil {
		t.Fatal(err)
	}
	m := &recordingManager{statuses: []int{400}}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), nil, now, false); code != exitRejected {
		t.Fatalf("exit %d", code)
	}
	if f.refused[win] == nil || f.deferred[win] != nil {
		t.Fatalf("parked %+v, deferred %+v; want parked only", f.refused[win], f.deferred[win])
	}

	f = videoStore()
	if err := f.RecordRefused(context.Background(), win, 400, `{"error":"old","path":"records[0]"}`, "an older payload"); err != nil {
		t.Fatal(err)
	}
	lm := &legacyManager{}
	if code := newPusher(f, lm.client(t)).pushWindows(context.Background(), nil, now, false); code != exitRejected {
		t.Fatalf("exit %d", code)
	}
	if len(lm.refused) != 1 || f.deferred[win] == nil || f.refused[win] != nil {
		t.Fatalf("sent %d, deferred %+v, parked %+v; want the changed parked hour sent, deferred and unparked", len(lm.refused), f.deferred[win], f.refused[win])
	}
}

func countKinds(rows []pushedRow) map[string]int {
	out := map[string]int{}
	for _, r := range rows {
		out[r.kind()]++
	}
	return out
}

// An operator's -since/-until run asked for the hour, so a deferred hour of
// its own range that is refused again still exits 3. A deferred hour outside
// the range, retried by the same run, does not.
func TestPushWindows_AnExplicitRunExits3ForADeferredHourOfItsRangeRefusedAgain(t *testing.T) {
	f, before, videoHour, after := uk2Store()
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{before, videoHour, after}, now, false); code != exitRejected {
		t.Fatalf("scheduled run: exit %d", code)
	}
	if code := p.pushWindows(context.Background(), []time.Time{videoHour}, now, true); code != exitRejected {
		t.Fatalf("explicit run over the deferred hour: exit %d, want exitRejected", code)
	}
	if code := p.pushWindows(context.Background(), []time.Time{before}, now, true); code != exitOK {
		t.Fatalf("explicit run over another hour: exit %d, want exitOK (the deferred hour is outside its range)", code)
	}
	if d := f.deferred[videoHour]; d == nil || d.refusals != 3 {
		t.Fatalf("deferred row %+v, want 3 refusals (every run re-sent it)", d)
	}
}

// A 409, or a 400 on kinds, to a request that lists only token is a manager
// that refuses this install's token usage: every hour would get the same
// answer. The run stops at the first one, as before deferral existed, and
// defers and parks nothing; the next run sends again. The log names the
// whole-install refusal, not saturn-aws-manager#334.
func TestPushWindows_AKindRefusalOfATokenOnlyRequestStopsTheRunAndDefersNothing(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{409, `{"error": "install billing path is legacy"}`},
		{400, `{"error": "unknown kind token", "path": "kinds[0]"}`},
	} {
		f := fixtureStore()
		other := win.Add(-time.Hour)
		f.rollups[other] = fixtureRollups()
		m := &recordingManager{statuses: []int{tc.status}, errBody: tc.body}
		p, logs := capturingPusher(f, m.client(t))
		if code := p.pushWindows(context.Background(), []time.Time{other, win}, now, false); code != exitRejected {
			t.Fatalf("%d: exit %d, want exitRejected", tc.status, code)
		}
		if len(m.bodies) != 1 {
			t.Fatalf("%d: %d requests, want 1 (the run stops at the first refusal)", tc.status, len(m.bodies))
		}
		if len(f.deferred) != 0 || len(f.refused) != 0 {
			t.Fatalf("%d: %d deferred, %d parked; want none", tc.status, len(f.deferred), len(f.refused))
		}
		if len(f.pushed[other]) != 0 {
			t.Fatalf("%d: pushed_usage kept %d intent rows for the refused hour, want none", tc.status, len(f.pushed[other]))
		}
		if out := logs.String(); !strings.Contains(out, "for the whole install") || strings.Contains(out, "DEFERRED") {
			t.Fatalf("%d: want the whole-install refusal logged and no DEFERRED line:\n%s", tc.status, out)
		}
	}
}

// The manager stores nothing for a request it refuses, so the pushed_usage
// rows RecordIntent added for it are removed. Left in place, the hour's video
// intent rows would keep kind "video" listed after its video is gone (a later
// re-rate), and an old manager would refuse the hour forever. Without them,
// the hour goes back to token only and is accepted.
func TestPushWindows_ARefusedRequestForgetsTheIntentRowsItAdded(t *testing.T) {
	f := videoStore()
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("first run: exit %d", code)
	}
	if f.deferred[win] == nil || len(f.pushed[win]) != 0 {
		t.Fatalf("deferred %+v, %d pushed_usage rows; want the hour deferred and no intent rows left", f.deferred[win], len(f.pushed[win]))
	}
	// The hour's video is re-rated away.
	delete(f.videoRollups, win)
	f.hasVideo = false
	if code := p.pushWindows(context.Background(), nil, now, false); code != exitOK {
		t.Fatalf("second run: exit %d, want exitOK", code)
	}
	if f.accepted[win] != 1 || f.deferred[win] != nil {
		t.Fatalf("accepted %d, deferred %+v; want the token-only hour accepted and undeferred", f.accepted[win], f.deferred[win])
	}
	if got := countKinds(f.pushed[win]); got[meteredusage.KindVideo] != 0 || got[meteredusage.KindToken] != 4 {
		t.Fatalf("pushed_usage %v, want the 4 token records only", got)
	}
}

// Intent rows that were already in pushed_usage (the manager may hold them)
// are never forgotten on a refusal: only the ones this attempt added.
func TestPushWindows_ARefusalKeepsIntentRowsFromEarlierPushes(t *testing.T) {
	f := fixtureStore()
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("token-only run: exit %d", code)
	}
	v := videoStore()
	f.hasVideo = true
	f.videoRollups[win] = v.videoRollups[win]
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("run with video: exit %d", code)
	}
	if got := countKinds(f.pushed[win]); got[meteredusage.KindToken] != 4 || got[meteredusage.KindVideo] != 0 {
		t.Fatalf("pushed_usage %v, want the 4 accepted token records kept and no video intent rows", got)
	}
}

// A deferred hour that fails some other way (here a 5xx past the retries) is
// moved to the back of the retry order, so it cannot stay first while other
// deferred hours wait.
func TestPushWindows_ADeferredHourThatFailsAnotherWayMovesToTheBackOfTheRetryOrder(t *testing.T) {
	f := videoStore()
	a, b := win.Add(-2*time.Hour), win.Add(-time.Hour)
	for _, h := range []time.Time{a, b} {
		f.rollups[h] = fixtureRollups()
		f.videoRollups[h] = f.videoRollups[win]
		if _, err := f.RecordDeferred(context.Background(), h, 409, "x"); err != nil {
			t.Fatal(err)
		}
	}
	// a is retried first and gets 503 on every attempt; b gets the 409 again.
	m := &recordingManager{statuses: []int{503, 503, 503, 503, 503, 409}, errBody: `{"error": "install billing path is legacy"}`}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), nil, now, false); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal (a 5xx outlasted the retries)", code)
	}
	if f.deferred[a].last <= f.deferred[b].last {
		t.Fatalf("a last=%d, b last=%d; want a behind b in the retry order", f.deferred[a].last, f.deferred[b].last)
	}
	if next, _ := f.DeferredHours(context.Background(), 1); len(next) != 1 || !next[0].Equal(b) {
		t.Fatalf("next run retries %v first, want %s", next, b)
	}
}

// intentRecorder is a fakeStore that remembers the ids each RecordIntent call
// added, so a test can check what became of them.
type intentRecorder struct {
	*fakeStore
	added map[time.Time][]string
}

func (s *intentRecorder) RecordIntent(ctx context.Context, h time.Time, rows []pushedRow) ([]string, error) {
	added, err := s.fakeStore.RecordIntent(ctx, h, rows)
	s.added[h] = append(s.added[h], added...)
	return added, err
}

// Post retries a 5xx or a network error before it returns, and the manager may
// have committed one of those attempts. So the pushed_usage intent rows an
// attempt added are removed only when the manager refused the only attempt;
// after any outcome where it may hold the request they stay, because a
// missing id is lost or double-billed usage and an extra one is harmless. The
// first-attempt rows are the positive control: those refusals still forget.
// Each row also checks that the hour's handling (deferred, parked, stopped
// run) is the same as without the retry.
func TestPushWindows_IntentRowsSurviveEveryOutcomeTheManagerMayHaveCommitted(t *testing.T) {
	legacy := `{"error": "install billing path is legacy"}`
	other := win.Add(time.Hour) // a later hour, sent after win when the run goes on
	for _, tc := range []struct {
		name     string
		statuses []int // nil: nothing listens, every attempt is a network error
		body     string
		wantExit int
		wantKept bool
		check    func(t *testing.T, f *fakeStore, requests int)
	}{
		{name: "unreachable on every attempt", wantExit: exitFatal, wantKept: true},
		{name: "5xx past the retries", statuses: []int{503}, wantExit: exitFatal, wantKept: true},
		{name: "5xx then 409", statuses: []int{502, 409}, body: legacy, wantExit: exitRejected, wantKept: true,
			check: func(t *testing.T, f *fakeStore, _ int) {
				if f.deferred[win] == nil || f.refused[win] != nil {
					t.Fatalf("deferred %+v, parked %+v; want the hour still deferred", f.deferred[win], f.refused[win])
				}
			}},
		{name: "5xx then a 400 content refusal", statuses: []int{500, 400}, wantExit: exitRejected, wantKept: true,
			check: func(t *testing.T, f *fakeStore, _ int) {
				if f.refused[win] == nil || f.deferred[win] != nil {
					t.Fatalf("parked %+v, deferred %+v; want the hour still parked", f.refused[win], f.deferred[win])
				}
			}},
		{name: "5xx then 401", statuses: []int{503, 401}, wantExit: exitRejected, wantKept: true,
			check: func(t *testing.T, f *fakeStore, requests int) {
				if requests != 2 || f.accepted[other] != 0 {
					t.Fatalf("%d requests, later hour accepted %d times; want the run stopped after the 401 (2 requests)", requests, f.accepted[other])
				}
			}},
		{name: "control: 409 on the only attempt", statuses: []int{409}, body: legacy, wantExit: exitRejected,
			check: func(t *testing.T, f *fakeStore, _ int) {
				if f.deferred[win] == nil {
					t.Fatal("the hour was not deferred")
				}
			}},
		{name: "control: 400 content refusal on the only attempt", statuses: []int{400}, wantExit: exitRejected,
			check: func(t *testing.T, f *fakeStore, _ int) {
				if f.refused[win] == nil {
					t.Fatal("the hour was not parked")
				}
			}},
		{name: "control: 401 on the only attempt", statuses: []int{401}, wantExit: exitRejected,
			check: func(t *testing.T, _ *fakeStore, requests int) {
				if requests != 1 {
					t.Fatalf("%d requests, want 1 (the run stops)", requests)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := videoStore()
			f.rollups[other] = fixtureRollups()
			s := &intentRecorder{fakeStore: f, added: map[time.Time][]string{}}
			m := &recordingManager{statuses: tc.statuses, errBody: tc.body}
			c := m.client(t)
			if tc.statuses == nil {
				c.BaseURL = closedURL(t)
			}
			if code := newPusher(s, c).pushWindows(context.Background(), []time.Time{win, other}, now, false); code != tc.wantExit {
				t.Fatalf("exit %d, want %d", code, tc.wantExit)
			}
			if len(s.added[win]) == 0 {
				t.Fatal("the attempt added no intent rows; the test checks nothing")
			}
			have := map[string]bool{}
			for _, r := range f.pushed[win] {
				have[r.UsageID] = true
			}
			for _, id := range s.added[win] {
				if have[id] != tc.wantKept {
					t.Fatalf("intent row %s kept=%v, want kept=%v (pushed_usage %v)", id, have[id], tc.wantKept, countKinds(f.pushed[win]))
				}
			}
			if tc.check != nil {
				m.mu.Lock()
				requests := len(m.bodies)
				m.mu.Unlock()
				tc.check(t, f, requests)
			}
		})
	}
}

// closedURL is the URL of a server that is no longer listening.
func closedURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL + "/"
	srv.Close()
	return u
}
