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
	if !strings.Contains(out, "1 hour(s) are deferred (1 refused this run)") {
		t.Fatalf("no WARN summary of deferred hours:\n%s", out)
	}
}

// A deferred hour is re-sent by every run, also once it has left the trailing
// window, until the manager accepts it. The accepted request is byte for byte
// the refused one (one snapshot of the hour, replacing what the manager held),
// so nothing is counted twice; acceptance removes the deferral and the run
// exits 0.
func TestPushWindows_ADeferredHourIsResentEveryRunUntilTheUpgradedManagerAcceptsIt(t *testing.T) {
	f, before, videoHour, after := uk2Store()
	m := &legacyManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{before, videoHour, after}, now, false); code != exitRejected {
		t.Fatalf("first run: exit %d", code)
	}
	refusedBody := m.bodies[1]
	// Later runs: the window has moved past every one of these hours.
	later := after.Add(time.Hour)
	for i := 0; i < 2; i++ {
		if code := p.pushWindows(context.Background(), []time.Time{later}, now, false); code != exitRejected {
			t.Fatalf("run %d with the old manager: exit %d, want exitRejected (the deferred hour is still refused)", i, code)
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
		if err := f.RecordDeferred(context.Background(), h, 409, "x"); err != nil {
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
	if err := f.RecordDeferred(context.Background(), win, 409, "x"); err != nil {
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
