package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
)

func formatHours(hs ...time.Time) string {
	var out []string
	for _, h := range hs {
		out = append(out, meteredusage.FormatWindowStart(h))
	}
	return strings.Join(out, ",")
}

// Usage recorded before the install was connected is sent without an
// operator: the first scheduled run with an accepted hour sets the floor, and
// the runs after it send the earlier hours, oldest first, after the window.
// An explicit run does not start the back-fill.
func TestPushWindows_FirstConnectionBackfillsPreConnectionUsage(t *testing.T) {
	pre1, pre2 := win.Add(-100*time.Hour), win.Add(-50*time.Hour)
	f := fixtureStore()
	f.rollups[pre1] = fixtureRollups()
	f.rollups[pre2] = fixtureRollups()
	f.backfill = []time.Time{pre2, pre1}

	explicitRun := &recordingManager{}
	if code := newPusher(f, explicitRun.client(t)).pushWindows(context.Background(), []time.Time{win}, now, true); code != exitOK {
		t.Fatalf("explicit run: exit %d", code)
	}
	if got := windowStartsOf(t, explicitRun); strings.Join(got, ",") != formatHours(win) {
		t.Fatalf("explicit run sent %v, want only its range (no floor, no back-fill)", got)
	}

	m := &recordingManager{}
	p, logs := capturingPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("first connected run: exit %d", code)
	}
	if f.autoFloor == nil || !f.autoFloor.Equal(win) {
		t.Fatalf("floor %v, want %s", f.autoFloor, win)
	}
	f.everPushed = true
	logs.Reset()
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("second run: exit %d", code)
	}
	if got := windowStartsOf(t, m); strings.Join(got, ",") != formatHours(win, win, pre1, pre2) {
		t.Fatalf("sent %v, want the window in both runs and then the pre-connection hours oldest first", got)
	}
	wantLog := "back-filling usage from before this install's first accepted push: 2 hour(s) not sent yet (oldest " +
		pre1.Format(time.RFC3339) + "); this run sends 2"
	if !strings.Contains(logs.String(), wantLog) {
		t.Fatalf("no back-fill progress line %q in:\n%s", wantLog, logs.String())
	}
	if strings.Contains(logs.String(), "not sent automatically") {
		t.Fatalf("the round-1 'not sent automatically' line is still logged:\n%s", logs.String())
	}

	logs.Reset()
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("third run: exit %d", code)
	}
	if got := windowStartsOf(t, m); len(got) != 5 || got[4] != meteredusage.FormatWindowStart(win) {
		t.Fatalf("third run sent %v, want only the window", got[4:])
	}
	if strings.Contains(logs.String(), "back-filling") {
		t.Fatalf("a finished back-fill is still reported:\n%s", logs.String())
	}
	if !f.autoFloor.Equal(win) {
		t.Fatalf("the back-fill moved the floor to %s", f.autoFloor)
	}
}

// The back-fill shares the per-run cap with missed hours after the floor,
// which go first; the trailing window goes before both. A long back-fill is
// spread over several runs.
func TestPushWindows_BackfillRespectsThePerRunCapAndTrailingWindowFirst(t *testing.T) {
	f := newFake()
	f.everPushed = true
	floorHour := win.Add(-10 * time.Hour)
	f.autoFloor = &floorHour
	f.sentBefore[floorHour] = true
	window := []time.Time{win.Add(-time.Hour), win}
	var unsent, backfill []time.Time
	for i := 9; i >= 5; i-- {
		unsent = append(unsent, win.Add(-time.Duration(i)*time.Hour))
	}
	for i := 200; i >= 1; i-- {
		backfill = append(backfill, floorHour.Add(-time.Duration(i)*time.Hour))
	}
	for _, h := range append(append(append([]time.Time(nil), window...), unsent...), backfill...) {
		f.rollups[h] = fixtureRollups()
	}
	f.unsent, f.backfill = unsent, backfill

	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), window, now, false); code != exitOK {
		t.Fatalf("first run: exit %d", code)
	}
	if f.unsentLimit != maxUnsentHoursPerRun || f.backfillLimit != maxUnsentHoursPerRun-len(unsent) {
		t.Fatalf("limits: unsent %d, back-fill %d; want %d and %d", f.unsentLimit, f.backfillLimit, maxUnsentHoursPerRun, maxUnsentHoursPerRun-len(unsent))
	}
	first := append(append(append([]time.Time(nil), window...), unsent...), backfill[:maxUnsentHoursPerRun-len(unsent)]...)
	if got := windowStartsOf(t, m); strings.Join(got, ",") != formatHours(first...) {
		t.Fatalf("first run sent %d hours, want the window, the %d missed hours, then the %d oldest back-fill hours", len(got), len(unsent), maxUnsentHoursPerRun-len(unsent))
	}

	n := len(m.bodies)
	if code := p.pushWindows(context.Background(), window, now, false); code != exitOK {
		t.Fatalf("second run: exit %d", code)
	}
	second := append(append([]time.Time(nil), window...), backfill[maxUnsentHoursPerRun-len(unsent):]...)
	if got := windowStartsOf(t, m)[n:]; strings.Join(got, ",") != formatHours(second...) {
		t.Fatalf("second run sent %d hours, want the window and the %d back-fill hours left", len(got), len(second)-2)
	}

	n = len(m.bodies)
	if code := p.pushWindows(context.Background(), window, now, false); code != exitOK {
		t.Fatalf("third run: exit %d", code)
	}
	if got := windowStartsOf(t, m)[n:]; strings.Join(got, ",") != formatHours(window...) {
		t.Fatalf("third run sent %v, want only the window", got)
	}
}

// Hours already accepted (here by an operator's -since/-until run, and by an
// earlier back-fill run) are not sent again by the back-fill.
func TestPushWindows_BackfillDoesNotResendAcceptedHours(t *testing.T) {
	b1, b2, b3 := win.Add(-30*time.Hour), win.Add(-20*time.Hour), win.Add(-15*time.Hour)
	f := fixtureStore()
	f.everPushed = true
	floorHour := win.Add(-10 * time.Hour)
	f.autoFloor = &floorHour
	for _, h := range []time.Time{b1, b2, b3} {
		f.rollups[h] = fixtureRollups()
	}
	f.backfill = []time.Time{b1, b2, b3}
	f.sentBefore[b2] = true // an earlier -since/-until run had it accepted (pushed_hour)

	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	n := len(m.bodies)
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("scheduled run: exit %d", code)
	}
	if got := windowStartsOf(t, m)[n:]; strings.Join(got, ",") != formatHours(win, b1, b3) {
		t.Fatalf("scheduled run sent %v, want the window, then b1 and b3 (b2 was accepted)", got)
	}
	n = len(m.bodies)
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("next run: exit %d", code)
	}
	if got := windowStartsOf(t, m)[n:]; strings.Join(got, ",") != formatHours(win) {
		t.Fatalf("next run sent %v, want only the window", got)
	}
}
