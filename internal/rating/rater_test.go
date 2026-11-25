package rating

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

type fakeStore struct {
	res          AggregateResult
	calls        int
	gotStart     time.Time
	gotEnd       time.Time
	returnsError error
	parkedHours  []time.Time
	parked       []Parked
	windows      [][2]time.Time
}

func (f *fakeStore) AggregateWindow(_ context.Context, start, end time.Time) (AggregateResult, error) {
	f.calls++
	f.gotStart, f.gotEnd = start, end
	f.windows = append(f.windows, [2]time.Time{start, end})
	return f.res, f.returnsError
}
func (f *fakeStore) ParkedHours(context.Context, time.Time) ([]time.Time, error) {
	return f.parkedHours, nil
}
func (f *fakeStore) ListParked(context.Context) ([]Parked, error) { return f.parked, nil }
func (f *fakeStore) Ping(context.Context) error                   { return nil }
func (f *fakeStore) Close() error                                 { return nil }

func captureLogger() (*logging.Logger, *bytes.Buffer, *bytes.Buffer) {
	var info, errBuf bytes.Buffer
	l := logging.New(logging.DEBUG)
	l.Info = log.New(&info, "", 0)
	l.Warn = log.New(&info, "", 0)
	l.Error = log.New(&errBuf, "", 0)
	return l, &info, &errBuf
}

var (
	h0 = time.Date(2026, 10, 22, 14, 0, 0, 0, time.UTC)
	h1 = h0.Add(time.Hour)
)

func TestAggregator_RefusesAnUnalignedWindowWithoutTouchingTheStore(t *testing.T) {
	l, _, _ := captureLogger()
	f := &fakeStore{}
	if _, err := New(f, l).Run(context.Background(), h0.Add(30*time.Minute), h1, false); err == nil {
		t.Fatal("want an error for a half-hour window start")
	}
	if _, err := New(f, l).Run(context.Background(), h1, h0, false); err == nil {
		t.Fatal("want an error for an inverted window")
	}
	if f.calls != 0 {
		t.Fatalf("store called %d times, want 0", f.calls)
	}
}

func TestAggregator_PassesTheWindowInUTC(t *testing.T) {
	l, _, _ := captureLogger()
	f := &fakeStore{}
	loc := time.FixedZone("x", 3*3600)
	if _, err := New(f, l).Run(context.Background(), h0.In(loc), h1.In(loc), false); err != nil {
		t.Fatal(err)
	}
	if f.gotStart.Location() != time.UTC || !f.gotStart.Equal(h0) || !f.gotEnd.Equal(h1) {
		t.Fatalf("store got [%v,%v)", f.gotStart, f.gotEnd)
	}
}

// Every reason usage is withheld from rated_usage needs a person, so each one
// alone must make the run an anomaly (cmd/rater exit 2) and log at ERROR.
func TestResult_EachWithheldReasonIsAnAnomalyAndLogsError(t *testing.T) {
	cases := map[string]AggregateResult{
		"unattributable":       {UnattributableEvents: 1},
		"unexplained missing":  {MissingUsageEvents: 1, UnexplainedMissingUsageEvents: 1},
		"invalid usage":        {InvalidUsageEvents: 1},
		"invalid serving mode": {InvalidServingModeEvents: 1},
	}
	for name, ar := range cases {
		t.Run(name, func(t *testing.T) {
			l, _, errBuf := captureLogger()
			res, err := New(&fakeStore{res: ar}, l).Run(context.Background(), h0, h1, false)
			if err != nil {
				t.Fatal(err)
			}
			if !res.HasAnomaly() {
				t.Fatalf("%s: HasAnomaly = false", name)
			}
			if errBuf.Len() == 0 {
				t.Fatalf("%s: nothing logged at ERROR", name)
			}
		})
	}
}

// Client aborts and upstream failures produce zero-usage rows on every install
// with traffic; paging on them would bury the real signals.
func TestResult_ExpectedMissingUsageIsReportedNotPaged(t *testing.T) {
	l, info, errBuf := captureLogger()
	res, err := New(&fakeStore{res: AggregateResult{MissingUsageEvents: 4, ExpectedMissingUsageEvents: 4}}, l).
		Run(context.Background(), h0, h1, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.HasAnomaly() || errBuf.Len() != 0 {
		t.Fatalf("anomaly=%v error log=%q, want neither", res.HasAnomaly(), errBuf.String())
	}
	if !strings.Contains(info.String(), "expected missing-usage") {
		t.Fatalf("info log %q does not report the expected missing usage", info.String())
	}
}

func TestAggregator_RoutineReconcileDeleteLogsError(t *testing.T) {
	l, _, errBuf := captureLogger()
	if _, err := New(&fakeStore{res: AggregateResult{ReconciledDeletions: 2}}, l).Run(context.Background(), h0, h1, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errBuf.String(), "ROUTINE") {
		t.Fatalf("error log %q, want the routine reconcile-delete page", errBuf.String())
	}
}

func TestAggregator_BackfillReconcileDeleteLogsInfoNotError(t *testing.T) {
	l, info, errBuf := captureLogger()
	if _, err := New(&fakeStore{res: AggregateResult{ReconciledDeletions: 2}}, l).Run(context.Background(), h0, h1, true); err != nil {
		t.Fatal(err)
	}
	if errBuf.Len() != 0 || !strings.Contains(info.String(), "EXPLICIT") {
		t.Fatalf("error=%q info=%q, want only the explicit-window info line", errBuf.String(), info.String())
	}
}

// An unknown topology revision loses margin attribution only; the record is
// still right, so it warns and is not an anomaly.
func TestResult_UnknownRevisionWarnsButIsNotAnAnomaly(t *testing.T) {
	l, info, errBuf := captureLogger()
	res, err := New(&fakeStore{res: AggregateResult{UnknownRevisionRollups: 1, RollupsWritten: 1}}, l).Run(context.Background(), h0, h1, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.HasAnomaly() || errBuf.Len() != 0 || !strings.Contains(info.String(), "topology revision 0") {
		t.Fatalf("anomaly=%v error=%q info=%q", res.HasAnomaly(), errBuf.String(), info.String())
	}
}

func TestEnsureUTCTimeZone(t *testing.T) {
	cases := map[string]string{
		"postgres://u@h/db":                  "postgres://u@h/db?timezone=UTC",
		"postgres://u@h/db?sslmode=disable":  "postgres://u@h/db?sslmode=disable&timezone=UTC",
		"host=h dbname=db":                   "host=h dbname=db timezone=UTC",
		"postgres://u@h/db?TimeZone=Etc/UTC": "postgres://u@h/db?TimeZone=Etc/UTC",
	}
	for in, want := range cases {
		if got := ensureUTCTimeZone(in); got != want {
			t.Errorf("ensureUTCTimeZone(%q) = %q, want %q", in, got, want)
		}
	}
}

// An empty base model is expected for a dedicated endpoint outside the catalog
// (D3): it warns, it is not an anomaly, and it never pages.
func TestResult_EmptyBaseModelWarnsButIsNotAnAnomaly(t *testing.T) {
	l, info, errBuf := captureLogger()
	res, err := New(&fakeStore{res: AggregateResult{EventsAggregated: 2, EmptyBaseModelEvents: 2, RollupsWritten: 1}}, l).Run(context.Background(), h0, h1, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.HasAnomaly() || errBuf.Len() != 0 || !strings.Contains(info.String(), "no base_model") {
		t.Fatalf("anomaly=%v error=%q info=%q", res.HasAnomaly(), errBuf.String(), info.String())
	}
}

// Conflicts are parked, not anomalies: each logs at ERROR, and the run reports
// the parked rows (cmd/rater exit 4), not HasAnomaly (exit 2).
func TestResult_ConflictsAreParkedAndLoggedNotAnomalies(t *testing.T) {
	cases := map[string]AggregateResult{
		"owner conflict": {OwnerConflictEvents: 1, ParkedWritten: 1},
		"ambiguous base": {AmbiguousBaseEvents: 1, ParkedWritten: 1},
		"ambiguous org":  {AmbiguousOrgEvents: 1, ParkedWritten: 1},
		"missing org":    {MissingOrgEvents: 1, ParkedWritten: 1},
	}
	for name, ar := range cases {
		l, _, errBuf := captureLogger()
		parked := []Parked{{WindowStart: h0, AuthID: "a", ResourceID: "r", ModelID: "m", ServingMode: "dedicated", Reason: "x", PromptTokens: 7}}
		res, err := New(&fakeStore{res: ar, parked: parked}, l).Run(context.Background(), h0, h1, false)
		if err != nil {
			t.Fatal(err)
		}
		if res.HasAnomaly() || !res.HasParked() || !strings.Contains(errBuf.String(), "PARKED (unbilled) usage hour=2026-10-22T14:00:00Z") || !strings.Contains(errBuf.String(), "prompt=7") {
			t.Fatalf("%s: anomaly=%v parked=%v error log %q", name, res.HasAnomaly(), res.HasParked(), errBuf.String())
		}
	}
}

// Parked hours outside the window but inside the re-check reach are
// re-aggregated one hour at a time; hours inside the window are not run twice.
func TestAggregator_RechecksParkedHoursOutsideTheWindow(t *testing.T) {
	l, _, _ := captureLogger()
	old := h0.Add(-72 * time.Hour)
	f := &fakeStore{parkedHours: []time.Time{old, h0}}
	a := New(f, l)
	a.now = func() time.Time { return h1.Add(time.Hour) }
	res, err := a.Run(context.Background(), h0, h1, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.RescannedParkedHours != 1 || len(f.windows) != 2 || !f.windows[1][0].Equal(old) || !f.windows[1][1].Equal(old.Add(time.Hour)) {
		t.Fatalf("rescanned %d, windows %v; want the window then [old, old+1h)", res.RescannedParkedHours, f.windows)
	}
}

func TestParkedRescanReach_StaysInsideThePrunerHardFloor(t *testing.T) {
	if ParkedRescanReach >= 7*24*time.Hour || ParkedRescanReach <= 24*time.Hour {
		t.Fatalf("ParkedRescanReach %v must be beyond the 24h window and strictly inside the 7-day prune floor", ParkedRescanReach)
	}
}

// Hugo, 2026-10-27: billing_event keeps 30 days with no exception for parked
// usage. A parked hour whose raw events are pruned is never re-aggregated: a
// window containing it is refused before any statement runs, and the ERROR
// line carries the manual-settlement marker.
func TestAggregator_RefusesAWindowContainingAPrunedParkedHour(t *testing.T) {
	l, _, _ := captureLogger()
	pruned := Parked{WindowStart: h0, AuthID: "a", ResourceID: "r", ModelID: "m", Reason: "ambiguous_org",
		EventCount: 3, RawEvents: 0, RawEventsPruned: true}
	f := &fakeStore{parked: []Parked{pruned}}
	_, err := New(f, l).Run(context.Background(), h0, h1, true)
	if err == nil || !strings.Contains(err.Error(), PrunedMarker) {
		t.Fatalf("err = %v, want a refusal carrying %q", err, PrunedMarker)
	}
	if f.calls != 0 {
		t.Fatalf("aggregated %d times, want 0", f.calls)
	}
}

func TestAggregator_PrunedParkedRowIsLoggedWithTheMarkerAndNotRescanned(t *testing.T) {
	l, _, errBuf := captureLogger()
	old := h0.Add(-40 * 24 * time.Hour)
	pruned := Parked{WindowStart: old, AuthID: "a", Reason: "ambiguous_org", EventCount: 3, RawEventsPruned: true}
	f := &fakeStore{parked: []Parked{pruned}, parkedHours: []time.Time{old}}
	a := New(f, l)
	a.now = func() time.Time { return h1 }
	res, err := a.Run(context.Background(), h0, h1, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.RescannedParkedHours != 0 || f.calls != 1 {
		t.Fatalf("rescanned %d, statements %d; want the pruned hour never re-aggregated", res.RescannedParkedHours, f.calls)
	}
	if !strings.Contains(errBuf.String(), PrunedMarker) {
		t.Fatalf("ERROR log lacks the marker:\n%s", errBuf.String())
	}
}
