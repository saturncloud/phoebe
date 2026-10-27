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
}

func (f *fakeStore) AggregateWindow(_ context.Context, start, end time.Time) (AggregateResult, error) {
	f.calls++
	f.gotStart, f.gotEnd = start, end
	return f.res, f.returnsError
}
func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Close() error               { return nil }

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
		"owner conflict":       {OwnerConflictEvents: 1},
		"ambiguous base":       {AmbiguousBaseEvents: 1},
		"ambiguous org":        {AmbiguousOrgEvents: 1},
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
