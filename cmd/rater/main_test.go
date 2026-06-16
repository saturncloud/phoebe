package main

import (
	"testing"
	"time"
)

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestResolveWindow_DefaultTrailingHours: with no flags, the window is the
// TRAILING N complete hours [floor(now)-N*1h, floor(now)) — so an event drained
// LATE into an already-rated hour (Valkey outage → WAL recovery) is re-caught by
// a later run instead of being lost forever (the upsert REPLACES each hour bucket,
// so the re-rate never doubles).
func TestResolveWindow_DefaultTrailingHours(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	start, end, explicit, err := resolveWindow("", "", defaultRateTrailingHours, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-07T10:00:00Z")) {
		t.Fatalf("start = %v, want 24h-trailing 2026-06-07T10:00Z", start)
	}
	if !end.Equal(mustTime("2026-06-08T10:00:00Z")) {
		t.Fatalf("end = %v, want 10:00", end)
	}
	// The default trailing window is NOT explicit — it is the routine cadence, so a
	// reconcile-delete on it must page (see the reconcile-exit contract).
	if explicit {
		t.Fatal("default trailing window must report windowExplicit=false")
	}

	// A custom N widens/narrows the trailing window; N=1 is the old last-hour-only.
	start, end, explicit, err = resolveWindow("", "", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-08T07:00:00Z")) || !end.Equal(mustTime("2026-06-08T10:00:00Z")) {
		t.Fatalf("window = [%v,%v), want [07:00,10:00) for N=3", start, end)
	}
	if explicit {
		t.Fatal("a custom N is still the trailing default, not an explicit window")
	}
}

// TestResolveWindow_RejectsBadTrailingHours: N < 1 would rate nothing — fail loud.
func TestResolveWindow_RejectsBadTrailingHours(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	for _, n := range []int{0, -1} {
		if _, _, _, err := resolveWindow("", "", n, now); err == nil {
			t.Fatalf("expected error for trailingHours=%d", n)
		}
	}
}

// TestResolveWindow_ExplicitFlags honours --since/--until (they WIN over the
// trailing default).
func TestResolveWindow_ExplicitFlags(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	start, end, explicit, err := resolveWindow("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", defaultRateTrailingHours, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-01T00:00:00Z")) || !end.Equal(mustTime("2026-06-02T00:00:00Z")) {
		t.Fatalf("window = [%v,%v), want the 24h day", start, end)
	}
	// Both flags set → an explicit operator backfill, so a reconcile-delete here is
	// intended convergence (exit 0), not a page.
	if !explicit {
		t.Fatal("an explicit --since/--until window must report windowExplicit=true")
	}
}

// TestResolveWindow_SingleFlagIsExplicit: setting EITHER flag alone (not both) still
// marks the window explicit — the operator named a bound, so a reconcile-delete on it
// is an intended backfill, not a routine-run page.
func TestResolveWindow_SingleFlagIsExplicit(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	// --since only (until defaults to floor(now), hour-aligned).
	if _, _, explicit, err := resolveWindow("2026-06-01T00:00:00Z", "", defaultRateTrailingHours, now); err != nil || !explicit {
		t.Fatalf("--since-only: explicit=%v err=%v, want explicit=true nil", explicit, err)
	}
	// --until only (since defaults to floor(now)-N*1h, hour-aligned).
	if _, _, explicit, err := resolveWindow("", "2026-06-08T09:00:00Z", defaultRateTrailingHours, now); err != nil || !explicit {
		t.Fatalf("--until-only: explicit=%v err=%v, want explicit=true nil", explicit, err)
	}
}

// TestResolveWindow_Inverted rejects start >= end.
func TestResolveWindow_Inverted(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	if _, _, _, err := resolveWindow("2026-06-02T00:00:00Z", "2026-06-01T00:00:00Z", defaultRateTrailingHours, now); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestResolveWindow_BadFormat rejects a non-RFC3339 value.
func TestResolveWindow_BadFormat(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	if _, _, _, err := resolveWindow("yesterday", "", defaultRateTrailingHours, now); err == nil {
		t.Fatal("expected error for unparseable --since")
	}
}

// TestRater_RoutineRunReconcileDeleteExitsNonzero pins the EXIT-CODE half of the
// reconcile observability contract: a ROUTINE run (the default trailing-hours window —
// windowExplicit == false) that DELETED a previously-billed rated_usage row
// (ReconciledDeletions > 0) is a revenue change with no operator behind it, which
// means data was lost / an upstream regression dropped events. It MUST exit nonzero
// (exitAnomaly) so a CronJob pages — even with NO other anomaly. This pins ONLY the
// exit code (exitCode()'s gate); the matching LOG-severity half (routine → ERROR) is
// pinned separately by TestRater_RoutineReconcileDeleteLogsError in internal/rating,
// where the reconcile-delete log line is emitted. RED before FIX 1's exit-code change:
// the old exit path keyed solely on HasAnomaly(), so a reconcile-delete with no other
// anomaly returned exitOK and the page never fired.
func TestRater_RoutineRunReconcileDeleteExitsNonzero(t *testing.T) {
	const (
		windowExplicit = false // routine default trailing-hours window
		hasAnomaly     = false // ONLY a reconcile-delete; no unpriced/unattributable/ambiguous
	)
	if got := exitCode(1, windowExplicit, hasAnomaly); got != exitAnomaly {
		t.Fatalf("routine run with a reconcile-delete: exit = %d, want exitAnomaly (%d) — a prior bill vanished on a routine cadence; page someone", got, exitAnomaly)
	}
	// And with MORE than one delete (count is just a signal, not a threshold).
	if got := exitCode(7, windowExplicit, hasAnomaly); got != exitAnomaly {
		t.Fatalf("routine run with 7 reconcile-deletes: exit = %d, want exitAnomaly (%d)", got, exitAnomaly)
	}
	// Sanity: a routine run with NO reconcile-delete and no anomaly is the clean path.
	if got := exitCode(0, windowExplicit, hasAnomaly); got != exitOK {
		t.Fatalf("clean routine run: exit = %d, want exitOK (%d)", got, exitOK)
	}
}

// TestRater_BackfillReconcileDeleteExitsZero pins the quiet half: an EXPLICIT
// operator backfill (--since/--until → windowExplicit == true) that DELETED a
// previously-billed row is intended convergence ("they asked for it", e.g. after a
// late price fix), so it exits 0 (INFO, not a page) when nothing else leaked. A
// genuine anomaly still forces exitAnomaly even on a backfill — the explicit flag
// suppresses ONLY the reconcile-delete signal, never a real anomaly.
func TestRater_BackfillReconcileDeleteExitsZero(t *testing.T) {
	const windowExplicit = true // operator named the window
	// Reconcile-delete on an explicit backfill, nothing else leaked → exit 0.
	if got := exitCode(3, windowExplicit, false); got != exitOK {
		t.Fatalf("explicit backfill with a reconcile-delete: exit = %d, want exitOK (%d) — convergence the operator asked for", got, exitOK)
	}
	// But a real anomaly during a backfill STILL exits nonzero (the flag never
	// suppresses unpriced/unattributable/ambiguous).
	if got := exitCode(3, windowExplicit, true); got != exitAnomaly {
		t.Fatalf("explicit backfill that ALSO leaked an anomaly: exit = %d, want exitAnomaly (%d) — --since must not mask a real anomaly", got, exitAnomaly)
	}
	// A clean explicit backfill (no deletes, no anomaly) is exit 0.
	if got := exitCode(0, windowExplicit, false); got != exitOK {
		t.Fatalf("clean explicit backfill: exit = %d, want exitOK (%d)", got, exitOK)
	}
}

// TestResolveWindow_RejectsUnaligned guards the under-bill footgun: a sub-hour
// window would overwrite a complete hourly rollup with a partial sum, so a
// non-hour-aligned --since/--until must fail loud rather than rate silently.
func TestResolveWindow_RejectsUnaligned(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	cases := []struct {
		name         string
		since, until string
	}{
		{"since has minutes", "2026-06-01T00:30:00Z", "2026-06-01T02:00:00Z"},
		{"until has minutes", "2026-06-01T00:00:00Z", "2026-06-01T01:30:00Z"},
		{"since has seconds", "2026-06-01T00:00:30Z", "2026-06-01T02:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := resolveWindow(tc.since, tc.until, defaultRateTrailingHours, now); err == nil {
				t.Fatalf("expected error for non-hour-aligned window %s..%s", tc.since, tc.until)
			}
		})
	}
	// A fully hour-aligned explicit window is still accepted.
	if _, _, _, err := resolveWindow("2026-06-01T00:00:00Z", "2026-06-01T03:00:00Z", defaultRateTrailingHours, now); err != nil {
		t.Fatalf("hour-aligned window should be accepted, got %v", err)
	}
}
