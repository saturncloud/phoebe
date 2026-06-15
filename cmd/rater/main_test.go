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
	start, end, err := resolveWindow("", "", defaultRateTrailingHours, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-07T10:00:00Z")) {
		t.Fatalf("start = %v, want 24h-trailing 2026-06-07T10:00Z", start)
	}
	if !end.Equal(mustTime("2026-06-08T10:00:00Z")) {
		t.Fatalf("end = %v, want 10:00", end)
	}

	// A custom N widens/narrows the trailing window; N=1 is the old last-hour-only.
	start, end, err = resolveWindow("", "", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-08T07:00:00Z")) || !end.Equal(mustTime("2026-06-08T10:00:00Z")) {
		t.Fatalf("window = [%v,%v), want [07:00,10:00) for N=3", start, end)
	}
}

// TestResolveWindow_RejectsBadTrailingHours: N < 1 would rate nothing — fail loud.
func TestResolveWindow_RejectsBadTrailingHours(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	for _, n := range []int{0, -1} {
		if _, _, err := resolveWindow("", "", n, now); err == nil {
			t.Fatalf("expected error for trailingHours=%d", n)
		}
	}
}

// TestResolveWindow_ExplicitFlags honours --since/--until (they WIN over the
// trailing default).
func TestResolveWindow_ExplicitFlags(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	start, end, err := resolveWindow("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", defaultRateTrailingHours, now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-01T00:00:00Z")) || !end.Equal(mustTime("2026-06-02T00:00:00Z")) {
		t.Fatalf("window = [%v,%v), want the 24h day", start, end)
	}
}

// TestResolveWindow_Inverted rejects start >= end.
func TestResolveWindow_Inverted(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	if _, _, err := resolveWindow("2026-06-02T00:00:00Z", "2026-06-01T00:00:00Z", defaultRateTrailingHours, now); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestResolveWindow_BadFormat rejects a non-RFC3339 value.
func TestResolveWindow_BadFormat(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	if _, _, err := resolveWindow("yesterday", "", defaultRateTrailingHours, now); err == nil {
		t.Fatal("expected error for unparseable --since")
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
			if _, _, err := resolveWindow(tc.since, tc.until, defaultRateTrailingHours, now); err == nil {
				t.Fatalf("expected error for non-hour-aligned window %s..%s", tc.since, tc.until)
			}
		})
	}
	// A fully hour-aligned explicit window is still accepted.
	if _, _, err := resolveWindow("2026-06-01T00:00:00Z", "2026-06-01T03:00:00Z", defaultRateTrailingHours, now); err != nil {
		t.Fatalf("hour-aligned window should be accepted, got %v", err)
	}
}
