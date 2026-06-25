package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestResolveWindows_TrailingHours (token-push-windows-trailing): the default produces
// one hour-aligned window per trailing hour, in [floor(now)-N, floor(now)).
func TestResolveWindows_TrailingHours(t *testing.T) {
	now := time.Date(2026, 6, 16, 14, 30, 0, 0, time.UTC) // mid-hour
	ws, err := resolveWindows("", "", 3, now)
	if err != nil {
		t.Fatalf("resolveWindows: %v", err)
	}
	want := []time.Time{
		time.Date(2026, 6, 16, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 16, 13, 0, 0, 0, time.UTC),
	}
	if len(ws) != len(want) {
		t.Fatalf("got %d windows, want %d: %v", len(ws), len(want), ws)
	}
	for i := range want {
		if !ws[i].Equal(want[i]) {
			t.Fatalf("window[%d] = %s, want %s", i, ws[i], want[i])
		}
	}
}

// TestResolveWindows_RejectsUnaligned (token-push-windows-unaligned): an explicit
// sub-hour -since fails loud (a partial-hour snapshot would delete-by-absence the rest
// of the hour).
func TestResolveWindows_RejectsUnaligned(t *testing.T) {
	now := time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)
	if _, err := resolveWindows("2026-06-16T11:30:00Z", "", 3, now); err == nil {
		t.Fatalf("expected an error for a non-hour-aligned -since")
	}
}

// TestResolveWindows_RejectsUnalignedUntil (token-push-windows-until-unaligned): the
// symmetric guard to -since — a non-hour-aligned -until is rejected too.
func TestResolveWindows_RejectsUnalignedUntil(t *testing.T) {
	now := time.Date(2026, 6, 16, 18, 0, 0, 0, time.UTC)
	if _, err := resolveWindows("2026-06-16T11:00:00Z", "2026-06-16T14:30:00Z", 24, now); err == nil {
		t.Fatalf("expected an error for a non-hour-aligned -until")
	}
}

// TestResolveWindows_RejectsInverted (token-push-windows-inverted): start >= end fails.
func TestResolveWindows_RejectsInverted(t *testing.T) {
	now := time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)
	if _, err := resolveWindows("2026-06-16T13:00:00Z", "2026-06-16T12:00:00Z", 3, now); err == nil {
		t.Fatalf("expected an error for an inverted window")
	}
}

// TestResolveWindows_ExplicitRange (token-push-windows-explicit): an explicit aligned
// range enumerates each hour in it.
func TestResolveWindows_ExplicitRange(t *testing.T) {
	now := time.Date(2026, 6, 16, 20, 0, 0, 0, time.UTC)
	ws, err := resolveWindows("2026-06-16T10:00:00Z", "2026-06-16T13:00:00Z", 24, now)
	if err != nil {
		t.Fatalf("resolveWindows: %v", err)
	}
	if len(ws) != 3 {
		t.Fatalf("got %d windows, want 3: %v", len(ws), ws)
	}
}

// TestResolveWindows_RejectsTooWide (token-push-windows-span-cap): an over-broad -since
// (a mistyped multi-year backfill) is rejected, not enumerated — each empty historical
// hour would be a delete-all. Symmetric to the future-clamp guard.
func TestResolveWindows_RejectsTooWide(t *testing.T) {
	now := time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)
	if _, err := resolveWindows("2020-01-01T00:00:00Z", "", 24, now); err == nil {
		t.Fatalf("expected an error for an over-wide (multi-year) -since")
	}
	// Just under the cap still works.
	start := now.Truncate(time.Hour).Add(-time.Duration(maxPushWindows-1) * time.Hour)
	if _, err := resolveWindows(start.Format(time.RFC3339), "", 24, now); err != nil {
		t.Fatalf("a range just under the cap should succeed: %v", err)
	}
}

// TestResolveWindows_RejectsFuture (token-push-windows-future-clamp): an -until past the
// current hour is rejected (future hours have no rows → would push delete-all).
func TestResolveWindows_RejectsFuture(t *testing.T) {
	now := time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC)
	if _, err := resolveWindows("2026-06-16T10:00:00Z", "2026-06-16T20:00:00Z", 24, now); err == nil {
		t.Fatalf("expected an error for an -until past the current hour")
	}
}

// TestLoadConfig_RejectsBadValues (token-push-config-validations): the fail-closed
// config guards reject a sub-1 pushTrailingHours, a non-positive requestTimeout, and an
// unknown YAML key (UnmarshalStrict).
func TestLoadConfig_RejectsBadValues(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "s.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Run("trailing<1", func(t *testing.T) {
		if _, _, err := loadConfig(write(t, "pushTrailingHours: 0\n")); err == nil {
			t.Fatalf("expected error for pushTrailingHours: 0")
		}
	})
	t.Run("non-positive timeout", func(t *testing.T) {
		if _, _, err := loadConfig(write(t, "requestTimeout: \"0s\"\n")); err == nil {
			t.Fatalf("expected error for requestTimeout: 0s")
		}
	})
	t.Run("unknown key", func(t *testing.T) {
		if _, _, err := loadConfig(write(t, "bogusKey: 1\n")); err == nil {
			t.Fatalf("expected error for an unknown YAML key (UnmarshalStrict)")
		}
	})
}

// TestEnsureUTCTimeZone (token-push-utc-dsn): the DSN UTC-pinning helper across its
// branches — already-set (untouched), URL with/without query, keyword form.
func TestEnsureUTCTimeZone(t *testing.T) {
	cases := map[string]string{
		"postgres://h/db":              "postgres://h/db?timezone=UTC",
		"postgres://h/db?sslmode=off":  "postgres://h/db?sslmode=off&timezone=UTC",
		"host=h dbname=db":             "host=h dbname=db timezone=UTC",
		"postgres://h/db?timezone=EST": "postgres://h/db?timezone=EST", // already set, untouched
	}
	for in, want := range cases {
		if got := ensureUTCTimeZone(in); got != want {
			t.Fatalf("ensureUTCTimeZone(%q) = %q, want %q", in, got, want)
		}
	}
}
