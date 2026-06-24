package main

import (
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
