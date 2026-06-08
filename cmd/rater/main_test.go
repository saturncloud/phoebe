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

// TestResolveWindow_DefaultLastCompleteHour: with no flags, the window is the
// last COMPLETE hour [floor(now)-1h, floor(now)) — the natural CronJob cadence.
func TestResolveWindow_DefaultLastCompleteHour(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	start, end, err := resolveWindow("", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(mustTime("2026-06-08T09:00:00Z")) {
		t.Fatalf("start = %v, want 09:00", start)
	}
	if !end.Equal(mustTime("2026-06-08T10:00:00Z")) {
		t.Fatalf("end = %v, want 10:00", end)
	}
}

// TestResolveWindow_ExplicitFlags honours --since/--until.
func TestResolveWindow_ExplicitFlags(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	start, end, err := resolveWindow("2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", now)
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
	if _, _, err := resolveWindow("2026-06-02T00:00:00Z", "2026-06-01T00:00:00Z", now); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestResolveWindow_BadFormat rejects a non-RFC3339 value.
func TestResolveWindow_BadFormat(t *testing.T) {
	now := mustTime("2026-06-08T10:37:42Z")
	if _, _, err := resolveWindow("yesterday", "", now); err == nil {
		t.Fatal("expected error for unparseable --since")
	}
}
