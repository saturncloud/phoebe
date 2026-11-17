package main

import (
	"path/filepath"
	"testing"
)

// These cover the binary's fail-closed money-path guards: it must refuse to run
// (exitFatal) when it has a manager URL but lacks the auth token or a database URL —
// never push usage without an authenticated destination, never run without the DB it
// reads rated_usage from.

// TestRun_NoTokenFatal (token-push-no-token): a missing SATURN_TOKEN is fatal — the
// manager authenticates the install by it; without it there is nothing to push with.
func TestRun_NoTokenFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "postgres://x") // present so the failure is the token
	dir := t.TempDir()
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_NoManagerURLIsNotConnectedAndSucceeds (token-push-no-url): with no
// managerURL the install is not connected to a manager (air-gapped, or managerURI
// ""). Token-push runs on every install (rulings.md 2026-11-17), so this is a
// normal state: nothing is sent, the usage stays in rated_usage, and the run
// exits 0 without touching the database or needing a token, so the CronJob never
// fails on such an install.
func TestRun_NoManagerURLIsNotConnectedAndSucceeds(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "") // would be fatal if the run got that far
	dir := t.TempDir()
	code := runWith([]string{"-f", filepath.Join(dir, "does-not-exist.yaml")})
	if code != exitOK {
		t.Fatalf("exit code = %d, want exitOK(%d)", code, exitOK)
	}
}

// TestRun_NoDatabaseURLFatal (token-push-no-db): an empty DATABASE_URL is fatal — the
// pusher reads rated_usage from it and cannot run without it. This reaches openDB,
// which rejects an empty DSN before any push.
func TestRun_NoDatabaseURLFatal(t *testing.T) {
	t.Setenv(tokenEnv, "tok")
	t.Setenv("DATABASE_URL", "")
	dir := t.TempDir()
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_UnalignedUntilFatal (token-push-windows-until-unaligned): an explicit
// non-hour-aligned -until is rejected before any DB/network work — the symmetric guard
// to the -since alignment check (a partial-hour window would delete-by-absence the rest
// of the hour).
func TestRun_UnalignedUntilFatal(t *testing.T) {
	t.Setenv(tokenEnv, "tok")
	t.Setenv("DATABASE_URL", "postgres://x")
	dir := t.TempDir()
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-until", "2026-06-16T14:30:00Z", // not hour-aligned
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}
