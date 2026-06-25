package main

import (
	"path/filepath"
	"testing"
)

// These cover the binary's fail-closed money-path guards: it must refuse to run
// (exitFatal) when it lacks the auth token, the manager URL, or a database URL —
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

// TestRun_NoManagerURLFatal (token-push-no-url): a missing managerURL is fatal — there
// is no destination to push to.
func TestRun_NoManagerURLFatal(t *testing.T) {
	t.Setenv(tokenEnv, "tok")
	t.Setenv("DATABASE_URL", "postgres://x")
	dir := t.TempDir()
	code := runWith([]string{"-f", filepath.Join(dir, "does-not-exist.yaml")})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_NoDatabaseURLFatal (token-push-no-db): an empty DATABASE_URL is fatal — the
// pusher reads rated_usage + resource_name from it and cannot run without it. This
// reaches openDB, which rejects an empty DSN before any push.
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
