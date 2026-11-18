package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/saturncloud/phoebe/internal/rating"
)

// These cover the binary's fail-closed money-path guards: it must refuse to run
// (exitFatal) when it has a manager URL but lacks the auth token or a database URL —
// never push usage without an authenticated destination, never run without the DB it
// reads rated_usage from.

// settingsFile writes a settings file with the given body and returns its path.
// The guards below must be reached with a file that exists: a missing file is
// itself fatal (TestRun_MissingSettingsFileIsFatal) and would hide them.
func settingsFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token-push.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestRun_NoTokenFatal (token-push-no-token): a missing SATURN_TOKEN is fatal — the
// manager authenticates the install by it; without it there is nothing to push with.
func TestRun_NoTokenFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "postgres://x") // present so the failure is the token
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-f", settingsFile(t, "debug: false\n"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_EmptyManagerURLInFileIsNotConnectedAndSucceeds (token-push-no-url): a
// settings file with managerURL "" (and no -manager-url) means the install is not
// connected to a manager (air-gapped, or managerURI ""). Token-push runs on every
// install (rulings.md 2026-11-17), so this is a normal state: nothing is sent, the
// usage stays in rated_usage, and the run exits 0 without needing a token. With no
// DATABASE_URL it does not touch the database (with one, it checks the install
// never pushed: TestRun_NoManagerURLAfterPushingIsFatal).
func TestRun_EmptyManagerURLInFileIsNotConnectedAndSucceeds(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "") // would be fatal if the run got that far
	code := runWith([]string{"-f", settingsFile(t, "debug: false\nmanagerURL: \"\"\npushTrailingHours: 24\n")})
	if code != exitOK {
		t.Fatalf("exit code = %d, want exitOK(%d)", code, exitOK)
	}
}

// TestRun_MissingSettingsFileIsFatal: the chart always renders and mounts the
// settings file, so a missing one is a broken mount or a wrong -f. It must fail
// the run, not pass as "not connected" (exit 0) and silently stop sending usage.
func TestRun_MissingSettingsFileIsFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "")
	code := runWith([]string{"-f", filepath.Join(t.TempDir(), "does-not-exist.yaml")})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_NoDatabaseURLFatal (token-push-no-db): an empty DATABASE_URL is fatal — the
// pusher reads rated_usage from it and cannot run without it. This reaches openDB,
// which rejects an empty DSN before any push.
func TestRun_NoDatabaseURLFatal(t *testing.T) {
	t.Setenv(tokenEnv, "tok")
	t.Setenv("DATABASE_URL", "")
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-f", settingsFile(t, "debug: false\n"),
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
	code := runWith([]string{
		"-manager-url", "http://manager.example",
		"-until", "2026-06-16T14:30:00Z", // not hour-aligned
		"-f", settingsFile(t, "debug: false\n"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// useStore makes runWith open the given store instead of Postgres, and
// reports whether it was opened.
func useStore(t *testing.T, s pushStore, err error) *bool {
	t.Helper()
	opened := false
	prev := openStore
	openStore = func(context.Context, rating.Config) (pushStore, func() error, error) {
		opened = true
		if err != nil {
			return nil, nil, err
		}
		return s, func() error { return nil }, nil
	}
	t.Cleanup(func() { openStore = prev })
	return &opened
}

const notConnectedSettings = "debug: false\nmanagerURL: \"\"\n"

// An install that has pushed before and now has no manager URL has lost it
// (a values change, a broken render). Its usage would silently stop reaching
// the manager, so the run fails, like the wiped-database guard.
func TestRun_NoManagerURLAfterPushingIsFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "postgres://phoebe")
	f := newFake()
	f.everPushed = true
	opened := useStore(t, f, nil)
	if code := runWith([]string{"-f", settingsFile(t, notConnectedSettings)}); code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
	if !*opened {
		t.Fatal("the database was not consulted")
	}
}

// An install that never pushed and has no manager URL is not connected yet:
// a normal state, exit 0. With no DATABASE_URL at all the database is not
// opened.
func TestRun_NoManagerURLNeverConnectedSucceeds(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "postgres://phoebe")
	opened := useStore(t, newFake(), nil)
	if code := runWith([]string{"-f", settingsFile(t, notConnectedSettings)}); code != exitOK {
		t.Fatalf("with a database that never pushed: exit code = %d, want exitOK(%d)", code, exitOK)
	}
	if !*opened {
		t.Fatal("the database was not consulted")
	}

	t.Setenv("DATABASE_URL", "")
	opened = useStore(t, nil, errors.New("must not be opened"))
	if code := runWith([]string{"-f", settingsFile(t, notConnectedSettings)}); code != exitOK {
		t.Fatalf("with no DATABASE_URL: exit code = %d, want exitOK(%d)", code, exitOK)
	}
	if *opened {
		t.Fatal("opened a database although DATABASE_URL is not set")
	}
}

// DATABASE_URL is set but the database cannot be read: the run cannot tell a
// lost manager URL from a never-connected install, so it fails.
func TestRun_NoManagerURLDatabaseUnreachableIsFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv("DATABASE_URL", "postgres://phoebe")
	useStore(t, nil, errors.New("postgres ping: connection refused"))
	if code := runWith([]string{"-f", settingsFile(t, notConnectedSettings)}); code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
	f := newFake()
	f.everPushedErr = errors.New("relation pushed_hour does not exist")
	useStore(t, f, nil)
	if code := runWith([]string{"-f", settingsFile(t, notConnectedSettings)}); code != exitFatal {
		t.Fatalf("EverPushed error: exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}
