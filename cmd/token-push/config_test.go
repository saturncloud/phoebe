package main

import (
	"os"
	"path/filepath"
	"strings"
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

// The push carries the install's SATURN_TOKEN and its usage, so the manager URL
// must be https. A plain http URL, another scheme, or a URL with no scheme is
// refused at load, and so is the same URL given as -manager-url.
func TestLoadConfig_RejectsNonHTTPSManagerURL(t *testing.T) {
	for _, u := range []string{
		"http://manager.saturnenterprise.io",
		"http://127.0.0.1:8080", // loopback http still needs the test-only key
		"ftp://manager.saturnenterprise.io",
		"manager.saturnenterprise.io",
		"//manager.saturnenterprise.io",
		"https://", // no host
	} {
		t.Run(u, func(t *testing.T) {
			_, _, err := loadConfig(settingsFile(t, "managerURL: \""+u+"\"\n"))
			if err == nil {
				t.Fatalf("loadConfig accepted managerURL %q; want a refusal", u)
			}
			if !strings.Contains(err.Error(), "https") {
				t.Fatalf("error %q does not say https is required", err)
			}
			t.Setenv(tokenEnv, "tok")
			t.Setenv("DATABASE_URL", "postgres://phoebe")
			opened := useStore(t, newFake(), nil)
			if code := runWith([]string{"-manager-url", u, "-f", settingsFile(t, "debug: false\n")}); code != exitFatal {
				t.Fatalf("-manager-url %q: exit %d, want exitFatal(%d)", u, code, exitFatal)
			}
			if *opened {
				t.Fatalf("-manager-url %q: the run reached the database; it must stop at the URL check", u)
			}
		})
	}
	_, opts, err := loadConfig(settingsFile(t, "managerURL: \"https://manager.saturnenterprise.io\"\n"))
	if err != nil || opts.managerURL != "https://manager.saturnenterprise.io" {
		t.Fatalf("https URL: managerURL %q, err %v; want it accepted", opts.managerURL, err)
	}
	if _, _, err := loadConfig(settingsFile(t, "managerURL: \"\"\n")); err != nil {
		t.Fatalf("empty managerURL (not connected) refused: %v", err)
	}
}

// allowInsecureLocalhostManagerURL exists for tests with a stub manager. It
// allows http to localhost, 127.0.0.1 and ::1 and nothing else: http to any
// other host is refused even with the key set.
func TestLoadConfig_AllowsHTTPLocalhostOnlyWithTheTestFlag(t *testing.T) {
	const key = "allowInsecureLocalhostManagerURL: true\n"
	for _, u := range []string{"http://localhost:8080", "http://127.0.0.1:41234/", "http://[::1]:9000", "http://LOCALHOST"} {
		_, opts, err := loadConfig(settingsFile(t, key+"managerURL: \""+u+"\"\n"))
		if err != nil || opts.managerURL != u {
			t.Fatalf("%s with the test key: managerURL %q, err %v; want it accepted", u, opts.managerURL, err)
		}
		if err := checkManagerURL(u, false); err == nil {
			t.Fatalf("%s accepted without the test key", u)
		}
	}
	for _, u := range []string{
		"http://example.com",
		"http://manager.saturnenterprise.io",
		"http://127.0.0.2",
		"http://localhost.example.com",
		"http://10.0.0.1",
		"http://[::ffff:127.0.0.1]",
		"ftp://localhost",
	} {
		if _, _, err := loadConfig(settingsFile(t, key+"managerURL: \""+u+"\"\n")); err == nil {
			t.Fatalf("%s accepted with the test key; it allows loopback http only", u)
		}
	}
	// The key also governs -manager-url: loopback http runs (a fresh install
	// with no usage exits 0 without sending), http to another host is refused
	// before the database is opened.
	t.Setenv(tokenEnv, "tok")
	t.Setenv("DATABASE_URL", "postgres://phoebe")
	fresh := newFake()
	fresh.empty = true
	opened := useStore(t, fresh, nil)
	if code := runWith([]string{"-manager-url", "http://127.0.0.1:1", "-f", settingsFile(t, key)}); code != exitOK || !*opened {
		t.Fatalf("-manager-url http://127.0.0.1:1 with the test key: exit %d (database opened %v), want exitOK", code, *opened)
	}
	opened = useStore(t, newFake(), nil)
	if code := runWith([]string{"-manager-url", "http://example.com", "-f", settingsFile(t, key)}); code != exitFatal || *opened {
		t.Fatalf("-manager-url http://example.com with the test key: exit %d (database opened %v), want exitFatal before the database", code, *opened)
	}
}
