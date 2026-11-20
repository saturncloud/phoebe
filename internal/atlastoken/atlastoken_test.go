package atlastoken

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

func jwtWith(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." + enc([]byte(payload)) + ".sig"
}

func tokenExpiring(at time.Time) string {
	return jwtWith(fmt.Sprintf(`{"sub":"auth-1","aud":"atlas","exp":%d}`, at.Unix()))
}

type logs struct{ warn, err bytes.Buffer }

func newSource(t *testing.T, now time.Time) (*File, string, *logs) {
	t.Helper()
	l := &logs{}
	lg := &logging.Logger{
		Debug: log.New(&bytes.Buffer{}, "", 0),
		Info:  log.New(&bytes.Buffer{}, "", 0),
		Warn:  log.New(&l.warn, "", 0),
		Error: log.New(&l.err, "", 0),
	}
	path := filepath.Join(t.TempDir(), "token")
	f, err := NewFile(path, lg)
	if err != nil {
		t.Fatal(err)
	}
	f.now = func() time.Time { return now }
	return f, path, l
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestNewFileRequiresAPath(t *testing.T) {
	if _, err := NewFile(" ", logging.New(logging.INFO)); err == nil {
		t.Fatal("NewFile accepted an empty path")
	}
}

func TestTokenReturnsAValidToken(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f, path, l := newSource(t, now)
	want := tokenExpiring(now.Add(20 * time.Hour))
	write(t, path, want+"\n")
	got, err := f.Token()
	if err != nil || got != want {
		t.Fatalf("Token() = %q, %v; want the file's token", got, err)
	}
	if l.warn.Len() != 0 || l.err.Len() != 0 {
		t.Fatalf("unexpected logs: warn=%q err=%q", l.warn.String(), l.err.String())
	}
}

// Atlas replaces the Secret before expiry; the next call uses the new token
// with no restart.
func TestTokenPicksUpARotatedTokenWithoutRestart(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f, path, _ := newSource(t, now)
	old := tokenExpiring(now.Add(11 * time.Hour))
	write(t, path, old)
	if got, _ := f.Token(); got != old {
		t.Fatalf("first token = %q", got)
	}
	next := tokenExpiring(now.Add(24 * time.Hour))
	write(t, path, next)
	if got, err := f.Token(); err != nil || got != next {
		t.Fatalf("after rotation Token() = %q, %v; want the new token", got, err)
	}
}

// A token close to its expiry is still sent (refresh just before expiry
// works) but warned about.
func TestTokenNearExpiryIsSentWithAWarning(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f, path, l := newSource(t, now)
	tok := tokenExpiring(now.Add(5 * time.Minute))
	write(t, path, tok)
	if got, err := f.Token(); err != nil || got != tok {
		t.Fatalf("Token() = %q, %v; want the token", got, err)
	}
	if !strings.Contains(l.warn.String(), "expires in") {
		t.Fatalf("no warning logged: %q", l.warn.String())
	}
}

func TestTokenRefusesWithoutSending(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cases := map[string]string{
		"expired":           tokenExpiring(now.Add(-time.Second)),
		"within skew":       tokenExpiring(now.Add(30 * time.Second)),
		"empty":             "",
		"not a jwt":         "legacy-shared-secret",
		"no expiry":         jwtWith(`{"sub":"auth-1"}`),
		"non numeric exp":   jwtWith(`{"exp":"tomorrow"}`),
		"payload not json":  "a." + base64.RawURLEncoding.EncodeToString([]byte("x")) + ".c",
		"payload not b64":   "a.!!!.c",
		"too many segments": "a.b.c.d",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			f, path, l := newSource(t, now)
			write(t, path, content)
			got, err := f.Token()
			if got != "" || !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Token() = %q, %v; want ErrUnavailable and no token", got, err)
			}
			if !strings.Contains(l.err.String(), "REFUSING") {
				t.Fatalf("refusal not logged as an error: %q", l.err.String())
			}
		})
	}
}

// A new install's pod can start before Atlas writes the Secret.
func TestTokenWithNoFileYetIsRefusedThenWorksOnceWritten(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f, path, _ := newSource(t, now)
	if _, err := f.Token(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Token() with no file = %v; want ErrUnavailable", err)
	}
	tok := tokenExpiring(now.Add(24 * time.Hour))
	write(t, path, tok)
	if got, err := f.Token(); err != nil || got != tok {
		t.Fatalf("Token() = %q, %v", got, err)
	}
}

func TestRepeatedRefusalsLogAtMostOncePerInterval(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f, _, l := newSource(t, now)
	for i := 0; i < 5; i++ {
		_, _ = f.Token()
	}
	if n := strings.Count(l.err.String(), "REFUSING"); n != 1 {
		t.Fatalf("logged %d refusals in one interval, want 1", n)
	}
	f.now = func() time.Time { return now.Add(logEvery) }
	_, _ = f.Token()
	if n := strings.Count(l.err.String(), "REFUSING"); n != 2 {
		t.Fatalf("logged %d refusals after the interval, want 2", n)
	}
}
