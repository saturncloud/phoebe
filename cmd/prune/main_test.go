package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prune.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// captureOutput redirects os.Stdout and os.Stderr — where logging.New writes —
// for the duration of f, returning everything printed. run() builds its logger
// after the redirect, so the capture sees the job's real output.
func captureOutput(t *testing.T, f func()) string {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = w, w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	_ = w.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	return <-done
}

// TestCmdPrune_MissingConfigAndNoDatabaseURL pins the env-only failure mode: a
// missing settings file is tolerated (INFO + ruled defaults), and with no
// DATABASE_URL the job then refuses cleanly with exit 1 — no panic, no partial
// work. Never touches the database, so it runs in the plain unit lane.
func TestCmdPrune_MissingConfigAndNoDatabaseURL(t *testing.T) {
	env := func(string) string { return "" }

	code := exitOK
	out := captureOutput(t, func() {
		code = run(filepath.Join(t.TempDir(), "does-not-exist.yaml"), env)
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want %d", code, exitFatal)
	}
	if !strings.Contains(out, "DATABASE_URL") {
		t.Fatalf("expected a clean DATABASE_URL error, got:\n%s", out)
	}
}

// TestCmdPrune_BelowFloorConfigRefusedBeforeDB pins the order of checks: a
// below-floor config is refused at validation, BEFORE the database is
// consulted — env records whether the job asked for DATABASE_URL at all.
func TestCmdPrune_BelowFloorConfigRefusedBeforeDB(t *testing.T) {
	configPath := writeConfig(t, "billingEventRetentionDays: 3\n")
	consulted := false
	env := func(string) string {
		consulted = true
		return ""
	}

	code := exitOK
	out := captureOutput(t, func() {
		code = run(configPath, env)
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want %d", code, exitFatal)
	}
	if consulted {
		t.Fatal("below-floor config must be refused before the job consults the database")
	}
	if !strings.Contains(out, "hard floor") {
		t.Fatalf("expected the hard-floor refusal in the log, got:\n%s", out)
	}
}

// TestTimeoutForm pins both branches of the failure-logging decision: only a
// deadline (surfaced through the store's %w wrap, or recorded on the table
// context before cancel) logs the timeout form; plain failures and plain
// cancellations take the plain form.
func TestTimeoutForm(t *testing.T) {
	if !timeoutForm(context.DeadlineExceeded, nil) {
		t.Fatal("a raw deadline error is a timeout")
	}
	if !timeoutForm(fmt.Errorf("prune billing_event batch: %w", context.DeadlineExceeded), nil) {
		t.Fatal("a deadline wrapped by the store is a timeout")
	}
	if !timeoutForm(errors.New("connection reset"), context.DeadlineExceeded) {
		t.Fatal("a ctxErr deadline captured before cancel is a timeout")
	}
	if timeoutForm(errors.New(`relation "billing_event" does not exist`), nil) {
		t.Fatal("a missing table is a plain failure, not a timeout")
	}
	if timeoutForm(errors.New("connection reset"), context.Canceled) {
		t.Fatal("cancellation is not the deadline timeout form")
	}
}
