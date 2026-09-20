package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/internal/metering"
)

func TestRunDryRunAndApplyGuard(t *testing.T) {
	ev := metering.Event{RequestID: "phoebe-recovery-test", TimestampUnixMs: 1}
	data, _ := json.Marshal(ev)
	path := filepath.Join(t.TempDir(), "evidence.jsonl")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"-input", path}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "dry-run only") || !strings.Contains(stdout.String(), "unique=1") {
		t.Fatalf("unexpected dry-run output: %s", stdout.String())
	}

	stdout.Reset()
	err := run(context.Background(), []string{"-input", path, "-apply"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "-expected-count is required") {
		t.Fatalf("expected apply guard error, got %v", err)
	}
	err = run(context.Background(), []string{"-input", path, "-apply", "-expected-count", "2"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected count mismatch, got %v", err)
	}
}
