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
	if !strings.Contains(stdout.String(), "dry-run only") || !strings.Contains(stdout.String(), "unique=1") ||
		!strings.Contains(stdout.String(), "event_set_sha256=") || !strings.Contains(stdout.String(), "-expected-digest ") {
		t.Fatalf("unexpected dry-run output: %s", stdout.String())
	}

	stdout.Reset()
	err := run(context.Background(), []string{"-input", path, "-apply"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "-expected-count is required") {
		t.Fatalf("expected apply guard error, got %v", err)
	}
	err = run(context.Background(), []string{"-input", path, "-apply", "-expected-count", "2"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "-expected-digest is required") {
		t.Fatalf("expected required-digest guard, got %v", err)
	}
	// A wrong count is refused even when a syntactically valid digest is given.
	err = run(context.Background(), []string{
		"-input", path, "-apply", "-expected-count", "2", "-expected-digest", strings.Repeat("0", 64),
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected count mismatch, got %v", err)
	}
}

// TestApplyRequiresDigestBindingCompleteEventSet pins the recovery apply
// contract: -apply is bound to the COMPLETE reviewed event set, not merely its
// cardinality or its request-id set. Without this, an operator can review
// artifact A during dry-run and replay a different artifact B that happens to
// have the same event count — or the same request ids with different token
// counts or org attribution — and bill the wrong thing.
func TestApplyRequiresDigestBindingCompleteEventSet(t *testing.T) {
	write := func(t *testing.T, events ...metering.Event) string {
		t.Helper()
		var buf bytes.Buffer
		for _, ev := range events {
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			buf.Write(data)
			buf.WriteByte('\n')
		}
		path := filepath.Join(t.TempDir(), "evidence.jsonl")
		if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	digestOf := func(t *testing.T, path string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), []string{"-input", path}, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		const marker = "event_set_sha256="
		at := strings.Index(stdout.String(), marker)
		if at < 0 {
			t.Fatalf("dry-run output has no %s: %s", marker, stdout.String())
		}
		return strings.TrimSpace(strings.SplitN(stdout.String()[at+len(marker):], "\n", 2)[0])
	}

	reviewed := write(t,
		metering.Event{RequestID: "req-a", OrgID: "org-1", PromptTokens: 10, TimestampUnixMs: 1},
		metering.Event{RequestID: "req-b", OrgID: "org-1", PromptTokens: 20, TimestampUnixMs: 2},
	)
	reviewedDigest := digestOf(t, reviewed)

	// The digest must be required, not optional: omitting it cannot silently
	// fall back to the weaker count-only binding.
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"-input", reviewed, "-apply", "-expected-count", "2"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "-expected-digest is required") {
		t.Fatalf("expected required-digest guard, got %v", err)
	}

	t.Run("same count different request ids is refused", func(t *testing.T) {
		other := write(t,
			metering.Event{RequestID: "req-c", OrgID: "org-1", PromptTokens: 10, TimestampUnixMs: 1},
			metering.Event{RequestID: "req-d", OrgID: "org-1", PromptTokens: 20, TimestampUnixMs: 2},
		)
		if got := digestOf(t, other); got == reviewedDigest {
			t.Fatal("different request ids must not share the reviewed digest")
		}
		var out, errOut bytes.Buffer
		err := run(context.Background(), []string{
			"-input", other, "-apply", "-expected-count", "2", "-expected-digest", reviewedDigest,
		}, &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected digest mismatch, got %v", err)
		}
	})

	t.Run("same request ids different payload is refused", func(t *testing.T) {
		// Identical ids and count; token counts and org differ. This is exactly
		// what a request-id-only digest could not detect.
		tampered := write(t,
			metering.Event{RequestID: "req-a", OrgID: "org-2", PromptTokens: 9999, TimestampUnixMs: 1},
			metering.Event{RequestID: "req-b", OrgID: "org-1", PromptTokens: 20, TimestampUnixMs: 2},
		)
		if got := digestOf(t, tampered); got == reviewedDigest {
			t.Fatal("altered token counts/org must not share the reviewed digest")
		}
		var out, errOut bytes.Buffer
		err := run(context.Background(), []string{
			"-input", tampered, "-apply", "-expected-count", "2", "-expected-digest", reviewedDigest,
		}, &out, &errOut)
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected digest mismatch on tampered payload, got %v", err)
		}
	})

	t.Run("digest is stable across input record order", func(t *testing.T) {
		reordered := write(t,
			metering.Event{RequestID: "req-b", OrgID: "org-1", PromptTokens: 20, TimestampUnixMs: 2},
			metering.Event{RequestID: "req-a", OrgID: "org-1", PromptTokens: 10, TimestampUnixMs: 1},
		)
		if got := digestOf(t, reordered); got != reviewedDigest {
			t.Fatalf("digest = %s, want %s — ordering must not change the binding", got, reviewedDigest)
		}
	})
}
