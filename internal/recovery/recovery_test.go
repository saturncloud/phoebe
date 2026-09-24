package recovery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	tidwall "github.com/tidwall/wal"

	"github.com/saturncloud/phoebe/internal/metering"
)

func testEvent(id string) metering.Event {
	return metering.Event{
		RequestID:       id,
		AuthID:          "auth-1",
		PromptTokens:    -1, // Invalid engine counts remain recoverable raw evidence.
		UsageFound:      true,
		TimestampUnixMs: 1_750_000_000_000,
	}
}

func TestLoadJSONLAndFloorLogsDedupeIdenticalEvents(t *testing.T) {
	ev1 := testEvent("phoebe-1")
	ev2 := testEvent("phoebe-2")
	one, _ := json.Marshal(ev1)
	two, _ := json.Marshal(ev2)
	path := filepath.Join(t.TempDir(), "evidence.log")
	content := string(one) + "\n" +
		"2026-09-20T00:00:00Z INFO unrelated application log\n" +
		"2026-09-20T00:00:00Z ERROR METERING_FLOOR request_id=phoebe-2 event=" + string(two) + "\n" +
		string(one) + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	evidence, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Records != 3 || evidence.Duplicates != 1 || len(evidence.Events) != 2 {
		t.Fatalf("unexpected evidence summary: %+v", evidence)
	}
	if evidence.Digest() == "" {
		t.Fatal("empty digest")
	}
}

func TestLoadRejectsConflictingDuplicate(t *testing.T) {
	ev1 := testEvent("phoebe-same")
	ev2 := ev1
	ev2.CompletionTokens = 1
	one, _ := json.Marshal(ev1)
	two, _ := json.Marshal(ev2)
	path := filepath.Join(t.TempDir(), "evidence.jsonl")
	if err := os.WriteFile(path, append(append(one, '\n'), two...), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "conflicting duplicate") {
		t.Fatalf("expected conflicting duplicate error, got %v", err)
	}
}

func TestLoadRejectsSchemaPoisonButRetainsInvalidCounts(t *testing.T) {
	ev := testEvent(strings.Repeat("x", 255))
	data, _ := json.Marshal(ev)
	path := filepath.Join(t.TempDir(), "evidence.jsonl")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "fewer than 255") {
		t.Fatalf("expected request id validation error, got %v", err)
	}

	ev.RequestID = "phoebe-negative-is-evidence"
	data, _ = json.Marshal(ev)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("raw invalid engine counts must remain recoverable: %v", err)
	}
}

func TestLoadWALUsesCopyAndLeavesSourceUnchanged(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	log, err := tidwall.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range []metering.Event{testEvent("phoebe-a"), testEvent("phoebe-b")} {
		data, _ := json.Marshal(ev)
		if err := log.Write(uint64(i+1), data); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	before := snapshotDir(t, dir)

	evidence, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Events) != 2 {
		t.Fatalf("got %d events", len(evidence.Events))
	}
	after := snapshotDir(t, dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("source WAL changed:\nbefore=%v\nafter=%v", before, after)
	}
}

func snapshotDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(dir, path)
			result[rel] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestReplayUsesEmitterStreamShapeAndPreservesIDs(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = rdb.Close() }()
	events := []metering.Event{testEvent("phoebe-one"), testEvent("phoebe-two")}

	written, err := Replay(context.Background(), rdb, "recovery-stream", time.Second, events)
	if err != nil {
		t.Fatal(err)
	}
	if written != 2 {
		t.Fatalf("written=%d", written)
	}
	rows, err := rdb.XRange(context.Background(), "recovery-stream", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("stream rows=%d", len(rows))
	}
	for i, row := range rows {
		raw, ok := row.Values["event"].(string)
		if !ok {
			t.Fatalf("row %d missing string event: %#v", i, row.Values)
		}
		var got metering.Event
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatal(err)
		}
		if got != events[i] {
			t.Fatalf("row %d mismatch: got %+v want %+v", i, got, events[i])
		}
	}
}

// TestLoadRejectsStatusCodesOutsideDatabaseRange pins validation against the
// billing_event_status_code_ck CHECK (NULL or 100..599). An out-of-range code
// would replay into Valkey successfully and only then poison the drainer on
// INSERT, stalling the queue on a row that can never be accepted. Reject it
// during dry-run, where it is an operator-visible error instead.
func TestLoadRejectsStatusCodesOutsideDatabaseRange(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		// Zero marshals as the omitted/NULL case, which the CHECK admits.
		{"zero is the NULL case", 0, false},
		{"lower bound", 100, false},
		{"upper bound", 599, false},
		{"just below lower bound", 99, true},
		{"just above upper bound", 600, true},
		{"negative", -1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := testEvent("req-status")
			ev.StatusCode = tc.status
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "evidence.jsonl")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			evidence, err := Load(path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("status_code %d was accepted; the database CHECK would reject it", tc.status)
				}
				if !strings.Contains(err.Error(), "status_code") {
					t.Fatalf("error %q does not name status_code", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("status_code %d must be accepted: %v", tc.status, err)
			}
			if len(evidence.Events) != 1 || evidence.Events[0].StatusCode != tc.status {
				t.Fatalf("events = %+v, want one event with status %d", evidence.Events, tc.status)
			}
		})
	}
}

// TestDigestBindsCompleteEventSet proves the digest is a function of the whole
// event content, not merely the request-id set, and is order-independent.
func TestDigestBindsCompleteEventSet(t *testing.T) {
	base := Evidence{Events: []metering.Event{testEvent("req-a"), testEvent("req-b")}}
	reordered := Evidence{Events: []metering.Event{testEvent("req-b"), testEvent("req-a")}}
	if base.Digest() != reordered.Digest() {
		t.Fatal("digest must not depend on event ordering")
	}
	if base.Digest() == "" {
		t.Fatal("digest must be computable for a valid event set")
	}

	// Same ids and cardinality, different payload -> different digest.
	tamperedTokens := Evidence{Events: []metering.Event{testEvent("req-a"), testEvent("req-b")}}
	tamperedTokens.Events[0].CompletionTokens = 4242
	if tamperedTokens.Digest() == base.Digest() {
		t.Fatal("altered token counts must change the digest")
	}
	tamperedOrg := Evidence{Events: []metering.Event{testEvent("req-a"), testEvent("req-b")}}
	tamperedOrg.Events[1].OrgID = "org-attacker"
	if tamperedOrg.Digest() == base.Digest() {
		t.Fatal("altered org attribution must change the digest")
	}

	// Same cardinality, different ids -> different digest.
	swapped := Evidence{Events: []metering.Event{testEvent("req-a"), testEvent("req-c")}}
	if swapped.Digest() == base.Digest() {
		t.Fatal("a different request-id set must change the digest")
	}

	// Different cardinality -> different digest.
	shorter := Evidence{Events: []metering.Event{testEvent("req-a")}}
	if shorter.Digest() == base.Digest() {
		t.Fatal("a different event count must change the digest")
	}
}

// TestLoadRefusesEvidenceMissingUsageFound: usage_found decides whether an attempt
// can ever become money, and Go decodes a missing bool to false. A record predating
// that field would therefore replay as "the engine supplied no usage", be excluded
// from money permanently, and report nothing — silent revenue loss with no error to
// notice. Refuse it so an operator can assert the correct value and re-import.
func TestLoadRefusesEvidenceMissingUsageFound(t *testing.T) {
	// A pre-hardening record: valid in every other respect, but no usage_found key.
	legacy := `{"request_id":"req-legacy","auth_id":"auth-1","prompt_tokens":10,` +
		`"completion_tokens":5,"timestamp_unix_ms":1750000000000}`
	path := filepath.Join(t.TempDir(), "legacy.jsonl")
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("a record with no usage_found must be refused, not replayed as unmetered")
	}
	if !strings.Contains(err.Error(), "usage_found") {
		t.Fatalf("error %q should name the missing field so an operator can fix it", err)
	}
}

// TestLoadAcceptsExplicitUsageFoundBothWays: the refusal is about ABSENCE, not
// about the value — an explicit false is legitimate evidence (an abort, a failed
// attempt) and must still replay.
func TestLoadAcceptsExplicitUsageFoundBothWays(t *testing.T) {
	for _, usageFound := range []bool{true, false} {
		ev := testEvent("req-explicit")
		ev.UsageFound = usageFound
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "usage_found") {
			t.Fatalf("the emitter must always write usage_found; got %s", data)
		}
		path := filepath.Join(t.TempDir(), "evidence.jsonl")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		evidence, err := Load(path)
		if err != nil {
			t.Fatalf("usage_found=%v must be accepted: %v", usageFound, err)
		}
		if len(evidence.Events) != 1 || evidence.Events[0].UsageFound != usageFound {
			t.Fatalf("events = %+v, want one event with usage_found=%v", evidence.Events, usageFound)
		}
	}
}
