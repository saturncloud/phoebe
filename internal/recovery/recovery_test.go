package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
		ServingMode:     "dedicated",
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
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(events[i])
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Fatalf("row %d mismatch: got %s want %s", i, gotJSON, wantJSON)
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

func writeEvidence(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "evidence.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestDecodeEvent_PreCutoverEvidenceReplaysAsDedicated: a record predating the
// 2026-09-29 serving-mode cutover has no serving_mode key (the field was
// omitempty, so the pre-cutover producer never wrote it for dedicated traffic).
// All pre-cutover traffic was dedicated (Hugo, 2026-09-30), so it replays as
// "dedicated" — the same rule migration 0007 applied to billing_event — instead of
// being stored NULL and withheld by the rater.
func TestDecodeEvent_PreCutoverEvidenceReplaysAsDedicated(t *testing.T) {
	rec := `{"request_id":"req-legacy","auth_id":"auth-1","prompt_tokens":10,` +
		`"completion_tokens":5,"usage_found":true,"timestamp_unix_ms":1750000000000}`
	ev, err := decodeEvent([]byte(rec))
	if err != nil || ev.ServingMode != "dedicated" {
		t.Fatalf("decodeEvent(%s) = (%q, %v), want serving_mode dedicated", rec, ev.ServingMode, err)
	}
	evidence, err := Load(writeEvidence(t, rec))
	if err != nil || len(evidence.Events) != 1 || evidence.Events[0].ServingMode != "dedicated" {
		t.Fatalf("Load(%s) = (%+v, %v), want one dedicated event", rec, evidence, err)
	}
}

// TestDecodeEvent_ExplicitEmptyOrNullServingModeRefused: only an ABSENT key is
// pre-cutover evidence. An explicit "" or null can only come from a post-cutover
// producer bug (the pre-cutover field was omitempty), so it must not be silently
// replayed and billed as dedicated; validate() refuses it, naming the field.
func TestDecodeEvent_ExplicitEmptyOrNullServingModeRefused(t *testing.T) {
	for _, mode := range []string{`""`, `null`} {
		rec := `{"request_id":"req-empty-mode","auth_id":"auth-1","prompt_tokens":10,` +
			`"completion_tokens":5,"usage_found":true,"serving_mode":` + mode +
			`,"timestamp_unix_ms":1750000000000}`
		ev, err := decodeEvent([]byte(rec))
		if err != nil {
			t.Fatalf("decodeEvent(%s) unexpected decode error: %v", rec, err)
		}
		if ev.ServingMode != "" {
			t.Fatalf("decodeEvent(%s) serving_mode = %q, want it left empty (not defaulted)", rec, ev.ServingMode)
		}
		_, err = Load(writeEvidence(t, rec))
		if err == nil || !strings.Contains(err.Error(), "serving_mode") {
			t.Fatalf("serving_mode %s must be refused, naming the field; got %v", mode, err)
		}
	}
}

// TestDecodeEvent_PreCutoverWALEvidenceReplaysAsDedicated: the same rule applies to
// WAL evidence, not only JSONL.
func TestDecodeEvent_PreCutoverWALEvidenceReplaysAsDedicated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	log, err := tidwall.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `{"request_id":"req-legacy","auth_id":"auth-1","usage_found":true,"timestamp_unix_ms":1750000000000}`
	if err := log.Write(1, []byte(legacy)); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	evidence, err := Load(dir)
	if err != nil || len(evidence.Events) != 1 || evidence.Events[0].ServingMode != "dedicated" {
		t.Fatalf("WAL pre-cutover record = (%+v, %v), want one dedicated event", evidence, err)
	}
}

// TestDecodeEvent_InvalidServingModeRefused: a present value that is neither
// shared nor dedicated would be withheld by the rater, so it must not pass
// validation.
func TestDecodeEvent_InvalidServingModeRefused(t *testing.T) {
	for _, mode := range []string{"Dedicated", "serverless", " shared"} {
		ev := testEvent("req-bad-mode")
		ev.ServingMode = mode
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		_, err = Load(writeEvidence(t, string(data)))
		if err == nil || !strings.Contains(err.Error(), "serving_mode") {
			t.Fatalf("serving_mode %q must be refused, naming the field; got %v", mode, err)
		}
	}
}

// TestDecodeEvent_ExplicitSharedAndDedicatedKeepTheirValue: an explicit valid mode
// (shared or dedicated) replays unchanged. Only an absent key maps to dedicated;
// an explicit empty, null or otherwise invalid value is refused.
func TestDecodeEvent_ExplicitSharedAndDedicatedKeepTheirValue(t *testing.T) {
	for _, mode := range []string{"dedicated", "shared"} {
		ev := testEvent("req-" + mode)
		ev.ServingMode = mode
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := Load(writeEvidence(t, string(data)))
		if err != nil {
			t.Fatalf("serving_mode %q must be accepted: %v", mode, err)
		}
		if len(evidence.Events) != 1 || evidence.Events[0].ServingMode != mode {
			t.Fatalf("events = %+v, want one event with serving_mode=%q", evidence.Events, mode)
		}
	}
}

// TestDecodeEvent_DifferentlyCasedServingModeKeepsItsValue: encoding/json matches
// keys to struct fields case-insensitively, so "Serving_Mode":"shared" decodes as
// shared. The absent-key check must use the same matching, or it would treat the
// key as missing and overwrite the decoded value with "dedicated", billing a
// shared event at the dedicated price.
func TestDecodeEvent_DifferentlyCasedServingModeKeepsItsValue(t *testing.T) {
	for _, key := range []string{"Serving_Mode", "SERVING_MODE"} {
		rec := `{"request_id":"req-cased","auth_id":"auth-1","prompt_tokens":10,` +
			`"completion_tokens":5,"usage_found":true,"` + key + `":"shared",` +
			`"timestamp_unix_ms":1750000000000}`
		ev, err := decodeEvent([]byte(rec))
		if err != nil || ev.ServingMode != "shared" {
			t.Fatalf("decodeEvent(%s) = (%q, %v), want serving_mode shared", rec, ev.ServingMode, err)
		}
		evidence, err := Load(writeEvidence(t, rec))
		if err != nil || len(evidence.Events) != 1 || evidence.Events[0].ServingMode != "shared" {
			t.Fatalf("Load(%s) = (%+v, %v), want one shared event", rec, evidence, err)
		}
	}
}

// TestDecodeEvent_DifferentlyCasedUsageFoundCountsAsPresent: the usage_found
// presence check must match keys the way encoding/json does. A differently-cased
// key is decoded into UsageFound, so it is present and its value is kept.
func TestDecodeEvent_DifferentlyCasedUsageFoundCountsAsPresent(t *testing.T) {
	for _, key := range []string{"Usage_Found", "USAGE_FOUND"} {
		for _, value := range []bool{true, false} {
			rec := fmt.Sprintf(`{"request_id":"req-cased","auth_id":"auth-1","prompt_tokens":10,`+
				`"completion_tokens":5,%q:%t,"serving_mode":"shared","timestamp_unix_ms":1750000000000}`,
				key, value)
			ev, err := decodeEvent([]byte(rec))
			if err != nil || ev.UsageFound != value {
				t.Fatalf("decodeEvent(%s) = (usage_found=%v, %v), want usage_found=%v", rec, ev.UsageFound, err, value)
			}
		}
	}
}

// TestDigestPreCutoverAbsentModeEqualsExplicitDedicated: decodeEvent maps an
// absent serving_mode key to "dedicated" before hashing, so a pre-cutover record
// and the same record with an explicit "serving_mode":"dedicated" must produce
// the same digest within one binary.
func TestDigestPreCutoverAbsentModeEqualsExplicitDedicated(t *testing.T) {
	const prefix = `{"request_id":"req-legacy","auth_id":"auth-1","prompt_tokens":10,` +
		`"completion_tokens":5,"usage_found":true,`
	absent, err := Load(writeEvidence(t, prefix+`"timestamp_unix_ms":1750000000000}`))
	if err != nil {
		t.Fatalf("Load(absent serving_mode): %v", err)
	}
	explicit, err := Load(writeEvidence(t, prefix+`"serving_mode":"dedicated","timestamp_unix_ms":1750000000000}`))
	if err != nil {
		t.Fatalf("Load(explicit dedicated): %v", err)
	}
	if absent.Digest() == "" {
		t.Fatal("digest must be computable for a valid pre-cutover record")
	}
	if absent.Digest() != explicit.Digest() {
		t.Fatalf("absent serving_mode digest %s != explicit dedicated digest %s", absent.Digest(), explicit.Digest())
	}
}
