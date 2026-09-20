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
