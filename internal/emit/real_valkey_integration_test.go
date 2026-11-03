//go:build admissionintegration

package emit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// TestRealValkeyProbeAtFullMemory: the probe reads the stream and memory on real
// Valkey, including past maxmemory, when the metric matters most.
func TestRealValkeyProbeAtFullMemory(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := DefaultConfig()
	cfg.StreamName = fmt.Sprintf("test:emit-probe:%d", time.Now().UnixNano())
	cfg.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, logging.New(logging.ERROR), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = rdb.ConfigSet(ctx, "maxmemory", "0").Err()
		_ = rdb.Del(ctx, cfg.StreamName).Err()
		_ = em.Close(ctx)
	})
	for range 5 {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": "x"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// CheckBufferBound depends on this field being in the memory section.
	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		t.Fatal(err)
	}
	if policy, ok := infoString(info, "maxmemory_policy"); !ok || policy == "" {
		t.Fatalf("INFO memory has no maxmemory_policy: %q", info)
	}
	p, err := em.Probe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p.StreamEntries != 5 || p.Undelivered != 5 {
		t.Fatalf("no group: %+v, want 5 entries, 5 undelivered", p)
	}
	if err := rdb.XGroupCreate(ctx, cfg.StreamName, "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{cfg.StreamName, ">"}, Count: 2, Block: -1}).Result(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	p, err = em.Probe(ctx)
	if err != nil {
		t.Fatalf("Probe past maxmemory: %v", err)
	}
	if p.StreamEntries != 5 || p.Undelivered != 5 || p.MaxMemory != 1 || p.UsedMemory <= 1 {
		t.Fatalf("past maxmemory: %+v, want 5 entries, 5 undelivered (2 pending + 3 unread), maxmemory 1", p)
	}
}

// TestRealValkeyProbeMissingStream: a real Valkey restart empties the stream key,
// and that is a healthy, empty Valkey, not a failed probe.
func TestRealValkeyProbeMissingStream(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := DefaultConfig()
	cfg.StreamName = fmt.Sprintf("test:emit-probe-missing:%d", time.Now().UnixNano())
	cfg.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, logging.New(logging.ERROR), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(ctx) })
	p, err := em.Probe(ctx)
	if err != nil {
		t.Fatalf("Probe of a missing stream: %v", err)
	}
	if p.StreamEntries != 0 || p.Undelivered != 0 {
		t.Fatalf("missing stream: %+v, want 0 entries, 0 undelivered", p)
	}
}

// TestRealValkeyRefusedXAddGoesToWALAndShipsOnce: a real OOM reply is what replaced
// MAXLEN as the stream's bound. A refused event must wait in the WAL, reach no log
// floor, and ship exactly once when Valkey has room again.
func TestRealValkeyRefusedXAddGoesToWALAndShipsOnce(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := DefaultConfig()
	cfg.StreamName = fmt.Sprintf("test:emit-refused:%d", time.Now().UnixNano())
	cfg.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.ShipInterval = 50 * time.Millisecond
	em, err := New(cfg, logging.New(logging.ERROR), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = rdb.ConfigSet(ctx, "maxmemory", "0").Err()
		_ = em.Close(ctx)
		_ = rdb.Del(ctx, cfg.StreamName).Err()
	})
	if err := rdb.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}

	const n = 20
	for i := range n {
		em.Emit(ctx, metering.Event{RequestID: fmt.Sprintf("refused-%d", i)})
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor("the WAL to hold every refused event", func() bool {
		pending, ok := em.WALPending()
		return ok && pending == n
	})
	if got := em.FloorEvents(); got != 0 {
		t.Fatalf("FloorEvents = %d, want 0: a refused XADD belongs in the WAL", got)
	}
	if got, err := rdb.XLen(ctx, cfg.StreamName).Result(); err != nil || got != 0 {
		t.Fatalf("XLEN while refused = %d, %v, want 0", got, err)
	}

	if err := rdb.ConfigSet(ctx, "maxmemory", "0").Err(); err != nil {
		t.Fatal(err)
	}
	waitFor("the WAL to ship", func() bool {
		pending, ok := em.WALPending()
		return ok && pending == 0
	})
	entries, err := rdb.XRange(ctx, cfg.StreamName, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != n {
		t.Fatalf("stream holds %d entries, want %d", len(entries), n)
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		ev, err := metering.UnmarshalEvent([]byte(entry.Values["event"].(string)))
		if err != nil {
			t.Fatal(err)
		}
		if seen[ev.RequestID] {
			t.Fatalf("%s is in the stream twice", ev.RequestID)
		}
		seen[ev.RequestID] = true
	}
	if got := em.FloorEvents(); got != 0 {
		t.Fatalf("FloorEvents after shipping = %d, want 0", got)
	}
}

// TestRealValkeyProbeUnknownLagReadsAsWholeStream: deleting an entry above a group's
// position makes real Valkey report nil lag, which must not read as caught up.
func TestRealValkeyProbeUnknownLagReadsAsWholeStream(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := DefaultConfig()
	cfg.StreamName = fmt.Sprintf("test:emit-probe-lag:%d", time.Now().UnixNano())
	cfg.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, logging.New(logging.ERROR), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = rdb.Del(ctx, cfg.StreamName).Err()
		_ = em.Close(ctx)
	})
	var ids []string
	for range 6 {
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": "x"}}).Result()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rdb.XGroupCreate(ctx, cfg.StreamName, "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{cfg.StreamName, ">"}, Count: 2, Block: -1}).Result(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XDel(ctx, cfg.StreamName, ids[3]).Err(); err != nil {
		t.Fatal(err)
	}
	groups, err := rdb.XInfoGroups(ctx, cfg.StreamName).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Lag != -1 {
		t.Fatalf("setup: groups = %+v, want one group with unknown lag", groups)
	}
	p, err := em.Probe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p.StreamEntries != 5 || p.Undelivered != 5 {
		t.Fatalf("%+v, want Undelivered equal to XLEN (5)", p)
	}
}
