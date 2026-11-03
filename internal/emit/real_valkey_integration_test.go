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
