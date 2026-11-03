//go:build admissionintegration

package drain

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/metering"
)

func (f *fakeStore) distinctWithPrefix(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for id := range f.rows {
		if strings.HasPrefix(id, prefix) {
			n++
		}
	}
	return n
}

// TestRealValkeyDrainerRecoversFromOOM runs the failure end to end on real Valkey:
// the stream fills to maxmemory with delivered entries and some undelivered ones,
// the emitter's XADD is refused, a restarted drainer starts anyway, stores the
// undelivered events, and trims, and XADD works again. Every undelivered event
// must reach the store.
func TestRealValkeyDrainerRecoversFromOOM(t *testing.T) {
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := testConfig(addr)
	cfg.StreamName = fmt.Sprintf("test:drain-oom:%d", time.Now().UnixNano())
	cfg.TrimInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		_ = rdb.ConfigSet(ctx, "maxmemory", "0").Err()
		_ = rdb.Del(ctx, cfg.StreamName).Err()
	})

	store := newFakeStore()
	first := New(cfg, testLogger(), rdb, store)
	if err := first.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	add := func(n int, prefix string) {
		t.Helper()
		for i := range n {
			raw, err := json.Marshal(metering.Event{RequestID: fmt.Sprintf("%s-%d", prefix, i)})
			if err != nil {
				t.Fatal(err)
			}
			padded := string(raw[:len(raw)-1]) + fmt.Sprintf(`,"pad":"%0700d"}`, 0)
			if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": padded}}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	add(3000, "delivered")
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: cfg.Group, Consumer: "gone", Streams: []string{cfg.StreamName, ">"}, Count: 3000, Block: -1}).Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, 3000)
	for _, m := range res[0].Messages {
		ids = append(ids, m.ID)
	}
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, ids...).Err(); err != nil {
		t.Fatal(err)
	}
	add(100, "undelivered")

	used, err := rdb.MemoryUsage(ctx, cfg.StreamName).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": "x"}}).Err(); !isValkeyOOM(err) {
		t.Fatalf("XADD on a full Valkey: %v, want OOM (stream used %d bytes)", err, used)
	}
	if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, cfg.Group, "0").Err(); !isValkeyOOM(err) {
		t.Fatalf("XGROUP CREATE on a full Valkey: %v, want OOM", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	restarted := New(cfg, testLogger(), rdb, store)
	done := make(chan error, 1)
	go func() { done <- restarted.Run(runCtx) }()

	deadline := time.Now().Add(20 * time.Second)
	for store.distinctWithPrefix("undelivered") < 100 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := store.distinctWithPrefix("undelivered"); got != 100 {
		t.Fatalf("stored %d of 100 undelivered events", got)
	}
	for time.Now().Before(deadline) {
		if n, _ := rdb.XLen(ctx, cfg.StreamName).Result(); n <= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, err := rdb.XLen(ctx, cfg.StreamName).Result()
	if err != nil || n > 1 {
		t.Fatalf("stream holds %d entries after trim (%v), want at most 1", n, err)
	}
	if err := rdb.ConfigSet(ctx, "maxmemory", "0").Err(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
