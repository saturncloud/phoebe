//go:build admissionintegration

package drain

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/logging"
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

// usedMemory reads used_memory from INFO memory.
func usedMemory(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	info, err := rdb.Info(context.Background(), "memory").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, "used_memory:"); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no used_memory in INFO memory: %q", info)
	return 0
}

// addPadded XADDs n valid events of about 700 bytes, as large as real ones.
func addPadded(t *testing.T, rdb *redis.Client, stream string, n int, prefix string) {
	t.Helper()
	for i := range n {
		raw, err := json.Marshal(metering.Event{RequestID: fmt.Sprintf("%s-%d", prefix, i)})
		if err != nil {
			t.Fatal(err)
		}
		padded := string(raw[:len(raw)-1]) + fmt.Sprintf(`,"pad":"%0700d"}`, 0)
		if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": padded}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// xaddValid XADDs one valid event, the way the emitter does.
func xaddValid(rdb *redis.Client, stream, requestID string) error {
	raw, err := json.Marshal(metering.Event{RequestID: requestID})
	if err != nil {
		return err
	}
	return rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": string(raw)}}).Err()
}

func realValkey(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("PHOEBE_TEST_ADMISSION_VALKEY_ADDR")
	if addr == "" {
		t.Fatal("PHOEBE_TEST_ADMISSION_VALKEY_ADDR is required")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func pollUntil(deadline time.Time, cond func() bool) bool {
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// TestRealValkeyDrainerRecoversFromOOM runs the failure end to end on real Valkey:
// the stream fills past maxmemory with delivered entries and some undelivered ones,
// XADD is refused and a real emitter falls back to its WAL. A restarted drainer
// starts anyway, stores the undelivered events and trims. maxmemory stays where it
// was, so XADD works again only because the trim freed memory: the emitter's WAL
// empties, and a plain XADD succeeds. Every event must reach the store.
func TestRealValkeyDrainerRecoversFromOOM(t *testing.T) {
	ctx := context.Background()
	rdb := realValkey(t)

	cfg := testConfig(rdb.Options().Addr)
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

	baseline := usedMemory(t, rdb)
	addPadded(t, rdb, cfg.StreamName, 3000, "delivered")
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
	addPadded(t, rdb, cfg.StreamName, 100, "undelivered")

	// Halfway between empty and full: past the limit now, and below it only if the
	// trim really frees memory.
	full := usedMemory(t, rdb)
	limit := baseline + (full-baseline)/2
	if err := rdb.ConfigSet(ctx, "maxmemory", strconv.FormatInt(limit, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := xaddValid(rdb, cfg.StreamName, "refused"); !isValkeyOOM(err) {
		t.Fatalf("XADD on a full Valkey: %v, want OOM (used %d, limit %d)", err, full, limit)
	}
	if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, cfg.Group, "0").Err(); !isValkeyOOM(err) {
		t.Fatalf("XGROUP CREATE on a full Valkey: %v, want OOM", err)
	}

	emitCfg := emit.DefaultConfig()
	emitCfg.StreamName = cfg.StreamName
	emitCfg.WALPath = filepath.Join(t.TempDir(), "wal")
	emitCfg.ShipInterval = 50 * time.Millisecond
	em, err := emit.New(emitCfg, logging.New(logging.ERROR), rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(ctx) })
	for i := range 20 {
		em.Emit(ctx, metering.Event{RequestID: fmt.Sprintf("emitted-%d", i)})
	}
	if !pollUntil(time.Now().Add(2*time.Second), func() bool { return em.WALPending() > 0 }) {
		t.Fatal("the emitter's events did not reach its WAL while Valkey was full")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	restarted := New(cfg, testLogger(), rdb, store)
	done := make(chan error, 1)
	go func() { done <- restarted.Run(runCtx) }()

	deadline := time.Now().Add(20 * time.Second)
	if !pollUntil(deadline, func() bool {
		return store.distinctWithPrefix("undelivered") == 100 && store.distinctWithPrefix("emitted") == 20 && em.WALPending() == 0
	}) {
		t.Fatalf("stored %d of 100 undelivered and %d of 20 emitted events, WAL pending %d",
			store.distinctWithPrefix("undelivered"), store.distinctWithPrefix("emitted"), em.WALPending())
	}
	if !pollUntil(deadline, func() bool {
		n, _ := rdb.XLen(ctx, cfg.StreamName).Result()
		return n <= 1
	}) {
		n, err := rdb.XLen(ctx, cfg.StreamName).Result()
		t.Fatalf("stream holds %d entries after trim (%v), want at most 1", n, err)
	}
	if after := usedMemory(t, rdb); after >= limit {
		t.Fatalf("used_memory %d after the trim, want below the limit %d", after, limit)
	}
	if err := xaddValid(rdb, cfg.StreamName, "after-trim"); err != nil {
		t.Fatalf("XADD after the trim with maxmemory unchanged: %v", err)
	}
	if !pollUntil(deadline, func() bool { return store.distinctWithPrefix("after-trim") == 1 }) {
		t.Fatal("the event added after the trim never reached the store")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRealValkeyDrainerRecoversFromOOMWithNoGroup: a stream with no consumer group
// fills past maxmemory, as when Valkey restarts while the drainer is down, so XGROUP
// CREATE is refused. The drainer stores and trims without a group until it can create
// one, without changing maxmemory. Every event reaches the store, and XADD works again.
func TestRealValkeyDrainerRecoversFromOOMWithNoGroup(t *testing.T) {
	ctx := context.Background()
	rdb := realValkey(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.StreamName = fmt.Sprintf("test:drain-oom-nogroup:%d", time.Now().UnixNano())
	cfg.TrimInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		_ = rdb.ConfigSet(ctx, "maxmemory", "0").Err()
		_ = rdb.Del(ctx, cfg.StreamName).Err()
	})

	baseline := usedMemory(t, rdb)
	addPadded(t, rdb, cfg.StreamName, 3000, "nogroup")
	limit := baseline + (usedMemory(t, rdb)-baseline)/2
	if err := rdb.ConfigSet(ctx, "maxmemory", strconv.FormatInt(limit, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := xaddValid(rdb, cfg.StreamName, "refused"); !isValkeyOOM(err) {
		t.Fatalf("XADD on a full Valkey: %v, want OOM", err)
	}
	if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, cfg.Group, "0").Err(); !isValkeyOOM(err) {
		t.Fatalf("XGROUP CREATE on a full Valkey: %v, want OOM", err)
	}

	store := newFakeStore()
	d := New(cfg, testLogger(), rdb, store)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	deadline := time.Now().Add(20 * time.Second)
	if !pollUntil(deadline, func() bool { return store.distinctWithPrefix("nogroup") == 3000 }) {
		t.Fatalf("stored %d of 3000 events", store.distinctWithPrefix("nogroup"))
	}
	groups, err := rdb.XInfoGroups(ctx, cfg.StreamName).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Name != cfg.Group {
		t.Fatalf("groups = %+v, want %q", groups, cfg.Group)
	}
	if d.DrainedWithoutGroup() == 0 {
		t.Fatal("DrainedWithoutGroup() = 0, want entries removed before the group existed")
	}
	got, err := rdb.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		t.Fatal(err)
	}
	if got["maxmemory"] != strconv.FormatInt(limit, 10) {
		t.Fatalf("maxmemory = %s, want it left at %d", got["maxmemory"], limit)
	}
	if !pollUntil(deadline, func() bool {
		n, _ := rdb.XLen(ctx, cfg.StreamName).Result()
		return n <= 1
	}) {
		t.Fatal("the stream was not trimmed after the group was created")
	}
	if err := xaddValid(rdb, cfg.StreamName, "after-recovery"); err != nil {
		t.Fatalf("XADD after recovery with maxmemory unchanged: %v", err)
	}
	if !pollUntil(deadline, func() bool { return store.distinctWithPrefix("after-recovery") == 1 }) {
		t.Fatal("the event added after recovery never reached the store")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
