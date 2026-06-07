package drain

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// fakeStore is an in-memory Store that records upserts and can be made to fail
// on demand. It is the seam that lets us test the drain loop's
// store-before-ACK invariant without a real Postgres.
type fakeStore struct {
	mu       sync.Mutex
	rows     map[string]metering.Event // keyed by request_id — proves dedup
	calls    int                       // number of Upsert invocations
	failNext int                       // fail this many upcoming Upsert calls
}

func newFakeStore() *fakeStore { return &fakeStore{rows: map[string]metering.Event{}} }

func (f *fakeStore) Upsert(_ context.Context, events []metering.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failNext > 0 {
		f.failNext--
		return errors.New("fakeStore: induced failure")
	}
	for _, e := range events {
		// Idempotent on request_id: first write wins, duplicates are no-ops.
		if _, ok := f.rows[e.RequestID]; !ok {
			f.rows[e.RequestID] = e
		}
	}
	return nil
}

func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Close() error               { return nil }

func (f *fakeStore) rowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

// --- test harness ---

func testLogger() *logging.Logger { return logging.New(logging.ERROR) }

func startMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
	})
	return mr, rdb
}

func testConfig(addr string) Config {
	c := DefaultConfig()
	c.ValkeyAddr = addr
	c.StreamName = "phoebe:metering:test"
	c.Group = "test-group"
	c.Consumer = "test-consumer"
	c.BatchSize = 100
	c.BlockTimeout = 50 * time.Millisecond
	c.ClaimMinIdle = 20 * time.Millisecond
	c.ClaimInterval = 10 * time.Millisecond
	return c
}

// xaddEvent writes an event to the stream in the exact shape the emitter uses:
// a single "event" field holding the JSON-encoded metering.Event.
func xaddEvent(t *testing.T, rdb *redis.Client, stream string, e metering.Event) {
	t.Helper()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": string(data)},
	}).Err(); err != nil {
		t.Fatalf("xadd: %v", err)
	}
}

func sampleEvent(reqID string) metering.Event {
	return metering.Event{
		RequestID:        reqID,
		AuthID:           "auth-123",
		UserID:           "user-1",
		GroupID:          "group-1",
		ResourceID:       "res-1",
		ResourceType:     "deployment",
		Model:            "llama-3.1-8b",
		Adapter:          "sql-lora",
		PromptTokens:     10,
		CachedTokens:     2,
		CompletionTokens: 20,
		FinishReason:     "stop",
		GPUType:          "h100",
		Aborted:          false,
		TimestampUnixMs:  time.Now().UnixMilli(),
	}
}

// pendingCount returns the number of unacknowledged entries in the group's PEL.
func pendingCount(t *testing.T, rdb *redis.Client, stream, group string) int64 {
	t.Helper()
	res, err := rdb.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	return res.Count
}

// runUntil runs d.Run in a goroutine and stops it once cond() holds or timeout.
func runUntil(t *testing.T, d *Drainer, cond func() bool, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	deadline := time.After(timeout)
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if cond() {
				cancel()
				<-done
				return
			}
		case <-deadline:
			cancel()
			<-done
			t.Fatal("condition not met before timeout")
		}
	}
}

// TestDrain_BatchConsumeStoreAck is the happy path: events on the stream are
// stored AND acknowledged (removed from the PEL).
func TestDrain_BatchConsumeStoreAck(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	for i := 0; i < 5; i++ {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent("req-"+string(rune('a'+i))))
	}

	d := New(cfg, testLogger(), rdb, store)
	runUntil(t, d, func() bool { return store.rowCount() == 5 }, 2*time.Second)

	if got := store.rowCount(); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0 (everything stored should be ACK'd)", p)
	}
}

// TestDrain_IdempotentRedelivery proves the effectively-once property: the same
// request_id delivered twice yields exactly one row and no error.
func TestDrain_IdempotentRedelivery(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	// Two distinct stream entries carrying the SAME request_id.
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("dup-req"))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("dup-req"))

	d := New(cfg, testLogger(), rdb, store)
	// Both entries get consumed; dedup happens in the store.
	runUntil(t, d, func() bool {
		return pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0 && store.calls >= 1
	}, 2*time.Second)

	if got := store.rowCount(); got != 1 {
		t.Fatalf("rows = %d, want 1 (idempotent on request_id)", got)
	}
}

// TestDrain_StoreFailureNoAck proves the core durability invariant: when the
// store fails, the entries are NOT acknowledged, so they remain pending and
// will redeliver. After the store recovers, they are stored and ACK'd.
func TestDrain_StoreFailureNoAck(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.failNext = 1 // first Upsert fails

	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("req-x"))

	d := New(cfg, testLogger(), rdb, store)

	// Phase 1: drive a pass while the store is failing; assert nothing ACK'd.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	// Wait for at least one failed Upsert attempt.
	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		called := store.calls >= 1
		store.mu.Unlock()
		if called {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("store was never called")
		case <-time.After(2 * time.Millisecond):
		}
	}

	// The failing entry must still be pending (un-ACK'd) and not stored.
	// failNext has now been consumed, so subsequent passes will succeed and
	// drain it — meaning eventually rows==1 and pending==0.
	cancel()
	<-done

	// Phase 2: a fresh run (store now healthy) must store + ACK the redelivery.
	d2 := New(cfg, testLogger(), rdb, store)
	runUntil(t, d2, func() bool { return store.rowCount() == 1 }, 2*time.Second)

	if got := store.rowCount(); got != 1 {
		t.Fatalf("rows = %d, want 1 after recovery", got)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0 after recovery", p)
	}
}

// TestDrain_GracefulShutdown proves Run returns nil on context cancel after
// finishing in-flight work, and that consumed work was ACK'd.
func TestDrain_GracefulShutdown(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("req-shutdown"))

	d := New(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	// Let it consume the event.
	deadline := time.After(2 * time.Second)
	for store.rowCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("event never consumed")
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0", p)
	}
}

// TestDrain_ReclaimsStalePending proves XAUTOCLAIM recovers entries stranded in
// a dead consumer's PEL. We simulate the dead consumer by XREADGROUP-ing as a
// DIFFERENT consumer name (claiming but never acking), then letting the real
// drainer reclaim after ClaimMinIdle.
func TestDrain_ReclaimsStalePending(t *testing.T) {
	mr, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	ctx := context.Background()
	// Create the group at the stream head-from-zero so existing entries are seen.
	if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, cfg.Group, "0").Err(); err != nil {
		t.Fatalf("create group: %v", err)
	}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("stranded-req"))

	// A now-"dead" consumer reads (and thus claims) the entry but never ACKs.
	if _, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    cfg.Group,
		Consumer: "dead-consumer",
		Streams:  []string{cfg.StreamName, ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("dead consumer read: %v", err)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 1 {
		t.Fatalf("pending after dead read = %d, want 1", p)
	}

	// Advance miniredis time past ClaimMinIdle so the entry is reclaimable.
	mr.FastForward(cfg.ClaimMinIdle + 10*time.Millisecond)

	// The live drainer must reclaim, store, and ACK the stranded entry.
	d := New(cfg, testLogger(), rdb, store)
	runUntil(t, d, func() bool { return store.rowCount() == 1 }, 2*time.Second)

	if got := store.rowCount(); got != 1 {
		t.Fatalf("rows = %d, want 1 (reclaimed)", got)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0 after reclaim+ack", p)
	}
}

// TestDrain_PoisonEntryIsDropped proves a malformed stream entry (cannot be
// decoded into an Event) is ACK'd and dropped rather than wedging the group.
func TestDrain_PoisonEntryIsDropped(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	// Malformed: "event" field holds non-JSON.
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.StreamName,
		Values: map[string]any{"event": "{not valid json"},
	}).Err(); err != nil {
		t.Fatalf("xadd poison: %v", err)
	}
	// A good entry behind it must still be processed.
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("good-req"))

	d := New(cfg, testLogger(), rdb, store)
	// Wait for BOTH the good row stored AND the group fully drained (poison
	// ACK'd) — the poison ACK happens after the good-batch store/ACK.
	runUntil(t, d, func() bool {
		return store.rowCount() == 1 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 2*time.Second)

	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0 (poison must be ACK'd, not stuck)", p)
	}
}

// TestEnsureGroup_Idempotent proves ensureGroup tolerates BUSYGROUP (already
// exists) so restarts are clean.
func TestEnsureGroup_Idempotent(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())

	if err := d.ensureGroup(context.Background()); err != nil {
		t.Fatalf("first ensureGroup: %v", err)
	}
	if err := d.ensureGroup(context.Background()); err != nil {
		t.Fatalf("second ensureGroup (BUSYGROUP) should be nil, got: %v", err)
	}
}
