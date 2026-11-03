package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	failIDs  map[string]bool           // poison: any batch containing one of these request_ids fails
	failAll  bool                      // outage: every Upsert fails
	pingErr  error                     // outage: Ping result

	schemaVersion uint // what SchemaVersion reports (never dirty)
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: map[string]metering.Event{}, failIDs: map[string]bool{}}
}

func (f *fakeStore) Upsert(_ context.Context, events []metering.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failNext > 0 {
		f.failNext--
		return errors.New("fakeStore: induced failure")
	}
	if f.failAll {
		return errors.New("fakeStore: store down")
	}
	// All-or-nothing like the real batch INSERT: one bad row fails the call.
	for _, e := range events {
		if f.failIDs[e.RequestID] {
			return errors.New("fakeStore: bad row " + e.RequestID)
		}
	}
	for _, e := range events {
		// Idempotent on request_id: first write wins, duplicates are no-ops.
		if _, ok := f.rows[e.RequestID]; !ok {
			f.rows[e.RequestID] = e
		}
	}
	return nil
}

func (f *fakeStore) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pingErr
}
func (f *fakeStore) Close() error { return nil }

func (f *fakeStore) SchemaVersion(context.Context) (uint, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schemaVersion, false, nil
}

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
		TopologyRevision: 3,
		Region:           "eu-north1",
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

// TestDrain_PoisonRowIsolatedFromBatch proves one persistently-bad row cannot
// wedge its batch: the batch INSERT fails, the row-at-a-time fallback stores
// every good row, and the poison row is dropped (logged + counted) and ACK'd —
// the stream fully drains instead of redelivering the batch forever.
func TestDrain_PoisonRowIsolatedFromBatch(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.failIDs["poison-row"] = true

	good := []string{"good-1", "good-2", "good-3", "good-4"}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent(good[0]))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent(good[1]))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("poison-row"))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent(good[2]))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent(good[3]))

	d := New(cfg, testLogger(), rdb, store)
	runUntil(t, d, func() bool {
		return store.rowCount() == 4 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 2*time.Second)

	store.mu.Lock()
	for _, id := range good {
		if _, ok := store.rows[id]; !ok {
			t.Errorf("good row %q not stored — poison starved its batch", id)
		}
	}
	_, poisonStored := store.rows["poison-row"]
	store.mu.Unlock()
	if poisonStored {
		t.Error("poison row was stored, want dropped")
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0 (poison must be ACK'd, never redelivered forever)", p)
	}
	if got := d.Poisoned(); got != 1 {
		t.Fatalf("Poisoned() = %d, want 1 (loss must be counted)", got)
	}
}

// TestDrain_AllGoodBatchFastPathSingleUpsert proves the poison fallback does
// not change the common case: an all-good batch is stored with exactly ONE
// batch Upsert call — no row-at-a-time statements.
func TestDrain_AllGoodBatchFastPathSingleUpsert(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()

	for i := 0; i < 5; i++ {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent("fast-"+string(rune('a'+i))))
	}

	d := New(cfg, testLogger(), rdb, store)
	runUntil(t, d, func() bool {
		return store.rowCount() == 5 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 2*time.Second)

	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Upsert called %d times for one all-good batch, want 1 (fast path)", calls)
	}
	if got := d.Poisoned(); got != 0 {
		t.Fatalf("Poisoned() = %d, want 0", got)
	}
}

// TestDrain_StoreOutageDuringFallbackDoesNotAck guards the fail-closed side of
// poison isolation: when the store is DOWN, the batch failure and every
// individual failure are an OUTAGE, not poison — nothing may be ACK'd (that
// would silently drop a whole batch of billing data). The entries stay pending
// and drain normally after the store recovers.
func TestDrain_StoreOutageDuringFallbackDoesNotAck(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.failAll = true
	store.pingErr = errors.New("store down")

	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("outage-1"))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("outage-2"))

	// Phase 1: run against the dead store until the fallback has been tried
	// (batch call + at least one individual call), then stop.
	d := New(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		tried := store.calls >= 2
		store.mu.Unlock()
		if tried {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("fallback was never attempted")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if got := store.rowCount(); got != 0 {
		t.Fatalf("rows = %d, want 0 during outage", got)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 2 {
		t.Fatalf("pending = %d, want 2 (outage must NOT be misread as poison and ACK'd)", p)
	}
	if got := d.Poisoned(); got != 0 {
		t.Fatalf("Poisoned() = %d, want 0 — outage rows are not poison", got)
	}

	// Phase 2: store recovers; the un-ACK'd entries must redeliver and land.
	store.mu.Lock()
	store.failAll = false
	store.pingErr = nil
	store.mu.Unlock()

	d2 := New(cfg, testLogger(), rdb, store)
	runUntil(t, d2, func() bool {
		return store.rowCount() == 2 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 2*time.Second)
}

// TestDrain_PoisonAckedDuringStoreOutage proves the poison-ACK is independent of
// store health: a malformed (decode-failed) entry is ACK'd and DROPPED even when
// the store is DOWN, while the good entries batched with it stay pending and
// redeliver after recovery. Previously the poison-ACK ran only AFTER the store
// work, so a store-outage early-return skipped it and the poison redelivered
// every pass until an outage-free run. A malformed entry can never be stored
// regardless of store health, so dropping it is always correct.
func TestDrain_PoisonAckedDuringStoreOutage(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.failAll = true // store is DOWN
	store.pingErr = errors.New("store down")

	// A malformed entry (cannot decode) plus a good entry — same batch.
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.StreamName,
		Values: map[string]any{"event": "{not valid json"},
	}).Err(); err != nil {
		t.Fatalf("xadd poison: %v", err)
	}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("good-during-outage"))

	// Phase 1: run against the dead store until the store has been hit (the
	// fallback ran, proving we got past decode), then stop.
	d := New(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		tried := store.calls >= 1
		store.mu.Unlock()
		if tried {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("store was never hit (decode/poison path may not have run)")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// The poison entry must be ACK'd (dropped) DESPITE the outage; the good
	// entry must remain pending (un-ACK'd, will redeliver). So exactly ONE entry
	// stays pending, not two.
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 1 {
		t.Fatalf("pending = %d, want 1 (poison ACK'd during outage, good entry still pending)", p)
	}
	if got := store.rowCount(); got != 0 {
		t.Fatalf("rows = %d, want 0 during outage", got)
	}

	// Phase 2: store recovers; the un-ACK'd GOOD entry redelivers and lands. The
	// poison must NOT reappear (it was already dropped).
	store.mu.Lock()
	store.failAll = false
	store.pingErr = nil
	store.mu.Unlock()

	d2 := New(cfg, testLogger(), rdb, store)
	runUntil(t, d2, func() bool {
		return store.rowCount() == 1 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 2*time.Second)

	store.mu.Lock()
	_, good := store.rows["good-during-outage"]
	store.mu.Unlock()
	if !good {
		t.Fatal("good entry never stored after recovery")
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

// TestDecodeEvent_OnlyAbsentServingModeStoredAsDedicated pins the drain
// queue's serving-mode rule end to end through eventArgs: an event without a
// serving_mode key (metered by a pre-cutover pod) is stored as 'dedicated', an
// explicit "" (a post-cutover producer bug) is stored as the empty string so
// the rater withholds it, and "shared" and "dedicated" are stored as sent.
func TestDecodeEvent_OnlyAbsentServingModeStoredAsDedicated(t *testing.T) {
	const servingModeIdx = 11
	cases := []struct {
		name string
		json string
		want any
	}{
		{"absent key", `{"request_id":"r","model":"m"}`, "dedicated"},
		{"explicit empty", `{"request_id":"r","model":"m","serving_mode":""}`, ""},
		{"explicit null", `{"request_id":"r","model":"m","serving_mode":null}`, ""},
		{"shared", `{"request_id":"r","model":"m","serving_mode":"shared"}`, "shared"},
		{"dedicated", `{"request_id":"r","model":"m","serving_mode":"dedicated"}`, "dedicated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := decodeEvent(redis.XMessage{ID: "1-0", Values: map[string]interface{}{"event": tc.json}})
			if err != nil {
				t.Fatalf("decodeEvent: %v", err)
			}
			if got := eventArgs(ev)[servingModeIdx]; got != tc.want {
				t.Fatalf("stored serving_mode = %v, want %v", got, tc.want)
			}
		})
	}
}

// oomHook refuses the write commands Valkey refuses past maxmemory. XGROUP CREATE
// is refused even when the group exists, which is what caused the crash loop.
type oomHook struct{}

func (oomHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (oomHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (oomHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if name := cmd.Name(); name == "xgroup" || name == "xadd" {
			err := errors.New("OOM command not allowed when used memory > 'maxmemory'.") //nolint:revive // verbatim
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func addEntries(t *testing.T, rdb *redis.Client, stream string, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := range n {
		id, err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": fmt.Sprint(i)}}).Result()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func readEntries(t *testing.T, rdb *redis.Client, cfg Config, group string, n int64) []string {
	t.Helper()
	res, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: group, Consumer: "c", Streams: []string{cfg.StreamName, ">"}, Count: n, Block: -1,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, st := range res {
		for _, m := range st.Messages {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

func remainingEntries(t *testing.T, rdb *redis.Client, stream string) []string {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	return ids
}

func assertEntries(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("stream holds %v, want %v", got, want)
	}
}

// TestTrimDelivered_KeepsUndeliveredAndPending is the invariant: only entries
// delivered AND acknowledged go. The oldest pending entry and everything newer
// stay, including entries never read, and an acknowledged entry newer than a
// pending one stays too.
func TestTrimDelivered_KeepsUndeliveredAndPending(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	ids := addEntries(t, rdb, cfg.StreamName, 6)
	read := readEntries(t, rdb, cfg, cfg.Group, 4) // ids[4] and ids[5] stay unread
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, read[0], read[1], read[3]).Err(); err != nil {
		t.Fatal(err)
	} // read[2] stays pending

	d.trimDelivered(ctx)

	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids[2:])
}

// TestTrimDelivered_AllAcknowledgedKeepsLastDeliveredAndUnread: with nothing
// pending, the stream shrinks to the last delivered entry, which MINID keeps, plus
// anything unread. It does not empty the stream.
func TestTrimDelivered_AllAcknowledgedKeepsLastDeliveredAndUnread(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	ids := addEntries(t, rdb, cfg.StreamName, 5)
	read := readEntries(t, rdb, cfg, cfg.Group, 3)
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, read...).Err(); err != nil {
		t.Fatal(err)
	}

	d.trimDelivered(ctx)

	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids[2:])
}

// TestTrimDelivered_NothingDeliveredTrimsNothing: a new group has read nothing,
// so the whole backlog is undelivered.
func TestTrimDelivered_NothingDeliveredTrimsNothing(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	ids := addEntries(t, rdb, cfg.StreamName, 3)

	d.trimDelivered(ctx)

	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids)
}

// TestTrimDelivered_NoGroupTrimsNothing: with no consumer group nothing is known
// to be delivered.
func TestTrimDelivered_NoGroupTrimsNothing(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	ids := addEntries(t, rdb, cfg.StreamName, 3)

	d.trimDelivered(context.Background())

	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids)
}

// TestTrimDelivered_SlowestGroupSetsTheBoundary: a second group behind the
// drainer's keeps its entries.
func TestTrimDelivered_SlowestGroupSetsTheBoundary(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XGroupCreate(ctx, cfg.StreamName, "other", "0").Err(); err != nil {
		t.Fatal(err)
	}
	ids := addEntries(t, rdb, cfg.StreamName, 5)
	read := readEntries(t, rdb, cfg, cfg.Group, 4)
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, read...).Err(); err != nil {
		t.Fatal(err)
	}
	other := readEntries(t, rdb, cfg, "other", 1)
	if err := rdb.XAck(ctx, cfg.StreamName, "other", other...).Err(); err != nil {
		t.Fatal(err)
	}

	d.trimDelivered(ctx)

	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids)
}

func TestOlderStreamID(t *testing.T) {
	for _, c := range []struct{ a, b, want string }{
		{"", "5-0", "5-0"},
		{"5-0", "6-0", "5-0"},
		{"6-0", "5-0", "5-0"},
		{"5-2", "5-10", "5-2"}, // numeric, not lexical
		{"10-0", "9-0", "9-0"}, // numeric, not lexical
		{"5-1", "5-1", "5-1"},
	} {
		got, err := olderStreamID(c.a, c.b)
		if err != nil || got != c.want {
			t.Errorf("olderStreamID(%q, %q) = %q, %v; want %q", c.a, c.b, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "5", "x-1", "1-x"} {
		if _, err := olderStreamID("1-0", bad); err == nil {
			t.Errorf("olderStreamID accepted %q", bad)
		}
	}
}

// TestEnsureGroup_ExistingGroupStartsWhenValkeyIsFull is the crash loop: past
// maxmemory XGROUP CREATE is refused even for a group that exists.
func TestEnsureGroup_ExistingGroupStartsWhenValkeyIsFull(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	rdb.AddHook(oomHook{})
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatalf("ensureGroup with an existing group on a full Valkey: %v", err)
	}
}

// switchableOOMHook is oomHook with a switch, because go-redis hooks cannot be
// removed, and with counts of the commands the tests below reason about.
type switchableOOMHook struct {
	on          *atomic.Bool
	xgroupCalls *atomic.Int64
	xreadCalls  *atomic.Int64
}

func newSwitchableOOMHook() switchableOOMHook {
	h := switchableOOMHook{on: &atomic.Bool{}, xgroupCalls: &atomic.Int64{}, xreadCalls: &atomic.Int64{}}
	h.on.Store(true)
	return h
}

func (h switchableOOMHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h switchableOOMHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h switchableOOMHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if name == "xgroup" {
			h.xgroupCalls.Add(1)
		}
		if name == "xreadgroup" {
			h.xreadCalls.Add(1)
		}
		if h.on.Load() && (name == "xgroup" || name == "xadd") {
			err := errors.New("OOM command not allowed when used memory > 'maxmemory'.") //nolint:revive // verbatim
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// funcHook adapts a function to a redis.Hook.
type funcHook func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error

func (h funcHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h funcHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h funcHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { return h(ctx, cmd, next) }
}

// countCommand adds a hook counting one command.
func countCommand(rdb *redis.Client, name string) *atomic.Int64 {
	var n atomic.Int64
	rdb.AddHook(funcHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if cmd.Name() == name {
			n.Add(1)
		}
		return next(ctx, cmd)
	}))
	return &n
}

func groupNames(t *testing.T, rdb *redis.Client, stream string) []string {
	t.Helper()
	groups, err := rdb.XInfoGroups(context.Background(), stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return nil
		}
		t.Fatalf("xinfo groups: %v", err)
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}
	return names
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRun_MissingGroupOnFullValkeyWaitsInsteadOfExiting: with no group to read,
// the drainer stays in the wait, retrying XGROUP CREATE and reading nothing through
// the group. Once the refusal lifts it creates the group and consumes.
func TestRun_MissingGroupOnFullValkeyWaitsInsteadOfExiting(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("before"))
	hook := newSwitchableOOMHook()
	rdb.AddHook(hook)
	d := New(cfg, testLogger(), rdb, store)
	d.groupRetry = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	waitFor(t, "XGROUP CREATE to be retried", 5*time.Second, func() bool { return hook.xgroupCalls.Load() >= 2 })
	if names := groupNames(t, rdb, cfg.StreamName); len(names) != 0 {
		t.Fatalf("groups = %v while XGROUP CREATE is refused, want none", names)
	}
	if n := hook.xreadCalls.Load(); n != 0 {
		t.Fatalf("XREADGROUP ran %d times with no group, want the drainer still waiting", n)
	}

	hook.on.Store(false)
	waitFor(t, "the group to be created", 5*time.Second, func() bool { return len(groupNames(t, rdb, cfg.StreamName)) == 1 })
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("after"))
	waitFor(t, "both events to be stored", 5*time.Second, func() bool { return store.rowCount() == 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run on a full Valkey with no group: %v", err)
	}
}

// TestRun_MissingGroupOnFullValkeyRetriesUntilShutdown: Run returns only because
// the context ended, and returns nil rather than failing.
func TestRun_MissingGroupOnFullValkeyRetriesUntilShutdown(t *testing.T) {
	_, rdb := startMiniredis(t)
	hook := newSwitchableOOMHook()
	rdb.AddHook(hook)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	d.groupRetry = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); err != nil {
		t.Fatalf("Run on a full Valkey with no group: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("Run returned before its context ended, want it to keep retrying")
	}
	if n := hook.xgroupCalls.Load(); n < 2 {
		t.Fatalf("XGROUP CREATE ran %d times, want it retried", n)
	}
	if n := hook.xreadCalls.Load(); n != 0 {
		t.Fatalf("XREADGROUP ran %d times with no group, want none", n)
	}
}

// TestRun_GroupLostWhileRunningOnFullValkeyIsRecreated: a running drainer gets
// NOGROUP (Valkey restarted) and XGROUP CREATE is refused. It keeps running, and
// recreates the group and consumes once the refusal lifts.
func TestRun_GroupLostWhileRunningOnFullValkeyIsRecreated(t *testing.T) {
	mr, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	hook := newSwitchableOOMHook()
	hook.on.Store(false)
	rdb.AddHook(hook)
	d := New(cfg, testLogger(), rdb, store)
	d.groupRetry = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	waitFor(t, "the group to exist", 5*time.Second, func() bool { return len(groupNames(t, rdb, cfg.StreamName)) == 1 })

	hook.on.Store(true)
	before := hook.xgroupCalls.Load()
	mr.Del(cfg.StreamName)
	waitFor(t, "XGROUP CREATE to be retried", 5*time.Second, func() bool { return hook.xgroupCalls.Load() >= before+2 })
	if names := groupNames(t, rdb, cfg.StreamName); len(names) != 0 {
		t.Fatalf("groups = %v while XGROUP CREATE is refused, want none", names)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the group was missing, want it still running", err)
	default:
	}

	hook.on.Store(false)
	waitFor(t, "the group to be recreated", 5*time.Second, func() bool { return len(groupNames(t, rdb, cfg.StreamName)) == 1 })
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("after-recreate"))
	waitFor(t, "the new event to be stored", 5*time.Second, func() bool { return store.rowCount() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRun_FullValkeyWithNoGroupFreesItselfAndLosesNothing is the way out: XGROUP
// CREATE stays refused until the drainer has stored and trimmed enough, as on a real
// full Valkey. Every event reaches the store, the group ends up existing, and the
// recovery is counted. A malformed entry does not stop it.
func TestRun_FullValkeyWithNoGroupFreesItselfAndLosesNothing(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.BatchSize = 10
	store := newFakeStore()
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": "{not valid json"}}).Err(); err != nil {
		t.Fatal(err)
	}
	const total = 35
	for i := range total {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent(fmt.Sprintf("full-%d", i)))
	}
	rdb.AddHook(funcHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if cmd.Name() == "xgroup" {
			if n := rdb.XLen(ctx, cfg.StreamName).Val(); n > 10 { // over maxmemory until trimmed
				err := errors.New("OOM command not allowed when used memory > 'maxmemory'.") //nolint:revive // verbatim
				cmd.SetErr(err)
				return err
			}
		}
		return next(ctx, cmd)
	}))
	d := New(cfg, testLogger(), rdb, store)
	d.groupRetry = time.Hour // retrying must come from progress, not from the sleep

	runUntil(t, d, func() bool {
		return store.rowCount() == total && len(groupNames(t, rdb, cfg.StreamName)) == 1 &&
			pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 5*time.Second)
	if d.DrainedWithoutGroup() == 0 {
		t.Fatal("DrainedWithoutGroup() = 0, want the recovery counted")
	}
	if d.Malformed() == 0 {
		t.Fatal("Malformed() = 0, want the malformed entry counted")
	}
}

// TestDrainWithoutGroup_StoreFailureTrimsNothing: an entry is removed only once it
// is stored, whether the store is down or the schema is behind.
func TestDrainWithoutGroup_StoreFailureTrimsNothing(t *testing.T) {
	down := newFakeStore()
	down.failAll = true
	down.pingErr = errors.New("store down")
	for name, store := range map[string]Store{
		"store down":    down,
		"schema behind": &schemaGapStore{fakeStore: newFakeStore()},
	} {
		t.Run(name, func(t *testing.T) {
			_, rdb := startMiniredis(t)
			cfg := testConfig(rdb.Options().Addr)
			for i := range 5 {
				xaddEvent(t, rdb, cfg.StreamName, sampleEvent(fmt.Sprintf("e-%d", i)))
			}
			ids := remainingEntries(t, rdb, cfg.StreamName)
			d := New(cfg, testLogger(), rdb, store)
			n, err := d.drainWithoutGroup(context.Background())
			if err == nil || n != 0 {
				t.Fatalf("drainWithoutGroup = %d, %v, want 0 and an error", n, err)
			}
			assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids)
		})
	}
}

// TestDrainWithoutGroup_GroupAppearingBeforeTheTrimKeepsEntries: a group created
// while the batch is being stored owns the entries from then on, so normal trim
// rules apply. A group existing from the start means nothing is read at all.
func TestDrainWithoutGroup_GroupAppearingBeforeTheTrimKeepsEntries(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	for i := range 5 {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent(fmt.Sprintf("e-%d", i)))
	}
	ids := remainingEntries(t, rdb, cfg.StreamName)
	other := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr})
	t.Cleanup(func() { _ = other.Close() })
	created := false
	rdb.AddHook(funcHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if cmd.Name() == "xrange" && !created {
			created = true
			if gerr := other.XGroupCreate(ctx, cfg.StreamName, "appeared", "0").Err(); gerr != nil {
				t.Errorf("create group: %v", gerr)
			}
		}
		return err
	}))
	store := newFakeStore()
	d := New(cfg, testLogger(), rdb, store)

	if n, err := d.drainWithoutGroup(ctx); err != nil || n != 0 {
		t.Fatalf("drainWithoutGroup = %d, %v, want nothing trimmed", n, err)
	}
	assertEntries(t, remainingEntries(t, rdb, cfg.StreamName), ids)

	calls := store.calls
	if n, err := d.drainWithoutGroup(ctx); err != nil || n != 0 {
		t.Fatalf("drainWithoutGroup with a group = %d, %v, want nothing trimmed", n, err)
	}
	if store.calls != calls {
		t.Fatal("drainWithoutGroup stored entries although a group exists")
	}
}

// TestWaitForGroup_NonOOMErrorFailsFast: a bad address must not retry forever. The
// context outlives the call, which proves it returned by itself.
func TestWaitForGroup_NonOOMErrorFailsFast(t *testing.T) {
	mr, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	d.groupRetry = time.Millisecond
	mr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := d.waitForGroup(ctx)
	if err == nil {
		t.Fatal("waitForGroup against a dead Valkey returned nil")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
		t.Fatalf("waitForGroup = %v (ctx: %v), want it to fail without waiting for the context", err, ctx.Err())
	}
}

// seedDeadConsumerPending leaves n events in another consumer's pending list, idle
// long enough to reclaim.
func seedDeadConsumerPending(t *testing.T, rdb *redis.Client, cfg Config, n int) {
	t.Helper()
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, cfg.Group, "0").Err(); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent(fmt.Sprintf("pending-%d", i)))
	}
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: cfg.Group, Consumer: "dead-consumer", Streams: []string{cfg.StreamName, ">"}, Count: int64(n), Block: -1,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(cfg.ClaimMinIdle + 5*time.Millisecond) // miniredis measures idle time in real time
}

// TestRun_ReclaimClearsBacklogWithoutWaitingForClaimInterval: after an outage the
// pending list sets the trim boundary, so it must clear at store speed, not at one
// batch per ClaimInterval and not at one batch per XREADGROUP block. New entries
// keep being read meanwhile.
func TestRun_ReclaimClearsBacklogWithoutWaitingForClaimInterval(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.BatchSize = 10
	cfg.ClaimInterval = time.Hour
	cfg.BlockTimeout = 2 * time.Second // a blocking read per batch would take 4s or more
	const backlog = 35
	seedDeadConsumerPending(t, rdb, cfg, backlog)
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("new"))
	store := newFakeStore()
	d := New(cfg, testLogger(), rdb, store)

	runUntil(t, d, func() bool {
		return store.rowCount() == backlog+1 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 1500*time.Millisecond)
}

// TestReclaim_ReportsMoreOnlyForAFullStoredBatch: more drives Run's immediate retry,
// so a short batch or a store failure must not report it. The cursor continues the
// walk after a success and goes back to the start after a failure, where the failed
// entries still are.
func TestReclaim_ReportsMoreOnlyForAFullStoredBatch(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.BatchSize = 2
	cfg.ClaimMinIdle = 0
	seedDeadConsumerPending(t, rdb, cfg, 5)
	store := &schemaGapStore{fakeStore: newFakeStore()}
	d := New(cfg, testLogger(), rdb, store)
	ctx := context.Background()

	if d.reclaim(ctx) {
		t.Fatal("reclaim reported more after a store failure")
	}
	if !d.storeDown || d.claimCursor != "0" {
		t.Fatalf("after a store failure storeDown=%v cursor=%q, want true and 0", d.storeDown, d.claimCursor)
	}

	store.migrate(0)
	if !d.reclaim(ctx) {
		t.Fatal("reclaim did not report more after a full batch")
	}
	if d.claimCursor == "0" {
		t.Fatal("reclaim discarded the returned cursor after a full batch with more waiting")
	}
	if !d.reclaim(ctx) {
		t.Fatal("reclaim did not report more after the second full batch")
	}
	if d.reclaim(ctx) {
		t.Fatal("reclaim reported more after a short batch")
	}
	if d.claimCursor != "0" {
		t.Fatalf("cursor = %q at the end of the pending list, want 0", d.claimCursor)
	}
	if got := store.rowCount(); got != 5 {
		t.Fatalf("rows = %d, want 5", got)
	}
}

// TestRun_FailingStoreKeepsReclaimAtClaimIntervalPace: a store failure must not turn
// the immediate retry into a hot loop, whether Ping fails or not.
func TestRun_FailingStoreKeepsReclaimAtClaimIntervalPace(t *testing.T) {
	down := newFakeStore()
	down.failAll = true
	down.pingErr = errors.New("store down")
	for name, store := range map[string]Store{
		"outage":        down,
		"schema behind": &schemaGapStore{fakeStore: newFakeStore()},
	} {
		t.Run(name, func(t *testing.T) {
			_, rdb := startMiniredis(t)
			cfg := testConfig(rdb.Options().Addr)
			cfg.BatchSize = 5
			cfg.ClaimInterval = time.Hour
			seedDeadConsumerPending(t, rdb, cfg, 20)
			claims := countCommand(rdb, "xautoclaim")
			d := New(cfg, testLogger(), rdb, store)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if err := d.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if n := claims.Load(); n > 1 {
				t.Fatalf("XAUTOCLAIM ran %d times with a one-hour ClaimInterval, want at most 1", n)
			}
		})
	}
}

// TestRun_NoNewReadsWhileStoreDown: during an outage the entries stay undelivered,
// so they do not all move into the pending list, while acknowledged entries are
// still trimmed. Once the store answers everything drains.
func TestRun_NoNewReadsWhileStoreDown(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.BatchSize = 1
	cfg.TrimInterval = 5 * time.Millisecond
	cfg.ClaimInterval = time.Hour // only the read path can notice the outage here
	store := newFakeStore()
	store.failAll = true
	store.pingErr = errors.New("store down")
	d := New(cfg, testLogger(), rdb, store)
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	addEntries(t, rdb, cfg.StreamName, 3)
	read := readEntries(t, rdb, cfg, cfg.Group, 3)
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, read...).Err(); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		xaddEvent(t, rdb, cfg.StreamName, sampleEvent(fmt.Sprintf("outage-%d", i)))
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// Longer than the 500ms back-off, so a second read would have happened.
	time.Sleep(800 * time.Millisecond)
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 1 {
		t.Fatalf("pending = %d during the outage, want only the one batch read before the store failed", p)
	}
	// The boundary is the pending entry, the first event, so every acknowledged
	// entry below it goes.
	entries := remainingEntries(t, rdb, cfg.StreamName)
	if len(entries) != 4 {
		t.Fatalf("stream = %v during the outage, want the 3 acknowledged entries trimmed and the 4 events kept", entries)
	}

	store.mu.Lock()
	store.failAll, store.pingErr = false, nil
	store.mu.Unlock()
	waitFor(t, "reading to resume after the outage", 5*time.Second, func() bool { return store.rowCount() == 3 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The batch that failed is still pending, and reclaim stores it.
	cfg.ClaimInterval = 10 * time.Millisecond
	runUntil(t, New(cfg, testLogger(), rdb, store), func() bool {
		return store.rowCount() == 4 && pendingCount(t, rdb, cfg.StreamName, cfg.Group) == 0
	}, 5*time.Second)
}

// TestDrain_MalformedEntryIsLoggedCountedAndTrimmed: the trim removes a dropped
// entry, so the log line must carry its raw values on one line, and the drop must
// be counted apart from store poison.
func TestDrain_MalformedEntryIsLoggedCountedAndTrimmed(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.TrimInterval = 5 * time.Millisecond
	store := newFakeStore()
	payloads := []string{`{"request_id":"lost-1","prompt_tokens":"100"}`, "{\"request_id\":\"\",\n\"model\":\"lost-2\"}"}
	for _, raw := range payloads {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: cfg.StreamName, Values: map[string]any{"event": raw}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("good"))
	logger, logs := capturingLogger()
	d := New(cfg, logger, rdb, store)

	runUntil(t, d, func() bool {
		return store.rowCount() == 1 && rdb.XLen(ctx, cfg.StreamName).Val() == 1
	}, 5*time.Second)

	if got := d.Malformed(); got != 2 {
		t.Fatalf("Malformed() = %d, want 2", got)
	}
	if got := d.Poisoned(); got != 0 {
		t.Fatalf("Poisoned() = %d, want 0: a decode failure is not store poison", got)
	}
	var dropLines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "drop malformed entry") {
			dropLines = append(dropLines, line)
		}
	}
	if len(dropLines) != 2 {
		t.Fatalf("drop lines = %q, want one line per entry", dropLines)
	}
	for i, raw := range payloads {
		want, err := json.Marshal(map[string]any{"event": raw})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(dropLines[i], string(want)) {
			t.Errorf("drop line %q does not hold the raw values %s", dropLines[i], want)
		}
	}
}

// TestTrimDelivered_FailsClosed: any doubt about the boundary trims nothing, because
// a wrong trim loses a billing record. In the fixture read[2] is pending in both
// groups, so a check that merely skipped one read would leave a usable boundary from
// the other group and trim the entries below it.
func TestTrimDelivered_FailsClosed(t *testing.T) {
	failCommand := func(name string) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if cmd.Name() == name {
				return fmt.Errorf("induced %s failure", name)
			}
			return nil
		}
	}
	for _, c := range []struct {
		name    string
		before  func(cmd redis.Cmder) error
		rewrite func(cmd redis.Cmder, first *bool)
	}{
		{name: "xpending fails", before: failCommand("xpending")},
		{name: "xinfo fails", before: failCommand("xinfo")},
		{name: "last-delivered id does not parse", rewrite: func(cmd redis.Cmder, _ *bool) {
			if c, ok := cmd.(*redis.XInfoGroupsCmd); ok {
				c.Val()[0].LastDeliveredID = "garbage"
			}
		}},
		{name: "pending lower id does not parse", rewrite: func(cmd redis.Cmder, first *bool) {
			if c, ok := cmd.(*redis.XPendingCmd); ok && *first && c.Val().Count > 0 {
				*first = false
				v := *c.Val()
				v.Lower = "garbage"
				c.SetVal(&v)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			_, rdb := startMiniredis(t)
			cfg := testConfig(rdb.Options().Addr)
			logger, _ := capturingLogger()
			d := New(cfg, logger, rdb, newFakeStore())
			if err := d.ensureGroup(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rdb.XGroupCreate(ctx, cfg.StreamName, "other", "0").Err(); err != nil {
				t.Fatal(err)
			}
			ids := addEntries(t, rdb, cfg.StreamName, 5)
			for _, group := range []string{cfg.Group, "other"} {
				read := readEntries(t, rdb, cfg, group, 3)
				if err := rdb.XAck(ctx, cfg.StreamName, group, read[:2]...).Err(); err != nil {
					t.Fatal(err)
				}
			}
			var trims atomic.Int64
			first := true
			rdb.AddHook(funcHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
				if cmd.Name() == "xtrim" {
					trims.Add(1)
				}
				if c.before != nil {
					if err := c.before(cmd); err != nil {
						cmd.SetErr(err)
						return err
					}
				}
				err := next(ctx, cmd)
				if err == nil && c.rewrite != nil {
					c.rewrite(cmd, &first)
				}
				return err
			}))

			d.trimDelivered(ctx)

			if n := trims.Load(); n != 0 {
				t.Fatalf("XTRIM ran %d times, want none", n)
			}
			clean := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr})
			defer func() { _ = clean.Close() }()
			assertEntries(t, remainingEntries(t, clean, cfg.StreamName), ids)
		})
	}
}

// TestTrimDelivered_EntriesDeliveredBetweenTheTwoReadsStay pins the read order:
// XINFO GROUPS before XPENDING. Entries delivered between the two are above the
// last-delivered id already captured. Read the other way round, XPENDING would see
// nothing pending and XINFO a newer last-delivered id, and the first of those
// entries would be trimmed while still unacknowledged.
func TestTrimDelivered_EntriesDeliveredBetweenTheTwoReadsStay(t *testing.T) {
	ctx := context.Background()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	ids := addEntries(t, rdb, cfg.StreamName, 5)
	read := readEntries(t, rdb, cfg, cfg.Group, 3)
	if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, read...).Err(); err != nil {
		t.Fatal(err)
	}
	other := redis.NewClient(&redis.Options{Addr: rdb.Options().Addr})
	t.Cleanup(func() { _ = other.Close() })
	fired := false
	rdb.AddHook(funcHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if name := cmd.Name(); !fired && (name == "xinfo" || name == "xpending") {
			fired = true // after the first of the two reads has its answer
			if got := readEntries(t, other, cfg, cfg.Group, 2); len(got) != 2 {
				t.Errorf("delivered %d entries between the reads, want 2", len(got))
			}
		}
		return err
	}))

	d.trimDelivered(ctx)

	if !fired {
		t.Fatal("the hook never fired")
	}
	assertEntries(t, remainingEntries(t, other, cfg.StreamName), ids[2:])
}

// TestTrimBoundary_LogsGroupThatStopsTheTrim: a group holding the trim back keeps
// Valkey filling up, so it is logged. A new install and the drainer's own group
// stay quiet.
func TestTrimBoundary_LogsGroupThatStopsTheTrim(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, rdb *redis.Client, cfg Config, d *Drainer)
		want  string
	}{
		{name: "no stream"},
		{name: "empty stream, group has delivered nothing", setup: func(t *testing.T, _ *redis.Client, _ Config, d *Drainer) {
			if err := d.ensureGroup(ctx); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "own group holds the boundary", setup: func(t *testing.T, rdb *redis.Client, cfg Config, d *Drainer) {
			if err := d.ensureGroup(ctx); err != nil {
				t.Fatal(err)
			}
			addEntries(t, rdb, cfg.StreamName, 3)
			readEntries(t, rdb, cfg, cfg.Group, 2)
		}},
		{name: "group has delivered nothing and entries wait", want: `consumer group "other" has delivered nothing`, setup: func(t *testing.T, rdb *redis.Client, cfg Config, _ *Drainer) {
			if err := rdb.XGroupCreateMkStream(ctx, cfg.StreamName, "other", "0").Err(); err != nil {
				t.Fatal(err)
			}
			addEntries(t, rdb, cfg.StreamName, 3)
		}},
		{name: "another group holds the boundary", want: `consumer group "other" holds the trim boundary`, setup: func(t *testing.T, rdb *redis.Client, cfg Config, d *Drainer) {
			if err := d.ensureGroup(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rdb.XGroupCreate(ctx, cfg.StreamName, "other", "0").Err(); err != nil {
				t.Fatal(err)
			}
			addEntries(t, rdb, cfg.StreamName, 4)
			if err := rdb.XAck(ctx, cfg.StreamName, cfg.Group, readEntries(t, rdb, cfg, cfg.Group, 3)...).Err(); err != nil {
				t.Fatal(err)
			}
			readEntries(t, rdb, cfg, "other", 1)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, rdb := startMiniredis(t)
			cfg := testConfig(rdb.Options().Addr)
			logger, logs := capturingLogger()
			d := New(cfg, logger, rdb, newFakeStore())
			if c.setup != nil {
				c.setup(t, rdb, cfg, d)
			}
			d.trimDelivered(ctx)
			got := logs.String()
			if c.want == "" && strings.Contains(got, "WARN") {
				t.Fatalf("logs = %q, want no warning", got)
			}
			if c.want != "" && !strings.Contains(got, "WARN: drainer: ") || !strings.Contains(got, c.want) {
				t.Fatalf("logs = %q, want a warning containing %q", got, c.want)
			}
		})
	}
}
