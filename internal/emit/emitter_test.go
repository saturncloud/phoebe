package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// ---- helpers ----------------------------------------------------------------

func testLogger() *logging.Logger { return logging.New(logging.DEBUG) }

func tmpWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.jsonl")
}

func testConfig(t *testing.T, mr *miniredis.Miniredis) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.WALPath = tmpWALPath(t)
	cfg.ShipInterval = 50 * time.Millisecond
	cfg.ShipBatchSize = 64
	cfg.WorkerCount = 2
	cfg.ChanBuf = 64
	if mr != nil {
		cfg.ValkeyAddr = mr.Addr()
	}
	return cfg
}

func redisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

func makeEvent(id string) metering.Event {
	return metering.Event{
		RequestID:        id,
		GroupID:          "g1",
		UserID:           "u1",
		Model:            "llama3",
		PromptTokens:     100,
		CompletionTokens: 50,
		FinishReason:     "stop",
	}
}

// drainWALEvents reads all events from the WAL file at path.
func drainWALEvents(t *testing.T, path string) []metering.Event {
	t.Helper()
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.close() //nolint:errcheck
	events, err := w.readAll()
	if err != nil {
		t.Fatalf("wal readAll: %v", err)
	}
	return events
}

// streamLen returns XLEN of the stream.
func streamLen(t *testing.T, rdb *redis.Client, stream string) int64 {
	t.Helper()
	n, err := rdb.XLen(context.Background(), stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	return n
}

// fixedClock returns a function that always returns the same time.
func fixedClock(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

// waitForCondition polls fn every 5 ms until it returns true or the timeout.
func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// ---- WAL unit tests ---------------------------------------------------------

func TestWAL_AppendAndRead(t *testing.T) {
	w, err := openWAL(filepath.Join(t.TempDir(), "wal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck

	events := []metering.Event{makeEvent("r1"), makeEvent("r2"), makeEvent("r3")}
	for _, e := range events {
		if err := w.append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := w.readAll()
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	if len(got) != len(events) {
		t.Fatalf("got %d events, want %d", len(got), len(events))
	}
	for i, g := range got {
		if g.RequestID != events[i].RequestID {
			t.Errorf("event[%d].RequestID = %q, want %q", i, g.RequestID, events[i].RequestID)
		}
	}
}

func TestWAL_TruncateClearsFile(t *testing.T) {
	w, err := openWAL(filepath.Join(t.TempDir(), "wal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck

	for i := range 5 {
		if err := w.append(makeEvent(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.truncate(); err != nil {
		t.Fatal(err)
	}
	got, err := w.readAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("after truncate: got %d events, want 0", len(got))
	}
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	w, err := openWAL(filepath.Join(t.TempDir(), "wal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck

	const n = 200
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = w.append(makeEvent(fmt.Sprintf("r%d", i)))
		}(i)
	}
	wg.Wait()

	got, err := w.readAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("got %d events, want %d", len(got), n)
	}
}

// ---- DurableEmitter: Valkey happy-path --------------------------------------

func TestEmitter_XADDtoValkey(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	rdb := redisClient(mr.Addr())

	em, err := NewWithClock(cfg, testLogger(), rdb, fixedClock(1_000_000))
	if err != nil {
		t.Fatal(err)
	}

	ev := makeEvent("req-001")
	em.Emit(context.Background(), ev)

	// Wait for the worker to process.
	waitForCondition(t, 2*time.Second, func() bool {
		return streamLen(t, rdb, cfg.StreamName) == 1
	})

	ctx := context.Background()
	msgs, err := rdb.XRange(ctx, cfg.StreamName, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 stream entry, got %d", len(msgs))
	}

	raw, ok := msgs[0].Values["event"]
	if !ok {
		t.Fatal("stream entry missing 'event' field")
	}
	var got metering.Event
	if err := json.Unmarshal([]byte(raw.(string)), &got); err != nil {
		t.Fatalf("unmarshal stream event: %v", err)
	}
	if got.RequestID != ev.RequestID {
		t.Errorf("RequestID = %q, want %q", got.RequestID, ev.RequestID)
	}
	if got.TimestampUnixMs != 1_000_000 {
		t.Errorf("TimestampUnixMs = %d, want 1000000", got.TimestampUnixMs)
	}

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

func TestEmitter_MultipleEvents(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	rdb := redisClient(mr.Addr())

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	for i := range n {
		em.Emit(context.Background(), makeEvent(fmt.Sprintf("req-%03d", i)))
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return streamLen(t, rdb, cfg.StreamName) == n
	})

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// ---- DurableEmitter: Valkey-down -> WAL fallback ----------------------------

func TestEmitter_ValkeyDown_FallsToWAL(t *testing.T) {
	cfg := testConfig(t, nil)
	cfg.ValkeyAddr = "localhost:0" // nothing listening here

	// No live Valkey — pass nil so emitter skips XADD immediately.
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	events := []metering.Event{makeEvent("w1"), makeEvent("w2"), makeEvent("w3")}
	for _, ev := range events {
		em.Emit(context.Background(), ev)
	}

	// Give workers a moment to handle the events.
	time.Sleep(100 * time.Millisecond)

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}

	got := drainWALEvents(t, cfg.WALPath)
	if len(got) != len(events) {
		t.Fatalf("WAL: got %d events, want %d", len(got), len(events))
	}
	ids := map[string]bool{}
	for _, e := range got {
		ids[e.RequestID] = true
	}
	for _, ev := range events {
		if !ids[ev.RequestID] {
			t.Errorf("event %q missing from WAL", ev.RequestID)
		}
	}
}

// TestEmitter_XADDFails_FallsToWAL starts with a live miniredis then closes it
// to simulate a mid-operation Valkey failure.
func TestEmitter_XADDFails_FallsToWAL(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ChanBuf = 0 // zero-buf: every Emit that can't go direct to channel falls through
	cfg.WorkerCount = 1
	cfg.XADDTimeout = 50 * time.Millisecond // short timeout so test is fast

	// Build a real redis client with no retries and a short dial timeout.
	rdb := redis.NewClient(&redis.Options{
		Addr:         mr.Addr(),
		MaxRetries:   0,
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	// Kill Valkey so XADD fails.
	mr.Close()

	// These events should all land in the WAL.
	events := []metering.Event{makeEvent("f1"), makeEvent("f2")}
	for _, ev := range events {
		em.Emit(context.Background(), ev)
	}

	time.Sleep(200 * time.Millisecond)

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}

	got := drainWALEvents(t, cfg.WALPath)
	if len(got) == 0 {
		t.Fatal("WAL is empty after Valkey failure; expected events to land there")
	}
}

// ---- DurableEmitter: WAL shipper drains on Valkey recovery ------------------

func TestEmitter_WALShipperDrainsOnRecovery(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = 30 * time.Millisecond

	// Start with nil rdb to force WAL writes.
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	events := []metering.Event{makeEvent("s1"), makeEvent("s2"), makeEvent("s3")}
	for _, ev := range events {
		em.Emit(context.Background(), ev)
	}
	time.Sleep(80 * time.Millisecond)

	// Verify events are in WAL.
	walEvents := drainWALEvents(t, cfg.WALPath)
	if len(walEvents) == 0 {
		t.Fatal("expected events in WAL before recovery")
	}

	// Close the nil-rdb emitter.
	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}

	// Now simulate recovery: create a new emitter with real Valkey.
	rdb := redisClient(mr.Addr())
	cfg2 := cfg
	cfg2.ShipInterval = 30 * time.Millisecond
	// Reuse the same WAL path (still has data).
	em2, err := New(cfg2, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	// Shipper should drain WAL into Valkey.
	waitForCondition(t, 3*time.Second, func() bool {
		n := streamLen(t, rdb, cfg.StreamName)
		return n >= int64(len(events))
	})

	if err := em2.Close(context.Background()); err != nil {
		t.Error(err)
	}

	// WAL should be empty after successful drain.
	remaining := drainWALEvents(t, cfg.WALPath)
	if len(remaining) != 0 {
		t.Fatalf("WAL should be empty after drain, got %d events", len(remaining))
	}
}

// ---- DurableEmitter: log floor when WAL fails --------------------------------

func TestEmitter_WALFails_LogFloor(t *testing.T) {
	cfg := testConfig(t, nil)
	// Point WAL at a path we can't write to.
	cfg.WALPath = "/proc/not-a-real-path/wal.jsonl"

	// New will fail to open the WAL — so we test walOrLog directly.
	// Instead, open a fresh emitter with a valid WAL, then force a WAL error
	// by replacing wal.f with a closed file.
	cfg.WALPath = tmpWALPath(t)
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close the WAL file to induce write errors.
	em.wal.f.Close()

	// walOrLog should fall through to the log floor without panicking.
	ev := makeEvent("floor-req")
	// This should not panic or block.
	em.walOrLog(ev)

	// Close the emitter (wal is already closed, so close may error — that's fine).
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = em.Close(ctx)
}

// ---- DurableEmitter: non-blocking Emit (channel-full path) ------------------

func TestEmitter_NonBlocking_ChannelFull(t *testing.T) {
	cfg := testConfig(t, nil)
	cfg.ChanBuf = 0     // zero buffer: channel is always "full"
	cfg.WorkerCount = 0 // no workers draining

	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Emit should return immediately even with no workers and zero buffer.
	start := time.Now()
	for i := range 20 {
		em.Emit(context.Background(), makeEvent(fmt.Sprintf("nb-%d", i)))
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Errorf("Emit blocked too long: %v", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = em.Close(ctx)
}

// TestEmitter_ConcurrentEmit verifies no data races under concurrent callers.
func TestEmitter_ConcurrentEmit(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ChanBuf = 512
	cfg.WorkerCount = 4
	rdb := redisClient(mr.Addr())

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 20
	const perGoroutine = 10
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				em.Emit(context.Background(), makeEvent(fmt.Sprintf("g%d-r%d", g, i)))
			}
		}(g)
	}
	wg.Wait()

	waitForCondition(t, 5*time.Second, func() bool {
		return streamLen(t, rdb, cfg.StreamName) == goroutines*perGoroutine
	})

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// ---- DurableEmitter: timestamp injection (deterministic clock) ---------------

func TestEmitter_TimestampInjected(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	rdb := redisClient(mr.Addr())

	const fixedMs = int64(1_700_000_000_000)
	em, err := NewWithClock(cfg, testLogger(), rdb, fixedClock(fixedMs))
	if err != nil {
		t.Fatal(err)
	}

	ev := makeEvent("ts-req")
	ev.TimestampUnixMs = 0 // emitter should overwrite this
	em.Emit(context.Background(), ev)

	waitForCondition(t, 2*time.Second, func() bool {
		return streamLen(t, rdb, cfg.StreamName) == 1
	})

	ctx := context.Background()
	msgs, _ := rdb.XRange(ctx, cfg.StreamName, "-", "+").Result()
	var got metering.Event
	_ = json.Unmarshal([]byte(msgs[0].Values["event"].(string)), &got)
	if got.TimestampUnixMs != fixedMs {
		t.Errorf("TimestampUnixMs = %d, want %d", got.TimestampUnixMs, fixedMs)
	}

	_ = em.Close(context.Background())
}

// ---- DurableEmitter: idempotency-friendliness (re-ship same request_id) -----

func TestEmitter_ReshipSameRequestID(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	rdb := redisClient(mr.Addr())

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	ev := makeEvent("idem-001")

	// Emit the same event twice — simulates a WAL re-ship after crash.
	em.Emit(context.Background(), ev)
	em.Emit(context.Background(), ev)

	// Both land in the stream (at-least-once); downstream dedup is consumer's job.
	waitForCondition(t, 2*time.Second, func() bool {
		return streamLen(t, rdb, cfg.StreamName) == 2
	})

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// ---- DurableEmitter: graceful close drains in-flight events -----------------

func TestEmitter_CloseFlushesInFlight(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ChanBuf = 512
	rdb := redisClient(mr.Addr())

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	const n = 30
	for i := range n {
		em.Emit(context.Background(), makeEvent(fmt.Sprintf("close-%d", i)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := em.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After Close, all events must be shipped.
	got := streamLen(t, rdb, cfg.StreamName)
	if got != n {
		t.Errorf("after Close: stream len = %d, want %d", got, n)
	}
}

// ---- DurableEmitter: satisfies metering.Emitter interface -------------------

func TestEmitter_ImplementsInterface(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	em, err := New(cfg, testLogger(), redisClient(mr.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	var _ metering.Emitter = em // compile-time interface check
	_ = em.Close(context.Background())
}

// ---- stress: many goroutines, no race detector findings ---------------------

func TestEmitter_RaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ChanBuf = 256
	cfg.WorkerCount = 8
	rdb := redisClient(mr.Addr())

	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	var total atomic.Int64
	const goroutines = 50
	const perG = 40
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perG {
				em.Emit(context.Background(), makeEvent(fmt.Sprintf("stress-g%d-r%d", g, i)))
				total.Add(1)
			}
		}(g)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := em.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Not all may have made it to Valkey (channel overflow falls to WAL) —
	// but nothing should panic, deadlock, or race.
	t.Logf("total emits: %d, stream len: %d", total.Load(), streamLen(t, rdb, cfg.StreamName))
}

// ---- WAL reopen: data persists across process restart -----------------------

func TestWAL_DataPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	// Write events in first open.
	{
		w, err := openWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 5 {
			if err := w.append(makeEvent(fmt.Sprintf("persist-%d", i))); err != nil {
				t.Fatal(err)
			}
		}
		w.close() //nolint:errcheck
	}

	// Reopen and verify.
	w2, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.close() //nolint:errcheck

	events, err := w2.readAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("after reopen: got %d events, want 5", len(events))
	}
}

// ---- File-not-writable: New returns error, not panic -----------------------

func TestNew_InvalidWALPath_ReturnsError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALPath = "/proc/not-writable/wal.jsonl"
	cfg.ValkeyAddr = "localhost:0"

	_, err := New(cfg, testLogger(), nil)
	if err == nil {
		t.Fatal("expected error for unwritable WAL path, got nil")
	}
}

// ---- os.File: verify fsync is called (basic smoke) -------------------------

func TestWAL_FsyncSmoke(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(filepath.Join(dir, "wal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.close() //nolint:errcheck

	ev := makeEvent("fsync-test")
	if err := w.append(ev); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Verify the file has non-zero size (fsync doesn't lose data).
	info, err := os.Stat(w.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("WAL file is empty after append+fsync")
	}
}
