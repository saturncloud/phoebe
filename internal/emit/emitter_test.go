package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// ---- helpers ----------------------------------------------------------------

func testLogger() *logging.Logger { return logging.New(logging.DEBUG) }

// tmpWALPath returns a fresh WAL directory path. The base name deliberately
// keeps the historical ".jsonl" file name — production configs do the same so
// that a pre-upgrade legacy file at the path is auto-imported.
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

// fastFailClient is a redis client that gives up quickly — used by outage
// tests so failing XADDs don't stretch the test wall-clock.
func fastFailClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		MaxRetries:   -1, // disable retries entirely
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})
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

func openTestWAL(t *testing.T, dir string) *wal {
	t.Helper()
	w, err := openWAL(dir, testLogger())
	if err != nil {
		t.Fatalf("openWAL(%s): %v", dir, err)
	}
	return w
}

// unshipped returns every event the wal still considers unshipped.
func unshipped(t *testing.T, w *wal) []metering.Event {
	t.Helper()
	events, _, _, err := w.pending(0)
	if err != nil {
		t.Fatalf("wal pending: %v", err)
	}
	return events
}

// drainWALEvents opens the WAL at path and returns its unshipped events.
//
// NOTE on the retained tail: tidwall/wal cannot truncate to empty, so a fully
// drained log retains its final (already-shipped) entry, and the
// shipped-through watermark lives only in memory. A freshly opened wal
// therefore reports that one entry as unshipped — at-least-once, deduped
// downstream on request_id. Callers asserting "fully drained" should expect
// AT MOST ONE residual event, not zero.
func drainWALEvents(t *testing.T, path string) []metering.Event {
	t.Helper()
	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck
	return unshipped(t, w)
}

// appendRaw writes raw (possibly garbage) bytes as the next WAL entry via the
// library — simulates an undecodable entry for corruption tests.
func appendRaw(t *testing.T, w *wal, data []byte) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.log.Write(w.nextIndex, data); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	w.nextIndex++
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

// streamRequestIDs returns the SET of request_ids currently in the stream.
func streamRequestIDs(t *testing.T, rdb *redis.Client, stream string) map[string]bool {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	ids := map[string]bool{}
	for _, m := range msgs {
		var ev metering.Event
		if err := json.Unmarshal([]byte(m.Values["event"].(string)), &ev); err != nil {
			t.Fatalf("unmarshal stream event: %v", err)
		}
		ids[ev.RequestID] = true
	}
	return ids
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
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	events := []metering.Event{makeEvent("r1"), makeEvent("r2"), makeEvent("r3")}
	for _, e := range events {
		if err := w.append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got := unshipped(t, w)
	if len(got) != len(events) {
		t.Fatalf("got %d events, want %d", len(got), len(events))
	}
	for i, g := range got {
		if g.RequestID != events[i].RequestID {
			t.Errorf("event[%d].RequestID = %q, want %q", i, g.RequestID, events[i].RequestID)
		}
	}
}

// TestWAL_MarkShippedReclaims verifies the truncate-after-ship watermark
// protocol: after markShipped(through), nothing is reported unshipped, and a
// later append is reported alone.
func TestWAL_MarkShippedReclaims(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	for i := range 5 {
		if err := w.append(makeEvent(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	events, through, skipped, err := w.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 || skipped != 0 {
		t.Fatalf("pending = %d events (%d skipped), want 5 (0)", len(events), skipped)
	}
	if err := w.markShipped(through); err != nil {
		t.Fatalf("markShipped: %v", err)
	}
	if got := unshipped(t, w); len(got) != 0 {
		t.Fatalf("after markShipped: %d unshipped, want 0", len(got))
	}

	// A new append after a full drain must be reported, and reported alone —
	// not bundled with the retained already-shipped tail entry.
	if err := w.append(makeEvent("after-drain")); err != nil {
		t.Fatal(err)
	}
	got := unshipped(t, w)
	if len(got) != 1 || got[0].RequestID != "after-drain" {
		t.Fatalf("after new append: unshipped = %+v, want exactly [after-drain]", got)
	}
}

// TestWAL_ConcurrentAppendDuringDrain is the regression guard for the
// lost-event race the old design defended against with rotation: an append
// landing DURING the ship window must survive the post-ship truncation. With
// sequential indexes this holds structurally — the concurrent append gets a
// higher index than the drain's `through`, which TruncateFront never touches.
func TestWAL_ConcurrentAppendDuringDrain(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	for i := range 3 {
		if err := w.append(makeEvent(fmt.Sprintf("old%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Start a drain: read the unshipped batch (ship window opens here).
	events, through, _, err := w.pending(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("pending %d events, want 3", len(events))
	}

	// Append DURING the ship window — after the read, before the truncate.
	if err := w.append(makeEvent("during-ship")); err != nil {
		t.Fatal(err)
	}

	// Complete the drain.
	if err := w.markShipped(through); err != nil {
		t.Fatal(err)
	}

	// The during-ship event must still be unshipped — NOT destroyed.
	got := unshipped(t, w)
	if len(got) != 1 || got[0].RequestID != "during-ship" {
		t.Fatalf("lost the concurrent append: unshipped = %+v, want exactly [during-ship]", got)
	}
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
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

	if got := unshipped(t, w); len(got) != n {
		t.Fatalf("got %d events, want %d", len(got), n)
	}
}

// TestWAL_CrashReopen: append N, close (standing in for a crash — tidwall
// fsyncs every write, so a closed log and a killed process leave the same
// bytes), reopen → all N still present and reported unshipped.
func TestWAL_CrashReopen(t *testing.T) {
	path := tmpWALPath(t)

	const n = 10
	w := openTestWAL(t, path)
	for i := range n {
		if err := w.append(makeEvent(fmt.Sprintf("persist-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	w2 := openTestWAL(t, path)
	defer w2.close() //nolint:errcheck
	got := unshipped(t, w2)
	if len(got) != n {
		t.Fatalf("after reopen: got %d events, want %d", len(got), n)
	}
	for i, g := range got {
		if want := fmt.Sprintf("persist-%d", i); g.RequestID != want {
			t.Errorf("event[%d] = %q, want %q (order must survive reopen)", i, g.RequestID, want)
		}
	}
}

// TestWAL_CorruptEntrySkipped: an undecodable entry between valid ones is
// skipped with a count — it must never abort the drain — and is truncated
// past so it cannot wedge the log forever.
func TestWAL_CorruptEntrySkipped(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	if err := w.append(makeEvent("ok-1")); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, w, []byte("\x00{{ definitely not json"))
	if err := w.append(makeEvent("ok-2")); err != nil {
		t.Fatal(err)
	}

	events, through, skipped, err := w.pending(0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if skipped != 1 {
		t.Errorf("skipped = %d, want 1", skipped)
	}
	if len(events) != 2 || events[0].RequestID != "ok-1" || events[1].RequestID != "ok-2" {
		t.Fatalf("events = %+v, want [ok-1 ok-2]", events)
	}
	// through covers the corrupt entry, so markShipped truncates past it.
	if err := w.markShipped(through); err != nil {
		t.Fatal(err)
	}
	if got := unshipped(t, w); len(got) != 0 {
		t.Fatalf("after markShipped: %d unshipped, want 0 (corrupt entry truncated past)", len(got))
	}
}

// TestWAL_OversizeEvent: a ~100KB field round-trips. The old bufio.Scanner
// default 64KB token limit killed the whole scan on such an event.
func TestWAL_OversizeEvent(t *testing.T) {
	w := openTestWAL(t, tmpWALPath(t))
	defer w.close() //nolint:errcheck

	big := makeEvent("big-1")
	big.Model = strings.Repeat("m", 100*1024)
	if err := w.append(big); err != nil {
		t.Fatalf("append oversize: %v", err)
	}
	if err := w.append(makeEvent("after-big")); err != nil {
		t.Fatal(err)
	}

	got := unshipped(t, w)
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].RequestID != "big-1" || len(got[0].Model) != 100*1024 {
		t.Fatalf("oversize event mangled: id=%q model len=%d", got[0].RequestID, len(got[0].Model))
	}
	if got[1].RequestID != "after-big" {
		t.Fatalf("event after oversize one lost: %+v", got[1])
	}
}

// TestWAL_CorruptDirQuarantinedOnOpen: a log directory tidwall refuses to open
// is moved aside to <dir>.corrupt.<ts> and a fresh log is created — a corrupt
// billing buffer must not block serving, and the bad bytes are kept for
// forensics.
func TestWAL_CorruptDirQuarantinedOnOpen(t *testing.T) {
	path := tmpWALPath(t)

	w := openTestWAL(t, path)
	if err := w.append(makeEvent("pre-corruption")); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	// Scribble over every segment file so Open fails with corruption.
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("expected segment files in wal dir")
	}
	for _, ent := range entries {
		if err := os.WriteFile(filepath.Join(path, ent.Name()), []byte("not a wal segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	w2, err := openWAL(path, testLogger())
	if err != nil {
		t.Fatalf("openWAL after corruption should recover, got: %v", err)
	}
	defer w2.close() //nolint:errcheck

	if got := unshipped(t, w2); len(got) != 0 {
		t.Fatalf("fresh wal after quarantine should be empty, got %d events", len(got))
	}
	if err := w2.append(makeEvent("post-recovery")); err != nil {
		t.Fatalf("append to recovered wal: %v", err)
	}

	// The quarantined directory must exist for forensics.
	matches, err := filepath.Glob(path + ".corrupt.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("want exactly one quarantined dir, got %v", matches)
	}
}

// TestWAL_LegacyImport: an old-style single-file JSONL WAL at the configured
// path (including a >64KB line and a torn final line) is imported into the new
// directory log and the file is renamed aside — lost-on-upgrade events would
// be silent revenue loss.
func TestWAL_LegacyImport(t *testing.T) {
	path := tmpWALPath(t)

	big := makeEvent("legacy-big")
	big.Model = strings.Repeat("m", 100*1024) // >64KB line: the old Scanner bug
	var legacyFile []byte
	for _, ev := range []metering.Event{makeEvent("legacy-1"), big, makeEvent("legacy-2")} {
		line, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		legacyFile = append(legacyFile, line...)
		legacyFile = append(legacyFile, '\n')
	}
	// Torn final line: a crash mid-append leaves a partial record.
	legacyFile = append(legacyFile, []byte(`{"request_id":"torn-`)...)
	if err := os.WriteFile(path, legacyFile, 0o600); err != nil {
		t.Fatal(err)
	}

	w := openTestWAL(t, path)
	defer w.close() //nolint:errcheck

	got := unshipped(t, w)
	if len(got) != 3 {
		t.Fatalf("imported %d events, want 3", len(got))
	}
	wantIDs := []string{"legacy-1", "legacy-big", "legacy-2"}
	for i, want := range wantIDs {
		if got[i].RequestID != want {
			t.Errorf("imported[%d] = %q, want %q", i, got[i].RequestID, want)
		}
	}
	if len(got[1].Model) != 100*1024 {
		t.Errorf("oversize legacy line truncated: model len = %d", len(got[1].Model))
	}

	// The legacy file is renamed aside, and the path is now a directory.
	if _, err := os.Stat(path + ".imported"); err != nil {
		t.Errorf("legacy file not renamed aside: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil || !fi.IsDir() {
		t.Errorf("WAL path is not a directory after import (err=%v)", err)
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

	rdb := fastFailClient(mr.Addr())

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

	// Verify events are in WAL (read via the emitter's own handle — opening a
	// second tidwall.Log on a live directory is not supported).
	if got := unshipped(t, em.wal); len(got) == 0 {
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

	// All events made it; the WAL retains AT MOST its already-shipped tail
	// entry (tidwall cannot truncate to empty — see drainWALEvents). With
	// multiple workers the append order is nondeterministic, so the tail can
	// be any one of the shipped events.
	remaining := drainWALEvents(t, cfg.WALPath)
	if len(remaining) > 1 {
		t.Fatalf("WAL should hold at most the retained tail after drain, got %d events", len(remaining))
	}
	if len(remaining) == 1 {
		shipped := map[string]bool{"s1": true, "s2": true, "s3": true}
		if !shipped[remaining[0].RequestID] {
			t.Fatalf("retained tail = %q, want one of the shipped events", remaining[0].RequestID)
		}
	}
}

// TestEmitter_MultiTickOutage_NoLoss is the clobber regression: Valkey down
// across several drain ticks with appends continuing each tick, every ship
// attempt failing — then Valkey recovers and ALL events from ALL ticks must
// arrive. (The old rotate design renamed each tick's WAL onto the same fixed
// .draining path, destroying every previous tick's unshipped snapshot.)
func TestEmitter_MultiTickOutage_NoLoss(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = 40 * time.Millisecond
	cfg.XADDTimeout = 50 * time.Millisecond
	cfg.ShipBatchSize = 4 // multiple batches per drain, for good measure

	rdb := fastFailClient(mr.Addr())
	em, err := New(cfg, testLogger(), rdb)
	if err != nil {
		t.Fatal(err)
	}

	// Take Valkey down for the whole multi-tick window.
	mr.Close()

	const ticks = 4
	const perTick = 5
	want := map[string]bool{}
	for tick := range ticks {
		for i := range perTick {
			id := fmt.Sprintf("outage-t%d-e%d", tick, i)
			want[id] = true
			em.Emit(context.Background(), makeEvent(id))
		}
		// Sleep past at least one ship interval so a failing drain tick runs
		// between each burst of appends.
		time.Sleep(cfg.ShipInterval + 20*time.Millisecond)
	}

	// Every event from every tick must still be held by the WAL.
	waitForCondition(t, 3*time.Second, func() bool {
		return len(unshipped(t, em.wal)) == ticks*perTick
	})

	// Valkey recovers at the same address.
	if err := mr.Restart(); err != nil {
		t.Fatalf("miniredis restart: %v", err)
	}

	// ALL events from ALL ticks arrive (set-wise: re-ships may duplicate
	// under the at-least-once contract, but nothing may be missing).
	waitForCondition(t, 5*time.Second, func() bool {
		got := streamRequestIDs(t, rdb, cfg.StreamName)
		for id := range want {
			if !got[id] {
				return false
			}
		}
		return true
	})

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// TestEmitter_PartialBatchProgress: ship succeeds for batch 1 and fails for
// batch 2 → batch-1 entries are reclaimed immediately (truncate after EACH
// shipped batch), batch-2 entries are retained and shipped on the next drain,
// with no duplicates beyond the at-least-once contract (here: zero, since no
// crash intervened).
func TestEmitter_PartialBatchProgress(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = time.Hour // shipper ticker must not interfere
	cfg.ShipBatchSize = 3

	em, err := New(cfg, testLogger(), redisClient(mr.Addr()))
	if err != nil {
		t.Fatal(err)
	}

	// Seed the WAL directly (Valkey is up, so Emit would bypass it).
	var want []string
	for i := range 6 {
		id := fmt.Sprintf("partial-%d", i)
		want = append(want, id)
		if err := em.wal.append(makeEvent(id)); err != nil {
			t.Fatal(err)
		}
	}

	var shipped []string
	calls := 0
	em.shipFn = func(events []metering.Event) bool {
		calls++
		if calls > 1 {
			return false // batch 2 fails
		}
		for _, ev := range events {
			shipped = append(shipped, ev.RequestID)
		}
		return true
	}

	em.drainWAL()

	if calls != 2 {
		t.Fatalf("ship calls = %d, want 2 (one success, one failure)", calls)
	}
	if len(shipped) != 3 {
		t.Fatalf("batch 1 shipped %d events, want 3", len(shipped))
	}
	// Batch-1 entries reclaimed, batch-2 entries retained.
	rest := unshipped(t, em.wal)
	if len(rest) != 3 {
		t.Fatalf("retained %d events, want 3 (batch 2 only)", len(rest))
	}
	for i, ev := range rest {
		if want := fmt.Sprintf("partial-%d", i+3); ev.RequestID != want {
			t.Errorf("retained[%d] = %q, want %q", i, ev.RequestID, want)
		}
	}

	// Next tick: ship recovers; batch 2 goes out exactly once.
	em.shipFn = func(events []metering.Event) bool {
		for _, ev := range events {
			shipped = append(shipped, ev.RequestID)
		}
		return true
	}
	em.drainWAL()

	if len(shipped) != 6 {
		t.Fatalf("total shipped = %d, want 6 (no re-ship of batch 1, no loss of batch 2)", len(shipped))
	}
	for i, id := range want {
		if shipped[i] != id {
			t.Errorf("shipped[%d] = %q, want %q", i, shipped[i], id)
		}
	}
	if len(unshipped(t, em.wal)) != 0 {
		t.Fatal("WAL still reports unshipped events after full drain")
	}

	em.shipFn = em.shipBatch // restore before Close's final drain
	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// TestEmitter_LegacyImportShipped: end-to-end upgrade path — an old-style
// wal.jsonl file (with a torn last line) at WALPath is imported on startup,
// shipped to Valkey, and the file is renamed aside.
func TestEmitter_LegacyImportShipped(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = 30 * time.Millisecond

	var legacyFile []byte
	want := map[string]bool{}
	for i := range 3 {
		ev := makeEvent(fmt.Sprintf("upgrade-%d", i))
		want[ev.RequestID] = true
		line, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		legacyFile = append(legacyFile, line...)
		legacyFile = append(legacyFile, '\n')
	}
	legacyFile = append(legacyFile, []byte(`{"request_id":"torn`)...)
	if err := os.WriteFile(cfg.WALPath, legacyFile, 0o600); err != nil {
		t.Fatal(err)
	}

	em, err := New(cfg, testLogger(), redisClient(mr.Addr()))
	if err != nil {
		t.Fatal(err)
	}

	rdb := redisClient(mr.Addr())
	waitForCondition(t, 3*time.Second, func() bool {
		got := streamRequestIDs(t, rdb, cfg.StreamName)
		for id := range want {
			if !got[id] {
				return false
			}
		}
		return true
	})

	if _, err := os.Stat(cfg.WALPath + ".imported"); err != nil {
		t.Errorf("legacy file not renamed aside: %v", err)
	}

	if err := em.Close(context.Background()); err != nil {
		t.Error(err)
	}
}

// ---- DurableEmitter: log floor when WAL fails --------------------------------

func TestEmitter_WALFails_LogFloor(t *testing.T) {
	cfg := testConfig(t, nil)
	em, err := New(cfg, testLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close the WAL to induce append errors (tidwall returns ErrClosed).
	if err := em.wal.close(); err != nil {
		t.Fatal(err)
	}

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

// ---- DurableEmitter: Emit racing Close never strands an event ----------------

// TestEmitter_CloseVsEmit_NeverStrands: the old workers exited via
// `default: return` the moment the channel was momentarily empty, and Emit
// kept queueing into the buffered channel after Close — stranding events with
// no WAL entry and no log floor. Now every emitted event must end up in
// Valkey, the WAL, or the log floor (here rdb is nil, so: WAL or floor).
func TestEmitter_CloseVsEmit_NeverStrands(t *testing.T) {
	cfg := testConfig(t, nil)
	cfg.ChanBuf = 8 // small buffer: maximize the strand window
	cfg.WorkerCount = 2
	cfg.ShipInterval = time.Hour

	// Capture the ERROR stream to count log-floor emissions. The log.Logger's
	// internal mutex serializes concurrent writes to the buffer.
	var floorBuf bytes.Buffer
	logger := logging.New(logging.ERROR)
	logger.Error.SetOutput(&floorBuf)

	em, err := New(cfg, logger, nil)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const perG = 50
	want := map[string]bool{}
	for g := range goroutines {
		for i := range perG {
			want[fmt.Sprintf("race-g%d-e%d", g, i)] = true
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := range perG {
				em.Emit(context.Background(), makeEvent(fmt.Sprintf("race-g%d-e%d", g, i)))
			}
		}(g)
	}

	// Close races the emitters.
	closeRes := make(chan error, 1)
	go func() {
		<-start
		closeRes <- em.Close(context.Background())
	}()

	close(start)
	wg.Wait()
	if err := <-closeRes; err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Account for every event: WAL ∪ floor must cover the full emitted set.
	got := map[string]bool{}
	for _, ev := range drainWALEvents(t, cfg.WALPath) {
		got[ev.RequestID] = true
	}
	for _, line := range strings.Split(floorBuf.String(), "\n") {
		if i := strings.Index(line, "METERING_FLOOR request_id="); i >= 0 {
			rest := line[i+len("METERING_FLOOR request_id="):]
			got[rest[:strings.Index(rest, " ")]] = true
		}
	}
	var missing []string
	for id := range want {
		if !got[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d events stranded (in neither WAL nor log floor), e.g. %v", len(missing), missing[:min(5, len(missing))])
	}
}

// TestEmitter_EmitAfterClose_NotLost: an Emit arriving strictly after Close
// must not panic (closed channel) and must land somewhere durable — the WAL
// is closed by then, so the log floor.
func TestEmitter_EmitAfterClose_NotLost(t *testing.T) {
	cfg := testConfig(t, nil)

	var floorBuf bytes.Buffer
	logger := logging.New(logging.ERROR)
	logger.Error.SetOutput(&floorBuf)

	em, err := New(cfg, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := em.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	em.Emit(context.Background(), makeEvent("late-arrival"))

	if !strings.Contains(floorBuf.String(), "METERING_FLOOR request_id=late-arrival") {
		t.Fatal("event emitted after Close did not reach the log floor")
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
