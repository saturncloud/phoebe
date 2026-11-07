package drain

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/metering"
)

// allRowsFailCase seeds n reclaimable entries that every Upsert rejects as a bad
// row, with a store that answers Ping, and returns a drainer over them.
func allRowsFailCase(t *testing.T, n int, probeErr error) (*Drainer, *fakeStore, *redis.Client, Config, *lockedBuffer) {
	t.Helper()
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.BatchSize = 10
	cfg.ClaimMinIdle = 0
	seedDeadConsumerPending(t, rdb, cfg, n)
	store := newFakeStore()
	for i := range n {
		store.failIDs[fmt.Sprintf("pending-%d", i)] = true
	}
	store.probeErr = probeErr
	logger, logs := capturingLogger()
	return New(cfg, logger, rdb, store), store, rdb, cfg, logs
}

func countLines(logs, substr string) int {
	n := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// TestRowAtATime_AllRowsFailProbeSucceedsDropsAsPoison: when every row of a batch
// fails on its own but the probe write succeeds, the store accepts writes, so the
// rows are bad: each is logged in full, counted, and ACK'd, and the drainer does
// not pause. A batch of one (low traffic) behaves like a batch of many.
func TestRowAtATime_AllRowsFailProbeSucceedsDropsAsPoison(t *testing.T) {
	for _, n := range []int{1, 4} {
		t.Run(fmt.Sprintf("batch of %d", n), func(t *testing.T) {
			d, store, rdb, cfg, logs := allRowsFailCase(t, n, nil)

			d.reclaim(context.Background())

			if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
				t.Fatalf("pending = %d, want 0: poison rows are ACK'd", p)
			}
			if got := d.Poisoned(); got != uint64(n) {
				t.Fatalf("Poisoned() = %d, want %d", got, n)
			}
			if d.storeDown {
				t.Fatal("storeDown set although the probe write succeeded")
			}
			if store.probes != 1 {
				t.Fatalf("ProbeWrite called %d times, want 1", store.probes)
			}
			if got := countLines(logs.String(), "drop poison row"); got != n {
				t.Fatalf("drop poison row lines = %d, want %d", got, n)
			}
			if !strings.Contains(logs.String(), `"request_id":"pending-0"`) {
				t.Fatalf("logs = %q, want the full event JSON", logs.String())
			}
			if !strings.Contains(logs.String(), "probe write succeeded") {
				t.Fatalf("logs = %q, want the probe outcome", logs.String())
			}
		})
	}
}

// TestRowAtATime_AllRowsFailProbeFailsPausesAndKeepsRows: when every row of a
// batch fails and the probe write fails too, the store is the problem. Nothing is
// ACK'd, nothing is counted or logged as poison, and storeDown is set so the
// drainer pauses with the rows still pending.
func TestRowAtATime_AllRowsFailProbeFailsPausesAndKeepsRows(t *testing.T) {
	for _, n := range []int{1, 4} {
		t.Run(fmt.Sprintf("batch of %d", n), func(t *testing.T) {
			d, store, rdb, cfg, logs := allRowsFailCase(t, n, errors.New("probe rejected"))

			d.reclaim(context.Background())

			if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != int64(n) {
				t.Fatalf("pending = %d, want %d: no row may be ACK'd when the probe fails", p, n)
			}
			if got := d.Poisoned(); got != 0 {
				t.Fatalf("Poisoned() = %d, want 0", got)
			}
			if !d.storeDown {
				t.Fatal("storeDown not set after the probe write failed")
			}
			if store.rowCount() != 0 {
				t.Fatalf("rows = %d, want 0", store.rowCount())
			}
			if strings.Contains(logs.String(), "drop poison row") {
				t.Fatalf("logs = %q, want no poison line", logs.String())
			}
			want := regexp.MustCompile(fmt.Sprintf(`all %d row\(s\) failed \(first row id=\d+-\d+ request_id=pending-0: fakeStore: bad row pending-0\) and the probe write failed: probe rejected`, n))
			if !want.MatchString(logs.String()) {
				t.Fatalf("logs = %q, want a match for %s", logs.String(), want)
			}
			if !strings.Contains(logs.String(), "probe write failed too, so this is a store failure") {
				t.Fatalf("logs = %q, want the probe outcome", logs.String())
			}
		})
	}
}

// TestRowAtATime_ProbeContextErrorIsAStoreFailure: a probe that runs out of time
// counts as a failed probe, so the rows stay.
func TestRowAtATime_ProbeContextErrorIsAStoreFailure(t *testing.T) {
	d, _, rdb, cfg, _ := allRowsFailCase(t, 1, context.DeadlineExceeded)
	d.reclaim(context.Background())
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 1 {
		t.Fatalf("pending = %d, want 1", p)
	}
	if !d.storeDown || d.Poisoned() != 0 {
		t.Fatalf("storeDown=%v Poisoned=%d, want a pause and no poison", d.storeDown, d.Poisoned())
	}
}

// TestRowAtATime_MixedBatchDropsOnlyBadRowsWithoutProbe: when at least one row is
// stored the store accepts writes, so no probe is made and only the bad rows are
// dropped as poison.
func TestRowAtATime_MixedBatchDropsOnlyBadRowsWithoutProbe(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.ClaimMinIdle = 0
	seedDeadConsumerPending(t, rdb, cfg, 3)
	store := newFakeStore()
	store.failIDs["pending-1"] = true
	store.probeErr = errors.New("must not be called")
	d := New(cfg, testLogger(), rdb, store)

	d.reclaim(context.Background())

	if store.probes != 0 {
		t.Fatalf("ProbeWrite called %d times for a batch that stored rows", store.probes)
	}
	if store.rowCount() != 2 || d.Poisoned() != 1 {
		t.Fatalf("rows=%d Poisoned=%d, want 2 and 1", store.rowCount(), d.Poisoned())
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 0 {
		t.Fatalf("pending = %d, want 0", p)
	}
	if d.storeDown {
		t.Fatal("storeDown set for a batch that stored rows")
	}
}

// TestRun_LoneRowWithFailedProbePausesInsteadOfDropping: through the Run loop, a
// batch of one bad row against a store whose probe fails stays pending and the
// drainer stops reading new entries, so nothing behind it is moved into the
// pending list either.
func TestRun_LoneRowWithFailedProbePausesInsteadOfDropping(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := retryConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.failIDs["lone"] = true
	store.probeErr = errors.New("probe rejected")
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("lone"))
	d := New(cfg, testLogger(), rdb, store)
	d.storeRetryBase = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 1 {
		t.Fatalf("pending = %d, want 1", p)
	}
	if d.Poisoned() != 0 || !d.storeDown {
		t.Fatalf("Poisoned=%d storeDown=%v, want 0 and a pause", d.Poisoned(), d.storeDown)
	}
}

// TestNoteStoreFailed_LogsPauseStartOnce: the start of a pause is one ERROR line
// in plain words; later failures during the same pause do not repeat it, and the
// resume line says how long the pause lasted.
func TestNoteStoreFailed_LogsPauseStartOnce(t *testing.T) {
	_, rdb := startMiniredis(t)
	logger, logs := capturingLogger()
	d := New(testConfig(rdb.Options().Addr), logger, rdb, newFakeStore())
	start := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return start }

	for range 3 {
		d.noteStoreFailed(errors.New("boom"))
	}
	want := `ERROR: drainer: paused: store write failed: boom; billing events stay in Valkey and none are dropped; retrying with backoff (see docs/billing-reconciliation.md, "Drainer paused")`
	if got := countLines(logs.String(), want); got != 1 {
		t.Fatalf("pause lines = %d, want 1; logs = %q", got, logs.String())
	}
	if !d.pausedSince.Equal(start) {
		t.Fatalf("pausedSince = %v, want %v", d.pausedSince, start)
	}
	d.now = func() time.Time { return start.Add(95 * time.Second) }
	d.noteStoreWorking(1)
	if !strings.Contains(logs.String(), "resuming after a pause of 1m35s") {
		t.Fatalf("logs = %q, want the pause length", logs.String())
	}
	if !d.pausedSince.IsZero() {
		t.Fatal("pausedSince not cleared on resume")
	}
	d.noteStoreFailed(errors.New("again"))
	if got := countLines(logs.String(), "drainer: paused: "); got != 2 {
		t.Fatalf("pause lines = %d after a second pause, want 2", got)
	}
}

// --- drainer state published to Valkey ---

func stateHash(t *testing.T, rdb *redis.Client, cfg Config) map[string]string {
	t.Helper()
	h, err := rdb.HGetAll(context.Background(), metering.DrainerStateKey(cfg.StreamName)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// failingTxHook fails every MULTI/EXEC pipeline while fail is set, like a Valkey
// past maxmemory refusing the writes in it.
type failingTxHook struct{ fail *atomic.Bool }

func (h failingTxHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h failingTxHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}
func (h failingTxHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.fail.Load() {
			err := errors.New("OOM command not allowed when used memory > 'maxmemory'")
			for _, c := range cmds {
				c.SetErr(err)
			}
			return err
		}
		return next(ctx, cmds)
	}
}

// TestPublishState_WritesFieldsAndCounterDeltas: a publish writes this consumer's
// seen and paused_since and adds the new drops to the shared counters; a second
// publish adds only what was dropped since the first.
func TestPublishState_WritesFieldsAndCounterDeltas(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	now := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return now }
	// Another drainer has already counted drops.
	key := metering.DrainerStateKey(cfg.StreamName)
	rdb.HSet(context.Background(), key, metering.DrainerFieldDroppedPoison, 5)

	d.poisoned.Add(2)
	d.malformed.Add(1)
	d.publishState(context.Background())
	h := stateHash(t, rdb, cfg)
	want := map[string]string{
		"test-consumer.seen":         "1700000000",
		"test-consumer.paused_since": "0",
		"dropped_poison":             "7",
		"dropped_undecodable":        "1",
	}
	for f, v := range want {
		if h[f] != v {
			t.Errorf("%s = %q, want %q (hash %v)", f, h[f], v, h)
		}
	}

	d.pausedSince = now.Add(-time.Minute)
	now = now.Add(15 * time.Second)
	d.poisoned.Add(1)
	d.publishState(context.Background())
	h = stateHash(t, rdb, cfg)
	if h["dropped_poison"] != "8" || h["dropped_undecodable"] != "1" {
		t.Fatalf("counters = %q/%q, want 8/1: each drop is counted once", h["dropped_poison"], h["dropped_undecodable"])
	}
	if h["test-consumer.paused_since"] != strconv.FormatInt(now.Add(-75*time.Second).Unix(), 10) {
		t.Fatalf("paused_since = %q", h["test-consumer.paused_since"])
	}
	if h["test-consumer.seen"] != "1700000015" {
		t.Fatalf("seen = %q", h["test-consumer.seen"])
	}
}

// TestPublishState_FailedPublishKeepsCountsForTheNext: a refused publish loses no
// counts and counts nothing twice: the next one that succeeds sends exactly the
// drops not yet sent.
func TestPublishState_FailedPublishKeepsCountsForTheNext(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	var fail atomic.Bool
	rdb.AddHook(failingTxHook{fail: &fail})
	logger, logs := capturingLogger()
	d := New(cfg, logger, rdb, newFakeStore())

	d.poisoned.Add(3)
	d.publishState(context.Background())
	fail.Store(true)
	d.poisoned.Add(2)
	d.malformed.Add(4)
	d.publishState(context.Background())
	d.publishState(context.Background())
	if h := stateHash(t, rdb, cfg); h["dropped_poison"] != "3" {
		t.Fatalf("dropped_poison = %q while publishes fail, want 3", h["dropped_poison"])
	}
	if got := countLines(logs.String(), "could not publish drainer state"); got != 1 {
		t.Fatalf("warn lines = %d, want 1 (throttled)", got)
	}
	fail.Store(false)
	d.publishState(context.Background())
	d.publishState(context.Background())
	h := stateHash(t, rdb, cfg)
	if h["dropped_poison"] != "5" || h["dropped_undecodable"] != "4" {
		t.Fatalf("counters = %q/%q, want 5/4", h["dropped_poison"], h["dropped_undecodable"])
	}
}

// TestPublishState_RemovesStaleConsumerFields: fields of another consumer not
// seen for 24 h are removed; a recent one, and this consumer, stay.
func TestPublishState_RemovesStaleConsumerFields(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	now := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return now }
	key := metering.DrainerStateKey(cfg.StreamName)
	old := now.Add(-25 * time.Hour).Unix()
	recent := now.Add(-23 * time.Hour).Unix()
	rdb.HSet(context.Background(), key,
		"old-pod.seen", old, "old-pod.paused_since", old,
		"recent-pod.seen", recent, "recent-pod.paused_since", 0,
		"orphan.paused_since", 5,
		"dropped_poison", 9)

	d.publishState(context.Background())

	h := stateHash(t, rdb, cfg)
	for _, f := range []string{"old-pod.seen", "old-pod.paused_since", "orphan.paused_since"} {
		if _, ok := h[f]; ok {
			t.Errorf("%s kept, want removed", f)
		}
	}
	for _, f := range []string{"recent-pod.seen", "recent-pod.paused_since", "test-consumer.seen", "dropped_poison"} {
		if _, ok := h[f]; !ok {
			t.Errorf("%s removed, want kept", f)
		}
	}
}

// TestPublishIfDue_ThrottlesAndPublishesPauseChangesAtOnce: publishes are at most
// every 15 s, except that a pause starting or ending is published at once.
func TestPublishIfDue_ThrottlesAndPublishesPauseChangesAtOnce(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	execs := countPipelines(rdb)
	d := New(cfg, testLogger(), rdb, newFakeStore())
	now := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return now }

	d.publishIfDue(context.Background()) // first pass
	now = now.Add(time.Second)
	d.publishIfDue(context.Background())
	if n := execs.Load(); n != 1 {
		t.Fatalf("publishes = %d, want 1 within 15 s", n)
	}
	d.noteStoreFailed(errors.New("boom"))
	d.publishIfDue(context.Background())
	if n := execs.Load(); n != 2 {
		t.Fatalf("publishes = %d, want the pause start published at once", n)
	}
	if h := stateHash(t, rdb, cfg); h["test-consumer.paused_since"] != "1700000001" {
		t.Fatalf("paused_since = %q", h["test-consumer.paused_since"])
	}
	now = now.Add(time.Second)
	d.noteStoreWorking(1)
	d.publishIfDue(context.Background())
	if h := stateHash(t, rdb, cfg); h["test-consumer.paused_since"] != "0" {
		t.Fatalf("paused_since = %q after resume, want 0", h["test-consumer.paused_since"])
	}
	now = now.Add(statePublishInterval)
	d.publishIfDue(context.Background())
	if n := execs.Load(); n != 4 {
		t.Fatalf("publishes = %d, want 4 after the interval", n)
	}
}

// TestRun_PausedDrainerPublishesState: the pass that waits out a pause still
// publishes, so a long pause stays visible.
func TestRun_PausedDrainerPublishesState(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := retryConfig(rdb.Options().Addr)
	store := newFakeStore()
	store.pingErr = errors.New("down")
	d := New(cfg, testLogger(), rdb, store)
	putInRetry(d, time.Hour)
	d.pausedSince = time.Now().Add(-time.Minute)
	d.lastStoreFail = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	d.pass(ctx)

	h := stateHash(t, rdb, cfg)
	if h["test-consumer.paused_since"] != strconv.FormatInt(d.pausedSince.Unix(), 10) {
		t.Fatalf("paused_since = %q, want %d", h["test-consumer.paused_since"], d.pausedSince.Unix())
	}
}

// countPipelines counts state publishes (MULTI/EXEC pipelines with an HSET) sent on rdb.
func countPipelines(rdb *redis.Client) *atomic.Int64 {
	var n atomic.Int64
	rdb.AddHook(pipelineCounter{n: &n})
	return &n
}

type pipelineCounter struct{ n *atomic.Int64 }

func (h pipelineCounter) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h pipelineCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h pipelineCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		// Only publishes: the client's connection setup is a pipeline too.
		for _, c := range cmds {
			if c.Name() == "hset" {
				h.n.Add(1)
				break
			}
		}
		return next(ctx, cmds)
	}
}
