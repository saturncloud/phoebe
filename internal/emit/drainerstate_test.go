package emit

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/metering"
)

// TestParseDrainerState: pause state counts only consumers seen within 120 s,
// the age is that of the newest seen time, and missing or unparsable fields
// read as absent rather than as a value.
func TestParseDrainerState(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	sec := func(d time.Duration) string { return itoa(now.Add(-d).Unix()) }
	for _, tc := range []struct {
		name   string
		fields map[string]string
		want   DrainerState
	}{
		{"fresh consumer not paused", map[string]string{"a.seen": sec(10 * time.Second), "a.paused_since": "0"},
			DrainerState{AnySeen: true, LastSeenAge: 10 * time.Second, Fresh: true}},
		{"fresh consumer paused", map[string]string{"a.seen": sec(5 * time.Second), "a.paused_since": sec(90 * time.Second), "dropped_poison": "3", "dropped_undecodable": "2"},
			DrainerState{PoisonDropped: 3, UndecodableDropped: 2, AnySeen: true, LastSeenAge: 5 * time.Second, Fresh: true, Paused: true, PausedFor: 90 * time.Second}},
		{"stale paused consumer does not count", map[string]string{"a.seen": sec(121 * time.Second), "a.paused_since": sec(time.Hour)},
			DrainerState{AnySeen: true, LastSeenAge: 121 * time.Second}},
		{"stale paused beside fresh running", map[string]string{
			"old.seen": sec(10 * time.Minute), "old.paused_since": sec(time.Hour),
			"new.seen": sec(2 * time.Second), "new.paused_since": "0"},
			DrainerState{AnySeen: true, LastSeenAge: 2 * time.Second, Fresh: true}},
		{"longest pause among fresh consumers", map[string]string{
			"a.seen": sec(time.Second), "a.paused_since": sec(30 * time.Second),
			"b.seen": sec(3 * time.Second), "b.paused_since": sec(300 * time.Second)},
			DrainerState{AnySeen: true, LastSeenAge: time.Second, Fresh: true, Paused: true, PausedFor: 300 * time.Second}},
		{"counters only", map[string]string{"dropped_poison": "4"},
			DrainerState{PoisonDropped: 4}},
		{"unparsable values ignored", map[string]string{"a.seen": "soon", "dropped_poison": "x"},
			DrainerState{}},
		{"future seen reads as age 0", map[string]string{"a.seen": itoa(now.Add(5 * time.Second).Unix())},
			DrainerState{AnySeen: true, Fresh: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseDrainerState(tc.fields, now); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// memoryInfo answers INFO memory, which miniredis does not support, and sends
// every other command to the real client.
type memoryInfo struct{ redis.Cmdable }

func (memoryInfo) Info(context.Context, ...string) *redis.StringCmd {
	return redis.NewStringResult("used_memory:10\r\nmaxmemory:20\r\n", nil)
}

// TestProbe_ReadsDrainerState: the probe reads the drainer's hash from the real
// key, and reports no state when the key does not exist.
func TestProbe_ReadsDrainerState(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t, mr)
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, testLogger(), memoryInfo{redisClient(mr.Addr())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	now := time.Unix(1_700_000_000, 0)
	em.now = func() time.Time { return now }

	p, err := em.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.Drainer != nil {
		t.Fatalf("Drainer = %+v with no key, want nil", p.Drainer)
	}

	mr.HSet(metering.DrainerStateKey(cfg.StreamName), "pod-1.seen", itoa(now.Unix()-4), "pod-1.paused_since", itoa(now.Unix()-60), "dropped_poison", "2")
	p, err = em.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := DrainerState{PoisonDropped: 2, AnySeen: true, LastSeenAge: 4 * time.Second, Fresh: true, Paused: true, PausedFor: time.Minute}
	if p.Drainer == nil || *p.Drainer != want {
		t.Fatalf("Drainer = %+v, want %+v", p.Drainer, want)
	}
}

// TestProbe_DrainerStateReadFailureDoesNotFailProbe: a failed read of the
// drainer hash leaves the drainer state unknown and the rest of the probe good.
func TestProbe_DrainerStateReadFailureDoesNotFailProbe(t *testing.T) {
	cfg := testConfig(t, nil)
	cfg.ShipInterval = time.Hour
	em, err := New(cfg, testLogger(), &fakeValkeyState{entries: 5, info: "used_memory:10\r\nmaxmemory:20\r\n", hashErr: errors.New("boom")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	p, err := em.Probe(context.Background())
	if err != nil {
		t.Fatalf("Probe = %v, want success despite the failed drainer read", err)
	}
	if p.Drainer != nil || p.StreamEntries != 5 || p.UsedMemory != 10 {
		t.Fatalf("probe = %+v, want stream and memory read and no drainer state", p)
	}
}
