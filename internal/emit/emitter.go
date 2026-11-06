package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// DurableEmitter implements metering.Emitter with the three-level durability
// ladder:
//
//  1. Valkey Streams (XADD) — hot buffer, async, non-blocking
//  2. Local-disk WAL — fsync'd sequential log (tidwall/wal), survives Valkey outages
//  3. Structured log — last resort if disk also fails; log pipeline can recover
//
// Emit is always non-blocking: it hands the event to an internal channel and
// returns immediately. Background workers drain the channel and do the actual
// I/O. If the channel is full, the event falls straight to the WAL (and then
// the log if the WAL fails too).
//
// Close drains in-flight events and flushes the WAL on graceful shutdown.
//
// XADD carries no MAXLEN: a length cap removes the oldest entries whether or not
// they were delivered, so it could only ever lose billing records. The drainer
// trims what it has delivered (internal/drain), and Valkey's maxmemory bounds the
// rest: a refused XADD falls to the WAL. CheckBufferBound warns when a Valkey does
// not enforce that bound.
type DurableEmitter struct {
	cfg    Config
	log    *logging.Logger
	rdb    redis.Cmdable
	wal    *wal
	ch     chan metering.Event
	now    func() time.Time
	wg     sync.WaitGroup
	stopCh chan struct{}

	// closeMu + closed guard the Emit/Close race: Emit sends on ch under the
	// read lock, Close closes ch under the write lock — so a send can never
	// hit a closed channel, and an Emit that observes closed goes straight to
	// the WAL instead of a channel no worker will ever drain again.
	closeMu sync.RWMutex
	closed  bool

	// shipFn ships one batch to Valkey, returning how many leading events are
	// handled; len(events) means all of them. Defaults to shipBatch; tests inject
	// failures here to exercise the drain's partial-progress behaviour without
	// timing games.
	shipFn func([]metering.Event) int

	// floorEvents counts events that reached the log floor: the log line is then
	// the only copy of that billing record.
	floorEvents atomic.Uint64

	// skippedEntries counts undecodable WAL entries the shipper truncated past:
	// those billing records are gone from the WAL.
	skippedEntries atomic.Uint64
}

var _ metering.Emitter = (*DurableEmitter)(nil)

// WALPending returns how many events wait in this replica's WAL for Valkey, and
// whether that count can be trusted. See wal.pendingCount.
func (e *DurableEmitter) WALPending() (uint64, bool) { return e.wal.pendingCount() }

// WALPoisoned reports whether this replica's WAL has no open log because the
// recovery reopen is failing.
func (e *DurableEmitter) WALPoisoned() bool { return e.wal.isPoisoned() }

// WALQuarantinedDirs returns how many corrupt WAL directories were moved aside
// and are waiting for manual recovery. A negative value means the directory could
// not be read and the count is unknown.
func (e *DurableEmitter) WALQuarantinedDirs() int { return e.wal.quarantinedDirs() }

// WALSkippedEntries returns how many undecodable WAL entries the shipper has
// truncated past since start.
func (e *DurableEmitter) WALSkippedEntries() uint64 { return e.skippedEntries.Load() }

// FloorEvents returns how many events reached the log floor since start.
func (e *DurableEmitter) FloorEvents() uint64 { return e.floorEvents.Load() }

// New creates and starts a DurableEmitter.
//
// rdb may be nil to force immediate WAL fallback (useful for testing the WAL
// path without a Valkey server). If rdb is nil the emitter still initialises
// correctly; XADD will never be attempted.
func New(cfg Config, log *logging.Logger, rdb redis.Cmdable) (*DurableEmitter, error) {
	if log == nil {
		return nil, fmt.Errorf("emit.New: logger must not be nil")
	}

	w, err := openWAL(cfg.WALPath, log)
	if err != nil {
		return nil, fmt.Errorf("emit.New: %w", err)
	}

	e := &DurableEmitter{
		cfg:    cfg,
		log:    log,
		rdb:    rdb,
		wal:    w,
		ch:     make(chan metering.Event, cfg.ChanBuf),
		now:    time.Now,
		stopCh: make(chan struct{}),
	}
	e.shipFn = e.shipBatch

	n := cfg.WorkerCount
	if n <= 0 {
		n = 1
	}
	for range n {
		e.wg.Add(1)
		go e.worker()
	}

	e.wg.Add(1)
	go e.shipper()

	return e, nil
}

// NewWithClock is like New but injects the clock — used by tests to make
// TimestampUnixMs deterministic.
func NewWithClock(cfg Config, log *logging.Logger, rdb redis.Cmdable, now func() time.Time) (*DurableEmitter, error) {
	em, err := New(cfg, log, rdb)
	if err != nil {
		return nil, err
	}
	em.now = now
	return em, nil
}

// Emit satisfies metering.Emitter. It stamps TimestampUnixMs, then tries to
// hand the event to the worker channel. If the channel is full — or the
// emitter is closed, so no worker will ever drain the channel again — it
// falls directly to the WAL (and to the log floor if the WAL also fails).
// It never blocks the caller, and an Emit racing Close never strands an
// event: it always lands in Valkey, the WAL, or the log floor.
func (e *DurableEmitter) Emit(_ context.Context, ev metering.Event) {
	ev.TimestampUnixMs = e.now().UnixMilli()

	// Fast path: hand off to channel. The read lock pairs with Close's write
	// lock so the send cannot race the close(e.ch).
	e.closeMu.RLock()
	if !e.closed {
		select {
		case e.ch <- ev:
			e.closeMu.RUnlock()
			return
		default:
			// Channel full — fall through to WAL directly.
		}
	}
	e.closeMu.RUnlock()

	e.walOrLog(ev)
}

// Close flushes every in-flight event, stops background goroutines, makes a
// final attempt to ship the WAL, and closes it. The provided context bounds
// the wait. Order matters for the no-stranding guarantee:
//
//  1. mark closed + close(e.ch) under the write lock — subsequent Emits go
//     straight to the WAL/floor, and no send can hit the closed channel;
//  2. workers range the channel to completion, so every queued event is
//     processed (xadd, falling to WAL/floor on error) — the old code's
//     `default: return` exited the instant the channel was momentarily
//     empty, stranding events a racing Emit had just queued;
//  3. only after the workers are done, a final drainWAL ships what fell to
//     disk during shutdown, then the WAL is closed.
func (e *DurableEmitter) Close(ctx context.Context) error {
	e.closeMu.Lock()
	if e.closed {
		e.closeMu.Unlock()
		return nil
	}
	e.closed = true
	close(e.ch)
	e.closeMu.Unlock()

	close(e.stopCh) // stop the shipper ticker

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	e.drainWAL()
	return e.wal.close()
}

// worker drains the event channel and ships each event to Valkey, falling to
// the WAL on XADD error. It exits only when the channel is closed AND fully
// drained — never on "momentarily empty", which would strand racing Emits.
func (e *DurableEmitter) worker() {
	defer e.wg.Done()
	for ev := range e.ch {
		e.xadd(ev)
	}
}

// xadd attempts XADD to Valkey. Falls to WAL on error.
func (e *DurableEmitter) xadd(ev metering.Event) {
	if e.rdb == nil {
		e.walOrLog(ev)
		return
	}

	data, err := json.Marshal(ev)
	if err != nil {
		e.log.Error.Printf("emit: marshal event request_id=%s: %v", ev.RequestID, err)
		e.walOrLog(ev)
		return
	}

	args := &redis.XAddArgs{
		Stream: e.cfg.StreamName,
		Values: map[string]interface{}{"event": string(data)},
	}

	timeout := e.cfg.XADDTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := e.rdb.XAdd(ctx, args).Result(); err != nil {
		e.log.Warn.Printf("emit: xadd request_id=%s: %v — falling to WAL", ev.RequestID, err)
		e.walOrLog(ev)
	}
}

// walOrLog writes the event to the WAL. If the WAL also fails, logs it as a
// structured line so a log pipeline can recover it (last resort — never drop).
func (e *DurableEmitter) walOrLog(ev metering.Event) {
	if err := e.wal.append(ev); err != nil {
		e.log.Error.Printf("emit: wal append request_id=%s: %v — emitting to log floor", ev.RequestID, err)
		e.logFloor(ev)
	}
}

// logFloor is the absolute last resort: a structured log line.
// It is parseable by a log-pipeline scraper for manual recovery.
func (e *DurableEmitter) logFloor(ev metering.Event) {
	e.floorEvents.Add(1)
	logFloorEvent(e.log, ev)
}

// shipper periodically drains the WAL into Valkey on recovery.
// It runs until stopCh is closed.
func (e *DurableEmitter) shipper() {
	defer e.wg.Done()

	interval := e.cfg.ShipInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.drainWAL()
		case <-e.stopCh:
			// No final drain here: Close runs it after the workers have
			// flushed the channel, so shutdown-window events ship too.
			return
		}
	}
}

// drainWAL ships the WAL's unshipped entries to Valkey in ShipBatchSize batches,
// keeping progress per event: everything confirmed shipped is marked before the
// next attempt, including the leading events of a batch that failed partway, so
// shipped events are not sent again. This matters on a full Valkey, where a
// refused XADD is the normal outcome and a resend would spend freed memory on
// duplicates. On a ship failure it stops: the remaining entries stay in the log
// and the next tick resumes from the watermark. Entries are only ever reclaimed
// after they are confirmed shipped, and concurrent appends land at higher indexes
// that truncation never touches — loss during a drain is structurally impossible.
// Delivery is at-least-once; the drainer dedups on request_id.
func (e *DurableEmitter) drainWAL() {
	if e.rdb == nil {
		return
	}

	batchSize := e.cfg.ShipBatchSize
	if batchSize <= 0 {
		batchSize = 256
	}

	for {
		events, indexes, through, skipped, err := e.wal.pending(batchSize)
		if err != nil {
			e.log.Error.Printf("emit: wal read for drain: %v", err)
			return
		}
		if len(skipped) > 0 {
			e.log.Error.Printf("emit: wal drain: skipped %d corrupt entries (undecodable; truncating past them)", len(skipped))
		}
		if through == 0 {
			return // nothing unshipped
		}
		if len(events) > 0 {
			if n := e.shipFn(events); n < len(events) {
				if n > 0 {
					e.markPartialBatch(indexes[n-1], n, skipped)
				}
				return // ship failed — the rest remains; next tick resumes here
			}
		}
		if err := e.wal.markShipped(through); err != nil {
			e.log.Error.Printf("emit: wal truncate after ship: %v", err)
			return
		}
		e.skippedEntries.Add(uint64(len(skipped)))
		if len(events) > 0 {
			e.log.Info.Printf("emit: wal drained batch (%d events shipped)", len(events))
		}
	}
}

// markPartialBatch records a batch that stopped after n events, the last of them
// at WAL index shippedThrough. skipped holds the batch's uncounted corrupt indexes.
// The skip counter only takes the ones the watermark passes: the rest are read again
// next tick and must not count twice.
func (e *DurableEmitter) markPartialBatch(shippedThrough uint64, n int, skipped []uint64) {
	if err := e.wal.markShipped(shippedThrough); err != nil {
		// The watermark did not move, so the next tick re-reads these entries and
		// counts them then. Re-shipping the prefix is safe: the drainer dedups on
		// request_id. Counting here would count them twice.
		e.log.Error.Printf("emit: wal truncate after partial ship: %v", err)
		return
	}
	for _, index := range skipped {
		if index <= shippedThrough {
			e.skippedEntries.Add(1)
		}
	}
	e.log.Info.Printf("emit: wal drained partial batch (%d events shipped)", n)
}

// shipBatch ships a slice of events to Valkey and returns how many leading events
// are handled, stopping at the first refused XADD. An event that cannot be
// marshaled counts as handled: retrying cannot make it succeed.
func (e *DurableEmitter) shipBatch(events []metering.Event) int {
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			e.log.Error.Printf("emit: shipper marshal request_id=%s: %v", ev.RequestID, err)
			continue // skip corrupt entry
		}

		args := &redis.XAddArgs{
			Stream: e.cfg.StreamName,
			Values: map[string]interface{}{"event": string(data)},
		}

		timeout := e.cfg.XADDTimeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, shipErr := e.rdb.XAdd(ctx, args).Result()
		cancel()

		if shipErr != nil {
			e.log.Warn.Printf("emit: shipper xadd request_id=%s: %v", ev.RequestID, shipErr)
			return i
		}
	}
	return len(events)
}

// MeteringProbe is one read of the metering buffer's health.
type MeteringProbe struct {
	StreamEntries int64 // entries in the stream, delivered or not
	Undelivered   int64 // entries not yet stored: pending plus not yet read
	UsedMemory    int64 // Valkey used_memory, bytes
	MaxMemory     int64 // Valkey maxmemory, bytes; 0 means no cap
}

// Probe reads the stream and Valkey's memory. Every command it runs still works
// when Valkey is full. It fails when no Valkey is configured or reachable.
func (e *DurableEmitter) Probe(ctx context.Context) (MeteringProbe, error) {
	var p MeteringProbe
	if e.rdb == nil {
		return p, fmt.Errorf("emit: no valkey configured")
	}
	var err error
	if p.StreamEntries, err = e.rdb.XLen(ctx, e.cfg.StreamName).Result(); err != nil {
		return p, fmt.Errorf("xlen: %w", err)
	}
	// Memory is read before the groups, so a slow unknown-lag scan cannot use up
	// the context INFO needs and turn the whole probe into an error.
	info, err := e.rdb.Info(ctx, "memory").Result()
	if err != nil {
		return p, fmt.Errorf("info memory: %w", err)
	}
	p.UsedMemory = infoInt(info, "used_memory")
	p.MaxMemory = infoInt(info, "maxmemory")
	groups, err := e.rdb.XInfoGroups(ctx, e.cfg.StreamName).Result()
	if err != nil {
		if !strings.Contains(err.Error(), "no such key") {
			return p, fmt.Errorf("xinfo groups: %w", err)
		}
		groups = nil
	}
	if len(groups) == 0 {
		p.Undelivered = p.StreamEntries // no consumer yet: nothing has been read
	}
	for _, g := range groups {
		undelivered := g.Pending + g.Lag
		if g.Lag < 0 {
			undelivered = e.undeliveredWithUnknownLag(ctx, g, p.StreamEntries)
		}
		if undelivered > p.Undelivered {
			p.Undelivered = undelivered // the group furthest behind
		}
	}
	return p, nil
}

// The scan that replaces an unknown lag runs on every metrics scrape of every
// replica, against a Valkey that may already be near maxmemory, and its replies
// carry whole entries. Client output buffers count toward used_memory, so the scan
// is kept small:
//   - unknownLagScanLimit bounds the total. The scan runs only when the stream
//     holds fewer entries than this; otherwise the stream length is reported as
//     the upper bound without scanning. A backlog at or over the limit therefore
//     reads as the stream length, which still fires PhoebeMeteringDrainerBehind.
//   - unknownLagScanPage bounds what one reply carries.
//   - unknownLagScanTimeout bounds how long the scan may take, separately from the
//     probe's own deadline.
const (
	unknownLagScanLimit   = 1000
	unknownLagScanPage    = 500
	unknownLagScanTimeout = 500 * time.Millisecond
)

// undeliveredWithUnknownLag handles a group whose lag Valkey could not compute
// (go-redis reports -1, for example after XDEL or XTRIM MINID). Unknown must not
// read as caught up, because this number feeds PhoebeMeteringDrainerBehind. The
// stream length overstates it, since it includes entries delivered and acked but not
// yet trimmed, so when the stream is shorter than unknownLagScanLimit this counts
// the entries after the group's last delivered ID, one page at a time. It reports
// the stream length as the upper bound when the stream is at or over the limit
// (without scanning), or when the scan fails, times out, or reaches the limit.
func (e *DurableEmitter) undeliveredWithUnknownLag(ctx context.Context, g redis.XInfoGroup, streamEntries int64) int64 {
	if streamEntries >= unknownLagScanLimit {
		return streamEntries
	}
	scanCtx, cancel := context.WithTimeout(ctx, unknownLagScanTimeout)
	defer cancel()
	start := "(" + g.LastDeliveredID
	var count int64
	for {
		if err := scanCtx.Err(); err != nil {
			e.log.Warn.Printf("emit: count entries after group %s position %s: %v; reporting the stream length", g.Name, g.LastDeliveredID, err)
			return streamEntries
		}
		page, err := e.rdb.XRangeN(scanCtx, e.cfg.StreamName, start, "+", unknownLagScanPage).Result()
		if err != nil {
			e.log.Warn.Printf("emit: count entries after group %s position %s: %v; reporting the stream length", g.Name, g.LastDeliveredID, err)
			return streamEntries
		}
		count += int64(len(page))
		if len(page) < unknownLagScanPage {
			return g.Pending + count
		}
		if count >= unknownLagScanLimit {
			return streamEntries
		}
		start = "(" + page[len(page)-1].ID
	}
}

// CheckBufferBound warns when Valkey does not enforce the bound this emitter
// relies on: XADD carries no MAXLEN, so maxmemory with noeviction is what stops the
// stream growing and makes a full Valkey refuse writes instead of dropping entries.
// It only warns, and never stops startup.
func (e *DurableEmitter) CheckBufferBound(ctx context.Context) {
	if e.rdb == nil {
		return
	}
	info, err := e.rdb.Info(ctx, "memory").Result()
	if err != nil {
		e.log.Warn.Printf("emit: skipped the valkey memory limit check: %v", err)
		return
	}
	if infoInt(info, "maxmemory") == 0 {
		e.log.Warn.Printf("emit: valkey maxmemory is 0: the metering stream has no cap, and an undrained backlog can grow until Valkey is OOM-killed, losing events")
	}
	switch policy, ok := infoString(info, "maxmemory_policy"); {
	case !ok:
		e.log.Warn.Printf("emit: could not read valkey maxmemory_policy: confirm it is noeviction, or Valkey can evict the metering stream")
	case policy != "noeviction":
		e.log.Warn.Printf("emit: valkey maxmemory_policy is %s, not noeviction: Valkey can evict the metering stream", policy)
	}
}

// infoInt reads one integer field from an INFO reply. Missing reads as 0.
func infoInt(info, field string) int64 {
	value, ok := infoString(info, field)
	if !ok {
		return 0
	}
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// infoString reads one field from an INFO reply.
func infoString(info, field string) (string, bool) {
	for _, line := range strings.Split(info, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && key == field {
			return value, true
		}
	}
	return "", false
}
