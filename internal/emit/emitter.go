package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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

	// shipFn ships one batch to Valkey, returning true iff ALL events landed.
	// Defaults to shipBatch; tests inject failures here to exercise the
	// drain's partial-progress behaviour without timing games.
	shipFn func([]metering.Event) bool
}

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
	if e.cfg.StreamMaxLen > 0 {
		args.MaxLen = e.cfg.StreamMaxLen
		args.Approx = true
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
	data, _ := json.Marshal(ev)
	e.log.Error.Printf("METERING_FLOOR request_id=%s event=%s", ev.RequestID, data)
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

// drainWAL ships the WAL's unshipped entries to Valkey in ShipBatchSize
// batches, truncating after EACH successfully shipped batch — so progress made
// before a mid-drain failure is retained, never re-done wholesale. On a ship
// failure it stops: the remaining entries stay in the log and the next tick
// resumes from the watermark. Entries are only ever reclaimed after their
// batch is confirmed shipped, and concurrent appends land at higher indexes
// that truncation never touches — loss during a drain is structurally
// impossible. Delivery is at-least-once; the drainer dedups on request_id.
func (e *DurableEmitter) drainWAL() {
	if e.rdb == nil {
		return
	}

	batchSize := e.cfg.ShipBatchSize
	if batchSize <= 0 {
		batchSize = 256
	}

	for {
		events, through, skipped, err := e.wal.pending(batchSize)
		if err != nil {
			e.log.Error.Printf("emit: wal read for drain: %v", err)
			return
		}
		if skipped > 0 {
			e.log.Error.Printf("emit: wal drain: skipped %d corrupt entries (undecodable; truncating past them)", skipped)
		}
		if through == 0 {
			return // nothing unshipped
		}
		if len(events) > 0 && !e.shipFn(events) {
			return // ship failed — entries remain; next tick resumes here
		}
		if err := e.wal.markShipped(through); err != nil {
			e.log.Error.Printf("emit: wal truncate after ship: %v", err)
			return
		}
		if len(events) > 0 {
			e.log.Info.Printf("emit: wal drained batch (%d events shipped)", len(events))
		}
	}
}

// shipBatch ships a slice of events to Valkey. Returns true iff ALL succeeded.
func (e *DurableEmitter) shipBatch(events []metering.Event) bool {
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			e.log.Error.Printf("emit: shipper marshal request_id=%s: %v", ev.RequestID, err)
			continue // skip corrupt entry
		}

		args := &redis.XAddArgs{
			Stream: e.cfg.StreamName,
			Values: map[string]interface{}{"event": string(data)},
		}
		if e.cfg.StreamMaxLen > 0 {
			args.MaxLen = e.cfg.StreamMaxLen
			args.Approx = true
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
			return false
		}
	}
	return true
}
