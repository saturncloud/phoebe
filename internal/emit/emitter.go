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
//  2. Local-disk WAL — fsync'd JSONL, survives Valkey outages
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

	w, err := openWAL(cfg.WALPath)
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
// hand the event to the worker channel. If the channel is full it falls
// directly to the WAL (and to the log floor if the WAL also fails). It never
// blocks the caller.
func (e *DurableEmitter) Emit(_ context.Context, ev metering.Event) {
	ev.TimestampUnixMs = e.now().UnixMilli()

	// Fast path: hand off to channel.
	select {
	case e.ch <- ev:
		return
	default:
		// Channel full — fall through to WAL directly.
	}

	e.walOrLog(ev)
}

// Close drains in-flight events, stops background goroutines, and closes the
// WAL file. The provided context bounds the drain wait.
func (e *DurableEmitter) Close(ctx context.Context) error {
	close(e.stopCh)
	// Drain the channel by closing it after signalling workers.
	// Workers exit when stopCh is closed; remaining items are processed.
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
	return e.wal.close()
}

// worker drains the event channel and ships each event to Valkey.
// Falls to WAL on XADD error.
func (e *DurableEmitter) worker() {
	defer e.wg.Done()
	for {
		select {
		case ev, ok := <-e.ch:
			if !ok {
				return
			}
			e.xadd(ev)
		case <-e.stopCh:
			// Drain remaining events before exiting.
			for {
				select {
				case ev := <-e.ch:
					e.xadd(ev)
				default:
					return
				}
			}
		}
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
			// Final drain on shutdown.
			e.drainWAL()
			return
		}
	}
}

// drainWAL rotates the WAL aside to an immutable snapshot, ships every event in
// the snapshot to Valkey, and deletes the snapshot on full success. Rotation (vs
// read-then-truncate) is what makes this lossless: appends concurrent with the
// network ship land in the fresh WAL, never in the snapshot being shipped, so a
// successful drop can never destroy an unshipped event. Partial success leaves
// the snapshot in place to be re-shipped next tick — at-least-once delivery,
// deduped at the consumer by request_id.
func (e *DurableEmitter) drainWAL() {
	if e.rdb == nil {
		return
	}

	// Re-ship any snapshot orphaned by a crash between rotate and drop.
	if snap := e.wal.recoverRotated(); snap != "" {
		e.shipSnapshot(snap)
	}

	events, snapPath, err := e.wal.rotate()
	if err != nil {
		e.log.Error.Printf("emit: wal rotate: %v", err)
		// rotate may still have produced a snapshot + a prefix of events; if so,
		// fall through to ship what we got rather than abandon them.
		if snapPath == "" {
			return
		}
	}
	if snapPath == "" || len(events) == 0 {
		return
	}
	e.shipRotated(events, snapPath)
}

// shipRotated ships the events from a freshly-rotated snapshot and drops the
// snapshot file on full success.
func (e *DurableEmitter) shipRotated(events []metering.Event, snapPath string) {
	batchSize := e.cfg.ShipBatchSize
	if batchSize <= 0 {
		batchSize = 256
	}

	allShipped := true
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}
		if !e.shipBatch(events[i:end]) {
			allShipped = false
			break
		}
	}

	if allShipped {
		if err := e.wal.dropRotated(snapPath); err != nil {
			e.log.Error.Printf("emit: wal drop snapshot after drain: %v", err)
		} else {
			e.log.Info.Printf("emit: wal drained (%d events shipped)", len(events))
		}
	}
}

// shipSnapshot reads an orphaned snapshot file (left by a crash between rotate
// and drop) and re-ships it, dropping it on full success. Best-effort: a read
// error logs and leaves the file for the next attempt.
func (e *DurableEmitter) shipSnapshot(snapPath string) {
	events, err := readEventsAt(snapPath)
	if err != nil {
		e.log.Error.Printf("emit: read orphaned snapshot %s: %v", snapPath, err)
		return
	}
	if len(events) == 0 {
		_ = e.wal.dropRotated(snapPath)
		return
	}
	e.log.Info.Printf("emit: recovering %d events from orphaned wal snapshot", len(events))
	e.shipRotated(events, snapPath)
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
