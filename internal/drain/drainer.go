package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// Drainer consumes the metering stream via a consumer group and writes each
// event to the Store. One Drainer == one consumer in the group.
type Drainer struct {
	cfg   Config
	log   *logging.Logger
	rdb   redis.Cmdable
	store Store

	// lastClaim throttles XAUTOCLAIM so reclaim runs ~every ClaimInterval rather
	// than on every loop pass.
	lastClaim time.Time
	now       func() time.Time

	// poisoned counts events that could not be stored even row-at-a-time
	// against a healthy store and were dropped (logged + ACK'd). A non-zero
	// value means billing rows were lost to the log; see upsertRowAtATime.
	poisoned atomic.Uint64

	// schemaPoll is the pause between schema-version probes in waitForSchema,
	// and schemaProbeTimeout bounds each probe. Tests shorten them.
	schemaPoll         time.Duration
	schemaProbeTimeout time.Duration
}

// Poisoned returns the number of events dropped as store-poison (failed their
// own single-row INSERT while the store was reachable) since start.
func (d *Drainer) Poisoned() uint64 { return d.poisoned.Load() }

// New constructs a Drainer. The group is created lazily on Run (XGROUP CREATE
// ... MKSTREAM) so the drainer can start before the emitter has produced
// anything.
func New(cfg Config, log *logging.Logger, rdb redis.Cmdable, store Store) *Drainer {
	return &Drainer{
		cfg:   cfg,
		log:   log,
		rdb:   rdb,
		store: store,
		now:   time.Now,

		schemaPoll:         2 * time.Second,
		schemaProbeTimeout: 10 * time.Second,
	}
}

// ensureGroup creates the consumer group if it does not already exist, creating
// the stream too (MKSTREAM). "$" starts the group at the stream tail, but since
// the emitter may already have produced entries we use "0" so a freshly created
// group still drains the existing backlog — no metering event is skipped.
//
// BUSYGROUP (group already exists) is the normal steady-state case and is not
// an error.
func (d *Drainer) ensureGroup(ctx context.Context) error {
	err := d.rdb.XGroupCreateMkStream(ctx, d.cfg.StreamName, d.cfg.Group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("drain: create group %q on %q: %w", d.cfg.Group, d.cfg.StreamName, err)
	}
	return nil
}

// isNoGroup reports Valkey's NOGROUP reply: the stream or its consumer group
// is gone (an in-memory Valkey restarted, or the key was deleted). The group
// is recreated instead of failing every read until a pod restart.
func isNoGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOGROUP")
}

// schemaReady reports whether a database at version (dirty or not) has every
// migration a binary that needs required fully applied. golang-migrate records
// the version it was ATTEMPTING and marks it dirty when that migration fails,
// so a dirty version V means V-1 and everything below it are complete: a dirty
// row blocks only at or below the required version. A failed LATER migration
// (dirty V > required) leaves this binary's schema intact; migrations are
// expand-only (migrations/README.md), so the drainer of the running release
// keeps consuming.
func schemaReady(version uint, dirty bool, required uint) bool {
	if dirty {
		return version > required
	}
	return version >= required
}

// waitForSchema blocks until the store's schema has every migration this
// binary needs (RequiredSchemaVersion; see schemaReady for the dirty rule),
// logging while it waits. During an upgrade the new drainer can start before
// the migrate job has run; writing then would put new columns into the old
// table.
//
// Waiting is not free: the emitter caps the stream (MAXLEN ~1M entries,
// internal/emit), so a long wait trims the oldest un-drained events. The wait
// is therefore bounded by SchemaWaitTimeout, after which waitForSchema returns
// an error so Run fails, the process exits non-zero and the pod restarts
// visibly (CrashLoopBackOff) instead of idling silently. Each probe has its own
// deadline (schemaProbeTimeout) so a hung query surfaces as an error in the
// warning log rather than a silent stall.
//
// Returns nil when the gate passes (or RequiredSchemaVersion is 0, which logs a
// warning that the gate is disabled), the
// context error when ctx ends, or the timeout error.
func (d *Drainer) waitForSchema(ctx context.Context) error {
	required := d.cfg.RequiredSchemaVersion
	if required == 0 {
		d.log.Warn.Printf("drainer: RequiredSchemaVersion is 0, so the schema gate is disabled (tests only); consuming without checking the database schema")
		return nil
	}
	start := d.now()
	var lastLog time.Time
	for {
		qctx, cancel := context.WithTimeout(ctx, d.schemaProbeTimeout)
		version, dirty, err := d.store.SchemaVersion(qctx)
		cancel()
		if err == nil && schemaReady(version, dirty, required) {
			if dirty {
				d.log.Warn.Printf("drainer: database schema at version %d is dirty (a newer migration failed partway); version %d this binary needs is intact and migrations are expand-only, so consuming",
					version, required)
			} else {
				d.log.Info.Printf("drainer: database schema at version %d (need %d); consuming", version, required)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if waited := d.now().Sub(start); waited >= d.cfg.SchemaWaitTimeout {
			return fmt.Errorf("drain: database schema not at version %d after %s (at %d, dirty=%v, err=%v); exiting so the pod restarts and alerts",
				required, waited, version, dirty, err)
		}
		if lastLog.IsZero() || d.now().Sub(lastLog) >= 30*time.Second {
			lastLog = d.now()
			d.log.Warn.Printf("drainer: waiting for database schema version %d (at %d, dirty=%v, err=%v); events wait in the stream, which the emitter caps at about %d entries (MAXLEN ~), so the oldest unconsumed events are trimmed if the migration does not run; exiting after %s",
				required, version, dirty, err, streamMaxLenHint, d.cfg.SchemaWaitTimeout)
		}
		d.sleep(ctx, d.schemaPoll)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// streamMaxLenHint is the emitter's default stream cap (emit.Config
// StreamMaxLen), quoted in the schema-wait warning. It is a log hint only.
const streamMaxLenHint = 1_000_000

func isBusyGroup(err error) bool {
	// go-redis surfaces the BUSYGROUP reply as a plain error string.
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists")
}

// Run drives the drain loop until ctx is cancelled (SIGTERM). On each pass it:
//  1. opportunistically reclaims entries stranded by a dead consumer (XAUTOCLAIM),
//  2. blocks on XREADGROUP for a batch of new entries,
//  3. stores the batch durably, THEN
//  4. XACKs the batch (never before the store commits).
//
// On ctx cancellation it returns after finishing any in-flight batch — an entry
// that was read but not yet stored is simply left un-ACK'd and redelivers to the
// next run (safe: idempotent store).
func (d *Drainer) Run(ctx context.Context) error {
	if err := d.waitForSchema(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if err := d.ensureGroup(ctx); err != nil {
		return err
	}
	d.log.Info.Printf("drainer: consuming stream=%s group=%s consumer=%s batch=%d",
		d.cfg.StreamName, d.cfg.Group, d.cfg.Consumer, d.cfg.BatchSize)

	for {
		if ctx.Err() != nil {
			d.log.Info.Printf("drainer: context cancelled, shutting down cleanly")
			return nil
		}

		// Opportunistic reclaim of dead-consumer pending entries.
		if d.now().Sub(d.lastClaim) >= d.cfg.ClaimInterval {
			d.reclaim(ctx)
			d.lastClaim = d.now()
		}

		n, err := d.readAndStore(ctx)
		if err != nil {
			// On a transient error, log and back off briefly rather than spin.
			// A cancelled context surfaces here too; the top-of-loop check exits.
			if ctx.Err() != nil {
				return nil
			}
			d.log.Error.Printf("drainer: drain pass: %v", err)
			d.sleep(ctx, 500*time.Millisecond)
			continue
		}
		// If we got a full batch there may be more backlog — loop immediately
		// without re-blocking. An empty/partial read means we've caught up.
		_ = n
	}
}

// readAndStore performs one XREADGROUP → store → XACK cycle for NEW entries
// (the ">" ID). Returns the number of entries processed.
func (d *Drainer) readAndStore(ctx context.Context) (int, error) {
	res, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    d.cfg.Group,
		Consumer: d.cfg.Consumer,
		Streams:  []string{d.cfg.StreamName, ">"},
		Count:    int64(d.cfg.BatchSize),
		Block:    d.cfg.BlockTimeout,
	}).Result()

	if err != nil {
		// redis.Nil means the block elapsed with no new entries — normal idle.
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		if isNoGroup(err) {
			// The stream or group vanished (Valkey restarted without
			// persistence). Recreate it from the start of whatever stream now
			// exists so nothing emitted since is skipped (QA B22 drill).
			if gerr := d.ensureGroup(ctx); gerr != nil {
				return 0, fmt.Errorf("xreadgroup: %w; recreate group: %v", err, gerr)
			}
			d.log.Warn.Printf("drainer: consumer group %q on %q was missing (%v); recreated", d.cfg.Group, d.cfg.StreamName, err)
			return 0, nil
		}
		return 0, fmt.Errorf("xreadgroup: %w", err)
	}

	var msgs []redis.XMessage
	for _, st := range res {
		msgs = append(msgs, st.Messages...)
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	return d.process(ctx, msgs)
}

// process decodes, stores, and ACKs a batch of stream messages. It is shared by
// the new-entry path and the reclaim path so both honour the
// store-before-ACK invariant identically.
func (d *Drainer) process(ctx context.Context, msgs []redis.XMessage) (int, error) {
	events := make([]metering.Event, 0, len(msgs))
	ackIDs := make([]string, 0, len(msgs))
	var poison []string // malformed entries: ACK to drop, never block the group.

	for _, m := range msgs {
		ev, err := decodeEvent(m)
		if err != nil {
			// A malformed entry can never be stored; if we never ACK it, it
			// redelivers forever and wedges the group (poison-pill). ACK it to
			// drop it, but log loudly so it's recoverable from logs.
			d.log.Error.Printf("drainer: drop malformed entry id=%s: %v", m.ID, err)
			poison = append(poison, m.ID)
			continue
		}
		events = append(events, ev)
		ackIDs = append(ackIDs, m.ID)
	}

	// Drop poison entries FIRST, before the store work — they are malformed and
	// unstorable BY DEFINITION, so dropping them is correct regardless of store
	// health. Doing it here (rather than after the store) means a store-outage
	// early-return below still drops them: otherwise the poison would redeliver
	// every pass until an outage-free run, re-logging the same bad entries and
	// wasting reclaim work, with no upside (they can never be stored). An ACK
	// failure here is harmless — they redeliver and get dropped again, no data
	// loss (the entries carry no storable billing data).
	if len(poison) > 0 {
		if err := d.ack(ctx, poison); err != nil {
			d.log.Warn.Printf("drainer: ack poison entries: %v", err)
		}
	}

	stored := len(events)
	if len(events) > 0 {
		// Store MUST commit before we ACK. The single batch statement is the
		// fast path; if it fails we do NOT treat the error as wholesale
		// transient — the batch INSERT is all-or-nothing, so one persistently
		// bad row (e.g. a value the schema rejects) would fail it forever and
		// the un-ACK'd batch would redeliver forever, starving every innocent
		// event batched with it. Fall back to row-at-a-time to isolate it.
		if err := d.store.Upsert(ctx, events); err != nil {
			d.log.Warn.Printf("drainer: batch upsert of %d failed, isolating row-at-a-time: %v", len(events), err)
			var rerr error
			ackIDs, stored, rerr = d.upsertRowAtATime(ctx, events, ackIDs)
			if rerr != nil {
				// Store outage (or cancellation), not a bad row. ACK only what
				// is known durable; the rest stays pending and redelivers.
				if aerr := d.ack(ctx, ackIDs); aerr != nil {
					d.log.Warn.Printf("drainer: ack after partial row-at-a-time: %v", aerr)
				}
				return stored, fmt.Errorf("store batch of %d: %w", len(events), rerr)
			}
		}
		if err := d.ack(ctx, ackIDs); err != nil {
			// The store committed but the ACK failed. Safe: the entries
			// redeliver and the idempotent upsert no-ops them. Surface as error
			// so the loop backs off, but the data is already durable.
			return stored, fmt.Errorf("ack after store: %w", err)
		}
	}

	return stored, nil
}

// upsertRowAtATime is the poison-isolation slow path, taken only after a batch
// Upsert failed. Each event is retried in its own statement: rows that succeed
// are durable as usual; a row that fails ON ITS OWN while the store is
// otherwise healthy can never be stored, so it is POISON — logged loudly (the
// full event JSON at ERROR, same spirit as the decode-poison path in process:
// the log line becomes the only remaining copy of a billing record), counted,
// and included in the returned ack set so the stream keeps draining instead of
// redelivering it forever.
//
// "Otherwise healthy" is the fail-closed guard: a store OUTAGE also fails
// every individual row, and declaring those poison would ACK-and-drop an
// entire batch of good billing data. So after an individual failure we Ping;
// if the store is unreachable we abort the walk and return an error — the
// caller ACKs only the rows already stored and the remainder redelivers.
//
// Returns the stream ids safe to ACK (stored rows + poison rows), the number
// of rows actually stored, and a non-nil error only for outage/cancellation.
func (d *Drainer) upsertRowAtATime(ctx context.Context, events []metering.Event, ids []string) (ackable []string, stored int, err error) {
	ackable = make([]string, 0, len(events))
	for i := range events {
		uerr := d.store.Upsert(ctx, events[i:i+1])
		if uerr == nil {
			ackable = append(ackable, ids[i])
			stored++
			continue
		}
		if ctx.Err() != nil {
			return ackable, stored, ctx.Err()
		}
		if perr := d.store.Ping(ctx); perr != nil {
			return ackable, stored, fmt.Errorf("store unreachable during row-at-a-time (row error: %v): %w", uerr, perr)
		}
		if isSchemaNotReady(uerr) {
			// The schema is behind this binary (a missing table or column),
			// not a bad row: every row would fail the same way. Like an
			// outage, ACK only what is stored; the rest redelivers once the
			// migration has run. Never drop billing data over a schema gap.
			return ackable, stored, fmt.Errorf("database schema not ready (row error): %w", uerr)
		}
		// POISON: the store is up but this row cannot be inserted. Log the
		// full event so it is recoverable from logs, count it, ACK it.
		raw, merr := json.Marshal(events[i])
		if merr != nil {
			raw = []byte(fmt.Sprintf("%+v", events[i]))
		}
		n := d.poisoned.Add(1)
		d.log.Error.Printf("drainer: drop poison row id=%s request_id=%s (total poisoned=%d): %v event=%s",
			ids[i], events[i].RequestID, n, uerr, raw)
		ackable = append(ackable, ids[i])
	}
	return ackable, stored, nil
}

// ack XACKs the given IDs in one call. Empty input is a no-op.
func (d *Drainer) ack(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return d.rdb.XAck(ctx, d.cfg.StreamName, d.cfg.Group, ids...).Err()
}

// reclaim steals entries that have sat unacknowledged in ANY consumer's PEL for
// longer than ClaimMinIdle — i.e. work stranded by a crashed consumer — and
// processes them through the same store-then-ACK path. It walks the cursor once
// per call; the next call continues if there's more.
func (d *Drainer) reclaim(ctx context.Context) {
	msgs, _, err := d.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   d.cfg.StreamName,
		Group:    d.cfg.Group,
		Consumer: d.cfg.Consumer,
		MinIdle:  d.cfg.ClaimMinIdle,
		Start:    "0",
		Count:    int64(d.cfg.BatchSize),
	}).Result()
	if err != nil {
		// Reclaim is best-effort; a failure here must not stop the main loop.
		// A missing group is recreated by the read path.
		d.log.Warn.Printf("drainer: xautoclaim: %v", err)
		return
	}
	if len(msgs) == 0 {
		return
	}
	if n, err := d.process(ctx, msgs); err != nil {
		d.log.Error.Printf("drainer: reclaim process: %v", err)
	} else if n > 0 {
		d.log.Info.Printf("drainer: reclaimed %d stranded entr(ies)", n)
	}
}

// decodeEvent extracts the JSON metering.Event from a stream message. The
// emitter XADDs it under the "event" field as a JSON string (see
// internal/emit/emitter.go xadd); we MUST read the same shape.
func decodeEvent(m redis.XMessage) (metering.Event, error) {
	var ev metering.Event
	raw, ok := m.Values["event"]
	if !ok {
		return ev, fmt.Errorf("entry %s missing 'event' field", m.ID)
	}
	s, ok := raw.(string)
	if !ok {
		return ev, fmt.Errorf("entry %s 'event' field is %T, want string", m.ID, raw)
	}
	// UnmarshalEvent maps only an absent serving_mode key (pre-cutover
	// evidence) to "dedicated"; an explicit "" or null is kept as "" so the
	// rater withholds it.
	ev, err := metering.UnmarshalEvent([]byte(s))
	if err != nil {
		return ev, fmt.Errorf("entry %s unmarshal: %w", m.ID, err)
	}
	if ev.RequestID == "" {
		return ev, fmt.Errorf("entry %s decoded event has empty request_id (idempotency key)", m.ID)
	}
	return ev, nil
}

// sleep waits for d, returning early if ctx is cancelled.
func (d *Drainer) sleep(ctx context.Context, dur time.Duration) {
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
