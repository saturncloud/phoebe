package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
}

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

	if len(events) > 0 {
		// Store MUST commit before we ACK. If the store fails we return the
		// error WITHOUT acking the good entries, so the whole batch redelivers.
		if err := d.store.Upsert(ctx, events); err != nil {
			return 0, fmt.Errorf("store batch of %d: %w", len(events), err)
		}
		if err := d.ack(ctx, ackIDs); err != nil {
			// The store committed but the ACK failed. Safe: the entries
			// redeliver and the idempotent upsert no-ops them. Surface as error
			// so the loop backs off, but the data is already durable.
			return len(events), fmt.Errorf("ack after store: %w", err)
		}
	}

	// Drop poison entries after the real work; an ACK failure here just means
	// they redeliver and get dropped again (still no data loss).
	if len(poison) > 0 {
		if err := d.ack(ctx, poison); err != nil {
			d.log.Warn.Printf("drainer: ack poison entries: %v", err)
		}
	}

	return len(events), nil
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
	if err := json.Unmarshal([]byte(s), &ev); err != nil {
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
