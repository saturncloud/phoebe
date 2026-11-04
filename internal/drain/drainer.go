package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	// than on every loop pass, unless the last reclaim returned a full batch.
	lastClaim time.Time
	// claimCursor is where the next XAUTOCLAIM continues in the pending list.
	claimCursor string
	// lastTrim throttles trimming the same way. Run also trims once before the
	// schema gate and main once before opening Postgres, which is what frees a
	// full Valkey after a restart. Those passes leave lastTrim zero, so the loop's
	// first pass trims again as soon as the gate opens.
	lastTrim time.Time
	now      func() time.Time

	// storeDown is set when a store write fails and cleared by a successful Ping.
	// While it is set the loop reads nothing, so an outage does not move the whole
	// stream into the pending list. Only the Run goroutine touches it.
	storeDown bool
	// lastStoreDownLog throttles the log line while storeDown stays set.
	lastStoreDownLog time.Time

	// poisoned counts events that could not be stored even row-at-a-time
	// against a healthy store and were dropped (logged + ACK'd). A non-zero
	// value means billing rows were lost to the log; see upsertRowAtATime.
	poisoned atomic.Uint64
	// malformed counts entries that could not be decoded and were dropped; see
	// decodeBatch. The log line is the only copy left once the entry is trimmed.
	malformed atomic.Uint64
	// drainedWithoutGroup counts entries removed by drainWithoutGroup.
	drainedWithoutGroup atomic.Uint64

	// schemaPoll is the pause between schema-version probes in waitForSchema,
	// and schemaProbeTimeout bounds each probe. Tests shorten them.
	schemaPoll         time.Duration
	schemaProbeTimeout time.Duration
	// groupRetry is the pause between attempts in waitForGroup. Tests shorten it.
	groupRetry time.Duration
}

// Poisoned returns the number of events dropped as store-poison (failed their
// own single-row INSERT while the store was reachable) since start. Entries that
// could not even be decoded are counted by Malformed, not here.
func (d *Drainer) Poisoned() uint64 { return d.poisoned.Load() }

// Malformed returns the number of entries dropped since start because they could
// not be decoded. A redelivered entry counts again.
func (d *Drainer) Malformed() uint64 { return d.malformed.Load() }

// DrainedWithoutGroup returns the number of entries stored and trimmed since start
// while Valkey was full and no consumer group could be created. A non-zero value
// means the drainer went through that recovery; see drainWithoutGroup.
func (d *Drainer) DrainedWithoutGroup() uint64 { return d.drainedWithoutGroup.Load() }

// TrimDelivered runs one trim pass. It uses only Valkey, never the store, so main
// can call it before Postgres is open, on a Drainer built with a nil store.
func (d *Drainer) TrimDelivered(ctx context.Context) { d.trimDelivered(ctx) }

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

		claimCursor: "0",

		schemaPoll:         2 * time.Second,
		schemaProbeTimeout: 10 * time.Second,
		groupRetry:         5 * time.Second,
	}
}

// ensureGroup creates the consumer group if it does not already exist, creating
// the stream too (MKSTREAM). "$" starts the group at the stream tail, but since
// the emitter may already have produced entries we use "0" so a freshly created
// group still drains the existing backlog — no metering event is skipped.
//
// BUSYGROUP (group already exists) is the normal steady-state case and is not
// an error.
//
// An existing group is checked for first, with a read: past maxmemory Valkey
// refuses XGROUP CREATE with OOM even when the group already exists, and that
// must not stop the one process that can free the memory.
func (d *Drainer) ensureGroup(ctx context.Context) error {
	if groups, err := d.rdb.XInfoGroups(ctx, d.cfg.StreamName).Result(); err == nil {
		for _, g := range groups {
			if g.Name == d.cfg.Group {
				return nil
			}
		}
	}
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

// isValkeyOOM reports Valkey's refusal of a write past maxmemory under noeviction.
func isValkeyOOM(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if strings.HasPrefix(err.Error(), "OOM ") {
			return true
		}
	}
	return false
}

// isNoSuchKey reports Valkey's reply to XINFO on a stream that does not exist.
func isNoSuchKey(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such key")
}

// waitForGroup creates the consumer group, and frees memory itself while Valkey is
// too full for that: XGROUP CREATE is refused past maxmemory, trimming needs a
// group, and nothing else removes entries, so waiting alone would never end. Each
// refusal stores and trims one batch (drainWithoutGroup) and tries again, sleeping
// only if nothing was removed. Crashing would not free anything, and the emitter
// is already safe on its WAL. Any other error is returned, so a bad address still
// fails fast.
func (d *Drainer) waitForGroup(ctx context.Context) error {
	var lastLog time.Time
	for {
		err := d.ensureGroup(ctx)
		if !isValkeyOOM(err) || ctx.Err() != nil {
			return err
		}
		removed, derr := d.drainWithoutGroup(ctx)
		if lastLog.IsZero() || d.now().Sub(lastLog) >= 30*time.Second {
			lastLog = d.now()
			d.log.Error.Printf("drainer: valkey is full and consumer group %q does not exist; storing entries without a group to free memory: %v (last batch: removed %d, err=%v)",
				d.cfg.Group, err, removed, derr)
		}
		if removed == 0 {
			d.sleep(ctx, d.groupRetry)
		}
	}
}

// drainWithoutGroup is the way out of a full Valkey with no consumer group. It
// reads the oldest batch with XRANGE and removes it with XTRIM, neither of which
// Valkey refuses past maxmemory, and stores the batch first. It returns the number
// of entries removed.
//
// An entry is removed only if it is stored or logged as undecodable or poison, so
// any store failure trims nothing. XTRIM MINID keeps the batch's last entry, which
// the group, created at "0", redelivers; the upsert is idempotent. A group that
// appears before the trim hands the entries back to the normal trim rules. One
// appearing between that check and the XTRIM can only lose entries that are
// already stored.
func (d *Drainer) drainWithoutGroup(ctx context.Context) (int64, error) {
	if exists, err := d.anyGroupExists(ctx); err != nil || exists {
		return 0, err
	}
	msgs, err := d.rdb.XRangeN(ctx, d.cfg.StreamName, "-", "+", int64(d.cfg.BatchSize)).Result()
	if err != nil {
		return 0, fmt.Errorf("xrange: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	events, ids, _ := d.decodeBatch(msgs)
	if len(events) > 0 {
		if _, _, err := d.storeBatch(ctx, events, ids); err != nil {
			return 0, fmt.Errorf("store %d entries: %w", len(events), err)
		}
	}
	if exists, err := d.anyGroupExists(ctx); err != nil || exists {
		return 0, err
	}
	last := msgs[len(msgs)-1].ID
	n, err := d.rdb.XTrimMinID(ctx, d.cfg.StreamName, last).Result()
	if err != nil {
		return 0, fmt.Errorf("xtrim minid %s: %w", last, err)
	}
	if n > 0 {
		total := d.drainedWithoutGroup.Add(uint64(n))
		d.log.Error.Printf("drainer: valkey is full with no consumer group; stored and trimmed %d entr(ies) below %s (total=%d)", n, last, total)
	}
	return n, nil
}

// anyGroupExists reports whether the stream has any consumer group. A read that
// fails is an error, so the caller trims nothing.
func (d *Drainer) anyGroupExists(ctx context.Context) (bool, error) {
	groups, err := d.rdb.XInfoGroups(ctx, d.cfg.StreamName).Result()
	if isNoSuchKey(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("xinfo groups: %w", err)
	}
	return len(groups) > 0, nil
}

// trimDelivered removes entries every consumer group has delivered and
// acknowledged, and nothing else. The boundary is the oldest of each group's
// last-delivered id and oldest pending id: everything below it was delivered,
// and is no longer pending, so it is durable in the store. MINID keeps the
// boundary entry itself.
//
// XINFO GROUPS is read before XPENDING on purpose. An entry delivered between the
// two reads is above the last-delivered id already captured, and one acknowledged
// between them only makes the boundary lower than it could be. Any doubt, such as
// a read that fails or an id that does not parse, skips the trim: the cost of not
// trimming is a Valkey that fills up and an emitter on its WAL, and the cost of
// trimming wrongly is a lost billing record.
func (d *Drainer) trimDelivered(ctx context.Context) {
	boundary, ok, err := d.trimBoundary(ctx)
	if err != nil {
		d.log.Warn.Printf("drainer: skip trim: %v", err)
		return
	}
	if !ok {
		return
	}
	n, err := d.rdb.XTrimMinID(ctx, d.cfg.StreamName, boundary).Result()
	if err != nil {
		d.log.Warn.Printf("drainer: xtrim minid %s: %v", boundary, err)
		return
	}
	if n > 0 {
		d.log.Info.Printf("drainer: trimmed %d delivered entr(ies) below %s", n, boundary)
	}
}

// trimBoundary returns the MINID to trim to. ok is false when nothing can be
// trimmed: no stream or group exists, or a group has delivered nothing yet. A
// group that stops the trim while entries wait, or that holds the boundary back
// for the drainer's own group, is logged, because it keeps Valkey filling up.
func (d *Drainer) trimBoundary(ctx context.Context) (string, bool, error) {
	groups, err := d.rdb.XInfoGroups(ctx, d.cfg.StreamName).Result()
	if isNoSuchKey(err) {
		return "", false, nil // a new install: nothing has been emitted yet
	}
	if err != nil {
		return "", false, fmt.Errorf("xinfo groups: %w", err)
	}
	if len(groups) == 0 {
		return "", false, nil
	}
	boundary, holder := "", ""
	lower := func(id, group string) error {
		older, err := olderStreamID(boundary, id)
		if err != nil {
			return err
		}
		// On a tie the drainer's own group counts as holding the boundary.
		if older == id && (older != boundary || group == d.cfg.Group) {
			holder = group
		}
		boundary = older
		return nil
	}
	for _, g := range groups {
		if g.LastDeliveredID == "" || g.LastDeliveredID == "0-0" || g.LastDeliveredID == "0" {
			if g.Lag != 0 {
				d.log.Warn.Printf("drainer: skip trim: consumer group %q has delivered nothing and %d entr(ies) wait for it", g.Name, g.Lag)
			}
			return "", false, nil
		}
		if err := lower(g.LastDeliveredID, g.Name); err != nil {
			return "", false, err
		}
		pending, err := d.rdb.XPending(ctx, d.cfg.StreamName, g.Name).Result()
		if err != nil {
			return "", false, fmt.Errorf("xpending %s: %w", g.Name, err)
		}
		if pending.Count > 0 {
			if err := lower(pending.Lower, g.Name); err != nil {
				return "", false, err
			}
		}
	}
	if holder != d.cfg.Group {
		d.log.Warn.Printf("drainer: consumer group %q holds the trim boundary at %s", holder, boundary)
	}
	return boundary, true, nil
}

// olderStreamID returns the older of two stream ids ("ms-seq"). An empty a means
// "no boundary yet" and returns b. An id that does not parse is an error.
func olderStreamID(a, b string) (string, error) {
	bm, bs, err := parseStreamID(b)
	if err != nil {
		return "", err
	}
	if a == "" {
		return b, nil
	}
	am, as, err := parseStreamID(a)
	if err != nil {
		return "", err
	}
	if am < bm || (am == bm && as <= bs) {
		return a, nil
	}
	return b, nil
}

func parseStreamID(id string) (ms, seq uint64, err error) {
	left, right, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, fmt.Errorf("stream id %q has no sequence", id)
	}
	if ms, err = strconv.ParseUint(left, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("stream id %q: %w", id, err)
	}
	if seq, err = strconv.ParseUint(right, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("stream id %q: %w", id, err)
	}
	return ms, seq, nil
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
// Waiting is not free: events stay in Valkey meanwhile, and they are lost if its
// pod restarts, because Valkey keeps nothing on disk. The wait is therefore
// bounded by SchemaWaitTimeout, after which waitForSchema returns an error so
// Run fails, the process exits non-zero and the pod restarts visibly
// (CrashLoopBackOff) instead of idling silently. Each probe has its own
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
			d.log.Warn.Printf("drainer: waiting for database schema version %d (at %d, dirty=%v, err=%v); events wait in the stream and are lost if Valkey restarts; exiting after %s",
				required, version, dirty, err, d.cfg.SchemaWaitTimeout)
		}
		d.sleep(ctx, d.schemaPoll)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func isBusyGroup(err error) bool {
	// go-redis surfaces the BUSYGROUP reply as a plain error string.
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists")
}

// Run drives the drain loop until ctx is cancelled (SIGTERM). It first trims once,
// before the schema gate: trimming needs no database and acknowledged entries are
// already stored, so a full Valkey is freed even while the gate blocks consuming.
// On each pass it then:
//  1. opportunistically reclaims entries stranded by a dead consumer (XAUTOCLAIM),
//     without waiting for ClaimInterval while each reclaim returns a full batch,
//  2. every TrimInterval, trims entries that are delivered and acknowledged,
//  3. reads a batch of new entries with XREADGROUP, blocking unless a backlog is
//     being reclaimed,
//  4. stores the batch durably, THEN
//  5. XACKs the batch (never before the store commits).
//
// While the store is failing it skips reclaiming and reading, and keeps trimming.
//
// On ctx cancellation it returns after finishing any in-flight batch — an entry
// that was read but not yet stored is simply left un-ACK'd and redelivers to the
// next run (safe: idempotent store).
func (d *Drainer) Run(ctx context.Context) error {
	// Trimming needs no database and acknowledged entries are already stored, so
	// a full Valkey is freed even while the schema gate blocks consuming. lastTrim
	// stays zero so the loop's first pass trims again.
	d.trimDelivered(ctx)
	if err := d.waitForSchema(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if err := d.waitForGroup(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	d.log.Info.Printf("drainer: consuming stream=%s group=%s consumer=%s batch=%d",
		d.cfg.StreamName, d.cfg.Group, d.cfg.Consumer, d.cfg.BatchSize)

	for {
		if ctx.Err() != nil {
			d.log.Info.Printf("drainer: context cancelled, shutting down cleanly")
			return nil
		}

		// While the store is failing, reading would only move more entries into
		// the pending list, which trimming must not pass and which only reclaim
		// can empty. They wait unread in the stream instead.
		if d.storeDown && !d.storeRecovered(ctx) {
			d.trimIfDue(ctx)
			d.sleep(ctx, 500*time.Millisecond)
			continue
		}

		// Opportunistic reclaim of dead-consumer pending entries. A full batch
		// means more may be waiting, so the next pass reclaims again: after a
		// store outage the pending list sets the trim boundary, and one batch per
		// ClaimInterval would keep a full Valkey full for hours.
		more := false
		if d.now().Sub(d.lastClaim) >= d.cfg.ClaimInterval {
			more = d.reclaim(ctx)
			if !more {
				d.lastClaim = d.now()
			}
		}

		d.trimIfDue(ctx)

		if d.storeDown {
			continue // reclaim just found the store failing
		}

		// New entries keep flowing during a long reclaim, but without blocking:
		// waiting for BlockTimeout would cap reclaim at one batch per block. A
		// negative Block omits BLOCK; zero would block forever.
		block := d.cfg.BlockTimeout
		if more {
			block = -1
		}
		n, err := d.readAndStore(ctx, block)
		if err != nil {
			// On a transient error, log and back off briefly rather than spin.
			// A cancelled context surfaces here too; the top-of-loop check exits.
			if ctx.Err() != nil {
				return nil
			}
			d.storeDown = errors.Is(err, errStoreFailed)
			d.log.Error.Printf("drainer: drain pass: %v", err)
			d.sleep(ctx, 500*time.Millisecond)
			continue
		}
		// If we got a full batch there may be more backlog — loop immediately
		// without re-blocking. An empty/partial read means we've caught up.
		_ = n
	}
}

// trimIfDue trims when TrimInterval has passed since the last pass of the loop.
func (d *Drainer) trimIfDue(ctx context.Context) {
	if d.now().Sub(d.lastTrim) >= d.cfg.TrimInterval {
		d.trimDelivered(ctx)
		d.lastTrim = d.now()
	}
}

// storeRecovered pings the store and clears storeDown if it answers. A store that
// answers Ping but still rejects writes (a schema gap) sets storeDown again on the
// next failed write, so it keeps the 500ms back-off of a failed pass.
func (d *Drainer) storeRecovered(ctx context.Context) bool {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.store.Ping(pctx); err != nil {
		if d.now().Sub(d.lastStoreDownLog) >= 30*time.Second {
			d.lastStoreDownLog = d.now()
			d.log.Error.Printf("drainer: store still failing; not reading new entries: %v", err)
		}
		return false
	}
	d.storeDown = false
	d.log.Info.Printf("drainer: store answering again; resuming")
	return true
}

// readAndStore performs one XREADGROUP → store → XACK cycle for NEW entries
// (the ">" ID). block is XREADGROUP's BLOCK; a negative value does not block.
// Returns the number of entries processed.
func (d *Drainer) readAndStore(ctx context.Context, block time.Duration) (int, error) {
	res, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    d.cfg.Group,
		Consumer: d.cfg.Consumer,
		Streams:  []string{d.cfg.StreamName, ">"},
		Count:    int64(d.cfg.BatchSize),
		Block:    block,
	}).Result()

	if err != nil {
		// redis.Nil means the block elapsed with no new entries — normal idle.
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		if isNoGroup(err) {
			// The stream or group vanished (Valkey restarted without
			// persistence). Recreate it from the start of whatever stream now
			// exists so nothing emitted since is skipped (QA B22 drill). On a full
			// Valkey this frees memory until the group can be created.
			if gerr := d.waitForGroup(ctx); gerr != nil {
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
	events, ackIDs, poison := d.decodeBatch(msgs)

	// Drop poison entries FIRST, before the store work — they are malformed and
	// unstorable BY DEFINITION, so dropping them is correct regardless of store
	// health. Doing it here (rather than after the store) means a store-outage
	// early-return below still drops them: otherwise the poison would redeliver
	// every pass until an outage-free run, re-logging the same bad entries and
	// wasting reclaim work, with no upside (they can never be stored). An ACK
	// failure here is harmless: they redeliver and get dropped again. The entries
	// may hold real billing records that could not be decoded; decodeBatch has
	// already logged them, and that log line is what survives the trim.
	if len(poison) > 0 {
		if err := d.ack(ctx, poison); err != nil {
			d.log.Warn.Printf("drainer: ack poison entries: %v", err)
		}
	}

	stored := len(events)
	if len(events) > 0 {
		// Store MUST commit before we ACK.
		var rerr error
		ackIDs, stored, rerr = d.storeBatch(ctx, events, ackIDs)
		if rerr != nil {
			// Store outage (or cancellation), not a bad row. ACK only what
			// is known durable; the rest stays pending and redelivers.
			if aerr := d.ack(ctx, ackIDs); aerr != nil {
				d.log.Warn.Printf("drainer: ack after partial row-at-a-time: %v", aerr)
			}
			return stored, fmt.Errorf("store batch of %d: %w: %w", len(events), errStoreFailed, rerr)
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

// decodeBatch splits a batch into decoded events with their stream ids, and the
// ids of entries that cannot be decoded. An undecodable entry can never be stored;
// if it were never ACK'd it would redeliver forever and wedge the group, so the
// caller drops it. It may be a real billing record (a field type changed across a
// rolling upgrade, say), and once the trim passes it this log line is the only
// copy left, so the line holds the raw values and repair is by hand.
func (d *Drainer) decodeBatch(msgs []redis.XMessage) (events []metering.Event, ids, malformed []string) {
	events = make([]metering.Event, 0, len(msgs))
	ids = make([]string, 0, len(msgs))
	for _, m := range msgs {
		ev, err := decodeEvent(m)
		if err != nil {
			raw, merr := json.Marshal(m.Values) // one line, whatever the payload holds
			if merr != nil {
				raw = []byte(strconv.Quote(fmt.Sprintf("%+v", m.Values)))
			}
			n := d.malformed.Add(1)
			d.log.Error.Printf("drainer: drop malformed entry id=%s (total malformed=%d): %v values=%s", m.ID, n, err, raw)
			malformed = append(malformed, m.ID)
			continue
		}
		events = append(events, ev)
		ids = append(ids, m.ID)
	}
	return events, ids, malformed
}

// errStoreFailed marks an error from process as a store failure, as opposed to a
// Valkey one, so Run can stop reading until the store answers.
var errStoreFailed = errors.New("store failed")

// storeBatch writes a batch. The single batch statement is the fast path; if it
// fails we do NOT treat the error as wholesale transient: the batch INSERT is
// all-or-nothing, so one persistently bad row (e.g. a value the schema rejects)
// would fail it forever and the un-ACK'd batch would redeliver forever, starving
// every innocent event batched with it. So it falls back to upsertRowAtATime,
// whose results it returns.
func (d *Drainer) storeBatch(ctx context.Context, events []metering.Event, ids []string) (ackable []string, stored int, err error) {
	uerr := d.store.Upsert(ctx, events)
	if uerr == nil {
		return ids, len(events), nil
	}
	d.log.Warn.Printf("drainer: batch upsert of %d failed, isolating row-at-a-time: %v", len(events), uerr)
	return d.upsertRowAtATime(ctx, events, ids)
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
// processes them through the same store-then-ACK path. Each call claims one batch
// and continues from where the last one stopped; a failure goes back to the start
// of the pending list, where the failed entries still are.
//
// more reports a full batch that was stored, so more may be waiting and Run calls
// again without waiting for ClaimInterval. An error, a short batch or a store
// failure returns false, so a failing store keeps the ClaimInterval pace and
// cannot cause a hot loop.
func (d *Drainer) reclaim(ctx context.Context) (more bool) {
	msgs, next, err := d.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   d.cfg.StreamName,
		Group:    d.cfg.Group,
		Consumer: d.cfg.Consumer,
		MinIdle:  d.cfg.ClaimMinIdle,
		Start:    d.claimCursor,
		Count:    int64(d.cfg.BatchSize),
	}).Result()
	if err != nil {
		// Reclaim is best-effort; a failure here must not stop the main loop.
		// A missing group is recreated by the read path.
		d.log.Warn.Printf("drainer: xautoclaim: %v", err)
		d.claimCursor = "0"
		return false
	}
	d.claimCursor = next
	if next == "" || next == "0-0" {
		d.claimCursor = "0" // the walk reached the end of the pending list
	}
	if len(msgs) == 0 {
		return false
	}
	n, err := d.process(ctx, msgs)
	if err != nil {
		d.log.Error.Printf("drainer: reclaim process: %v", err)
		d.claimCursor = "0"
		d.storeDown = errors.Is(err, errStoreFailed)
		return false
	}
	if n > 0 {
		d.log.Info.Printf("drainer: reclaimed %d stranded entr(ies)", n)
	}
	return len(msgs) == d.cfg.BatchSize
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
