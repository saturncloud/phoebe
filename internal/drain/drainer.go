package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
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

	// storeDown is set when a store write fails. While it is set the loop reads no
	// new entries, so a failing store does not move the whole stream into the
	// pending list; each retry rereads the entries this consumer holds instead.
	// Ping alone never clears it, because a store can answer Ping and still reject
	// every write (a schema gap). It is cleared by a retry that stores at least one
	// row and leaves this consumer's pending list empty, or, when there is nothing
	// left to write at all (no pending entries and no new ones), by a pass that read
	// nothing. That last case resumes reading but keeps storeRetryDelay, so a store
	// that is still failing goes on doubling the wait instead of starting again at
	// storeRetryBase. Only the Run goroutine touches it.
	storeDown bool
	// storeRetryDelay is how long to wait after a failed store write before trying
	// again. It doubles with each failure up to storeRetryCap, and only a pass that
	// stores a row sets it back to zero.
	storeRetryDelay time.Duration
	// lastStoreFail is when the last store write failed.
	lastStoreFail time.Time
	// storeRetryBase is the first value of storeRetryDelay. Tests shorten it.
	storeRetryBase time.Duration
	// lastStoreDownLog throttles the log line while storeDown stays set.
	lastStoreDownLog time.Time
	// lastGroupHoldLog throttles the log line about another consumer group holding
	// the stream during drainWithoutGroup.
	lastGroupHoldLog time.Time
	// pausedSince is when the current pause began, that is when storeDown last went
	// from false to true; zero while not paused. Only the Run goroutine touches it.
	pausedSince time.Time

	// State published to Valkey (see publishState). Only the Run goroutine
	// touches these. sentPoisoned and sentMalformed are the parts of poisoned and
	// malformed already added to the shared counters; the rest is sent by the
	// next publish that succeeds, so a refused write loses no counts.
	lastPublish          time.Time
	publishedPausedSince time.Time
	publishedOnce        bool
	sentPoisoned         uint64
	sentMalformed        uint64
	lastPublishWarn      time.Time
	lastPublishFailed    bool

	// poisoned counts events that could not be stored even row-at-a-time
	// against a store that accepts writes and were dropped (logged + ACK'd). A
	// non-zero value means billing rows were lost to the log; see
	// upsertRowAtATime.
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

// maxStoreRetryDelay caps storeRetryDelay, so a store that recovers after a long
// failure is noticed within this long. storeRetryCap lowers it further when
// ClaimMinIdle is short.
const maxStoreRetryDelay = 30 * time.Second

// storeRetryCap is the longest wait between store retries: maxStoreRetryDelay, or
// half of ClaimMinIdle if that is shorter, and never less than storeRetryBase. Each
// retry rereads this consumer's pending entries, which resets their idle time, so
// while the wait stays below ClaimMinIdle another replica's XAUTOCLAIM cannot take
// a batch this consumer is still retrying.
func (d *Drainer) storeRetryCap() time.Duration {
	return max(min(maxStoreRetryDelay, d.cfg.ClaimMinIdle/2), d.storeRetryBase)
}

// Poisoned returns the number of events dropped as store-poison (failed their
// own single-row INSERT while the store accepted writes) since start. Entries that
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
		storeRetryBase:     500 * time.Millisecond,
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

// drainWithoutGroup is the way out of a full Valkey without the drainer's consumer
// group. It reads the oldest batch with XRANGE and removes it with XTRIM, neither
// of which Valkey refuses past maxmemory, and stores the batch first. It returns
// the number of entries removed.
//
// An entry is removed only if it is stored or logged as undecodable or poison, so
// any store failure trims nothing. MINID is the id just after the batch's last
// entry, so the whole batch goes and a batch of one still makes progress.
//
// If the drainer's own group exists, the normal trim rules apply and nothing is
// done here. Another group on the stream (a debug consumer, say) does not stop the
// recovery, but its entries are its own: the trim stops at the oldest of its
// last-delivered and oldest pending ids, as trimBoundary does, and only entries
// below that limit are read and stored. If that leaves nothing to remove, nothing
// is stored or trimmed, and the group is logged so the operator can see what holds
// the stream. A group appearing before the trim is
// read again and counts the same way. One appearing between that read and the
// XTRIM can only lose entries that are already stored.
func (d *Drainer) drainWithoutGroup(ctx context.Context) (int64, error) {
	others, own, err := d.readGroups(ctx)
	if err != nil || own {
		return 0, err
	}
	// With another group on the stream, only entries below its limit can be
	// trimmed, so only those are read. Storing the rest would store, and log as
	// undecodable, the same entries again on every pass until that group moves.
	end := "+"
	if len(others) > 0 {
		first, err := d.rdb.XRangeN(ctx, d.cfg.StreamName, "-", "+", 1).Result()
		if err != nil {
			return 0, fmt.Errorf("xrange: %w", err)
		}
		if len(first) == 0 {
			return 0, nil
		}
		limit, held, err := d.otherGroupsLimit(ctx, others, first[0].ID)
		if err != nil || held {
			return 0, err
		}
		if limit != "" {
			end = "(" + limit // exclusive: MINID keeps the limit entry
		}
	}
	msgs, err := d.rdb.XRangeN(ctx, d.cfg.StreamName, "-", end, int64(d.cfg.BatchSize)).Result()
	if err != nil {
		return 0, fmt.Errorf("xrange: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	oldest := msgs[0].ID
	events, ids, _ := d.decodeBatch(msgs)
	if len(events) > 0 {
		if _, _, err := d.storeBatch(ctx, events, ids); err != nil {
			return 0, fmt.Errorf("store %d entries: %w", len(events), err)
		}
	}
	if others, own, err = d.readGroups(ctx); err != nil || own {
		return 0, err
	}
	limit, held, err := d.otherGroupsLimit(ctx, others, oldest)
	if err != nil || held {
		return 0, err
	}
	minID, err := nextStreamID(msgs[len(msgs)-1].ID)
	if err != nil {
		return 0, err
	}
	if minID, err = olderStreamID(limit, minID); err != nil {
		return 0, err
	}
	n, err := d.rdb.XTrimMinID(ctx, d.cfg.StreamName, minID).Result()
	if err != nil {
		return 0, fmt.Errorf("xtrim minid %s: %w", minID, err)
	}
	if n > 0 {
		total := d.drainedWithoutGroup.Add(uint64(n))
		d.log.Error.Printf("drainer: valkey is full and consumer group %q does not exist; stored and trimmed %d entr(ies) below %s (total=%d, other groups=%d)",
			d.cfg.Group, n, minID, total, len(others))
	}
	return n, nil
}

// readGroups returns the stream's consumer groups other than the drainer's own,
// and whether its own exists. A read that fails is an error, so the caller trims
// nothing.
func (d *Drainer) readGroups(ctx context.Context) (others []redis.XInfoGroup, own bool, err error) {
	groups, err := d.rdb.XInfoGroups(ctx, d.cfg.StreamName).Result()
	if isNoSuchKey(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("xinfo groups: %w", err)
	}
	for _, g := range groups {
		if g.Name == d.cfg.Group {
			own = true
			continue
		}
		others = append(others, g)
	}
	return others, own, nil
}

// otherGroupsLimit returns the MINID drainWithoutGroup may not pass because of the
// given groups, or "" if there are none. held is true, and logged, when those
// groups leave nothing at or above oldest (the stream's oldest entry) to remove.
func (d *Drainer) otherGroupsLimit(ctx context.Context, groups []redis.XInfoGroup, oldest string) (limit string, held bool, err error) {
	if len(groups) == 0 {
		return "", false, nil
	}
	boundary, holder, stopped, err := d.groupsBoundary(ctx, groups)
	if err != nil {
		return "", false, err
	}
	if stopped != nil {
		d.logGroupHold(stopped.Name, "0-0")
		return "", true, nil
	}
	older, err := olderStreamID(boundary, oldest)
	if err != nil {
		return "", false, err
	}
	if older == boundary { // MINID keeps the boundary entry, so nothing would go
		d.logGroupHold(holder, boundary)
		return "", true, nil
	}
	return boundary, false, nil
}

// logGroupHold says which group stops the recovery from a full Valkey.
func (d *Drainer) logGroupHold(group, at string) {
	if !d.lastGroupHoldLog.IsZero() && d.now().Sub(d.lastGroupHoldLog) < 30*time.Second {
		return
	}
	d.lastGroupHoldLog = d.now()
	d.log.Warn.Printf("drainer: valkey is full and consumer group %q holds the stream at %s; nothing can be freed until it reads further or is deleted, and consumer group %q cannot be created until then",
		group, at, d.cfg.Group)
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
// trimmed: no stream or group exists, a group has delivered nothing yet, or the
// drainer's own group is missing, so nothing shows the entries are stored. A
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
	boundary, holder, stopped, err := d.groupsBoundary(ctx, groups)
	if err != nil {
		return "", false, err
	}
	if stopped != nil {
		if stopped.Lag != 0 {
			d.log.Warn.Printf("drainer: skip trim: consumer group %q has delivered nothing and %d entr(ies) wait for it", stopped.Name, stopped.Lag)
		}
		return "", false, nil
	}
	if !slices.ContainsFunc(groups, func(g redis.XInfoGroup) bool { return g.Name == d.cfg.Group }) {
		// Another group having read an entry says nothing about the store.
		d.log.Warn.Printf("drainer: skip trim: consumer group %q does not exist, so entries below %s may not be stored", d.cfg.Group, boundary)
		return "", false, nil
	}
	if holder != d.cfg.Group {
		d.log.Warn.Printf("drainer: consumer group %q holds the trim boundary at %s", holder, boundary)
	}
	return boundary, true, nil
}

// groupsBoundary returns the oldest of the given groups' last-delivered and oldest
// pending ids, and the group holding it. stopped is set instead when a group has
// delivered nothing yet, so there is no boundary. groups must not be empty.
func (d *Drainer) groupsBoundary(ctx context.Context, groups []redis.XInfoGroup) (boundary, holder string, stopped *redis.XInfoGroup, err error) {
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
	for i := range groups {
		g := &groups[i]
		if g.LastDeliveredID == "" || g.LastDeliveredID == "0-0" || g.LastDeliveredID == "0" {
			return "", "", g, nil
		}
		if err := lower(g.LastDeliveredID, g.Name); err != nil {
			return "", "", nil, err
		}
		pending, err := d.rdb.XPending(ctx, d.cfg.StreamName, g.Name).Result()
		if err != nil {
			return "", "", nil, fmt.Errorf("xpending %s: %w", g.Name, err)
		}
		if pending.Count > 0 {
			if err := lower(pending.Lower, g.Name); err != nil {
				return "", "", nil, err
			}
		}
	}
	return boundary, holder, nil, nil
}

// nextStreamID returns the smallest id after id, which need not exist in the stream.
func nextStreamID(id string) (string, error) {
	ms, seq, err := parseStreamID(id)
	if err != nil {
		return "", err
	}
	if seq < math.MaxUint64 {
		return fmt.Sprintf("%d-%d", ms, seq+1), nil
	}
	if ms == math.MaxUint64 {
		return "", fmt.Errorf("stream id %q has no successor", id)
	}
	return fmt.Sprintf("%d-0", ms+1), nil
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
// While the store is failing it skips reclaiming and reading new entries, and keeps
// trimming. It waits longer after each failed write, and each retry writes the
// entries this consumer already holds, one batch per pass, until that pending list
// is empty. Only when a retry finds nothing of its own left to write (another
// replica reclaimed it, the group was recreated, or every entry was undecodable or
// poison) does it read one new batch as the test of the store, so the pending list
// holds at most one batch per replica. See pass for the rules.
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
			// One last publish, on its own short deadline, so counts from the
			// final passes are not lost with the process.
			d.publishState(context.WithoutCancel(ctx))
			d.log.Info.Printf("drainer: context cancelled, shutting down cleanly")
			return nil
		}
		d.pass(ctx)
	}
}

// pass runs one iteration of the Run loop.
//
// While the store is failing (storeDown), a pass waits out storeRetryDelay and then
// retries by rereading this consumer's pending entries (the "0" id). The outcome
// decides what happens to storeDown, because only a stored row shows that the store
// accepts writes:
//   - a full batch came back, so more may be pending: storeDown stays set and the
//     next pass rereads again at once (noteRetryProgress), so this consumer's whole
//     pending list is written before any new entry is read;
//   - a short batch with at least one row stored: the pending list is drained and
//     the store works, so storeDown and the wait are cleared (noteStoreWorking);
//   - nothing stored and the pending list drained (an empty reread, a group
//     recreated after NOGROUP, or only undecodable or poison entries): the same pass
//     reads one new batch with ">" as the test of the store. A failure there
//     doubles the wait from where it was. Reading nothing there clears storeDown but
//     keeps the wait, because nothing showed the store accepts writes.
func (d *Drainer) pass(ctx context.Context) {
	d.publishIfDue(ctx)

	// While the store is failing, reading would only move more entries into
	// the pending list, which trimming must not pass. They wait unread in the
	// stream instead. Only a write shows that the store accepts writes, so when
	// the wait is over and it answers Ping, this pass retries.
	retrying := d.storeDown
	if retrying && (d.now().Sub(d.lastStoreFail) < d.storeRetryDelay || !d.storeAnswers(ctx)) {
		d.trimIfDue(ctx)
		d.sleep(ctx, d.storeRetryDelay)
		return
	}

	// Opportunistic reclaim of dead-consumer pending entries. A full batch
	// means more may be waiting, so the next pass reclaims again: after a
	// store outage the pending list sets the trim boundary, and one batch per
	// ClaimInterval would keep a full Valkey full for hours.
	more := false
	if !retrying && d.now().Sub(d.lastClaim) >= d.cfg.ClaimInterval {
		more = d.reclaim(ctx)
		if !more {
			d.lastClaim = d.now()
		}
	}

	d.trimIfDue(ctx)

	if d.storeDown && !retrying {
		return // reclaim just found the store failing
	}

	// New entries keep flowing during a long reclaim, but without blocking:
	// waiting for BlockTimeout would cap reclaim at one batch per block. A
	// negative Block omits BLOCK; zero would block forever.
	block := d.cfg.BlockTimeout
	start := ">"
	if more || retrying {
		block = -1
	}
	if retrying {
		start = "0" // this consumer's own pending entries
	}
	stored, read, err := d.readAndStore(ctx, start, block)
	probed := false
	if err == nil && retrying && stored == 0 && read < d.cfg.BatchSize {
		// Nothing of this consumer's is left to retry and nothing was written, so
		// one new batch is the test of the store.
		probed = true
		stored, read, err = d.readAndStore(ctx, ">", -1)
	}
	if err != nil {
		// On a transient error, log and back off briefly rather than spin.
		// A cancelled context surfaces here too; the top-of-loop check exits.
		if ctx.Err() != nil {
			return
		}
		d.log.Error.Printf("drainer: drain pass: %v", err)
		if errors.Is(err, errStoreFailed) {
			d.noteStoreFailed(err) // the check at the top of the loop waits
			return
		}
		if stored > 0 {
			d.noteStoreWorking(stored) // rows were stored, and only the ACK failed
		}
		d.sleep(ctx, 500*time.Millisecond)
		return
	}
	if retrying && !probed && read >= d.cfg.BatchSize {
		d.noteRetryProgress(stored)
		return
	}
	// If we got a full batch there may be more backlog — loop immediately
	// without re-blocking. An empty/partial read means we've caught up.
	d.noteStoreWorking(stored)
}

// trimIfDue trims when TrimInterval has passed since the last pass of the loop.
func (d *Drainer) trimIfDue(ctx context.Context) {
	if d.now().Sub(d.lastTrim) >= d.cfg.TrimInterval {
		d.trimDelivered(ctx)
		d.lastTrim = d.now()
	}
}

// noteStoreFailed records a failed store write: reads stop, and the wait before the
// next attempt doubles, from storeRetryBase up to storeRetryCap. The start of a
// pause is logged once at ERROR; while it lasts, the throttled lines in pass and
// storeAnswers say it is still failing.
func (d *Drainer) noteStoreFailed(err error) {
	if !d.storeDown {
		d.pausedSince = d.now()
		d.log.Error.Printf("drainer: paused: store write failed: %v; billing events stay in Valkey and none are dropped; retrying with backoff (see docs/billing-reconciliation.md, \"Drainer paused\")", err)
	}
	d.storeDown = true
	d.lastStoreFail = d.now()
	d.storeRetryDelay = min(max(2*d.storeRetryDelay, d.storeRetryBase), d.storeRetryCap())
}

// noteStoreWorking records a pass with no store failure, which stored the given
// number of rows, and ends the stop on reading new entries. Only a pass that
// stored a row resets the wait: a pass that wrote nothing has not shown that the
// store accepts writes, so the wait is kept and a store that is still failing
// doubles it from there instead of starting again at storeRetryBase.
func (d *Drainer) noteStoreWorking(stored int) {
	paused := d.pauseLength()
	if stored > 0 {
		d.storeRetryDelay = 0
		if d.storeDown {
			d.log.Info.Printf("drainer: store accepting writes again; resuming after a pause of %s", paused)
		}
	} else if d.storeDown {
		d.log.Info.Printf("drainer: no pending entries left to retry; resuming reads after a pause of %s (the store has not yet accepted a write)", paused)
	}
	d.storeDown = false
	d.pausedSince = time.Time{}
}

// pauseLength is how long the current pause has lasted, rounded to the second.
func (d *Drainer) pauseLength() time.Duration {
	if d.pausedSince.IsZero() {
		return 0
	}
	return d.now().Sub(d.pausedSince).Round(time.Second)
}

// noteRetryProgress records a retry that reread a full batch of this consumer's
// pending entries without a store failure. More may be pending, so storeDown stays
// set and the next pass rereads at once. A stored row also resets the wait.
func (d *Drainer) noteRetryProgress(stored int) {
	d.lastStoreFail = time.Time{}
	if stored > 0 {
		d.storeRetryDelay = 0
	}
}

// storeAnswers pings the store. An answer is not enough to clear storeDown, because
// a store that answers Ping can still reject writes (a schema gap). It only makes a
// retry worth trying; a failure there lengthens the wait through noteStoreFailed.
func (d *Drainer) storeAnswers(ctx context.Context) bool {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.store.Ping(pctx); err != nil {
		if d.now().Sub(d.lastStoreDownLog) >= 30*time.Second {
			d.lastStoreDownLog = d.now()
			d.log.Error.Printf("drainer: store still failing; not reading new entries: %v", err)
		}
		return false
	}
	return true
}

// readAndStore performs one XREADGROUP → store → XACK cycle from start: ">" for
// new entries, or "0" to reread the entries this consumer already holds, so a
// retry that fails adds nothing to the pending list. block is XREADGROUP's BLOCK;
// a negative value does not block. It returns the number of rows the store
// committed and the number of entries read. A group recreated after NOGROUP reads
// nothing.
func (d *Drainer) readAndStore(ctx context.Context, start string, block time.Duration) (stored, read int, err error) {
	res, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    d.cfg.Group,
		Consumer: d.cfg.Consumer,
		Streams:  []string{d.cfg.StreamName, start},
		Count:    int64(d.cfg.BatchSize),
		Block:    block,
	}).Result()

	if err != nil {
		// redis.Nil means the block elapsed with no new entries — normal idle.
		if errors.Is(err, redis.Nil) {
			return 0, 0, nil
		}
		if isNoGroup(err) {
			// The stream or group vanished (Valkey restarted without
			// persistence). Recreate it from the start of whatever stream now
			// exists so nothing emitted since is skipped (QA B22 drill). On a full
			// Valkey this frees memory until the group can be created.
			if gerr := d.waitForGroup(ctx); gerr != nil {
				return 0, 0, fmt.Errorf("xreadgroup: %w; recreate group: %v", err, gerr)
			}
			d.log.Warn.Printf("drainer: consumer group %q on %q was missing (%v); recreated", d.cfg.Group, d.cfg.StreamName, err)
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("xreadgroup: %w", err)
	}

	var msgs []redis.XMessage
	for _, st := range res {
		msgs = append(msgs, st.Messages...)
	}
	if len(msgs) == 0 {
		return 0, 0, nil
	}
	stored, err = d.process(ctx, msgs)
	return stored, len(msgs), err
}

// process decodes, stores, and ACKs a batch of stream messages. It is shared by
// the new-entry path and the reclaim path so both honour the
// store-before-ACK invariant identically. It returns the number of rows the store
// committed, which excludes undecodable and poison entries.
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
// are durable as usual. A row that fails ON ITS OWN while the store accepts
// writes can never be stored, so it is POISON: logged loudly (the full event
// JSON at ERROR, same spirit as the decode-poison path in process: the log line
// becomes the only remaining copy of a billing record), counted, and included in
// the returned ack set so the stream keeps draining instead of redelivering it
// forever.
//
// "Accepts writes" is the fail-closed guard: a store OUTAGE also fails every
// individual row, and declaring those poison would ACK-and-drop an entire batch
// of good billing data. So after an individual failure we Ping, and check for a
// schema gap and for errors that mean the whole store refuses writes; any of
// those aborts the walk with an error, the caller ACKs only the rows already
// stored, and the remainder redelivers.
//
// A row that failed for any other reason is not dropped during the walk. If at
// least one row of the batch was stored, the store plainly accepts writes and
// the failed rows are poison. If NO row was stored, a failure the checks above
// do not recognise could still be the store's, so a probe write
// (Store.ProbeWrite) decides: if it fails, the batch is a store failure and
// nothing is ACK'd, so the drainer pauses with the rows still pending; if it
// succeeds, the rows are poison. Without the probe a batch of one (the usual
// size at low traffic) could not be told apart from a broken store.
//
// Returns the stream ids safe to ACK (stored rows + poison rows), the number
// of rows actually stored, and a non-nil error only for an outage, a schema gap,
// a store that refuses writes (isStoreNotWritable), a failed probe, or
// cancellation.
func (d *Drainer) upsertRowAtATime(ctx context.Context, events []metering.Event, ids []string) (ackable []string, stored int, err error) {
	ackable = make([]string, 0, len(events))
	var failed []int // indexes of rows that failed on their own
	rowErrs := make(map[int]error)
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
		if isStoreNotWritable(uerr) {
			// The store answers Ping but refuses every write (a read-only
			// replica, missing privileges, a full disk, a shutdown). Like an
			// outage, ACK only what is stored; the rest redelivers once the
			// store takes writes again.
			return ackable, stored, fmt.Errorf("store not writable (row error): %w", uerr)
		}
		failed = append(failed, i)
		rowErrs[i] = uerr
	}
	if len(failed) == 0 {
		return ackable, stored, nil
	}
	if stored == 0 {
		// Every row failed. Ask the store whether it takes a write at all.
		first := rowErrs[failed[0]]
		if perr := d.store.ProbeWrite(ctx); perr != nil {
			d.log.Warn.Printf("drainer: all %d row(s) of the batch failed and the probe write failed too, so this is a store failure: keeping the rows and pausing: %v", len(events), perr)
			return ackable, 0, fmt.Errorf("all %d row(s) failed (first row id=%s request_id=%s: %v) and the probe write failed: %w",
				len(events), ids[failed[0]], events[failed[0]].RequestID, first, perr)
		}
		d.log.Warn.Printf("drainer: all %d row(s) of the batch failed but the probe write succeeded, so the store accepts writes: dropping the rows as poison (first row error: %v)", len(events), first)
	}
	for _, i := range failed {
		// POISON: the store accepts writes but this row cannot be inserted.
		// Log the full event so it is recoverable from logs, count it, ACK it.
		raw, merr := json.Marshal(events[i])
		if merr != nil {
			raw = []byte(fmt.Sprintf("%+v", events[i]))
		}
		n := d.poisoned.Add(1)
		d.log.Error.Printf("drainer: drop poison row id=%s request_id=%s (total poisoned=%d): %v event=%s",
			ids[i], events[i].RequestID, n, rowErrs[i], raw)
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
		if errors.Is(err, errStoreFailed) {
			d.noteStoreFailed(err)
		}
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
