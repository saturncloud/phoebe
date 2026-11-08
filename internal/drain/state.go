package drain

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/metering"
)

// The drainer has no metrics port, so it publishes its state to a Valkey hash
// (metering.DrainerStateKey) and the interceptor turns it into metrics. A
// publish is best effort: it has its own short deadline, a failure is logged at
// WARN (throttled), and nothing about the drain depends on it.
const (
	// statePublishInterval is the longest gap between publishes while the
	// drainer runs its loop. The interceptor treats a consumer as gone when it
	// has not published for 120 s.
	statePublishInterval = 15 * time.Second
	// stateChangeRetry is how soon a publish that failed is tried again when a
	// pause has started or ended since the last successful one. Otherwise such a
	// change is published at once.
	stateChangeRetry = time.Second
	// statePublishTimeout bounds one publish, so a slow Valkey cannot hold up
	// the drain.
	statePublishTimeout = time.Second
	// staleConsumerAge is how long another consumer's fields stay after its last
	// publish. A drainer pod gets a new consumer name when it is replaced, so
	// without this the hash would keep every old pod's fields.
	staleConsumerAge = 24 * time.Hour
)

// publishIfDue publishes the drainer state every statePublishInterval, and at
// once when a pause has started or ended since the last successful publish.
func (d *Drainer) publishIfDue(ctx context.Context) {
	since := d.now().Sub(d.lastPublish)
	changed := !d.publishedOnce || !d.pausedSince.Equal(d.publishedPausedSince)
	if since >= statePublishInterval || (changed && (!d.lastPublishFailed || since >= stateChangeRetry)) {
		d.publishState(ctx)
	}
}

// publishState writes this consumer's fields (seen, paused_since), adds the
// drops counted since the last successful publish to the shared counters, and
// removes the fields of consumers not seen for staleConsumerAge, all in one
// MULTI/EXEC. Valkey runs all of it or none of it: past maxmemory it refuses the
// writes and discards the whole transaction, so the unsent counts are kept and
// sent by the next publish that succeeds.
func (d *Drainer) publishState(ctx context.Context) {
	if d.rdb == nil {
		return
	}
	now := d.now()
	d.lastPublish = now
	ctx, cancel := context.WithTimeout(ctx, statePublishTimeout)
	defer cancel()

	key := metering.DrainerStateKey(d.cfg.StreamName)
	poisoned, malformed := d.poisoned.Load(), d.malformed.Load()
	newPoisoned, newMalformed := poisoned-d.sentPoisoned, malformed-d.sentMalformed
	var pausedSince int64
	if !d.pausedSince.IsZero() {
		pausedSince = d.pausedSince.Unix()
	}
	// A failed read only skips the cleanup; the writes are still tried.
	var stale []string
	if fields, err := d.rdb.HGetAll(ctx, key).Result(); err == nil {
		stale = staleStateFields(fields, d.cfg.Consumer, now)
	}
	_, err := d.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key,
			d.cfg.Consumer+metering.DrainerFieldSeen, now.Unix(),
			d.cfg.Consumer+metering.DrainerFieldPausedSince, pausedSince)
		if newPoisoned > 0 {
			p.HIncrBy(ctx, key, metering.DrainerFieldDroppedPoison, int64(newPoisoned))
		}
		if newMalformed > 0 {
			p.HIncrBy(ctx, key, metering.DrainerFieldDroppedUndecodable, int64(newMalformed))
		}
		if len(stale) > 0 {
			p.HDel(ctx, key, stale...)
		}
		return nil
	})
	if err != nil {
		d.lastPublishFailed = true
		if d.lastPublishWarn.IsZero() || now.Sub(d.lastPublishWarn) >= 30*time.Second {
			d.lastPublishWarn = now
			d.log.Warn.Printf("drainer: could not publish drainer state to %s (draining is not affected; %d poison and %d undecodable drop(s) wait to be counted): %v",
				key, newPoisoned, newMalformed, err)
		}
		return
	}
	d.lastPublishFailed = false
	d.sentPoisoned += newPoisoned
	d.sentMalformed += newMalformed
	d.publishedPausedSince = d.pausedSince
	d.publishedOnce = true
}

// staleStateFields returns the fields of consumers other than self whose seen
// time is older than staleConsumerAge, or that have per-consumer fields but no
// seen time that parses.
func staleStateFields(fields map[string]string, self string, now time.Time) []string {
	fresh := map[string]bool{}
	for f, v := range fields {
		c, ok := strings.CutSuffix(f, metering.DrainerFieldSeen)
		if !ok {
			continue
		}
		if sec, err := strconv.ParseInt(v, 10, 64); err == nil && now.Sub(time.Unix(sec, 0)) < staleConsumerAge {
			fresh[c] = true
		}
	}
	var stale []string
	for f := range fields {
		c, ok := strings.CutSuffix(f, metering.DrainerFieldSeen)
		if !ok {
			c, ok = strings.CutSuffix(f, metering.DrainerFieldPausedSince)
		}
		if ok && c != self && !fresh[c] {
			stale = append(stale, f)
		}
	}
	return stale
}
