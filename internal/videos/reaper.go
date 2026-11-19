package videos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/prune"
)

// ReaperConfig bounds the reaper.
type ReaperConfig struct {
	// QueueTimeout fails a job that waited longer (design 5.3: 60 min).
	QueueTimeout time.Duration
	// RowRetention deletes job and billing rows older than this. Its default,
	// prune.VideoRowRetentionDays, is what the rater's video reach
	// (rating.VideoRateReach) is derived from: the rater never re-rates video
	// in an hour whose jobs this may have deleted. Tests set it; production
	// uses the default.
	RowRetention time.Duration
	// Every is how often the reaper runs.
	Every time.Duration
	// Requeue re-adds a queued job to its stream when its stream is empty and
	// the job has waited this long since it was last added (lost entries).
	Requeue time.Duration
}

func (c *ReaperConfig) defaults() {
	if c.QueueTimeout <= 0 {
		c.QueueTimeout = 60 * time.Minute
	}
	if c.RowRetention <= 0 {
		c.RowRetention = prune.VideoRowRetentionDays * 24 * time.Hour
	}
	if c.Every <= 0 {
		c.Every = 30 * time.Second
	}
	if c.Requeue <= 0 {
		c.Requeue = 2 * time.Minute
	}
}

// Reaper keeps the job table honest. It runs in the phoebe-videos service
// itself, always on, rather than in the prune CronJob (which ships
// suspended): file expiry is a promise to the customer (expires_at) and
// prompts must not outlive their videos.
type Reaper struct {
	cfg   ReaperConfig
	store *Store
	queue *Queue
	blob  Blob
	log   *logging.Logger
	now   func() time.Time

	lastPrune time.Time
}

func NewReaper(cfg ReaperConfig, store *Store, queue *Queue, blob Blob, log *logging.Logger) *Reaper {
	cfg.defaults()
	return &Reaper{cfg: cfg, store: store, queue: queue, blob: blob, log: log, now: time.Now}
}

func (r *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Every)
	defer t.Stop()
	for {
		r.Once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once runs every reaper step one time.
func (r *Reaper) Once(ctx context.Context) {
	now := r.now().UTC()
	timedOut, err := r.store.ExpireQueued(ctx, now.Add(-r.cfg.QueueTimeout), now)
	if err != nil {
		r.log.Error.Printf("videos reaper: queue timeouts: %v", err)
	}
	if len(timedOut) > 0 {
		r.log.Warn.Printf("videos reaper: %d job(s) failed with queue_timeout", len(timedOut))
	}
	r.closeGrants(ctx, timedOut)
	requeued, failed, err := r.store.ExpireLeases(ctx, now)
	if err != nil {
		r.log.Error.Printf("videos reaper: expired leases: %v", err)
	}
	if requeued+len(failed) > 0 {
		r.log.Warn.Printf("videos reaper: expired leases: %d requeued, %d failed", requeued, len(failed))
	}
	r.closeGrants(ctx, failed)
	r.reenqueue(ctx, now)
	r.expireFiles(ctx, now)
	if now.Sub(r.lastPrune) >= time.Hour {
		jobs, events, err := r.store.PruneRows(ctx, now.Add(-r.cfg.RowRetention))
		if err != nil {
			r.log.Error.Printf("videos reaper: prune rows: %v", err)
		} else {
			r.lastPrune = now
			if jobs+events > 0 {
				r.log.Info.Printf("videos reaper: pruned %d job and %d billing row(s)", jobs, events)
			}
		}
	}
}

// closeGrants tells Atlas that jobs the reaper failed have ended, so no
// upload is signed for them again (best effort, like every close).
func (r *Reaper) closeGrants(ctx context.Context, jobs []*Job) {
	for _, j := range jobs {
		CloseGrant(ctx, r.blob, r.log, j)
	}
}

// reenqueue puts queued jobs back on their stream when it lost them: a
// requeued job (the lease expired), or every job of an endpoint whose stream
// is empty while jobs wait (Valkey restarted without its data). A duplicate
// entry is harmless: the claim in Postgres takes a job once.
func (r *Reaper) reenqueue(ctx context.Context, now time.Time) {
	jobs, err := r.store.Queued(ctx, now.Add(-r.cfg.Requeue), 1000)
	if err != nil {
		r.log.Error.Printf("videos reaper: list queued: %v", err)
		return
	}
	empty := map[string]bool{}
	for _, j := range jobs {
		lost := j.enqueuedNever()
		if !lost {
			isEmpty, seen := empty[j.ResourceID]
			if !seen {
				n, err := r.queue.Len(ctx, j.ResourceID)
				if err != nil {
					r.log.Error.Printf("videos reaper: queue length of %s: %v", j.ResourceID, err)
					return
				}
				isEmpty = n == 0
				empty[j.ResourceID] = isEmpty
			}
			lost = isEmpty
		}
		if !lost {
			continue
		}
		if err := r.queue.Add(ctx, j.ResourceID, j.ID); err != nil {
			r.log.Error.Printf("videos reaper: re-add %s: %v", j.ID, err)
			return
		}
		if err := r.store.MarkEnqueued(ctx, j.ID, now); err != nil {
			r.log.Warn.Printf("videos reaper: mark %s enqueued: %v", j.ID, err)
		}
		r.log.Info.Printf("videos reaper: re-added %s to the queue of %s", j.ID, j.ResourceID)
	}
}

func (r *Reaper) expireFiles(ctx context.Context, now time.Time) {
	jobs, err := r.store.ExpiredFiles(ctx, now, 500)
	if err != nil {
		r.log.Error.Printf("videos reaper: list expired files: %v", err)
		return
	}
	for _, j := range jobs {
		givenUp, err := r.deleteFiles(ctx, j)
		if err != nil {
			r.log.Error.Printf("videos reaper: delete files of %s (retried after newer jobs): %v", j.ID, err)
			if err := r.store.MarkFilesDeleteFailed(ctx, j.ID, now); err != nil {
				r.log.Error.Printf("videos reaper: mark delete of %s failed: %v", j.ID, err)
			}
			continue
		}
		if len(givenUp) > 0 {
			r.log.Error.Printf("videos reaper: files of %s can never be deleted through Atlas; "+
				"they are marked deleted and must be removed from the store by hand: %s",
				j.ID, strings.Join(givenUp, "; "))
		}
		if err := r.store.MarkFilesDeleted(ctx, j.ID, now); err != nil {
			r.log.Error.Printf("videos reaper: mark files of %s deleted: %v", j.ID, err)
		}
	}
}

// deleteFiles deletes every object job j stored. A key whose delete is
// refused for good (see deleteRefusedForGood) is skipped and returned with
// its error; any other failure stops and is returned, so the job is retried.
func (r *Reaper) deleteFiles(ctx context.Context, j *Job) (givenUp []string, err error) {
	for _, key := range []string{j.InputKey, j.VideoKey, j.ThumbnailKey} {
		if key == "" {
			continue
		}
		err := r.blob.Delete(ctx, key)
		switch {
		case err == nil:
		case deleteRefusedForGood(err):
			givenUp = append(givenUp, fmt.Sprintf("%s (%v)", key, err))
		default:
			return nil, err
		}
	}
	return givenUp, nil
}

// deleteRefusedForGood is a delete no retry can make succeed: the key is not
// one this service's layout makes, or Atlas no longer has the job (404).
//
// A 403 from Atlas is NOT final: Atlas also answers 403 to a wrong or rotated
// internal token, on every route. Treating it as final would let one
// misconfiguration mark every expired job's files deleted while the objects
// stay in the bucket, with nothing left to retry them. Such a job is retried
// behind newer ones instead (ExpiredFiles orders it last).
func deleteRefusedForGood(err error) bool {
	if errors.Is(err, errNotVideoKey) {
		return true
	}
	var ae *atlasError
	return errors.As(err, &ae) && ae.status == http.StatusNotFound
}
