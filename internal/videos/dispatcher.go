package videos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// DispatchConfig bounds the dispatcher.
type DispatchConfig struct {
	// RunTimeout is the most one engine call may take (design 5.3: 30 min).
	RunTimeout time.Duration
	// Lease is how long a claim lasts without renewal; renewed every Lease/3.
	Lease time.Duration
	// Retention is how long files are kept after completion (design 6.2).
	Retention time.Duration
	// MaxOutputBytes fails a job whose video is larger (design 6.3: 512 MiB).
	MaxOutputBytes int64
	// WorkDir holds a video while it is probed, labelled and uploaded.
	WorkDir string
	// Prefix is the object key prefix.
	Prefix string
	// Poll is how often idle endpoints are checked for new jobs.
	Poll time.Duration
	// NotReadyBackoff is how long an endpoint that refused connections is
	// left alone (it is starting, or scaled down).
	NotReadyBackoff time.Duration
}

func (c *DispatchConfig) defaults() {
	if c.RunTimeout <= 0 {
		c.RunTimeout = 30 * time.Minute
	}
	if c.Lease <= 0 {
		c.Lease = 2 * time.Minute
	}
	if c.Retention <= 0 {
		c.Retention = 24 * time.Hour
	}
	if c.MaxOutputBytes <= 0 {
		c.MaxOutputBytes = 512 << 20
	}
	if c.WorkDir == "" {
		c.WorkDir = os.TempDir()
	}
	if c.Poll <= 0 {
		c.Poll = time.Second
	}
	if c.NotReadyBackoff <= 0 {
		c.NotReadyBackoff = 15 * time.Second
	}
}

// Dispatcher takes jobs from each endpoint's queue, at most the endpoint's
// max_concurrency at a time, runs them on the engine and stores the result.
// Several dispatcher replicas may run: every claim is decided in Postgres.
type Dispatcher struct {
	cfg    DispatchConfig
	store  *Store
	queue  *Queue
	blob   Blob
	engine Engine
	media  MediaTool
	log    *logging.Logger
	now    func() time.Time

	mu      sync.Mutex
	backoff map[string]time.Time
	wg      sync.WaitGroup
}

func NewDispatcher(cfg DispatchConfig, store *Store, queue *Queue, blob Blob, engine Engine, media MediaTool, log *logging.Logger) *Dispatcher {
	cfg.defaults()
	return &Dispatcher{cfg: cfg, store: store, queue: queue, blob: blob, engine: engine, media: media,
		log: log, now: time.Now, backoff: map[string]time.Time{}}
}

// Run dispatches until ctx ends, then waits for running jobs to finish or
// their own timeouts to pass. A job still running at shutdown keeps its lease
// until it expires, and the reaper then requeues it.
func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Poll)
	defer t.Stop()
	for {
		d.tick(ctx)
		select {
		case <-ctx.Done():
			d.wg.Wait()
			return
		case <-t.C:
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) {
	resources, err := d.store.ActiveResources(ctx)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Error.Printf("videos dispatcher: list endpoints: %v", err)
		}
		return
	}
	now := d.now()
	for _, resource := range resources {
		d.mu.Lock()
		until, waiting := d.backoff[resource]
		d.mu.Unlock()
		if waiting && now.Before(until) {
			continue
		}
		d.fill(ctx, resource)
	}
}

// fill starts jobs on one endpoint until it is busy or its queue is empty.
func (d *Dispatcher) fill(ctx context.Context, resource string) {
	for ctx.Err() == nil {
		entry, err := d.queue.Next(ctx, resource, d.cfg.Lease)
		if err != nil {
			d.log.Error.Printf("videos dispatcher: read queue of %s: %v", resource, err)
			return
		}
		if entry == nil {
			return
		}
		job, busy, err := d.store.Claim(ctx, entry.JobID, d.cfg.Lease, d.now().UTC())
		if err != nil {
			d.log.Error.Printf("videos dispatcher: claim %s: %v", entry.JobID, err)
			return
		}
		if busy {
			// Leave the entry pending: this consumer reads it first next time,
			// so the endpoint keeps its order.
			return
		}
		if err := d.queue.Done(ctx, resource, entry.StreamID); err != nil {
			d.log.Warn.Printf("videos dispatcher: ack %s: %v", entry.JobID, err)
		}
		if job == nil {
			continue // cancelled, expired or already claimed elsewhere
		}
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.run(context.WithoutCancel(ctx), job)
		}()
	}
}

func (d *Dispatcher) run(ctx context.Context, job *Job) {
	attempt := job.Attempts
	leaseCtx, stopLease := context.WithCancel(ctx)
	defer stopLease()
	go d.renew(leaseCtx, job.ID, attempt)

	started := d.now()
	d.log.Info.Printf("videos dispatcher: running %s on %s (attempt %d)", job.ID, job.ResourceID, attempt)
	dir, err := os.MkdirTemp(d.cfg.WorkDir, job.ID+"-")
	if err != nil {
		d.fail(ctx, job, attempt, CodeEngine, "internal error", fmt.Errorf("work dir: %w", err))
		return
	}
	defer os.RemoveAll(dir)
	videoPath := filepath.Join(dir, "video.mp4")

	var input []byte
	if job.InputKey != "" {
		input, err = d.readInput(ctx, job.InputKey)
		if err != nil {
			d.retryOrFail(ctx, job, attempt, fmt.Errorf("read input image: %w", err))
			return
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, d.cfg.RunTimeout)
	defer cancel()
	written, err := d.engine.Generate(runCtx, job, input, videoPath, d.cfg.MaxOutputBytes)
	wall := d.now().Sub(started)
	switch {
	case err == nil:
	case errors.Is(err, ErrOutputTooLarge):
		d.fail(ctx, job, attempt, CodeOutputTooLarge,
			fmt.Sprintf("the generated video is larger than %d bytes", d.cfg.MaxOutputBytes), err)
		return
	case errors.Is(err, context.DeadlineExceeded) || runCtx.Err() != nil:
		d.fail(ctx, job, attempt, CodeTimeout,
			fmt.Sprintf("generation took longer than %s", d.cfg.RunTimeout), err)
		return
	case errors.Is(err, ErrEngineNotReady):
		// The engine refused the connection: it is starting or scaled down.
		// The attempt does not count, and the endpoint is left alone briefly;
		// the queue timeout bounds the wait.
		d.notReady(ctx, job, attempt, err)
		return
	case errors.Is(err, ErrEngineGone):
		d.retryOrFail(ctx, job, attempt, err)
		return
	default:
		d.fail(ctx, job, attempt, CodeEngine, engineMessage(err), err)
		return
	}
	d.complete(ctx, job, attempt, videoPath, written, wall)
}

func (d *Dispatcher) readInput(ctx context.Context, key string) ([]byte, error) {
	r, size, err := d.blob.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if size > MaxInputImageBytes {
		return nil, fmt.Errorf("input image is %d bytes", size)
	}
	buf := make([]byte, size)
	_, err = readFull(r, buf)
	return buf, err
}

func (d *Dispatcher) complete(ctx context.Context, job *Job, attempt int, videoPath string, written int64, wall time.Duration) {
	probe, err := d.media.ProbeFile(ctx, videoPath)
	if err != nil {
		d.fail(ctx, job, attempt, CodeEngine, "the engine returned a file that is not a playable video", err)
		return
	}
	if err := AppendXMP(videoPath, XMPPacket(job.BaseModel, job.ID)); err != nil {
		d.fail(ctx, job, attempt, CodeEngine, "internal error", fmt.Errorf("label: %w", err))
		return
	}
	keys := Keys{Prefix: d.cfg.Prefix}
	videoKey := keys.Video(job.OrgID, job.ID)
	info, err := os.Stat(videoPath)
	if err != nil {
		d.fail(ctx, job, attempt, CodeEngine, "internal error", err)
		return
	}
	if err := d.putFile(ctx, videoKey, videoPath, info.Size(), "video/mp4"); err != nil {
		d.retryOrFail(ctx, job, attempt, fmt.Errorf("store video: %w", err))
		return
	}
	thumbKey := ""
	thumbPath := filepath.Join(filepath.Dir(videoPath), "thumbnail.webp")
	if err := d.media.Thumbnail(ctx, videoPath, thumbPath); err != nil {
		// A thumbnail is a convenience; the video is what was asked for.
		d.log.Warn.Printf("videos dispatcher: thumbnail of %s: %v", job.ID, err)
	} else if st, err := os.Stat(thumbPath); err == nil {
		k := keys.Thumbnail(job.OrgID, job.ID)
		if err := d.putFile(ctx, k, thumbPath, st.Size(), "image/webp"); err != nil {
			d.log.Warn.Printf("videos dispatcher: store thumbnail of %s: %v", job.ID, err)
		} else {
			thumbKey = k
		}
	}
	resolution, err := Resolution(job.Size)
	if err != nil {
		d.fail(ctx, job, attempt, CodeEngine, "internal error", err)
		return
	}
	now := d.now().UTC()
	ok, err := d.store.Finish(ctx, job.ID, attempt, finish{
		status: StatusCompleted, at: now, videoKey: videoKey, thumbKey: thumbKey,
		videoBytes: info.Size(), frames: probe.Frames, outputSeconds: probe.OutputSeconds(),
		gpuSeconds: gpuSeconds(job.GPUs, wall.Milliseconds()), expiresAt: now.Add(d.cfg.Retention),
		resolution: resolution,
	})
	if err != nil || !ok {
		// Not ours any more (the lease was taken over) or the write failed:
		// remove what this attempt stored so nothing outlives its record.
		d.log.Error.Printf("videos dispatcher: finish %s (attempt %d) ok=%v: %v", job.ID, attempt, ok, err)
		_ = d.blob.Delete(ctx, videoKey)
		if thumbKey != "" {
			_ = d.blob.Delete(ctx, thumbKey)
		}
		return
	}
	d.log.Info.Printf("videos dispatcher: completed %s frames=%d seconds=%s bytes=%d (engine wrote %d) wall=%s",
		job.ID, probe.Frames, probe.OutputSeconds(), info.Size(), written, wall.Round(time.Millisecond))
}

func (d *Dispatcher) putFile(ctx context.Context, key, path string, size int64, ctype string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return d.blob.Put(ctx, key, f, size, ctype)
}

func (d *Dispatcher) fail(ctx context.Context, job *Job, attempt int, code, message string, cause error) {
	d.log.Warn.Printf("videos dispatcher: %s failed (%s): %v", job.ID, code, cause)
	if _, err := d.store.Finish(ctx, job.ID, attempt, finish{status: StatusFailed, code: code,
		message: message, at: d.now().UTC()}); err != nil {
		d.log.Error.Printf("videos dispatcher: record failure of %s: %v", job.ID, err)
	}
}

// retryOrFail retries once when the attempt failed for a reason outside the
// job (the pod went away, storage blipped); a second such failure fails it.
// The job is not charged either way until it completes.
func (d *Dispatcher) retryOrFail(ctx context.Context, job *Job, attempt int, cause error) {
	if attempt >= 2 {
		d.fail(ctx, job, attempt, CodeEngine, "the video could not be generated", cause)
		return
	}
	d.log.Warn.Printf("videos dispatcher: %s attempt %d failed, retrying once: %v", job.ID, attempt, cause)
	d.requeue(ctx, job, attempt, false)
}

func (d *Dispatcher) notReady(ctx context.Context, job *Job, attempt int, cause error) {
	d.log.Info.Printf("videos dispatcher: endpoint %s not ready for %s, waiting: %v", job.ResourceID, job.ID, cause)
	d.mu.Lock()
	d.backoff[job.ResourceID] = d.now().Add(d.cfg.NotReadyBackoff)
	d.mu.Unlock()
	d.requeue(ctx, job, attempt, true)
}

func (d *Dispatcher) requeue(ctx context.Context, job *Job, attempt int, uncount bool) {
	ok, err := d.store.Requeue(ctx, job.ID, attempt, d.now().UTC(), uncount)
	if err != nil || !ok {
		d.log.Error.Printf("videos dispatcher: requeue %s ok=%v: %v", job.ID, ok, err)
		return
	}
	if err := d.queue.Add(ctx, job.ResourceID, job.ID); err != nil {
		d.log.Warn.Printf("videos dispatcher: re-add %s to the queue (the reaper will): %v", job.ID, err)
	}
}

func (d *Dispatcher) renew(ctx context.Context, id string, attempt int) {
	t := time.NewTicker(d.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ok, err := d.store.Renew(ctx, id, attempt, d.now().UTC().Add(d.cfg.Lease))
			if err != nil {
				d.log.Warn.Printf("videos dispatcher: renew lease of %s: %v", id, err)
			} else if !ok {
				return
			}
		}
	}
}
