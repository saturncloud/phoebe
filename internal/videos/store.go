package videos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Store is the video_job and video_billing_event tables (migration 0013).
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// ErrLimit is returned by Insert when a limit refuses the job.
var (
	errStoreOrgLimit  = errors.New("org jobs-in-flight limit")
	errStoreQueueFull = errors.New("endpoint queue full")
)

const jobColumns = `id, org_id, auth_id, COALESCE(user_id, ''), COALESCE(group_id, ''), resource_id,
	served_model, base_model, serving_mode, upstream, COALESCE(region, ''), task, prompt, seconds, size,
	fps, gpus, max_concurrency, params, status, progress, COALESCE(error_code, ''), COALESCE(error_message, ''), attempts,
	enqueued_at, created_at, started_at, completed_at, expires_at, files_deleted_at, COALESCE(input_key, ''),
	COALESCE(video_key, ''), COALESCE(thumbnail_key, ''), COALESCE(video_bytes, 0),
	COALESCE(output_frames, 0), COALESCE(output_seconds::text, '')`

type scanner interface{ Scan(...any) error }

func scanJob(s scanner) (*Job, error) {
	var j Job
	var params []byte
	if err := s.Scan(&j.ID, &j.OrgID, &j.AuthID, &j.UserID, &j.GroupID, &j.ResourceID,
		&j.ServedModel, &j.BaseModel, &j.ServingMode, &j.Upstream, &j.Region, &j.Task, &j.Prompt,
		&j.Seconds, &j.Size, &j.FPS, &j.GPUs, &j.MaxConcurrency, &params, &j.Status, &j.Progress, &j.ErrorCode,
		&j.ErrorMessage, &j.Attempts, &j.EnqueuedAt, &j.CreatedAt, &j.StartedAt, &j.CompletedAt, &j.ExpiresAt,
		&j.FilesDeleted, &j.InputKey, &j.VideoKey, &j.ThumbnailKey, &j.VideoBytes, &j.OutputFrames,
		&j.OutputSeconds); err != nil {
		return nil, err
	}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &j.Params); err != nil {
			return nil, fmt.Errorf("video_job %s params: %w", j.ID, err)
		}
	}
	return &j, nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Insert writes a new queued job, unless the org already has jobsInFlight jobs
// queued or running on the endpoint (errStoreOrgLimit) or the endpoint has
// queueCap queued jobs (errStoreQueueFull). The counts and the insert run
// under one transaction-scoped advisory lock per endpoint, so two concurrent
// creates cannot both pass a limit with one slot left.
func (s *Store) Insert(ctx context.Context, j *Job, jobsInFlight, queueCap int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('video_job:' || $1, 0))`, j.ResourceID); err != nil {
		return err
	}
	var orgOpen, queued int
	if err := tx.QueryRowContext(ctx, `SELECT
			count(*) FILTER (WHERE org_id = $2),
			count(*) FILTER (WHERE status = 'queued')
		FROM video_job WHERE resource_id = $1 AND status IN ('queued', 'in_progress')`,
		j.ResourceID, j.OrgID).Scan(&orgOpen, &queued); err != nil {
		return err
	}
	if orgOpen >= jobsInFlight {
		return errStoreOrgLimit
	}
	if queued >= queueCap {
		return errStoreQueueFull
	}
	params, err := json.Marshal(j.Params)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO video_job (id, org_id, auth_id, user_id, group_id,
			resource_id, served_model, base_model, serving_mode, upstream, region, task, prompt, seconds,
			size, fps, gpus, max_concurrency, params, status, input_key, created_at, enqueued_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $21, $18,
			'queued', $19, $20, $20)`,
		j.ID, j.OrgID, j.AuthID, nullable(j.UserID), nullable(j.GroupID), j.ResourceID, j.ServedModel,
		j.BaseModel, j.ServingMode, j.Upstream, nullable(j.Region), j.Task, j.Prompt, j.Seconds, j.Size,
		j.FPS, j.GPUs, params, nullable(j.InputKey), j.CreatedAt, j.MaxConcurrency); err != nil {
		return err
	}
	return tx.Commit()
}

// Get reads a job that belongs to the org on the endpoint. Any other job,
// including one that exists for another org or endpoint, is sql.ErrNoRows:
// the caller cannot learn that an id exists.
func (s *Store) Get(ctx context.Context, id, orgID, resourceID string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM video_job
		WHERE id = $1 AND org_id = $2 AND resource_id = $3`, id, orgID, resourceID)
	return scanJob(row)
}

// GetByID reads a job by id alone (dispatcher and reaper use only).
func (s *Store) GetByID(ctx context.Context, id string) (*Job, error) {
	return scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM video_job WHERE id = $1`, id))
}

// List returns up to limit+1 of the org's jobs on the endpoint, ordered by
// (created_at, id) in order, starting after the job named by after (which
// must itself belong to the org and endpoint; otherwise sql.ErrNoRows).
func (s *Store) List(ctx context.Context, orgID, resourceID, after string, limit int, asc bool) ([]*Job, error) {
	cmp, dir := "<", "DESC"
	if asc {
		cmp, dir = ">", "ASC"
	}
	args := []any{orgID, resourceID, limit + 1}
	cursor := ""
	if after != "" {
		var created time.Time
		if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM video_job
			WHERE id = $1 AND org_id = $2 AND resource_id = $3`, after, orgID, resourceID).Scan(&created); err != nil {
			return nil, err
		}
		args = append(args, created, after)
		cursor = fmt.Sprintf(` AND (created_at, id) %s ($4, $5)`, cmp)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM video_job
		WHERE org_id = $1 AND resource_id = $2`+cursor+`
		ORDER BY created_at `+dir+`, id `+dir+` LIMIT $3`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// CancelQueued cancels a queued job of the org on the endpoint and writes its
// zero-quantity billing row. It reports the job after the change, or the job
// unchanged when it was not queued.
func (s *Store) CancelQueued(ctx context.Context, id, orgID, resourceID string, now time.Time) (*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM video_job
		WHERE id = $1 AND org_id = $2 AND resource_id = $3 FOR UPDATE`, id, orgID, resourceID))
	if err != nil {
		return nil, err
	}
	if j.Status != StatusQueued {
		return j, tx.Commit()
	}
	if err := finishTx(ctx, tx, j, finish{status: StatusCancelled, code: CodeCancelled,
		message: "cancelled by the caller", at: now}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetByID(ctx, id)
}

// Claim moves a queued job to in_progress with a lease, when the endpoint
// has fewer than the job's max_concurrency jobs in progress. It returns nil, nil when the
// job is no longer queued (cancelled, timed out, already claimed) or the
// endpoint is busy (busy=true).
func (s *Store) Claim(ctx context.Context, id string, lease time.Duration, now time.Time) (j *Job, busy bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var resourceID, status string
	var maxConcurrency int
	if err := tx.QueryRowContext(ctx, `SELECT resource_id, status, max_concurrency FROM video_job WHERE id = $1`, id).
		Scan(&resourceID, &status, &maxConcurrency); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if status != StatusQueued {
		return nil, false, nil
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('video_job:' || $1, 0))`, resourceID); err != nil {
		return nil, false, err
	}
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM video_job WHERE resource_id = $1 AND status = 'in_progress'`,
		resourceID).Scan(&running); err != nil {
		return nil, false, err
	}
	if running >= maxConcurrency {
		return nil, true, nil
	}
	res, err := tx.ExecContext(ctx, `UPDATE video_job SET status = 'in_progress', progress = 50,
			started_at = $2, lease_until = $3, attempts = attempts + 1
		WHERE id = $1 AND status = 'queued'`, id, now, now.Add(lease))
	if err != nil {
		return nil, false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, false, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	j, err = s.GetByID(ctx, id)
	return j, false, err
}

// Renew extends a running job's lease; false when the job is no longer ours
// to run (the reaper requeued or failed it).
func (s *Store) Renew(ctx context.Context, id string, attempts int, until time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE video_job SET lease_until = $3
		WHERE id = $1 AND status = 'in_progress' AND attempts = $2`, id, attempts, until)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// finish is how a job ends.
type finish struct {
	status        string
	code, message string
	at            time.Time
	// Completed jobs only.
	videoKey, thumbKey string
	videoBytes         int64
	frames             int
	outputSeconds      string // exact decimal
	gpuSeconds         string // exact decimal
	expiresAt          time.Time
	resolution         string
}

// Complete or Fail a running job, as attempt number attempts. It does nothing
// (false) when the job is no longer that attempt in progress, so a dispatcher
// whose lease was taken over cannot overwrite the new attempt's outcome.
func (s *Store) Finish(ctx context.Context, id string, attempts int, f finish) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM video_job WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return false, err
	}
	if j.Status != StatusInProgress || j.Attempts != attempts {
		return false, nil
	}
	if err := finishTx(ctx, tx, j, f); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// finishTx moves job j (locked by the caller) to its final status and writes
// its one billing row, in the caller's transaction. Only a completed job
// carries quantities and a request count.
func finishTx(ctx context.Context, tx *sql.Tx, j *Job, f finish) error {
	resolution := f.resolution
	if resolution == "" {
		r, err := Resolution(j.Size)
		if err != nil {
			return err
		}
		resolution = r
	}
	if f.status == StatusCompleted {
		if _, err := tx.ExecContext(ctx, `UPDATE video_job SET status = 'completed', progress = 100,
				completed_at = $2, expires_at = $3, lease_until = NULL, video_key = $4, thumbnail_key = $5,
				video_bytes = $6, output_frames = $7, output_seconds = $8::numeric, gpu_seconds = $9::numeric
			WHERE id = $1`, j.ID, f.at, f.expiresAt, f.videoKey, nullable(f.thumbKey), f.videoBytes,
			f.frames, f.outputSeconds, f.gpuSeconds); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE video_job SET status = $2, progress = 0, completed_at = $3,
				lease_until = NULL, error_code = $4, error_message = $5
			WHERE id = $1`, j.ID, f.status, f.at, f.code, f.message); err != nil {
			return err
		}
	}
	outSeconds, frames, count, gpuSeconds := "0", 0, 0, "0"
	if f.status == StatusCompleted {
		outSeconds, frames, count, gpuSeconds = f.outputSeconds, f.frames, 1, f.gpuSeconds
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO video_billing_event (job_id, created_at, org_id, auth_id,
			user_id, group_id, resource_id, model_id, base_model, serving_mode, task, resolution, region,
			status, output_seconds, output_frames, request_count, gpu_seconds)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::numeric, $16, $17, $18::numeric)
		ON CONFLICT (job_id) DO NOTHING`,
		j.ID, f.at, j.OrgID, j.AuthID, nullable(j.UserID), nullable(j.GroupID), j.ResourceID, j.ServedModel,
		j.BaseModel, j.ServingMode, j.Task, resolution, nullable(j.Region), f.status, outSeconds, frames,
		count, gpuSeconds)
	return err
}

// Requeue puts a running job whose attempt failed for a transient reason back
// in the queue (the dispatcher retries once if the pod went away; design 5.2).
//
// uncount gives the attempt back: the engine was not ready (it refused the
// connection), so no work was attempted.
func (s *Store) Requeue(ctx context.Context, id string, attempts int, now time.Time, uncount bool) (bool, error) {
	back := 0
	if uncount {
		back = 1
	}
	res, err := s.db.ExecContext(ctx, `UPDATE video_job SET status = 'queued', progress = 0,
			lease_until = NULL, started_at = NULL, enqueued_at = $3, attempts = attempts - $4
		WHERE id = $1 AND status = 'in_progress' AND attempts = $2`, id, attempts, now, back)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// QueuedForResource lists queued job ids of an endpoint, oldest first.
func (s *Store) Queued(ctx context.Context, olderThan time.Time, limit int) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM video_job
		WHERE status = 'queued' AND enqueued_at < $1 ORDER BY created_at LIMIT $2`, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// MarkEnqueued records that a queued job's id was added to its stream again.
func (s *Store) MarkEnqueued(ctx context.Context, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE video_job SET enqueued_at = $2 WHERE id = $1 AND status = 'queued'`, id, at)
	return err
}

// ActiveResources lists endpoints with queued or running jobs.
func (s *Store) ActiveResources(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT resource_id FROM video_job WHERE status IN ('queued', 'in_progress')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ExpireQueued fails jobs that waited longer than the queue timeout.
func (s *Store) ExpireQueued(ctx context.Context, createdBefore, now time.Time) (int, error) {
	return s.failWhere(ctx, `status = 'queued' AND created_at < $1`, createdBefore, finish{
		status: StatusFailed, code: CodeQueueTimeout, at: now,
		message: "the job waited in the queue longer than the queue timeout",
	})
}

// ExpireLeases handles running jobs whose dispatcher stopped renewing the
// lease (it crashed or lost its connection): the first attempt is requeued,
// a second one fails, so a job that kills its dispatcher cannot loop forever.
func (s *Store) ExpireLeases(ctx context.Context, now time.Time) (requeued, failed int, err error) {
	res, err := s.db.ExecContext(ctx, `UPDATE video_job SET status = 'queued', progress = 0, lease_until = NULL,
			started_at = NULL, enqueued_at = 'epoch'
		WHERE status = 'in_progress' AND lease_until < $1 AND attempts < 2`, now)
	if err != nil {
		return 0, 0, err
	}
	n, _ := res.RowsAffected()
	f, err := s.failWhere(ctx, `status = 'in_progress' AND lease_until < $1`, now, finish{
		status: StatusFailed, code: CodeEngine, at: now,
		message: "the job's worker stopped twice while generating it",
	})
	return int(n), f, err
}

func (s *Store) failWhere(ctx context.Context, where string, arg any, f finish) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM video_job WHERE `+where+` LIMIT 500`, arg)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	done := 0
	for _, id := range ids {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return done, err
		}
		j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM video_job WHERE id = $1 AND `+
			whereWithID(where)+` FOR UPDATE`, id, arg))
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			continue
		}
		if err != nil {
			_ = tx.Rollback()
			return done, err
		}
		if err := finishTx(ctx, tx, j, f); err != nil {
			_ = tx.Rollback()
			return done, err
		}
		if err := tx.Commit(); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// whereWithID renumbers a one-argument condition ($1) to follow the id ($1).
func whereWithID(where string) string {
	out := []byte{}
	for i := 0; i < len(where); i++ {
		if where[i] == '$' && i+1 < len(where) && where[i+1] == '1' {
			out = append(out, '$', '2')
			i++
			continue
		}
		out = append(out, where[i])
	}
	return string(out)
}

// ExpiredFiles lists completed jobs whose files are past expires_at, or jobs
// that ended without completing but still have an input image stored.
func (s *Store) ExpiredFiles(ctx context.Context, now time.Time, limit int) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM video_job
		WHERE files_deleted_at IS NULL AND (
			(expires_at IS NOT NULL AND expires_at < $1)
			OR (status IN ('failed', 'cancelled') AND completed_at < $1)
		) ORDER BY created_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// MarkFilesDeleted records that a job's files are gone and erases its prompt
// (design 6.2: the customer's text is not kept longer than their video).
func (s *Store) MarkFilesDeleted(ctx context.Context, id string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE video_job SET files_deleted_at = $2, prompt = NULL,
			params = params - 'negative_prompt'
		WHERE id = $1 AND files_deleted_at IS NULL`, id, now)
	return err
}

// MarkFilesDeletedNow is DELETE /v1/videos/{id} on a finished job: the files
// expire at once.
func (s *Store) ExpireNow(ctx context.Context, id string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE video_job SET expires_at = $2
		WHERE id = $1 AND (expires_at IS NULL OR expires_at > $2)`, id, now)
	return err
}

// PruneRows deletes job and billing rows created before cutoff whose files
// are gone (or never existed), in batches.
func (s *Store) PruneRows(ctx context.Context, cutoff time.Time) (jobs, events int64, err error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM video_job WHERE id IN (
		SELECT id FROM video_job WHERE created_at < $1
			AND status IN ('completed', 'failed', 'cancelled')
			AND (files_deleted_at IS NOT NULL OR (input_key IS NULL AND video_key IS NULL))
		LIMIT 5000)`, cutoff)
	if err != nil {
		return 0, 0, err
	}
	jobs, _ = res.RowsAffected()
	res, err = s.db.ExecContext(ctx, `DELETE FROM video_billing_event WHERE job_id IN (
		SELECT job_id FROM video_billing_event WHERE created_at < $1 LIMIT 5000)`, cutoff)
	if err != nil {
		return jobs, 0, err
	}
	events, _ = res.RowsAffected()
	return jobs, events, nil
}
