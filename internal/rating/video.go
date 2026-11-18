package rating

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
	"github.com/saturncloud/phoebe/internal/prune"
)

// VideoRateReach is how far back the rater re-rates video: one day inside
// phoebe-videos' row retention (prune.VideoRowRetentionDays), a margin for the
// reaper's cadence and for clock skew between the videos pod and the
// database. An hour older than that may have lost some or all of its
// video_billing_event rows, so re-running it would replace its rollups with a
// partial sum or reconcile-delete them, and token-push would then delete the
// manager's records. Token rating is not limited by it.
const VideoRateReach = time.Duration(prune.VideoRowRetentionDays-1) * 24 * time.Hour

// VideoFloor is the oldest hour the rater may re-rate video in at now.
func VideoFloor(now time.Time) time.Time {
	return now.UTC().Truncate(time.Hour).Add(-VideoRateReach)
}

// Video generation usage (video models design, approved 2026-11-18, sections
// 7.2 and 7.3). phoebe-videos writes one video_billing_event row per finished
// job (migration 0016); the aggregator rolls them up by the hour into
// rated_video_usage (migration 0017) in the same transaction as the token
// rollup, and cmd/token-push sends them as kind "video" records.
//
// THE GRAIN is (auth_id, owner_type, owner_id, resource_id, model_id,
// serving_mode, base_model, task, resolution, region, hour), carrying org_id.
// Every grain column is a wire attribute and enters the record's usage_id.
// base_model and region are grain here, not carried attributes as for tokens:
// two base models or two regions in one hour are two records, never a conflict.
//
// THE SAME FAIL-CLOSED RULES AS TOKENS:
//   - Evidence that cannot become any record (an empty auth, resource or model
//     id; negative quantities; a request_count that disagrees with the job's
//     status) is withheld per event, counted, logged at ERROR, and drives
//     cmd/rater's exit 2. The raw rows stay in video_billing_event.
//   - CONFLICTS are PARKED per gate identity (auth, owner, resource, model,
//     serving mode) and hour in parked_video_usage, with their quantities: an
//     owner conflict (a job carrying both a user and a group), an ambiguous org
//     (two orgs for one identity in the hour) and a missing org (org_id '' on
//     every job of the identity). Only that identity is held back.
//
// A job's hour is video_billing_event.created_at, the instant it finished.
//
// Failed and cancelled jobs are aggregated: they add to event_count only.
// phoebe-videos writes them with every quantity 0, gpu_seconds included
// (design 7.2: "a row with zero quantities and their status"), so a rollup of
// failed jobs only is sent with all-zero quantities and prices at $0.

// VideoAggregateResult is what one video aggregation statement reports. The
// event counts partition the in-window video_billing_event rows:
//
//	EventsAggregated + InvalidUsage + Unattributable + OwnerConflict
//	  + AmbiguousOrg + MissingOrg == in-window video_billing_event rows
type VideoAggregateResult struct {
	RollupsWritten   int64
	EventsAggregated int64
	// ReconciledDeletions and ParkedDeletions mean what they mean for tokens
	// (AggregateResult), for rated_video_usage rows.
	ReconciledDeletions int64
	ParkedDeletions     int64

	// InvalidUsageEvents: a negative quantity, or a request_count that is not 1
	// for a completed job (0016's checks already force 0 for the others).
	InvalidUsageEvents int64
	// UnattributableEvents: auth_id, resource_id or model_id is ''.
	UnattributableEvents int64
	// OwnerConflictEvents: the job carried both a user and a group.
	OwnerConflictEvents int64
	// AmbiguousOrgEvents: jobs of an identity-hour that named more than one org.
	AmbiguousOrgEvents int64
	// MissingOrgEvents: jobs of an identity-hour none of which named an org.
	MissingOrgEvents int64

	ParkedWritten  int64
	ParkedResolved int64

	// EmptyBaseModelEvents counts AGGREGATED jobs with base_model ''. Sent as ""
	// and priced by the manager (a shared one is reported unpriced). Not a
	// partition bucket.
	EmptyBaseModelEvents int64

	// OutOfReachHours counts the window's hours before VideoFloor that hold
	// video rollups: their video was not re-rated (its jobs may be pruned)
	// and the rollups are kept as the record.
	OutOfReachHours int64
	// UnratedOutOfReachHours counts the window's hours before VideoFloor
	// that hold completed video jobs but no video rollup and no park: usage
	// the rater never rated and now never will (phoebe-videos prunes the jobs
	// at 30 days). Settled by hand through the manager's credit path.
	UnratedOutOfReachHours int64
	// TombstonesWritten counts the video usage_ids this run recorded as
	// removed by the rater (video_reconcile_deletion): deleted rollups and
	// rollups whose usage_id changed.
	TombstonesWritten int64

	// UnratedInReachHours counts, on a routine run, the hours between
	// VideoFloor and the start of its window that hold completed, rateable
	// video jobs of an identity with no rollup and no unresolved park: the
	// rater was down (or suspended) for longer than its trailing window. They
	// can still be rated: re-rate them with --since/--until.
	UnratedInReachHours int64
}

// add sums two video results field by field.
func (r VideoAggregateResult) add(b VideoAggregateResult) VideoAggregateResult {
	r.RollupsWritten += b.RollupsWritten
	r.EventsAggregated += b.EventsAggregated
	r.ReconciledDeletions += b.ReconciledDeletions
	r.ParkedDeletions += b.ParkedDeletions
	r.InvalidUsageEvents += b.InvalidUsageEvents
	r.UnattributableEvents += b.UnattributableEvents
	r.OwnerConflictEvents += b.OwnerConflictEvents
	r.AmbiguousOrgEvents += b.AmbiguousOrgEvents
	r.MissingOrgEvents += b.MissingOrgEvents
	r.ParkedWritten += b.ParkedWritten
	r.ParkedResolved += b.ParkedResolved
	r.EmptyBaseModelEvents += b.EmptyBaseModelEvents
	r.OutOfReachHours += b.OutOfReachHours
	r.UnratedOutOfReachHours += b.UnratedOutOfReachHours
	r.UnratedInReachHours += b.UnratedInReachHours
	r.TombstonesWritten += b.TombstonesWritten
	return r
}

// aggregateVideoWindowSQL filters, groups, upserts, reconciles and counts the
// video rollups of [$1, $2) in ONE statement, for the same reason as
// aggregateWindowSQL: a row phoebe-videos commits mid-run is visible to both
// the rollups and the counts or to neither.
//
// The hour bucket is session-TZ-independent (date_trunc on a UTC wall clock).
const aggregateVideoWindowSQL = `
WITH ev AS (
    SELECT
        -- '' is unattributable: the wire requires each of these to be non-empty.
        NULLIF(auth_id, '')     AS auth_id,
        NULLIF(resource_id, '') AS resource_id,
        NULLIF(model_id, '')    AS model_id,
        -- '' means no org: the payer is unknown (missing_org).
        NULLIF(org_id, '')      AS org_id,
        -- 0016 constrains serving_mode, task and resolution to the wire's enums
        -- with CHECKs, so they need no filter here.
        serving_mode, task, resolution,
        base_model,
        COALESCE(region, '')    AS region,
        CASE
            WHEN COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '') <> '' THEN 'user'
            WHEN COALESCE(group_id, '') <> '' THEN 'group'
            ELSE ''
        END AS owner_type,
        CASE
            WHEN COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '') <> '' THEN user_id
            WHEN COALESCE(group_id, '') <> '' THEN group_id
            ELSE ''
        END AS owner_id,
        (COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '') AS owner_conflict,
        -- request_count counts completed jobs only (design 7.3): 1 for a completed
        -- job, 0 otherwise. 0016 has no check that output_seconds, output_frames
        -- or gpu_seconds are non-negative, so they are checked here.
        -- NUMERIC accepts 'NaN', and Postgres orders NaN above every number,
        -- so NaN >= 0 is true: refuse it by name, or one NaN job would turn
        -- its grain's sum into NaN, which no record can carry.
        -- No '|' in an envelope field: the wire usage_id joins org, owner and
        -- resource with '|' unescaped, so such a job could share its id with
        -- another grain's record. It could never be sent (the wire requires
        -- hex ids), so it is invalid evidence here, not a rollup.
        (output_seconds >= 0 AND output_frames >= 0 AND gpu_seconds >= 0
         AND output_seconds <> 'NaN'::numeric AND gpu_seconds <> 'NaN'::numeric
         AND strpos(org_id || COALESCE(user_id, '') || COALESCE(group_id, '') || resource_id, '|') = 0
         AND request_count = CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS valid_usage,
        output_seconds, output_frames, request_count, gpu_seconds,
        date_trunc('hour', created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS window_start
    FROM video_billing_event
    WHERE created_at >= $1
      AND created_at <  $2
),
attributable AS (
    SELECT * FROM ev
    WHERE valid_usage
      AND auth_id     IS NOT NULL
      AND resource_id IS NOT NULL
      AND model_id    IS NOT NULL
),
billable AS (
    SELECT * FROM attributable WHERE NOT owner_conflict
),
-- The payer gate over the identity (the grain without base model, task,
-- resolution and region): who pays is a property of the endpoint, so two orgs
-- across two resolutions are still a conflict.
gate AS (
    SELECT auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start,
        MAX(org_id)                AS org_id,
        COUNT(DISTINCT org_id) > 1 AS ambiguous_org
    FROM billable
    GROUP BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start
),
grouped_raw AS (
    SELECT auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
        base_model, task, resolution, region, window_start,
        window_start + interval '1 hour' AS window_end,
        SUM(output_seconds)              AS output_seconds,
        SUM(output_frames)::bigint       AS output_frames,
        SUM(request_count)::bigint       AS request_count,
        SUM(gpu_seconds)                 AS gpu_seconds,
        COUNT(*)::bigint                 AS event_count
    FROM billable
    GROUP BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
             base_model, task, resolution, region, window_start
),
grouped AS (
    SELECT r.*, g.org_id, g.ambiguous_org
    FROM grouped_raw r
    JOIN gate g
      ON  g.auth_id      = r.auth_id
      AND g.owner_type   = r.owner_type
      AND g.owner_id     = r.owner_id
      AND g.resource_id  = r.resource_id
      AND g.model_id     = r.model_id
      AND g.serving_mode = r.serving_mode
      AND g.window_start = r.window_start
),
kept AS (
    SELECT * FROM grouped WHERE NOT ambiguous_org AND org_id IS NOT NULL
),
conflicts AS (
    SELECT window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
        CASE WHEN bool_or(ambiguous_org) THEN 'ambiguous_org' ELSE 'missing_org' END AS reason,
        SUM(event_count)::bigint   AS event_count,
        SUM(output_seconds)        AS output_seconds,
        SUM(output_frames)::bigint AS output_frames,
        SUM(request_count)::bigint AS request_count,
        SUM(gpu_seconds)           AS gpu_seconds
    FROM grouped
    WHERE ambiguous_org OR org_id IS NULL
    GROUP BY window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode
    UNION ALL
    -- Owner-conflict jobs have no owner, so they park under owner ('', '').
    SELECT window_start, auth_id, '', '', resource_id, model_id, serving_mode, 'owner_conflict',
        COUNT(*)::bigint, SUM(output_seconds), SUM(output_frames)::bigint,
        SUM(request_count)::bigint, SUM(gpu_seconds)
    FROM attributable
    WHERE owner_conflict
    GROUP BY window_start, auth_id, resource_id, model_id, serving_mode
),
parked_upsert AS (
    INSERT INTO parked_video_usage (window_start, auth_id, owner_type, owner_id, resource_id, model_id,
        serving_mode, reason, event_count, output_seconds, output_frames, request_count, gpu_seconds)
    SELECT window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, reason,
        event_count, output_seconds, output_frames, request_count, gpu_seconds
    FROM conflicts
    ON CONFLICT ON CONSTRAINT parked_video_usage_pk DO UPDATE SET
        event_count    = EXCLUDED.event_count,
        output_seconds = EXCLUDED.output_seconds,
        output_frames  = EXCLUDED.output_frames,
        request_count  = EXCLUDED.request_count,
        gpu_seconds    = EXCLUDED.gpu_seconds,
        last_seen_at   = now(),
        -- A conflict that comes back after being resolved is parked again.
        resolved_at    = NULL
    RETURNING 1
),
parked_resolved AS (
    UPDATE parked_video_usage p SET resolved_at = now()
    WHERE p.window_start >= $1
      AND p.window_start <  $2
      AND p.resolved_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM conflicts c
          WHERE c.window_start = p.window_start AND c.auth_id = p.auth_id
            AND c.owner_type = p.owner_type AND c.owner_id = p.owner_id
            AND c.resource_id = p.resource_id AND c.model_id = p.model_id
            AND c.serving_mode = p.serving_mode AND c.reason = p.reason
      )
    RETURNING 1
),
deleted AS (
    DELETE FROM rated_video_usage ru
    WHERE ru.window_start >= $1
      AND ru.window_start <  $2
      AND NOT EXISTS (
          SELECT 1 FROM kept k
          WHERE k.auth_id      = ru.auth_id
            AND k.owner_type   = ru.owner_type
            AND k.owner_id     = ru.owner_id
            AND k.resource_id  = ru.resource_id
            AND k.model_id     = ru.model_id
            AND k.serving_mode = ru.serving_mode
            AND k.base_model   = ru.base_model
            AND k.task         = ru.task
            AND k.resolution   = ru.resolution
            AND k.region       = ru.region
            AND k.window_start = ru.window_start
      )
    RETURNING ru.window_start, ru.auth_id, ru.owner_type, ru.owner_id, ru.resource_id,
              ru.model_id, ru.serving_mode, ru.usage_id
),
-- TOMBSTONES (0017 video_reconcile_deletion): every deleted rollup's usage_id,
-- in this statement, so token-push can tell a record the rater removed from
-- one that was lost. A deletion explained by a park is 'parked' (an audit
-- label: token-push holds a parked identity's ids by the park, not by the
-- reason); any other is 'reconcile', including a rollup whose jobs became
-- owner-conflicted (those park under owner ('', ''), a different identity, so
-- the record is released and the usage waits in the owner_conflict park).
-- usage_id IS NULL is defensive: every committed row is stamped (the stamp
-- runs in this transaction); only rows from a database that applied 0017
-- before usage_id existed can be NULL, and their records are held.
tombstoned AS (
    INSERT INTO video_reconcile_deletion (window_start, usage_id, auth_id, owner_type, owner_id,
        resource_id, model_id, serving_mode, reason)
    SELECT d.window_start, d.usage_id, d.auth_id, d.owner_type, d.owner_id, d.resource_id, d.model_id,
        d.serving_mode,
        CASE WHEN EXISTS (
            SELECT 1 FROM conflicts c
            WHERE c.window_start = d.window_start AND c.auth_id = d.auth_id
              AND c.owner_type = d.owner_type AND c.owner_id = d.owner_id
              AND c.resource_id = d.resource_id AND c.model_id = d.model_id
              AND c.serving_mode = d.serving_mode
              AND c.reason <> 'owner_conflict') THEN 'parked' ELSE 'reconcile' END
    FROM deleted d
    WHERE d.usage_id IS NOT NULL
    ON CONFLICT ON CONSTRAINT video_reconcile_deletion_pk DO UPDATE SET
        reason = EXCLUDED.reason, deleted_at = now()
    RETURNING 1
),
upserted AS (
    INSERT INTO rated_video_usage (
        id, auth_id, owner_type, owner_id, resource_id, org_id, model_id, serving_mode,
        base_model, task, resolution, region, window_start, window_end,
        output_seconds, output_frames, request_count, gpu_seconds, event_count
    )
    SELECT
        -- md5 of the LENGTH-PREFIXED grain (injective, so a '|' inside a field
        -- cannot collide two keys). Local row id only.
        md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || length(base_model)::text || ':' || base_model
          || '|' || length(task)::text || ':' || task
          || '|' || length(resolution)::text || ':' || resolution
          || '|' || length(region)::text || ':' || region
          || '|' || extract(epoch FROM window_start)::bigint::text),
        auth_id, owner_type, owner_id, resource_id, org_id, model_id, serving_mode,
        base_model, task, resolution, region, window_start, window_end,
        output_seconds, output_frames, request_count, gpu_seconds, event_count
    FROM kept
    ORDER BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
             base_model, task, resolution, region, window_start
    ON CONFLICT ON CONSTRAINT rated_video_usage_grain_uq DO UPDATE SET
        org_id         = EXCLUDED.org_id,
        window_end     = EXCLUDED.window_end,
        output_seconds = EXCLUDED.output_seconds,
        output_frames  = EXCLUDED.output_frames,
        request_count  = EXCLUDED.request_count,
        gpu_seconds    = EXCLUDED.gpu_seconds,
        event_count    = EXCLUDED.event_count,
        rated_at       = now()
    RETURNING event_count, base_model
)
SELECT
    (SELECT COUNT(*)::bigint                      FROM upserted) AS rollups_written,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted) AS events_aggregated,
    -- A deletion is explained by a park only if the park can hold a record:
    -- owner-conflict jobs park under owner ('', '') but never formed one, so
    -- a vanished no-owner rollup of the same endpoint is still a reconcile
    -- deletion and pages.
    (SELECT COUNT(*)::bigint FROM deleted d WHERE NOT EXISTS (
        SELECT 1 FROM conflicts c
        WHERE c.window_start = d.window_start AND c.auth_id = d.auth_id
          AND c.owner_type = d.owner_type AND c.owner_id = d.owner_id
          AND c.resource_id = d.resource_id AND c.model_id = d.model_id
          AND c.serving_mode = d.serving_mode
          AND c.reason <> 'owner_conflict'))                      AS reconciled_deletions,
    (SELECT COUNT(*)::bigint FROM deleted d WHERE EXISTS (
        SELECT 1 FROM conflicts c
        WHERE c.window_start = d.window_start AND c.auth_id = d.auth_id
          AND c.owner_type = d.owner_type AND c.owner_id = d.owner_id
          AND c.resource_id = d.resource_id AND c.model_id = d.model_id
          AND c.serving_mode = d.serving_mode
          AND c.reason <> 'owner_conflict'))                      AS parked_deletions,
    -- The withheld buckets, in precedence order so they PARTITION the window.
    (SELECT COUNT(*)::bigint FROM ev WHERE NOT valid_usage)       AS invalid_usage_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE valid_usage
        AND (auth_id IS NULL OR resource_id IS NULL OR model_id IS NULL))
                                                                  AS unattributable_events,
    (SELECT COUNT(*)::bigint FROM attributable WHERE owner_conflict)
                                                                  AS owner_conflict_events,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped WHERE ambiguous_org)
                                                                  AS ambiguous_org_events,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped
      WHERE org_id IS NULL AND NOT ambiguous_org)                 AS missing_org_events,
    (SELECT COUNT(*)::bigint FROM parked_upsert)                  AS parked_written,
    (SELECT COUNT(*)::bigint FROM parked_resolved)                AS parked_resolved,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted
      WHERE base_model = '')                                      AS empty_base_model_events,
    (SELECT COUNT(*)::bigint FROM tombstoned)                     AS tombstones_written`

// aggregateVideoWindow runs aggregateVideoWindowSQL inside tx.
func aggregateVideoWindow(ctx context.Context, tx *sql.Tx, start, end time.Time) (VideoAggregateResult, error) {
	var r VideoAggregateResult
	err := tx.QueryRowContext(ctx, aggregateVideoWindowSQL, start.UTC(), end.UTC()).
		Scan(&r.RollupsWritten, &r.EventsAggregated, &r.ReconciledDeletions, &r.ParkedDeletions,
			&r.InvalidUsageEvents, &r.UnattributableEvents, &r.OwnerConflictEvents,
			&r.AmbiguousOrgEvents, &r.MissingOrgEvents,
			&r.ParkedWritten, &r.ParkedResolved, &r.EmptyBaseModelEvents, &r.TombstonesWritten)
	if err != nil {
		return VideoAggregateResult{}, fmt.Errorf("rating: aggregate video window [%s,%s): %w",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}
	regrained, err := stampVideoUsageIDs(ctx, tx, start, end)
	if err != nil {
		return VideoAggregateResult{}, err
	}
	r.TombstonesWritten += regrained
	return r, nil
}

// stampVideoUsageIDs stores each rollup's wire usage_id in [start, end), in
// the aggregation's transaction. INVARIANT at commit: every rollup of the
// window carries the id of its own fields, and every id a deleted or
// regrained rollup carried
// before has a tombstone: the statement before this tombstones deleted rows,
// this pass tombstones changed ids. Both run in one transaction, so no reader
// sees one half. The id is computed by the same function
// token-push sends with (meteredusage.VideoUsageID), here, once, so the
// tombstones and 0017 down's guard can match records by id. A rollup whose id
// changes (the grain kept its row but got a new org_id) leaves a 'regrained'
// tombstone for its old id. An id that is live again loses its tombstone, so
// a tombstone never releases a record the rater still produces. It returns the
// number of tombstones written.
func stampVideoUsageIDs(ctx context.Context, tx *sql.Tx, start, end time.Time) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, window_start, org_id, owner_type, owner_id, resource_id,
		auth_id, model_id, serving_mode, base_model, task, resolution, region, COALESCE(usage_id, '')
		FROM rated_video_usage WHERE window_start >= $1 AND window_start < $2`, start.UTC(), end.UTC())
	if err != nil {
		return 0, fmt.Errorf("rating: read video rollups to stamp: %w", err)
	}
	type stamp struct {
		id, old, want                                     string
		ws                                                time.Time
		auth, ownerType, ownerID, resourceID, model, mode string
	}
	var all []stamp
	for rows.Next() {
		var (
			s   stamp
			org string
			a   meteredusage.VideoAttributes
		)
		if err := rows.Scan(&s.id, &s.ws, &org, &s.ownerType, &s.ownerID, &s.resourceID,
			&a.AuthID, &a.ModelID, &a.ServingMode, &a.BaseModel, &a.Task, &a.Resolution, &a.Region, &s.old); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("rating: scan video rollup to stamp: %w", err)
		}
		s.auth, s.model, s.mode = a.AuthID, a.ModelID, a.ServingMode
		s.want = meteredusage.VideoUsageID(meteredusage.FormatWindowStart(s.ws), org, s.ownerType, s.ownerID, s.resourceID, a)
		all = append(all, s)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rating: read video rollups to stamp: %w", err)
	}
	_ = rows.Close()

	// Ids are unique per grain: valid_usage refuses the '|' that could make
	// two grains share one. Stamping clears every changing row first anyway,
	// so the unique index never depends on the order of the updates.
	var todo []stamp
	var live []string
	for _, s := range all {
		live = append(live, s.want)
		if s.old != s.want {
			todo = append(todo, s)
		}
	}
	for _, s := range todo {
		if _, err := tx.ExecContext(ctx, `UPDATE rated_video_usage SET usage_id = NULL WHERE id = $1`, s.id); err != nil {
			return 0, fmt.Errorf("rating: clear video usage_id: %w", err)
		}
	}
	var written int64
	for _, s := range todo {
		if s.old != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO video_reconcile_deletion (window_start, usage_id, auth_id,
				owner_type, owner_id, resource_id, model_id, serving_mode, reason)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'regrained')
				ON CONFLICT ON CONSTRAINT video_reconcile_deletion_pk DO UPDATE SET reason = 'regrained', deleted_at = now()`,
				s.ws, s.old, s.auth, s.ownerType, s.ownerID, s.resourceID, s.model, s.mode); err != nil {
				return 0, fmt.Errorf("rating: tombstone regrained video usage_id: %w", err)
			}
			written++
		}
		if _, err := tx.ExecContext(ctx, `UPDATE rated_video_usage SET usage_id = $2 WHERE id = $1`, s.id, s.want); err != nil {
			return 0, fmt.Errorf("rating: stamp video usage_id: %w", err)
		}
	}
	if len(live) > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM video_reconcile_deletion
			WHERE window_start >= $1 AND window_start < $2 AND usage_id = ANY($3)`, start.UTC(), end.UTC(), live); err != nil {
			return 0, fmt.Errorf("rating: clear tombstones of live video usage_ids: %w", err)
		}
	}
	return written, nil
}

// HasAnomaly reports video evidence that cannot become any record and is not
// parked, including completed jobs the rater never rated (UnratedInReachHours,
// UnratedOutOfReachHours). cmd/rater exits 2 on it.
func (r VideoAggregateResult) HasAnomaly() bool {
	return r.UnattributableEvents > 0 || r.InvalidUsageEvents > 0 || r.UnratedOutOfReachHours > 0 ||
		r.UnratedInReachHours > 0
}
