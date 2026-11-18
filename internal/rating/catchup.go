package rating

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CatchUpHoursPerRun bounds how many never-rated hours one rater run catches
// up. Each hour is one aggregation statement over one hour of billing_event,
// so a run of 168 does about seven times the work of the routine 24-hour
// window (seconds to minutes on a healthy database), far inside the chart's
// 3600 s activeDeadlineSeconds. Progress is recorded after every hour, so a
// killed run loses nothing.
const CatchUpHoursPerRun = 24 * 7

// CatchUpState is the one-time catch-up's progress (table rater_state).
type CatchUpState struct {
	// Cursor is the last hour the catch-up rated; zero if it has not started.
	Cursor time.Time
	// Done is true once the catch-up found nothing left to rate.
	Done bool
}

// CatchUpResult summarises one run's catch-up.
type CatchUpResult struct {
	// AlreadyDone: the catch-up had completed on an earlier run; nothing ran.
	AlreadyDone bool
	// Completed: this run found nothing left to rate and set the marker.
	Completed bool
	// Hours lists the hours this run rated, oldest first.
	Hours []time.Time
	// AggregateResult sums the aggregation results of those hours.
	AggregateResult
}

// CatchUp rates, once per install, the billing_event hours before `before`
// (the routine window's start) that were never rated: hours with raw events
// but no rated_usage and no parked_usage row. They exist on an install that
// ran phoebe with the rater suspended: the drainer kept writing billing_event,
// and the trailing window never reached back to those hours.
//
// Oldest first, at most limit hours per run, continuing after the recorded
// cursor on later runs. When a run finds no more than limit hours left it
// rates them and marks the catch-up done (rater_state.catchup_completed_at);
// from then on the rater rates only its trailing window.
//
// SAFE TO RE-RUN. An hour that already has rated_usage or parked_usage rows is
// never touched, so the catch-up can never overwrite a rollup an earlier run
// wrote (with a partial sum, if the pruner has since removed some of that
// hour's events) or re-open a parked hour. Rating an hour that had no rows is
// the same statement the routine window runs: its reconcile has nothing to
// delete, and re-rating produces identical rows (the upsert replaces each
// bucket and the row id is a hash of the grain). token-push sends the newly
// rated hours as never-accepted hours (its missed-hour and back-fill lists).
func (a *Aggregator) CatchUp(ctx context.Context, before time.Time, limit int) (CatchUpResult, error) {
	var res CatchUpResult
	if limit < 1 {
		return res, fmt.Errorf("rating: catch-up limit %d must be >= 1", limit)
	}
	before = before.UTC()
	st, err := a.store.CatchUpState(ctx)
	if err != nil {
		return res, err
	}
	if st.Done {
		res.AlreadyDone = true
		return res, nil
	}
	notBefore := time.Unix(0, 0).UTC()
	if !st.Cursor.IsZero() {
		notBefore = st.Cursor.UTC().Add(time.Hour)
	}
	// One more than the limit, to know whether anything is left after this run.
	hours, err := a.store.NeverRatedHours(ctx, notBefore, before, limit+1)
	if err != nil {
		return res, err
	}
	more := len(hours) > limit
	if more {
		hours = hours[:limit]
	}
	for _, h := range hours {
		ar, err := a.aggregateAndLog(ctx, h, h.Add(time.Hour), false)
		if err != nil {
			return res, err
		}
		if err := a.store.RecordCatchUpProgress(ctx, h); err != nil {
			return res, err
		}
		res.Hours = append(res.Hours, h)
		res.AggregateResult = addResults(res.AggregateResult, ar)
	}
	if !more {
		if err := a.store.CompleteCatchUp(ctx); err != nil {
			return res, err
		}
		res.Completed = true
	}
	switch {
	case len(hours) == 0:
		a.log.Info.Printf("rating: catch-up complete: no billing_event hour before %s was left unrated; from now on only the trailing window is rated", before.Format(time.RFC3339))
	case res.Completed:
		a.log.Info.Printf("rating: catch-up rated %d never-rated hour(s), %s to %s, and is complete; token-push sends them as never-accepted hours", len(hours), hours[0].Format(time.RFC3339), hours[len(hours)-1].Format(time.RFC3339))
	default:
		a.log.Info.Printf("rating: catch-up rated %d never-rated hour(s), %s to %s (at most %d per run); more remain, and the next run continues after %s", len(hours), hours[0].Format(time.RFC3339), hours[len(hours)-1].Format(time.RFC3339), limit, hours[len(hours)-1].Format(time.RFC3339))
	}
	return res, nil
}

// addResults sums two aggregation results field by field.
func addResults(a, b AggregateResult) AggregateResult {
	a.RollupsWritten += b.RollupsWritten
	a.EventsAggregated += b.EventsAggregated
	a.ReconciledDeletions += b.ReconciledDeletions
	a.ParkedDeletions += b.ParkedDeletions
	a.MissingUsageEvents += b.MissingUsageEvents
	a.ExpectedMissingUsageEvents += b.ExpectedMissingUsageEvents
	a.UnexplainedMissingUsageEvents += b.UnexplainedMissingUsageEvents
	a.InvalidUsageEvents += b.InvalidUsageEvents
	a.UnattributableEvents += b.UnattributableEvents
	a.InvalidServingModeEvents += b.InvalidServingModeEvents
	a.OwnerConflictEvents += b.OwnerConflictEvents
	a.AmbiguousBaseEvents += b.AmbiguousBaseEvents
	a.AmbiguousOrgEvents += b.AmbiguousOrgEvents
	a.MissingOrgEvents += b.MissingOrgEvents
	a.ParkedWritten += b.ParkedWritten
	a.ParkedResolved += b.ParkedResolved
	a.EmptyBaseModelEvents += b.EmptyBaseModelEvents
	a.UnknownRevisionRollups += b.UnknownRevisionRollups
	return a
}

// CatchUpState implements Store. No row means the catch-up has not started.
func (s *PostgresStore) CatchUpState(ctx context.Context) (CatchUpState, error) {
	var cursor, completed sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT catchup_cursor, catchup_completed_at FROM rater_state`).Scan(&cursor, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return CatchUpState{}, nil
	}
	if err != nil {
		return CatchUpState{}, fmt.Errorf("rating: read rater_state: %w", err)
	}
	st := CatchUpState{Done: completed.Valid}
	if cursor.Valid {
		st.Cursor = cursor.Time.UTC()
	}
	return st, nil
}

// neverRatedHoursSQL walks the hours that have billing_event rows in
// [$1, $2) with one index probe per hour (billing_event_rating_instant_ix, on
// the rating instant), instead of grouping the whole table, then keeps the
// hours with no rated_usage and no parked_usage row. Hour buckets match
// aggregateWindowSQL's.
const neverRatedHoursSQL = `
WITH RECURSIVE h(hour) AS (
    SELECT date_trunc('hour', min(COALESCE(event_ts, created_at)) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
      FROM billing_event
     WHERE COALESCE(event_ts, created_at) >= $1::timestamptz
       AND COALESCE(event_ts, created_at) <  $2::timestamptz
  UNION ALL
    SELECT (SELECT date_trunc('hour', min(COALESCE(b.event_ts, b.created_at)) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              FROM billing_event b
             WHERE COALESCE(b.event_ts, b.created_at) >= h.hour + interval '1 hour'
               AND COALESCE(b.event_ts, b.created_at) <  $2::timestamptz)
      FROM h
     WHERE h.hour IS NOT NULL
)
SELECT h.hour FROM h
 WHERE h.hour IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM rated_usage r WHERE r.window_start = h.hour)
   AND NOT EXISTS (SELECT 1 FROM parked_usage p WHERE p.window_start = h.hour)
 ORDER BY h.hour
 LIMIT $3`

// NeverRatedHours implements Store.
func (s *PostgresStore) NeverRatedHours(ctx context.Context, notBefore, before time.Time, limit int) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, neverRatedHoursSQL, notBefore.UTC(), before.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("rating: list never-rated hours: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []time.Time
	for rows.Next() {
		var h time.Time
		if err := rows.Scan(&h); err != nil {
			return nil, fmt.Errorf("rating: scan never-rated hour: %w", err)
		}
		out = append(out, h.UTC())
	}
	return out, rows.Err()
}

// RecordCatchUpProgress implements Store.
func (s *PostgresStore) RecordCatchUpProgress(ctx context.Context, hour time.Time) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO rater_state (catchup_cursor) VALUES ($1)
		ON CONFLICT (singleton) DO UPDATE SET catchup_cursor = EXCLUDED.catchup_cursor`, hour.UTC()); err != nil {
		return fmt.Errorf("rating: record catch-up progress: %w", err)
	}
	return nil
}

// CompleteCatchUp implements Store.
func (s *PostgresStore) CompleteCatchUp(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO rater_state (catchup_completed_at) VALUES (now())
		ON CONFLICT (singleton) DO UPDATE SET catchup_completed_at = now()`); err != nil {
		return fmt.Errorf("rating: mark catch-up complete: %w", err)
	}
	return nil
}
