package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// identity is the rater's gate identity: who used what, at which serving mode,
// without the graph and revision. Parking and holding work at this grain.
type identity struct {
	AuthID, OwnerType, OwnerID, ResourceID, ModelID, ServingMode string
}

// rollupRow is one rated_usage row as token-push reads it: the record's grain,
// its payer and pricing facts, and its quantities. No money.
type rollupRow struct {
	ID string
	identity
	OrgID, BaseModel, Adapter, Region    sql.NullString
	GraphK8sName                         string
	TopologyRevision                     int64
	Prompt, Cached, Completion, Requests int64
}

// pushedRow is a usage_id the manager holds for an hour, with its identity.
type pushedRow struct {
	UsageID string
	identity
}

// pushStore is token-push's data seam. pgStore is the production one; the unit
// tests use an in-memory fake.
type pushStore interface {
	// UsagePresent is the liveness guard's input: whether rated_usage and
	// parked_usage each hold any row.
	UsagePresent(ctx context.Context) (rated, parked bool, err error)
	Rollups(ctx context.Context, hour time.Time) ([]rollupRow, error)
	// ParkedIdentities lists the identities of the hour whose usage is parked
	// and unresolved, for reasons that can hide an already-sent record
	// (ambiguous_org, ambiguous_base, missing_org). Owner-conflict events never
	// formed a record, so there is nothing of theirs to hold.
	ParkedIdentities(ctx context.Context, hour time.Time) ([]identity, error)
	// Pushed lists the usage_ids the manager holds for the hour after
	// token-push's last request for it.
	Pushed(ctx context.Context, hour time.Time) ([]pushedRow, error)
	// ResolvedHours lists hours before `before` with a parked row resolved after
	// the hour's last accepted push: they must be re-sent however old.
	ResolvedHours(ctx context.Context, before time.Time) ([]time.Time, error)
	// EverPushed reports whether the manager has ever accepted an hour from
	// this install (pushed_hour has a row). With rated_usage empty, it tells a
	// fresh install (false) from a wiped one (true).
	EverPushed(ctx context.Context) (bool, error)
	// HourEverSent reports whether this install ever sent the hour: a
	// pushed_hour row (accepted) or a pushed_usage row (sent, maybe accepted).
	// Only such an hour may be sent as an empty snapshot.
	HourEverSent(ctx context.Context, hour time.Time) (bool, error)
	// UnsentHours lists, oldest first and at most limit, the hours before
	// `before` that have rated_usage, come after the automatic re-send floor
	// (token_push_state.auto_floor: the first hour accepted by a scheduled run),
	// and were never accepted (no pushed_hour row). No floor yet: none.
	UnsentHours(ctx context.Context, before time.Time, limit int) ([]time.Time, error)
	// PreConnectionHours counts the hours with rated_usage before the automatic
	// re-send floor that were never accepted, and returns the oldest. Scheduled
	// runs never send them. No floor yet: zero.
	PreConnectionHours(ctx context.Context) (count int, oldest time.Time, err error)
	// RecordIntent adds rows to pushed_usage BEFORE the request is sent. If the
	// manager commits and token-push dies before RecordAccepted, the ids are
	// still known as sent; listing an id the manager does not have in `held` is
	// harmless, forgetting one it does have is not.
	RecordIntent(ctx context.Context, hour time.Time, rows []pushedRow) error
	// RecordAccepted makes pushed_usage for the hour exactly rows (what the
	// manager now holds) and stamps pushed_hour. With setAutoFloor (a scheduled
	// run), it also sets token_push_state.auto_floor to the hour if no floor is
	// set yet; an explicit -since/-until run passes false and never moves it.
	RecordAccepted(ctx context.Context, hour time.Time, rows []pushedRow, setAutoFloor bool) error
}

type pgStore struct{ db *sql.DB }

func (s pgStore) UsagePresent(ctx context.Context) (rated, parked bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM rated_usage), EXISTS (SELECT 1 FROM parked_usage)`).Scan(&rated, &parked)
	return rated, parked, err
}

// hourQuery reads every rollup of one hour. No money: the manager prices.
const hourQuery = `
SELECT ru.id, ru.auth_id, ru.owner_type, ru.owner_id, ru.resource_id, ru.model_id, ru.serving_mode,
       ru.org_id, ru.base_model, ru.adapter, ru.region, ru.graph_k8s_name, ru.topology_revision,
       ru.prompt_tokens, ru.cached_tokens, ru.completion_tokens, ru.event_count
  FROM rated_usage ru
 WHERE ru.window_start = $1
 ORDER BY ru.id`

func (s pgStore) Rollups(ctx context.Context, hour time.Time) ([]rollupRow, error) {
	rows, err := s.db.QueryContext(ctx, hourQuery, hour)
	if err != nil {
		return nil, fmt.Errorf("query rated_usage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []rollupRow
	for rows.Next() {
		var r rollupRow
		if err := rows.Scan(&r.ID, &r.AuthID, &r.OwnerType, &r.OwnerID, &r.ResourceID, &r.ModelID, &r.ServingMode,
			&r.OrgID, &r.BaseModel, &r.Adapter, &r.Region, &r.GraphK8sName, &r.TopologyRevision,
			&r.Prompt, &r.Cached, &r.Completion, &r.Requests); err != nil {
			return nil, fmt.Errorf("scan rated_usage: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanIdentities(rows *sql.Rows, withUsageID bool) ([]pushedRow, error) {
	defer func() { _ = rows.Close() }()
	var out []pushedRow
	for rows.Next() {
		var p pushedRow
		dest := []any{&p.AuthID, &p.OwnerType, &p.OwnerID, &p.ResourceID, &p.ModelID, &p.ServingMode}
		if withUsageID {
			dest = append([]any{&p.UsageID}, dest...)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s pgStore) ParkedIdentities(ctx context.Context, hour time.Time) ([]identity, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT auth_id, owner_type, owner_id, resource_id, model_id, serving_mode
		FROM parked_usage WHERE window_start = $1 AND resolved_at IS NULL AND reason <> 'owner_conflict'`, hour)
	if err != nil {
		return nil, fmt.Errorf("query parked_usage: %w", err)
	}
	ps, err := scanIdentities(rows, false)
	if err != nil {
		return nil, fmt.Errorf("scan parked_usage: %w", err)
	}
	out := make([]identity, len(ps))
	for i, p := range ps {
		out[i] = p.identity
	}
	return out, nil
}

func (s pgStore) Pushed(ctx context.Context, hour time.Time) ([]pushedRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT usage_id, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode
		FROM pushed_usage WHERE window_start = $1 ORDER BY usage_id`, hour)
	if err != nil {
		return nil, fmt.Errorf("query pushed_usage: %w", err)
	}
	out, err := scanIdentities(rows, true)
	if err != nil {
		return nil, fmt.Errorf("scan pushed_usage: %w", err)
	}
	return out, nil
}

func (s pgStore) ResolvedHours(ctx context.Context, before time.Time) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT p.window_start
		FROM parked_usage p LEFT JOIN pushed_hour h ON h.window_start = p.window_start
		WHERE p.resolved_at IS NOT NULL AND p.window_start < $1
		  AND (h.pushed_at IS NULL OR p.resolved_at > h.pushed_at)
		ORDER BY p.window_start`, before)
	if err != nil {
		return nil, fmt.Errorf("query resolved parked hours: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []time.Time
	for rows.Next() {
		var h time.Time
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h.UTC())
	}
	return out, rows.Err()
}

func (s pgStore) EverPushed(ctx context.Context) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pushed_hour)`).Scan(&found)
	return found, err
}

func (s pgStore) HourEverSent(ctx context.Context, hour time.Time) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pushed_hour WHERE window_start = $1)
		OR EXISTS (SELECT 1 FROM pushed_usage WHERE window_start = $1)`, hour).Scan(&found)
	return found, err
}

// unsentHoursQuery walks the hours after the automatic re-send floor one by
// one and keeps those with usage and no pushed_hour row. Hours before the
// first hour accepted by a scheduled run are not re-sent automatically: usage
// from before the install was connected, and hours refused (409) or failed
// before that first acceptance. Send them with -since/-until. With no token_push_state row the cross join is empty, so
// nothing qualifies until a scheduled run has an hour accepted. Walking the
// series costs index probes per hour since the floor, not a scan of all of
// rated_usage.
const unsentHoursQuery = `
SELECT g.h
  FROM token_push_state s,
       generate_series(s.auto_floor + interval '1 hour', $1::timestamptz - interval '1 hour', interval '1 hour') AS g(h)
 WHERE NOT EXISTS (SELECT 1 FROM pushed_hour p WHERE p.window_start = g.h)
   AND EXISTS (SELECT 1 FROM rated_usage ru WHERE ru.window_start = g.h)
 ORDER BY g.h
 LIMIT $2`

// preConnectionQuery counts the never-accepted hours with usage before the
// floor. No token_push_state row: the comparison is NULL and the count 0.
const preConnectionQuery = `
SELECT count(DISTINCT ru.window_start), min(ru.window_start)
  FROM rated_usage ru
 WHERE ru.window_start < (SELECT auto_floor FROM token_push_state)
   AND NOT EXISTS (SELECT 1 FROM pushed_hour p WHERE p.window_start = ru.window_start)`

func (s pgStore) PreConnectionHours(ctx context.Context) (int, time.Time, error) {
	var n int
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, preConnectionQuery).Scan(&n, &oldest); err != nil {
		return 0, time.Time{}, fmt.Errorf("count pre-connection hours: %w", err)
	}
	return n, oldest.Time.UTC(), nil
}

func (s pgStore) UnsentHours(ctx context.Context, before time.Time, limit int) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, unsentHoursQuery, before, limit)
	if err != nil {
		return nil, fmt.Errorf("query unsent hours: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []time.Time
	for rows.Next() {
		var h time.Time
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h.UTC())
	}
	return out, rows.Err()
}

func insertPushed(ctx context.Context, tx *sql.Tx, hour time.Time, rows []pushedRow) error {
	for _, r := range rows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO pushed_usage (window_start, usage_id, auth_id, owner_type, owner_id,
			resource_id, model_id, serving_mode) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT ON CONSTRAINT pushed_usage_pk DO UPDATE SET pushed_at = now()`,
			hour, r.UsageID, r.AuthID, r.OwnerType, r.OwnerID, r.ResourceID, r.ModelID, r.ServingMode); err != nil {
			return fmt.Errorf("record pushed usage: %w", err)
		}
	}
	return nil
}

func (s pgStore) inTx(ctx context.Context, f func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s pgStore) RecordIntent(ctx context.Context, hour time.Time, rows []pushedRow) error {
	return s.inTx(ctx, func(tx *sql.Tx) error { return insertPushed(ctx, tx, hour, rows) })
}

func (s pgStore) RecordAccepted(ctx context.Context, hour time.Time, rows []pushedRow, setAutoFloor bool) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		keep := make([]string, len(rows))
		for i, r := range rows {
			keep[i] = r.UsageID
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM pushed_usage WHERE window_start = $1 AND NOT (usage_id = ANY($2))`,
			hour, keep); err != nil {
			return fmt.Errorf("trim pushed usage: %w", err)
		}
		if err := insertPushed(ctx, tx, hour, rows); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now())
			ON CONFLICT ON CONSTRAINT pushed_hour_pk DO UPDATE SET pushed_at = now()`, hour); err != nil {
			return fmt.Errorf("record pushed hour: %w", err)
		}
		if setAutoFloor {
			if _, err := tx.ExecContext(ctx, `INSERT INTO token_push_state (auto_floor) VALUES ($1)
				ON CONFLICT (singleton) DO NOTHING`, hour); err != nil {
				return fmt.Errorf("record the automatic re-send floor: %w", err)
			}
		}
		return nil
	})
}
