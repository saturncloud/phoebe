package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
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

// videoRollupRow is one rated_video_usage row as token-push reads it. Every
// grain column is NOT NULL (” for an unknown base model or region), and
// OutputSeconds and GPUSeconds are the exact NUMERIC sums as Postgres prints
// them.
type videoRollupRow struct {
	ID string
	identity
	OrgID, BaseModel, Task, Resolution, Region string
	OutputSeconds, GPUSeconds                  string
	OutputFrames, Requests                     int64
}

// refusedHour is a parked hour: refused for its content, with the hash of
// the payload that was refused.
type refusedHour struct {
	Hour          time.Time
	PayloadSHA256 string
}

// pushedRow is a usage_id the manager holds for an hour, with its kind and
// identity. Kind "" is read as "token", the pushed_usage column default.
type pushedRow struct {
	UsageID string
	Kind    string
	identity
	// held: an id the request lists in `held`, not a record it sends. Its
	// pushed_usage row keeps the pushed_at of the push that sent it, so
	// pushed_at says when the manager last received that record (0017
	// down relies on it).
	held bool
}

// kind returns the row's kind, "token" when unset.
func (p pushedRow) kind() string {
	if p.Kind == "" {
		return meteredusage.KindToken
	}
	return p.Kind
}

// pushStore is token-push's data seam. pgStore is the production one; the unit
// tests use an in-memory fake.
type pushStore interface {
	// UsagePresent is the liveness guard's input: whether rated_usage and
	// parked_usage each hold any row.
	UsagePresent(ctx context.Context) (rated, parked bool, err error)
	// EverPushedToken reports whether the manager was ever sent a token record
	// (pushed_usage has a token row). It tells an install that only ever sent
	// video usage apart from one whose rated_usage was wiped.
	EverPushedToken(ctx context.Context) (bool, error)
	// VideoUsageState reports whether any video usage is recorded
	// (rated_video_usage has a row, or parked_video_usage has an UNRESOLVED
	// row: an install whose only video rollup the rater just parked), and
	// whether the manager was ever sent a video record (pushed_usage has a
	// video row). token-push lists kind "video" only when video usage is
	// recorded. A resolved parked row does not count: parked rows are never
	// pruned, so an old one would hide a wiped rated_video_usage forever and
	// every re-sent hour would delete the manager's video records by absence.
	// rated is whether rated_video_usage alone has a row.
	VideoUsageState(ctx context.Context) (has, rated, everPushed bool, err error)
	Rollups(ctx context.Context, hour time.Time) ([]rollupRow, error)
	// VideoRollups reads every rated_video_usage row of one hour.
	VideoRollups(ctx context.Context, hour time.Time) ([]videoRollupRow, error)
	// ParkedIdentities lists the identities of the hour whose usage is parked
	// and unresolved, for reasons that can hide an already-sent record
	// (ambiguous_org, ambiguous_base, missing_org). Owner-conflict events never
	// formed a record, so there is nothing of theirs to hold.
	ParkedIdentities(ctx context.Context, hour time.Time) ([]identity, error)
	// ParkedVideoIdentities is ParkedIdentities for parked_video_usage
	// (ambiguous_org and missing_org; never owner_conflict).
	ParkedVideoIdentities(ctx context.Context, hour time.Time) ([]identity, error)
	// Pushed lists the usage_ids the manager holds for the hour after
	// token-push's last request for it.
	Pushed(ctx context.Context, hour time.Time) ([]pushedRow, error)
	// ResolvedHours lists hours before `before` with a parked row (token or
	// video) resolved after the hour's last accepted push: they must be re-sent
	// however old.
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
	// `before` that have rated_usage or rated_video_usage, come after the automatic re-send floor
	// (token_push_state.auto_floor: the first hour accepted by a scheduled run),
	// were never accepted (no pushed_hour row) and are not parked (no
	// refused_hour row). No floor yet: none.
	UnsentHours(ctx context.Context, before time.Time, limit int) ([]time.Time, error)
	// BackfillHours lists, oldest first and at most limit, the hours BEFORE the
	// automatic re-send floor that have rated_usage or rated_video_usage, were
	// never accepted and are
	// not parked: usage from before the install's first connection, sent by
	// scheduled runs once the floor is set. rated_usage is never pruned, so this
	// reaches the install's first hour of usage. No floor yet: none.
	BackfillHours(ctx context.Context, limit int) ([]time.Time, error)
	// PreConnectionHours counts the hours BackfillHours would list with no
	// limit, and returns the oldest. No floor yet: zero.
	PreConnectionHours(ctx context.Context) (count int, oldest time.Time, err error)
	// RefusedHours lists the parked hours (refused_hour), least recently
	// checked first, with the SHA-256 of the payload that was refused.
	RefusedHours(ctx context.Context) ([]refusedHour, error)
	// RecordRefused parks the hour, or updates its row: status (0 = refused
	// locally), message, payload hash, one more refusal, last_refused_at and
	// checked_at now. first_refused_at is kept.
	RecordRefused(ctx context.Context, hour time.Time, status int, message, payloadSHA256 string) error
	// MarkRefusedChecked sets checked_at to now for parked hours a run rebuilt
	// and found unchanged, so the next run re-checks other parked hours first.
	MarkRefusedChecked(ctx context.Context, hours []time.Time) error
	// RecordIntent adds rows to pushed_usage BEFORE the request is sent. If the
	// manager commits and token-push dies before RecordAccepted, the ids are
	// still known as sent; listing an id the manager does not have in `held` is
	// harmless, forgetting one it does have is not.
	RecordIntent(ctx context.Context, hour time.Time, rows []pushedRow) error
	// RecordAccepted makes pushed_usage for the hour and the request's kinds
	// exactly rows (what the manager now holds) and stamps pushed_hour. Rows of
	// a kind the request did not list are kept: the manager kept those
	// records too. With setAutoFloor (a scheduled run), it also sets
	// token_push_state.auto_floor to the hour if no floor is set yet; an
	// explicit -since/-until run passes false and never moves it. An accepted
	// hour is no longer parked: its refused_hour row is deleted.
	RecordAccepted(ctx context.Context, hour time.Time, kinds []string, rows []pushedRow, setAutoFloor bool) error
}

type pgStore struct{ db *sql.DB }

func (s pgStore) EverPushedToken(ctx context.Context) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pushed_usage WHERE kind = 'token')`).Scan(&found)
	return found, err
}

func (s pgStore) UsagePresent(ctx context.Context) (rated, parked bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM rated_usage), EXISTS (SELECT 1 FROM parked_usage)`).Scan(&rated, &parked)
	return rated, parked, err
}

func (s pgStore) VideoUsageState(ctx context.Context) (has, rated, everPushed bool, err error) {
	var parked bool
	err = s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM rated_video_usage),
		        EXISTS (SELECT 1 FROM parked_video_usage WHERE resolved_at IS NULL),
		        EXISTS (SELECT 1 FROM pushed_usage WHERE kind = 'video')`).Scan(&rated, &parked, &everPushed)
	return rated || parked, rated, everPushed, err
}

// videoHourQuery reads every video rollup of one hour. NUMERIC as text, so the
// sums reach the wire exactly.
const videoHourQuery = `
SELECT v.id, v.auth_id, v.owner_type, v.owner_id, v.resource_id, v.model_id, v.serving_mode,
       v.org_id, v.base_model, v.task, v.resolution, v.region,
       v.output_seconds::text, v.output_frames, v.request_count, v.gpu_seconds::text
  FROM rated_video_usage v
 WHERE v.window_start = $1
 ORDER BY v.id`

func (s pgStore) VideoRollups(ctx context.Context, hour time.Time) ([]videoRollupRow, error) {
	rows, err := s.db.QueryContext(ctx, videoHourQuery, hour)
	if err != nil {
		return nil, fmt.Errorf("query rated_video_usage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []videoRollupRow
	for rows.Next() {
		var r videoRollupRow
		if err := rows.Scan(&r.ID, &r.AuthID, &r.OwnerType, &r.OwnerID, &r.ResourceID, &r.ModelID, &r.ServingMode,
			&r.OrgID, &r.BaseModel, &r.Task, &r.Resolution, &r.Region,
			&r.OutputSeconds, &r.OutputFrames, &r.Requests, &r.GPUSeconds); err != nil {
			return nil, fmt.Errorf("scan rated_video_usage: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
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
			dest = append([]any{&p.UsageID, &p.Kind}, dest...)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s pgStore) ParkedIdentities(ctx context.Context, hour time.Time) ([]identity, error) {
	return s.parkedIdentities(ctx, "parked_usage", hour)
}

func (s pgStore) ParkedVideoIdentities(ctx context.Context, hour time.Time) ([]identity, error) {
	return s.parkedIdentities(ctx, "parked_video_usage", hour)
}

// parkedIdentities reads one parked table; table is one of two constants.
func (s pgStore) parkedIdentities(ctx context.Context, table string, hour time.Time) ([]identity, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT auth_id, owner_type, owner_id, resource_id, model_id, serving_mode
		FROM `+table+` WHERE window_start = $1 AND resolved_at IS NULL AND reason <> 'owner_conflict'`, hour)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", table, err)
	}
	ps, err := scanIdentities(rows, false)
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", table, err)
	}
	out := make([]identity, len(ps))
	for i, p := range ps {
		out[i] = p.identity
	}
	return out, nil
}

func (s pgStore) Pushed(ctx context.Context, hour time.Time) ([]pushedRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT usage_id, kind, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode
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
	rows, err := s.db.QueryContext(ctx, `SELECT p.window_start
		FROM (SELECT window_start, resolved_at FROM parked_usage
		      UNION ALL
		      SELECT window_start, resolved_at FROM parked_video_usage) p
		LEFT JOIN pushed_hour h ON h.window_start = p.window_start
		WHERE p.resolved_at IS NOT NULL AND p.window_start < $1
		  AND (h.pushed_at IS NULL OR p.resolved_at > h.pushed_at)
		GROUP BY p.window_start
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
// one and keeps those with usage, no pushed_hour row and no refused_hour row
// (a parked hour is re-checked separately, so it never uses up the cap). With
// no token_push_state row the cross join is empty, so nothing qualifies until
// a scheduled run has an hour accepted. Walking the series costs index probes
// per hour since the floor, not a scan of all of rated_usage.
const unsentHoursQuery = `
SELECT g.h
  FROM token_push_state s,
       generate_series(s.auto_floor + interval '1 hour', $1::timestamptz - interval '1 hour', interval '1 hour') AS g(h)
 WHERE NOT EXISTS (SELECT 1 FROM pushed_hour p WHERE p.window_start = g.h)
   AND NOT EXISTS (SELECT 1 FROM refused_hour r WHERE r.window_start = g.h)
   AND (EXISTS (SELECT 1 FROM rated_usage ru WHERE ru.window_start = g.h)
        OR EXISTS (SELECT 1 FROM rated_video_usage v WHERE v.window_start = g.h))
 ORDER BY g.h
 LIMIT $2`

// backfillHoursCTE walks the hours from the oldest rated_usage or
// rated_video_usage hour (LEAST ignores a NULL) up to the
// floor (exclusive) and keeps those with usage, never accepted and not
// parked: the usage from before the install's first connection. Accepted
// hours (by an earlier back-fill run or an operator's -since/-until run) have
// a pushed_hour row and are not re-sent. No token_push_state row, or both
// rollup tables empty (min is NULL): no rows.
const backfillHoursCTE = `
WITH backfill AS (
SELECT g.h
  FROM token_push_state s,
       generate_series(LEAST((SELECT min(window_start) FROM rated_usage), (SELECT min(window_start) FROM rated_video_usage)),
                       s.auto_floor - interval '1 hour', interval '1 hour') AS g(h)
 WHERE NOT EXISTS (SELECT 1 FROM pushed_hour p WHERE p.window_start = g.h)
   AND NOT EXISTS (SELECT 1 FROM refused_hour r WHERE r.window_start = g.h)
   AND (EXISTS (SELECT 1 FROM rated_usage ru WHERE ru.window_start = g.h)
        OR EXISTS (SELECT 1 FROM rated_video_usage v WHERE v.window_start = g.h))
)`

const backfillHoursQuery = backfillHoursCTE + `
SELECT h FROM backfill ORDER BY h LIMIT $1`

// preConnectionQuery counts what backfillHoursQuery would list with no limit.
const preConnectionQuery = backfillHoursCTE + `
SELECT count(*), min(h) FROM backfill`

func (s pgStore) PreConnectionHours(ctx context.Context) (int, time.Time, error) {
	var n int
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, preConnectionQuery).Scan(&n, &oldest); err != nil {
		return 0, time.Time{}, fmt.Errorf("count pre-connection hours: %w", err)
	}
	return n, oldest.Time.UTC(), nil
}

func scanHours(rows *sql.Rows) ([]time.Time, error) {
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

func (s pgStore) UnsentHours(ctx context.Context, before time.Time, limit int) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, unsentHoursQuery, before, limit)
	if err != nil {
		return nil, fmt.Errorf("query unsent hours: %w", err)
	}
	return scanHours(rows)
}

func (s pgStore) BackfillHours(ctx context.Context, limit int) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, backfillHoursQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("query back-fill hours: %w", err)
	}
	return scanHours(rows)
}

func (s pgStore) RefusedHours(ctx context.Context) ([]refusedHour, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT window_start, payload_sha256 FROM refused_hour ORDER BY checked_at, window_start`)
	if err != nil {
		return nil, fmt.Errorf("query refused hours: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []refusedHour
	for rows.Next() {
		var r refusedHour
		if err := rows.Scan(&r.Hour, &r.PayloadSHA256); err != nil {
			return nil, err
		}
		r.Hour = r.Hour.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// maxRefusedMessage bounds the stored response body; the start of it names
// the field the manager refused.
const maxRefusedMessage = 4096

func (s pgStore) RecordRefused(ctx context.Context, hour time.Time, status int, message, payloadSHA256 string) error {
	if len(message) > maxRefusedMessage {
		message = message[:maxRefusedMessage]
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO refused_hour (window_start, status, message, payload_sha256)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT ON CONSTRAINT refused_hour_pk DO UPDATE SET
			status = EXCLUDED.status, message = EXCLUDED.message, payload_sha256 = EXCLUDED.payload_sha256,
			refusals = refused_hour.refusals + 1, last_refused_at = now(), checked_at = now()`,
		hour, status, message, payloadSHA256); err != nil {
		return fmt.Errorf("record refused hour: %w", err)
	}
	return nil
}

func (s pgStore) MarkRefusedChecked(ctx context.Context, hours []time.Time) error {
	if len(hours) == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE refused_hour SET checked_at = now() WHERE window_start = ANY($1)`, hours); err != nil {
		return fmt.Errorf("mark refused hours checked: %w", err)
	}
	return nil
}

func insertPushed(ctx context.Context, tx *sql.Tx, hour time.Time, rows []pushedRow) error {
	for _, r := range rows {
		onConflict := `DO UPDATE SET pushed_at = now(), kind = EXCLUDED.kind`
		if r.held {
			onConflict = `DO NOTHING`
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pushed_usage (window_start, usage_id, kind, auth_id, owner_type, owner_id,
			resource_id, model_id, serving_mode) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT ON CONSTRAINT pushed_usage_pk `+onConflict,
			hour, r.UsageID, r.kind(), r.AuthID, r.OwnerType, r.OwnerID, r.ResourceID, r.ModelID, r.ServingMode); err != nil {
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

func (s pgStore) RecordAccepted(ctx context.Context, hour time.Time, kinds []string, rows []pushedRow, setAutoFloor bool) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		keep := make([]string, len(rows))
		for i, r := range rows {
			keep[i] = r.UsageID
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM pushed_usage WHERE window_start = $1 AND kind = ANY($3)
			AND NOT (usage_id = ANY($2))`, hour, keep, kinds); err != nil {
			return fmt.Errorf("trim pushed usage: %w", err)
		}
		if err := insertPushed(ctx, tx, hour, rows); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pushed_hour (window_start, pushed_at) VALUES ($1, now())
			ON CONFLICT ON CONSTRAINT pushed_hour_pk DO UPDATE SET pushed_at = now()`, hour); err != nil {
			return fmt.Errorf("record pushed hour: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM refused_hour WHERE window_start = $1`, hour); err != nil {
			return fmt.Errorf("unpark accepted hour: %w", err)
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
