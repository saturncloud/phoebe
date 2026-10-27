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
	// HasAnyUsage is the liveness guard: false only when rated_usage AND
	// parked_usage are both empty (a fresh or wiped database).
	HasAnyUsage(ctx context.Context) (bool, error)
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
	// RecordIntent adds rows to pushed_usage BEFORE the request is sent. If the
	// manager commits and token-push dies before RecordAccepted, the ids are
	// still known as sent; listing an id the manager does not have in `held` is
	// harmless, forgetting one it does have is not.
	RecordIntent(ctx context.Context, hour time.Time, rows []pushedRow) error
	// RecordAccepted makes pushed_usage for the hour exactly rows (what the
	// manager now holds) and stamps pushed_hour.
	RecordAccepted(ctx context.Context, hour time.Time, rows []pushedRow) error
}

type pgStore struct{ db *sql.DB }

func (s pgStore) HasAnyUsage(ctx context.Context) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM rated_usage) OR EXISTS (SELECT 1 FROM parked_usage)`).Scan(&found)
	return found, err
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

func (s pgStore) RecordAccepted(ctx context.Context, hour time.Time, rows []pushedRow) error {
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
		return nil
	})
}
