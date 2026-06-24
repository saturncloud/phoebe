package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/rating"
)

// pusher holds the dependencies for building and POSTing window snapshots.
type pusher struct {
	db         *sql.DB
	log        *logging.Logger
	managerURL string
	token      string
	timeout    time.Duration
}

// rollup is one rated_usage row resolved to its billing org, as it crosses the wire.
// Money fields are exact-decimal STRINGS (C8) — read as ::text from Postgres, emitted
// as JSON strings, never a Go float.
type rollup struct {
	RatedUsageID          string `json:"rated_usage_id"`
	Rev                   int    `json:"rev"`
	OrgID                 string `json:"org_id"`
	ResourceID            string `json:"resource_id"`
	ModelID               string `json:"model_id"`
	Cost                  string `json:"cost"`
	AppliedPromptRate     string `json:"applied_prompt_rate"`
	AppliedCachedRate     string `json:"applied_cached_rate"`
	AppliedCompletionRate string `json:"applied_completion_rate"`
	PromptTokens          int64  `json:"prompt_tokens"`
	CachedTokens          int64  `json:"cached_tokens"`
	CompletionTokens      int64  `json:"completion_tokens"`
	BillablePromptTokens  int64  `json:"billable_prompt_tokens"`
}

// snapshot is the hourly POST body for one window_start: the COMPLETE current set of
// rated_usage rollups for that hour (Decision 3 — snapshot, not increments, so the
// manager can delete-by-absence). rollups MUST be non-nil (an empty window is a
// meaningful "[]" — it tells the manager every prior rollup for this window is gone).
type snapshot struct {
	WindowStart string   `json:"window_start"` // RFC3339, hour-aligned UTC
	Rollups     []rollup `json:"rollups"`
}

// openDB opens the shared Atlas Postgres the same way the rater does (pgx stdlib,
// UTC-pinned DSN, small batch-job pool). token-push reads rated_usage + resource_name
// from this DB; it never writes.
func openDB(ctx context.Context, cfg rating.Config) (*sql.DB, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("token-push: DATABASE_URL is empty (Postgres holds rated_usage and resource_name; the pusher cannot run without it)")
	}
	db, err := sql.Open("pgx", ensureUTCTimeZone(cfg.DatabaseURL))
	if err != nil {
		return nil, fmt.Errorf("token-push: open postgres: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("token-push: postgres ping: %w", err)
	}
	return db, nil
}

// snapshotQuery reads every rated_usage row whose window_start == $1 and LEFT JOINs
// resource_name to resolve the billing org. The LEFT JOIN (not INNER) is deliberate:
// a row whose resource_id has NO resource_name match must still be SEEN here so it can
// be counted and screamed about (C7), not silently filtered out by the join. Money and
// rates are read as ::text so they never become a float in Go (C8).
//
// resource_id → resource_name.resource_id → resource_name.org_id (a direct FK to
// org.id). resource_name is the canonical resource→org map for every resource type, in
// the same shared Atlas Postgres; phoebe does not own it (assumed-to-exist).
const snapshotQuery = `
SELECT
    ru.id,
    ru.resource_id,
    ru.model_id,
    rn.org_id,                       -- NULL when the deployment is absent from resource_name
    ru.cost::text,
    ru.applied_prompt_rate::text,
    ru.applied_cached_rate::text,
    ru.applied_completion_rate::text,
    ru.prompt_tokens,
    ru.cached_tokens,
    ru.completion_tokens,
    ru.billable_prompt_tokens
FROM rated_usage ru
LEFT JOIN resource_name rn ON rn.resource_id = ru.resource_id
WHERE ru.window_start = $1
ORDER BY ru.id`

// buildSnapshot reads the rollups for windowStart and resolves each to its org. It
// returns the snapshot (only org-resolved rows) and the count of rows it had to OMIT
// because their resource_id resolved to no org. An omitted row is an anomaly: the rater
// only writes attributable (non-NULL resource_id) rows, so a miss here means the
// deployment vanished from resource_name. Omitted rows are NEVER pushed with a guessed
// or empty org (C2/C7) — they are held back and the caller exits non-zero.
func (p *pusher) buildSnapshot(ctx context.Context, windowStart time.Time) (snapshot, int, error) {
	rows, err := p.db.QueryContext(ctx, snapshotQuery, windowStart)
	if err != nil {
		return snapshot{}, 0, fmt.Errorf("query rated_usage for %s: %w", windowStart.Format(time.RFC3339), err)
	}
	defer func() { _ = rows.Close() }()

	// Non-nil so an empty window marshals to "rollups": [] (a meaningful delete-all
	// signal), never JSON null.
	resolved := make([]rollup, 0)
	unattributable := 0

	for rows.Next() {
		var r rollup
		var orgID sql.NullString
		if err := rows.Scan(
			&r.RatedUsageID,
			&r.ResourceID,
			&r.ModelID,
			&orgID,
			&r.Cost,
			&r.AppliedPromptRate,
			&r.AppliedCachedRate,
			&r.AppliedCompletionRate,
			&r.PromptTokens,
			&r.CachedTokens,
			&r.CompletionTokens,
			&r.BillablePromptTokens,
		); err != nil {
			return snapshot{}, 0, fmt.Errorf("scan rated_usage row: %w", err)
		}

		if !orgID.Valid || orgID.String == "" {
			// Held back, never billed to a guessed org. Scream with the identifying
			// fields (NOT the cost amount in the clear beyond what's needed) so an
			// operator can find the orphaned deployment.
			p.log.Error.Printf(
				"token-push: rated_usage %s (resource_id=%s, model_id=%s) has no org in resource_name — OMITTED from the snapshot (held, not billed); deployment missing from resource_name",
				r.RatedUsageID, r.ResourceID, r.ModelID,
			)
			unattributable++
			continue
		}
		r.OrgID = orgID.String
		// rev: phoebe's rated_usage carries no re-rate counter yet; the manager's
		// idempotency is driven by the (rated_usage_id, emitted_cost) delta, not rev, so
		// 0 is correct here. A real rev counter is a follow-up phoebe schema change.
		r.Rev = 0
		resolved = append(resolved, r)
	}
	if err := rows.Err(); err != nil {
		return snapshot{}, 0, fmt.Errorf("iterate rated_usage rows: %w", err)
	}

	return snapshot{
		WindowStart: windowStart.UTC().Format(time.RFC3339),
		Rollups:     resolved,
	}, unattributable, nil
}

// pushWindows builds and POSTs a snapshot for each window, in order. It returns the
// process exit code: exitFatal if any window fails to push (so a CronJob retries the
// whole run — pushes are idempotent), exitUnattrib if every push succeeded but some
// rows were unattributable (omitted), else exitOK.
func (p *pusher) pushWindows(ctx context.Context, windows []time.Time) int {
	totalUnattributable := 0
	for _, w := range windows {
		snap, unattributable, err := p.buildSnapshot(ctx, w)
		if err != nil {
			p.log.Error.Printf("token-push: build snapshot for %s: %v", w.Format(time.RFC3339), err)
			return exitFatal
		}
		totalUnattributable += unattributable

		if err := p.postSnapshot(ctx, snap); err != nil {
			p.log.Error.Printf("token-push: push snapshot for %s: %v", w.Format(time.RFC3339), err)
			return exitFatal
		}
		p.log.Info.Printf("token-push: pushed window %s (%d rollups, %d unattributable omitted)",
			snap.WindowStart, len(snap.Rollups), unattributable)
	}

	if totalUnattributable > 0 {
		return exitUnattrib
	}
	return exitOK
}

// postSnapshot POSTs one window snapshot to the manager. A non-2xx response is an
// error (the manager fail-closes on a bad snapshot; the pusher never treats a rejected
// push as success). The body is read and bounded so a misbehaving manager can't hang
// or exhaust the pod.
func (p *pusher) postSnapshot(ctx context.Context, snap snapshot) error {
	body, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	url := p.managerURL + tokenUsagePath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "token "+p.token)

	client := &http.Client{Timeout: p.timeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Drain a bounded amount so the connection can be reused / closed cleanly and a
	// huge error body can't exhaust memory.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d: %s", url, resp.StatusCode, string(respBody))
	}
	return nil
}
