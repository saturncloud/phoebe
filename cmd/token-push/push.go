package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	// client is shared across all window POSTs so TCP/TLS connections to the manager
	// are reused across the (default 24) windows of a run, instead of a fresh handshake
	// per window.
	client *http.Client
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
		// Format with an explicit +00:00 offset, NOT the "Z" military form: the manager
		// is Python, and Python 3.10's datetime.fromisoformat() rejects a trailing "Z"
		// (only fixed in 3.11). "+00:00" is parseable by fromisoformat AND by Go's
		// RFC3339 — the safe cross-language form. (time.RFC3339 emits "Z" for UTC, so we
		// pin the numeric offset zone explicitly.)
		WindowStart: windowStart.UTC().Format("2006-01-02T15:04:05+00:00"),
		Rollups:     resolved,
	}, unattributable, nil
}

// ratedUsageHasAnyRow reports whether rated_usage contains at least one row. Used as a
// liveness guard before pushing: an entirely empty table means an all-empty (delete-all)
// run, which token-push refuses. Bounded with LIMIT 1 — it does not count.
func (p *pusher) ratedUsageHasAnyRow(ctx context.Context) (bool, error) {
	var one int
	err := p.db.QueryRowContext(ctx, `SELECT 1 FROM rated_usage LIMIT 1`).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// pushWindows builds and POSTs a snapshot for each window. It returns the process exit
// code with FATAL DOMINATING: exitFatal if ANY window failed to push (the job is broken
// — most urgent, takes precedence even if another window was also withheld); else
// exitUnattrib if any window was WITHHELD because it contained an unattributable row
// (held revenue / lost attribution, but the job otherwise ran); else exitOK.
//
// CRITICAL — a window with ANY unattributable row is NOT pushed (Ben's ruling). The
// snapshot is delete-by-absence on the manager: a rated_usage_id the manager has on
// file for this window but ABSENT from the snapshot is treated as a reconcile-DELETE
// (un-bill). So pushing a window with a row OMITTED (because its resource_id no longer
// resolves to an org) would silently DELETE the prior, possibly-already-billed charge
// for that row — a money-loss, not a "hold". We instead WITHHOLD the whole window
// (leave the manager's prior good state for it standing) and scream + exit 2. This is
// the price-fetch fail-closed posture: stale-but-billed beats silently-un-billed.
// Convergence resumes automatically on the next run once the resource_name row is
// restored (the trailing re-push window re-covers the hour). The interim cost is
// possibly over-holding a window that had a genuinely NEW unresolvable row — over-
// holding is the safe direction. (A future wire `held` marker scopes delete-by-absence
// so a held row need not block its whole window; that lands install+manager together.)
//
// Windows are independent: a withheld or failed window does not abort the others, so a
// single bad window can't block reconvergence of every other hour.
func (p *pusher) pushWindows(ctx context.Context, windows []time.Time) int {
	// LIVENESS GUARD against an empty rated_usage table (fresh install, DB wipe,
	// disaster recovery). An empty window snapshots to "rollups": [] which signals
	// delete-all for that hour; if the WHOLE table is empty, every window would push a
	// delete-all and wipe up to trailingHours of previously-billed data on the manager.
	// A fresh install with genuinely no usage also has nothing to push. So if the table
	// has zero rows AT ALL, refuse to push anything and exit fatal (the operator must
	// confirm this is intended — token-push will not delete the manager's history on an
	// empty local table). This is the safe install-side half; the full fix (a manager
	// "no-data != delete" contract) is escalated.
	hasAny, err := p.ratedUsageHasAnyRow(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: liveness check on rated_usage: %v", err)
		return exitFatal
	}
	if !hasAny {
		p.log.Error.Printf("token-push: rated_usage is EMPTY — refusing to push (an all-empty run would signal delete-all for every window and wipe the manager's prior billing data). If this install genuinely has no usage, there is nothing to push; if the table was wiped, restore it before running.")
		return exitFatal
	}

	withheld := 0
	failed := 0
	for _, w := range windows {
		snap, unattributable, err := p.buildSnapshot(ctx, w)
		if err != nil {
			p.log.Error.Printf("token-push: build snapshot for %s: %v", w.Format(time.RFC3339), err)
			failed++
			continue
		}

		if unattributable > 0 {
			// Do NOT push: this snapshot omits a row, and absence == delete on the
			// manager. Withhold the whole window rather than risk un-billing a prior
			// charge. Already screamed per-row in buildSnapshot.
			p.log.Error.Printf(
				"token-push: WITHHOLDING window %s — it has %d unattributable row(s); pushing the omitted snapshot would delete-by-absence a possibly-billed rollup. Leaving the manager's prior state for this window untouched (restore the resource_name mapping; the next run re-pushes).",
				snap.WindowStart, unattributable,
			)
			withheld++
			continue
		}

		if err := p.postSnapshot(ctx, snap); err != nil {
			p.log.Error.Printf("token-push: push snapshot for %s: %v", w.Format(time.RFC3339), err)
			failed++
			continue
		}
		p.log.Info.Printf("token-push: pushed window %s (%d rollups)", snap.WindowStart, len(snap.Rollups))
	}

	if failed > 0 {
		return exitFatal
	}
	if withheld > 0 {
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

	// Trim a trailing slash on the base URL so a configured "https://m/" doesn't
	// produce "//customer/token-usage" (which can 404 or redirect, silently failing
	// the push).
	url := strings.TrimRight(p.managerURL, "/") + tokenUsagePath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "token "+p.token)

	resp, err := p.client.Do(req)
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
