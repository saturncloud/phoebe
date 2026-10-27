package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
	"github.com/saturncloud/phoebe/internal/rating"
)

// poster sends one hour to the manager. *meteredusage.Client in production; a
// stub in tests.
type poster interface {
	Post(ctx context.Context, req meteredusage.Request) (meteredusage.Response, error)
}

// pusher holds the dependencies for building and sending hourly requests.
type pusher struct {
	db     *sql.DB
	log    *logging.Logger
	client poster
}

// openDB opens phoebe's Postgres the same way the rater does (pgx stdlib,
// UTC-pinned DSN, small batch-job pool). token-push only reads rated_usage.
func openDB(ctx context.Context, cfg rating.Config) (*sql.DB, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("token-push: DATABASE_URL is empty (Postgres holds rated_usage; the pusher cannot run without it)")
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

// hourQuery reads every rollup of one hour: the record's grain, its payer and
// pricing facts, and its quantities. No money: the manager prices.
//
// org_id, region, base_model and adapter are nullable. A NULL org means the
// payer is unknown and the hour is withheld (see buildRequest); NULL region and
// adapter are sent as "" (unknown / none), which the wire allows.
const hourQuery = `
SELECT
    ru.id,
    ru.org_id,
    ru.owner_type,
    ru.owner_id,
    ru.resource_id,
    ru.auth_id,
    ru.model_id,
    ru.serving_mode,
    ru.base_model,
    ru.adapter,
    ru.graph_k8s_name,
    ru.topology_revision,
    ru.region,
    ru.prompt_tokens,
    ru.cached_tokens,
    ru.completion_tokens,
    ru.event_count
FROM rated_usage ru
WHERE ru.window_start = $1
ORDER BY ru.id`

// buildRequest reads one hour's rollups and turns them into the complete token
// request for that hour. It returns the request and the number of rollups it
// could NOT turn into a record (no org, or a record that would break a wire
// rule). If that number is nonzero the caller must not send the request: it
// is a snapshot, and the manager would delete the missing records by absence.
func (p *pusher) buildRequest(ctx context.Context, windowStart time.Time) (meteredusage.Request, int, error) {
	ws := meteredusage.FormatWindowStart(windowStart)
	req := meteredusage.Request{
		WindowStart: ws,
		Kinds:       []string{meteredusage.KindToken},
		// Non-nil: an hour with no usage is [] (delete-by-absence), never null.
		Records: make([]meteredusage.Record, 0),
		Held:    make([]string, 0),
	}

	rows, err := p.db.QueryContext(ctx, hourQuery, windowStart)
	if err != nil {
		return req, 0, fmt.Errorf("query rated_usage for %s: %w", ws, err)
	}
	defer func() { _ = rows.Close() }()

	unsendable := 0
	for rows.Next() {
		var (
			id                       string
			org, base, adapter, regn sql.NullString
			rev                      int64
			rec                      = meteredusage.Record{Kind: meteredusage.KindToken}
			a                        = &rec.Attributes
			q                        = &rec.Quantities
		)
		if err := rows.Scan(&id, &org, &rec.OwnerType, &rec.OwnerID, &rec.ResourceID,
			&a.AuthID, &a.ModelID, &a.ServingMode, &base, &adapter, &a.GraphK8sName, &rev, &regn,
			&q.PromptTokens, &q.CachedTokens, &q.CompletionTokens, &q.RequestCount); err != nil {
			return req, 0, fmt.Errorf("scan rated_usage row: %w", err)
		}
		if !org.Valid || org.String == "" {
			// The org pays. Never sent with a guessed or empty payer.
			p.log.Error.Printf("token-push: rated_usage %s (resource_id=%s, model_id=%s) has no org_id; Atlas did not inject X-Saturn-Org-Id for this deployment or its propagation broke. The hour is withheld",
				id, rec.ResourceID, a.ModelID)
			unsendable++
			continue
		}
		rec.OrgID = org.String
		a.BaseModel = base.String
		a.Adapter = adapter.String
		a.Region = regn.String
		a.TopologyRevision = meteredusage.TopologyRevisionAttr(rev)
		rec.UsageID = meteredusage.TokenUsageID(ws, rec.OrgID, rec.OwnerType, rec.OwnerID, rec.ResourceID, *a)
		req.Records = append(req.Records, rec)
	}
	if err := rows.Err(); err != nil {
		return req, 0, fmt.Errorf("iterate rated_usage rows: %w", err)
	}
	return req, unsendable, nil
}

// ratedUsageHasAnyRow reports whether rated_usage contains at least one row.
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

// pushWindows builds and sends one request per hour and returns the exit code.
// Hours are independent: a withheld, rejected or failed hour does not stop the
// others, so one bad hour cannot block every other hour from converging.
//
// WITHHOLDING (fail closed). An hour with a rollup that cannot become a valid
// record is NOT sent. A request is the complete snapshot of the hour, so
// sending it without that rollup would make the manager delete the record it
// already holds for it (possibly already settled). Withholding leaves the
// manager's previous state for the hour standing; the next run re-sends the hour
// once the cause is fixed. Over-holding is the safe direction. (Ben's ruling on
// the previous push; unchanged by the new wire.)
//
// Exit code precedence: fatal (1) > rejected (3) > withheld (2) > ok (0).
func (p *pusher) pushWindows(ctx context.Context, windows []time.Time) int {
	// LIVENESS GUARD. An empty hour is sent as records: [], which deletes the
	// manager's records for that hour. If the whole table is empty (fresh
	// install, wiped database), every hour would be a delete-all, so refuse.
	hasAny, err := p.ratedUsageHasAnyRow(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: liveness check on rated_usage: %v", err)
		return exitFatal
	}
	if !hasAny {
		p.log.Error.Printf("token-push: rated_usage is EMPTY; refusing to push (every hour would be sent as empty and delete the manager's records). If this install genuinely has no usage there is nothing to push; if the table was wiped, restore it first")
		return exitFatal
	}

	withheld, rejected, failed := 0, 0, 0
	for _, w := range windows {
		req, unsendable, err := p.buildRequest(ctx, w)
		if err != nil {
			p.log.Error.Printf("token-push: build request for %s: %v", w.Format(time.RFC3339), err)
			failed++
			continue
		}
		if unsendable > 0 {
			p.log.Error.Printf("token-push: WITHHOLDING %s: %d rollup(s) cannot become a record; sending the rest would delete their records on the manager. The manager keeps its previous state for this hour; the next run re-sends it once fixed",
				req.WindowStart, unsendable)
			withheld++
			continue
		}
		if err := meteredusage.Validate(req); err != nil {
			// The manager would answer 400 and store nothing; refuse here, where
			// the evidence is. Same withholding rule as above.
			p.log.Error.Printf("token-push: WITHHOLDING %s: the request breaks a wire rule the manager enforces (%v). Fix the rollup and re-run", req.WindowStart, err)
			withheld++
			continue
		}

		resp, err := p.client.Post(ctx, req)
		var rej *meteredusage.RejectedError
		switch {
		case err == nil:
			p.log.Info.Printf("token-push: sent %s: %d token records (manager accepted %d, deleted %d)",
				req.WindowStart, len(req.Records), resp.Accepted, resp.Deleted)
		case errors.As(err, &rej):
			// Not retried: the manager would answer the same. 400 = a wire rule
			// we did not catch; 401 = the customer token; 409 = this install is
			// on the legacy billing path; 422 = the hour is in a billing period
			// already settled to Stripe (correction goes to the human credit
			// path). Each needs a person.
			p.log.Error.Printf("token-push: manager REJECTED %s with %d (not retried; needs a person): %s",
				req.WindowStart, rej.Status, rej.Body)
			rejected++
		default:
			p.log.Error.Printf("token-push: send %s: %v (the next run re-sends this hour)", req.WindowStart, err)
			failed++
		}
	}

	switch {
	case failed > 0:
		return exitFatal
	case rejected > 0:
		return exitRejected
	case withheld > 0:
		return exitWithheld
	default:
		return exitOK
	}
}
