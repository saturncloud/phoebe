package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
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
	store  pushStore
	log    *logging.Logger
	client poster
}

// openDB opens phoebe's Postgres the same way the rater does (pgx stdlib,
// UTC-pinned DSN, small batch-job pool).
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

// hourPlan is one hour's request and the bookkeeping that goes with it.
type hourPlan struct {
	req meteredusage.Request
	// sent is what the manager will hold for the hour once it accepts req: the
	// records plus the held ids.
	sent []pushedRow
	// heldBack counts rollups token-push itself could not turn into a record
	// (no org, or a wire rule). Parked identities are the rater's to report.
	heldBack int
}

// buildHour turns one hour's rollups into the hour's request.
//
// A rollup that cannot become a valid record holds back ONLY its own identity
// (coordination 2026-10-27): every other record of the hour is sent. The same
// goes for identities the rater has parked. For each held-back or parked
// identity, the usage_ids the manager already holds for it (pushed_usage) go
// into `held`, so delete-by-absence cannot remove usage the manager accepted.
func (p *pusher) buildHour(ctx context.Context, hour time.Time) (hourPlan, error) {
	ws := meteredusage.FormatWindowStart(hour)
	plan := hourPlan{req: meteredusage.Request{
		WindowStart: ws,
		Kinds:       []string{meteredusage.KindToken},
		// Non-nil: an hour with no usage is [] (delete-by-absence), never null.
		Records: make([]meteredusage.Record, 0),
		Held:    make([]string, 0),
	}}

	rollups, err := p.store.Rollups(ctx, hour)
	if err != nil {
		return plan, err
	}
	holdIdentity := map[identity]bool{}
	live := map[string]bool{}
	for _, r := range rollups {
		rec := meteredusage.Record{
			Kind: meteredusage.KindToken, OrgID: r.OrgID.String, OwnerType: r.OwnerType, OwnerID: r.OwnerID,
			ResourceID: r.ResourceID,
			Attributes: meteredusage.TokenAttributes{
				AuthID: r.AuthID, ModelID: r.ModelID, ServingMode: r.ServingMode,
				BaseModel: r.BaseModel.String, Adapter: r.Adapter.String, GraphK8sName: r.GraphK8sName,
				TopologyRevision: meteredusage.TopologyRevisionAttr(r.TopologyRevision), Region: r.Region.String,
			},
			Quantities: meteredusage.TokenQuantities{PromptTokens: r.Prompt, CachedTokens: r.Cached,
				CompletionTokens: r.Completion, RequestCount: r.Requests},
		}
		rec.UsageID = meteredusage.TokenUsageID(ws, rec.OrgID, rec.OwnerType, rec.OwnerID, rec.ResourceID, rec.Attributes)
		if err := meteredusage.ValidateTokenRecord(ws, rec); err != nil {
			// A NULL org lands here too (org_id must be 32 hex): a row written
			// before the rater parked missing orgs.
			p.log.Error.Printf("token-push: HOLDING BACK rated_usage %s (hour %s, auth_id=%s, resource_id=%s, model_id=%s): %v. Only this identity is held back; the rest of the hour is sent",
				r.ID, ws, r.AuthID, r.ResourceID, r.ModelID, err)
			holdIdentity[r.identity] = true
			plan.heldBack++
			continue
		}
		plan.req.Records = append(plan.req.Records, rec)
		plan.sent = append(plan.sent, pushedRow{UsageID: rec.UsageID, identity: r.identity})
		live[rec.UsageID] = true
	}

	parked, err := p.store.ParkedIdentities(ctx, hour)
	if err != nil {
		return plan, err
	}
	for _, id := range parked {
		holdIdentity[id] = true
	}
	if len(holdIdentity) > 0 {
		pushed, err := p.store.Pushed(ctx, hour)
		if err != nil {
			return plan, err
		}
		for _, pr := range pushed {
			if holdIdentity[pr.identity] && !live[pr.UsageID] {
				plan.req.Held = append(plan.req.Held, pr.UsageID)
				plan.sent = append(plan.sent, pr)
			}
		}
	}
	return plan, nil
}

// pushWindows sends one request per hour and returns the exit code. The hours
// are the trailing window plus every hour with parked usage resolved since its
// last accepted push (however old). Hours are independent: a failed or rejected
// hour does not stop the others.
//
// Exit code precedence: fatal (1) > rejected (3) > held back (2) > ok (0).
func (p *pusher) pushWindows(ctx context.Context, windows []time.Time, now time.Time) int {
	// LIVENESS GUARD. An empty hour is sent as records: [], which deletes the
	// manager's records for that hour. If the database holds no usage at all
	// (fresh install, wiped database), every hour would be a delete-all.
	hasAny, err := p.store.HasAnyUsage(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: liveness check: %v", err)
		return exitFatal
	}
	if !hasAny {
		p.log.Error.Printf("token-push: rated_usage and parked_usage are EMPTY; refusing to push (every hour would be sent as empty and delete the manager's records). If this install genuinely has no usage there is nothing to push; if the database was wiped, restore it first")
		return exitFatal
	}

	resolved, err := p.store.ResolvedHours(ctx, now.UTC().Truncate(time.Hour))
	if err != nil {
		p.log.Error.Printf("token-push: list resolved parked hours: %v", err)
		return exitFatal
	}
	hours := mergeHours(windows, resolved)

	heldBack, rejected, failed := 0, 0, 0
	for _, h := range hours {
		plan, err := p.buildHour(ctx, h)
		if err != nil {
			p.log.Error.Printf("token-push: build request for %s: %v", h.Format(time.RFC3339), err)
			failed++
			continue
		}
		heldBack += plan.heldBack
		if err := meteredusage.Validate(plan.req); err != nil {
			// Every record passed on its own, so this is a request-level rule
			// (for instance a duplicate usage_id): a bug. Send nothing for the
			// hour rather than earn a 400.
			p.log.Error.Printf("token-push: NOT SENDING %s: the request breaks a wire rule (%v)", plan.req.WindowStart, err)
			heldBack++
			continue
		}
		if err := p.store.RecordIntent(ctx, h, plan.sent); err != nil {
			p.log.Error.Printf("token-push: record intent for %s: %v (not sent)", plan.req.WindowStart, err)
			failed++
			continue
		}

		resp, err := p.client.Post(ctx, plan.req)
		var rej *meteredusage.RejectedError
		switch {
		case err == nil:
			if err := p.store.RecordAccepted(ctx, h, plan.sent); err != nil {
				// The manager has the hour; our bookkeeping keeps a superset
				// (RecordIntent), which is the safe direction. The next run
				// re-sends and records it.
				p.log.Error.Printf("token-push: sent %s but could not record it: %v", plan.req.WindowStart, err)
				failed++
				continue
			}
			p.log.Info.Printf("token-push: sent %s: %d token records, %d held (manager accepted %d, deleted %d)",
				plan.req.WindowStart, len(plan.req.Records), len(plan.req.Held), resp.Accepted, resp.Deleted)
		case errors.As(err, &rej):
			// Final: 400 = a wire rule we did not catch; 401 = the customer
			// token; 409 = this install is on the legacy billing path; 422 = the
			// hour is in a billing period already settled to Stripe (correction
			// goes to the human credit path). Each needs a person.
			p.log.Error.Printf("token-push: manager REJECTED %s with %d (not retried; needs a person): %s",
				plan.req.WindowStart, rej.Status, rej.Body)
			rejected++
		default:
			p.log.Error.Printf("token-push: send %s: %v (the next run re-sends this hour)", plan.req.WindowStart, err)
			failed++
		}
	}

	switch {
	case failed > 0:
		return exitFatal
	case rejected > 0:
		return exitRejected
	case heldBack > 0:
		return exitWithheld
	default:
		return exitOK
	}
}

// mergeHours returns the sorted union of two hour lists.
func mergeHours(a, b []time.Time) []time.Time {
	seen := map[time.Time]bool{}
	var out []time.Time
	for _, l := range [][]time.Time{a, b} {
		for _, h := range l {
			h = h.UTC()
			if !seen[h] {
				seen[h] = true
				out = append(out, h)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}
