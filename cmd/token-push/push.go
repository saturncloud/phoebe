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

// maxUnsentHoursPerRun bounds how many never-accepted hours one run re-sends
// (oldest first); the next run continues. A week of hours is far below the
// run's deadline at one request per hour.
const maxUnsentHoursPerRun = 24 * 7

// pushWindows sends one request per hour and returns the exit code. The hours
// are, in this order: the trailing window (or the -since/-until range), every
// hour with parked usage resolved since its last accepted push (however old),
// and up to maxUnsentHoursPerRun hours with usage after the automatic re-send
// floor that were never accepted, oldest first. The window goes first so a
// long backlog can never use up the run's deadline before the current hours
// are sent. Hours are independent: a failed or rejected hour does not stop
// the others.
//
// explicit is true for an operator's -since/-until run. Such a run never sets
// the automatic re-send floor, so backfilling old usage does not make later
// scheduled runs re-send the hours around it.
//
// Exit code precedence: fatal (1) > rejected (3) > held back (2) > ok (0).
func (p *pusher) pushWindows(ctx context.Context, windows []time.Time, now time.Time, explicit bool) int {
	// LIVENESS GUARD. An empty hour is sent as records: [], which deletes the
	// manager's records for that hour. A database whose usage is gone must
	// never send.
	everPushed, err := p.store.EverPushed(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: liveness check: %v", err)
		return exitFatal
	}
	hasRated, hasParked, err := p.store.UsagePresent(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: liveness check: %v", err)
		return exitFatal
	}
	if !hasRated && everPushed {
		// Usage was sent before and rated_usage is empty now: rated_usage is never
		// pruned, so this is a wiped table, whatever parked_usage holds. Sending
		// would delete the manager's records.
		p.log.Error.Printf("token-push: rated_usage is EMPTY although usage was sent before (pushed_hour has rows); refusing to push (every hour would be sent as empty and delete the manager's records). Restore the database")
		return exitFatal
	}
	if !hasRated && !hasParked {
		// NO USAGE YET (main.go package doc): a fresh install. Nothing to send.
		p.log.Info.Printf("token-push: no Token Factory usage recorded yet; nothing to send")
		return exitOK
	}
	if everPushed {
		n, oldest, err := p.store.PreConnectionHours(ctx)
		if err != nil {
			p.log.Error.Printf("token-push: %v", err)
			return exitFatal
		}
		if n > 0 {
			p.log.Info.Printf("token-push: %d hour(s) of usage from before this install's first accepted push (oldest %s) are not sent automatically; send them with -since/-until (%d days per run at most)",
				n, oldest.Format(time.RFC3339), maxPushWindows/24)
		}
	}

	floor := now.UTC().Truncate(time.Hour)
	resolved, err := p.store.ResolvedHours(ctx, floor)
	if err != nil {
		p.log.Error.Printf("token-push: list resolved parked hours: %v", err)
		return exitFatal
	}
	unsent, err := p.store.UnsentHours(ctx, floor, maxUnsentHoursPerRun)
	if err != nil {
		p.log.Error.Printf("token-push: list unsent hours: %v", err)
		return exitFatal
	}
	if len(unsent) > 0 {
		p.log.Info.Printf("token-push: re-sending %d hour(s) with usage the manager never accepted, oldest %s", len(unsent), unsent[0].Format(time.RFC3339))
	}
	hours := uniqueHours(windows, resolved, unsent)

	heldBack, rejected, failed := 0, 0, 0
	for _, h := range hours {
		plan, err := p.buildHour(ctx, h)
		if err != nil {
			p.log.Error.Printf("token-push: build request for %s: %v", h.Format(time.RFC3339), err)
			failed++
			continue
		}
		heldBack += plan.heldBack
		if len(plan.req.Records) == 0 && len(plan.req.Held) == 0 {
			// An empty snapshot deletes, by absence, whatever the manager holds
			// for the hour. Send one only for an hour this install sent before:
			// otherwise the manager's records for it did not come from this
			// database (for instance a database wiped and restarted), and an
			// empty request would delete them.
			sentBefore, err := p.store.HourEverSent(ctx, h)
			if err != nil {
				p.log.Error.Printf("token-push: check whether %s was sent before: %v", plan.req.WindowStart, err)
				failed++
				continue
			}
			if !sentBefore {
				p.log.Info.Printf("token-push: skipping %s: no usage, and this install never sent the hour, so there is nothing to replace", plan.req.WindowStart)
				continue
			}
		}
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
			if err := p.store.RecordAccepted(ctx, h, plan.sent, !explicit); err != nil {
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
			// token; 409 = a manager that still refuses token usage from an install
			// on the legacy billing path (older than 2026-11-17). Each needs a
			// person.
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

// uniqueHours concatenates the hour lists in order, keeping the first
// occurrence of each hour. It does not sort: the order is the send order.
func uniqueHours(lists ...[]time.Time) []time.Time {
	seen := map[time.Time]bool{}
	var out []time.Time
	for _, l := range lists {
		for _, h := range l {
			h = h.UTC()
			if !seen[h] {
				seen[h] = true
				out = append(out, h)
			}
		}
	}
	return out
}
