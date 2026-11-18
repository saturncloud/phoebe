package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// maxUnsentHoursPerRun bounds how many never-accepted hours one run sends:
// missed hours after the automatic re-send floor first, then back-filled hours
// from before it, each oldest first. The next run continues. A week of hours
// is far below the run's deadline at one request per hour. Parked hours do not
// count against it.
const maxUnsentHoursPerRun = 24 * 7

// maxParkedRechecksPerRun bounds how many parked hours outside the other lists
// one run rebuilds to see whether their payload changed (least recently
// checked first). Rebuilding costs database reads only; an unchanged hour is
// not sent.
const maxParkedRechecksPerRun = 24 * 7

// refusalParksTheHour reports whether a manager refusal is about the hour's
// content, so re-sending the same payload would be refused again: a 400 whose
// body is the manager's validation answer ({"error": ..., "path": ...}). Any
// other 400 (a proxy in front of the manager, for instance) is not parked:
// parking stops the hour from being re-sent, so a refusal that may be about
// the install rather than the hour must keep failing loudly instead.
func refusalParksTheHour(rej *meteredusage.RejectedError) bool {
	if rej.Status != http.StatusBadRequest {
		return false
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rej.Body), &body); err != nil {
		return false
	}
	_, hasError := body["error"]
	_, hasPath := body["path"]
	return hasError && hasPath
}

// refusalIsForTheInstall reports whether a manager refusal applies to every
// hour of this install (401 token, 403 inactive or uninstalled install, 409 a
// manager older than the 2026-11-17 rule refusing a legacy install). The rest
// of the run would get the same answer, so the run stops sending.
func refusalIsForTheInstall(rej *meteredusage.RejectedError) bool {
	switch rej.Status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict:
		return true
	}
	return false
}

func payloadSHA256(req meteredusage.Request) (string, error) {
	b, err := json.Marshal(req) // the bytes meteredusage.Client sends
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// pushWindows sends one request per hour and returns the exit code. The hours
// are, in this order: the trailing window (or the -since/-until range), every
// hour with parked usage resolved since its last accepted push (however old),
// up to maxUnsentHoursPerRun never-accepted hours (after the automatic re-send
// floor first, then the back-fill of usage from before it, each oldest first),
// and up to maxParkedRechecksPerRun parked hours. The window goes first so a
// long backlog can never use up the run's deadline before the current hours
// are sent. Hours are independent: a failed or refused hour does not stop the
// others, except a refusal for the whole install (401/403/409), which stops
// the run.
//
// A PARKED hour (refused_hour: the manager refused its content, or it breaks
// a request-level wire rule) is rebuilt but sent again only if its payload
// changed. An explicit run sends every hour of its range, parked or not.
//
// explicit is true for an operator's -since/-until run. Such a run never sets
// the automatic re-send floor, so backfilling old usage does not make later
// scheduled runs re-send the hours around it.
//
// Exit code precedence: fatal (1) > rejected (3) > held back (2) > ok (0).
// Parking an hour counts once, in the run that parks it (3 for a manager
// refusal, 2 for a local wire-rule failure); a parked hour left unchanged
// adds nothing.
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
	var backfill []time.Time
	if room := maxUnsentHoursPerRun - len(unsent); room > 0 {
		if backfill, err = p.store.BackfillHours(ctx, room); err != nil {
			p.log.Error.Printf("token-push: list back-fill hours: %v", err)
			return exitFatal
		}
	}
	remaining, oldest, err := p.store.PreConnectionHours(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: %v", err)
		return exitFatal
	}
	if remaining > 0 {
		p.log.Info.Printf("token-push: back-filling usage from before this install's first accepted push: %d hour(s) not sent yet (oldest %s); this run sends %d (at most %d never-accepted hours per run, missed hours after the first push go first)",
			remaining, oldest.Format(time.RFC3339), len(backfill), maxUnsentHoursPerRun)
	}

	refused, err := p.store.RefusedHours(ctx)
	if err != nil {
		p.log.Error.Printf("token-push: list parked hours: %v", err)
		return exitFatal
	}
	parked := make(map[time.Time]string, len(refused))
	for _, r := range refused {
		parked[r.Hour.UTC()] = r.PayloadSHA256
	}
	hours := uniqueHours(windows, resolved, unsent, backfill)
	listed := make(map[time.Time]bool, len(hours))
	for _, h := range hours {
		listed[h] = true
	}
	var recheck []time.Time
	for _, r := range refused {
		if len(recheck) == maxParkedRechecksPerRun {
			break
		}
		if !listed[r.Hour.UTC()] {
			recheck = append(recheck, r.Hour.UTC())
		}
	}
	hours = uniqueHours(hours, recheck)

	heldBack, rejected, failed, newlyParked := 0, 0, 0, 0
	var unchanged []time.Time
send:
	for i, h := range hours {
		plan, err := p.buildHour(ctx, h)
		if err != nil {
			p.log.Error.Printf("token-push: build request for %s: %v", h.Format(time.RFC3339), err)
			failed++
			continue
		}
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
				heldBack += plan.heldBack
				p.log.Info.Printf("token-push: skipping %s: no usage, and this install never sent the hour, so there is nothing to replace", plan.req.WindowStart)
				continue
			}
		}
		sum, err := payloadSHA256(plan.req)
		if err != nil {
			p.log.Error.Printf("token-push: encode request for %s: %v", plan.req.WindowStart, err)
			failed++
			continue
		}
		prev, wasParked := parked[h]
		if wasParked && prev == sum && !explicit {
			// Refused for this exact content before: the same request would be
			// refused again. Counted in the parked-hours line below.
			unchanged = append(unchanged, h)
			p.log.Debug.Printf("token-push: %s is parked and its payload has not changed; not sent", plan.req.WindowStart)
			continue
		}
		heldBack += plan.heldBack
		if err := meteredusage.Validate(plan.req); err != nil {
			// Every record passed on its own, so this is a request-level rule
			// (for instance a duplicate usage_id): a bug. Send nothing for the
			// hour rather than earn a 400, and park it.
			heldBack++
			if err := p.store.RecordRefused(ctx, h, 0, err.Error(), sum); err != nil {
				p.log.Error.Printf("token-push: NOT SENDING %s and could not park it: %v", plan.req.WindowStart, err)
				failed++
				continue
			}
			parked[h] = sum
			newlyParked++
			p.log.Error.Printf("token-push: PARKED %s: the request breaks a wire rule (%v); not sent, and not re-sent until its usage changes (refused_hour)", plan.req.WindowStart, err)
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
			if wasParked {
				delete(parked, h)
				p.log.Info.Printf("token-push: %s was parked; its payload changed and the manager accepted it", plan.req.WindowStart)
			}
			p.log.Info.Printf("token-push: sent %s: %d token records, %d held (manager accepted %d, deleted %d)",
				plan.req.WindowStart, len(plan.req.Records), len(plan.req.Held), resp.Accepted, resp.Deleted)
		case errors.As(err, &rej) && refusalParksTheHour(rej):
			// 400 with the manager's validation body: this payload will never
			// be accepted. Park the hour so it is not re-sent every run and
			// does not hold up other hours.
			rejected++
			if err := p.store.RecordRefused(ctx, h, rej.Status, rej.Body, sum); err != nil {
				p.log.Error.Printf("token-push: manager REJECTED %s with %d (%s) and it could not be parked: %v", plan.req.WindowStart, rej.Status, rej.Body, err)
				failed++
				continue
			}
			parked[h] = sum
			newlyParked++
			p.log.Error.Printf("token-push: PARKED %s: manager REJECTED it with %d: %s. Not re-sent until its usage changes (refused_hour); needs a person",
				plan.req.WindowStart, rej.Status, rej.Body)
		case errors.As(err, &rej) && refusalIsForTheInstall(rej):
			rejected++
			p.log.Error.Printf("token-push: manager REJECTED %s with %d: %s. This refusal is for the whole install (401 token, 403 inactive or uninstalled install, 409 a manager older than the 2026-11-17 rule), so the run stops; %d hour(s) not sent this run. Needs a person",
				plan.req.WindowStart, rej.Status, rej.Body, len(hours)-i-1)
			break send
		case errors.As(err, &rej):
			p.log.Error.Printf("token-push: manager REJECTED %s with %d (not retried; needs a person): %s",
				plan.req.WindowStart, rej.Status, rej.Body)
			rejected++
		default:
			p.log.Error.Printf("token-push: send %s: %v (the next run re-sends this hour)", plan.req.WindowStart, err)
			failed++
		}
	}
	if err := p.store.MarkRefusedChecked(ctx, unchanged); err != nil {
		p.log.Error.Printf("token-push: %v", err)
		failed++
	}
	if len(parked) > 0 {
		var oldestParked time.Time
		for h := range parked {
			if oldestParked.IsZero() || h.Before(oldestParked) {
				oldestParked = h
			}
		}
		p.log.Warn.Printf("token-push: %d hour(s) are parked (%d newly this run), oldest %s: the manager refused their content or they break a wire rule, so they are not re-sent until their usage changes. They are listed in refused_hour; after fixing the cause, send one with -since/-until",
			len(parked), newlyParked, oldestParked.Format(time.RFC3339))
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
