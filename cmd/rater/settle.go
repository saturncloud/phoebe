package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/prune"
	"github.com/saturncloud/phoebe/internal/rating"
)

// SETTLING PARKED USAGE BY HAND (Hugo, 2026-10-27: parked usage follows the
// 30-day pruning rule unchanged and is settled by hand).
//
//	phoebe-rater -settle-parked -hour 2026-09-14T04:00:00Z -kind token \
//	    -auth-id A -owner user/U -resource-id R -model-id M -serving-mode dedicated \
//	    -parked-reason missing_org -settled-by alice -settle-reason "no payer; written off"
//
// The identity flags are the columns of one `-list-parked` line, copied as
// printed (OWNER "-" is the empty owner of an owner_conflict row). The command
// settles exactly that one unresolved row: it keeps the row and sets
// resolved_at, settled_at, settled_by and settle_reason. From then on rater
// runs neither page for it nor re-aggregate its hour, and token-push re-sends
// the hour without the identity, as for any resolved row (so a record the
// manager held for it is deleted by absence; the settlement covers the row's
// full counts). Do the charge or credit in the manager first.
//
// It refuses a row whose usage can still be billed: an hour younger than
// prune.DefaultBillingEventRetentionDays (30 days) whose raw events are all
// still present. Fix the cause and re-run that hour instead.
//
// Exit 0 when the row is settled, 1 otherwise (nothing changed).

// settleOptions are the -settle-parked flags.
type settleOptions struct {
	on                                             *bool
	hour, kind, authID, owner, resourceID, modelID *string
	servingMode, parkedReason, by, reason          *string
}

func settleFlags(fs *flag.FlagSet) settleOptions {
	return settleOptions{
		on:           fs.Bool("settle-parked", false, "Record that one unresolved parked row was settled by hand (needs -hour, -auth-id, -owner, -resource-id, -model-id, -serving-mode, -parked-reason, -settled-by, -settle-reason); no aggregation"),
		hour:         fs.String("hour", "", "-settle-parked: the row's HOUR, RFC3339, as -list-parked prints it"),
		kind:         fs.String("kind", rating.KindToken, "-settle-parked: the row's KIND, token or video"),
		authID:       fs.String("auth-id", "", "-settle-parked: the row's AUTH_ID"),
		owner:        fs.String("owner", "", "-settle-parked: the row's OWNER as printed (user/<id>, group/<id>, or - for none)"),
		resourceID:   fs.String("resource-id", "", "-settle-parked: the row's RESOURCE_ID"),
		modelID:      fs.String("model-id", "", "-settle-parked: the row's MODEL_ID"),
		servingMode:  fs.String("serving-mode", "", "-settle-parked: the row's MODE"),
		parkedReason: fs.String("parked-reason", "", "-settle-parked: the row's REASON (owner_conflict, ambiguous_org, ambiguous_base, missing_org)"),
		by:           fs.String("settled-by", "", "-settle-parked: who settled it (required)"),
		reason:       fs.String("settle-reason", "", "-settle-parked: why, and what was done in the manager (required)"),
	}
}

// key validates the flags and returns the row's key.
func (o settleOptions) key() (rating.ParkedKey, error) {
	var missing []string
	for name, v := range map[string]string{
		"-hour": *o.hour, "-auth-id": *o.authID, "-owner": *o.owner, "-resource-id": *o.resourceID,
		"-model-id": *o.modelID, "-serving-mode": *o.servingMode, "-parked-reason": *o.parkedReason,
		"-settled-by": strings.TrimSpace(*o.by), "-settle-reason": strings.TrimSpace(*o.reason),
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return rating.ParkedKey{}, fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	if *o.kind != rating.KindToken && *o.kind != rating.KindVideo {
		return rating.ParkedKey{}, fmt.Errorf("-kind %q: want %s or %s", *o.kind, rating.KindToken, rating.KindVideo)
	}
	h, err := time.Parse(time.RFC3339, *o.hour)
	if err != nil {
		return rating.ParkedKey{}, fmt.Errorf("-hour %q: %v", *o.hour, err)
	}
	h = h.UTC()
	if !h.Truncate(time.Hour).Equal(h) {
		return rating.ParkedKey{}, fmt.Errorf("-hour %s is not the start of an hour", h.Format(time.RFC3339))
	}
	ownerType, ownerID := "", ""
	if *o.owner != "-" {
		t, id, ok := strings.Cut(*o.owner, "/")
		if !ok || t == "" {
			return rating.ParkedKey{}, fmt.Errorf("-owner %q: want user/<id>, group/<id>, or -", *o.owner)
		}
		ownerType, ownerID = t, id
	}
	return rating.ParkedKey{Kind: *o.kind, WindowStart: h, AuthID: *o.authID, OwnerType: ownerType, OwnerID: ownerID,
		ResourceID: *o.resourceID, ModelID: *o.modelID, ServingMode: *o.servingMode, Reason: *o.parkedReason}, nil
}

// settleStore is what settleParked needs from the store.
type settleStore interface {
	ListParked(ctx context.Context) ([]rating.Parked, error)
	ListSettled(ctx context.Context) ([]rating.Parked, error)
	SettleParked(ctx context.Context, k rating.ParkedKey, by, reason string) (time.Time, error)
}

// settleMinAge is how old a parked hour must be before it can be settled by
// hand while its raw events are all present: the ruled billing_event horizon
// (Hugo, 2026-10-27: settled by hand after 30 days).
const settleMinAge = time.Duration(prune.DefaultBillingEventRetentionDays) * 24 * time.Hour

// settleParked settles the one unresolved parked row named by k.
func settleParked(ctx context.Context, store settleStore, w io.Writer, log *logging.Logger,
	k rating.ParkedKey, by, reason string, now time.Time) int {
	parked, err := store.ListParked(ctx)
	if err != nil {
		log.Error.Printf("rater: %v", err)
		return exitFatal
	}
	var row *rating.Parked
	for i := range parked {
		if parked[i].Key() == k {
			row = &parked[i]
			break
		}
	}
	if row == nil {
		settled, err := store.ListSettled(ctx)
		if err != nil {
			log.Error.Printf("rater: %v", err)
			return exitFatal
		}
		for _, p := range settled {
			if p.Key() == k {
				log.Error.Printf("rater: -settle-parked: that row was already settled at %s by %q (%s); nothing changed",
					p.SettledAt.Format(time.RFC3339), p.SettledBy, p.SettleReason)
				return exitFatal
			}
		}
		log.Error.Printf("rater: -settle-parked: no unresolved parked row matches (copy the fields from `phoebe-rater -list-parked`); nothing changed")
		return exitFatal
	}
	if !row.RawEventsPruned && now.UTC().Sub(row.WindowStart) < settleMinAge {
		log.Error.Printf("rater: -settle-parked: hour %s is younger than %d days and its raw events are all present, so its usage can still be billed: fix the cause and re-run the hour with -since/-until. Parked usage is settled by hand only after %d days (rulings, 2026-10-27); nothing changed",
			row.WindowStart.Format(time.RFC3339), prune.DefaultBillingEventRetentionDays, prune.DefaultBillingEventRetentionDays)
		return exitFatal
	}
	at, err := store.SettleParked(ctx, k, strings.TrimSpace(by), strings.TrimSpace(reason))
	if errors.Is(err, rating.ErrParkedRowNotUnresolved) {
		log.Error.Printf("rater: -settle-parked: the row was resolved by another run meanwhile; nothing changed")
		return exitFatal
	}
	if err != nil {
		log.Error.Printf("rater: %v", err)
		return exitFatal
	}
	row.SettledAt, row.SettledBy, row.SettleReason = at, strings.TrimSpace(by), strings.TrimSpace(reason)
	fmt.Fprintln(w, "settled:")
	printSettled(w, []rating.Parked{*row})
	return exitOK
}

// printSettled writes the rows settled by hand, with a header, after the
// unresolved rows in -list-parked. They no longer page; they are kept as the
// record of the unbilled usage and of who settled it and why.
func printSettled(w io.Writer, settled []rating.Parked) {
	if len(settled) == 0 {
		return
	}
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SETTLED_HOUR\tKIND\tREASON\tAUTH_ID\tOWNER\tRESOURCE_ID\tMODEL_ID\tMODE\tEVENTS\tUSAGE\tSETTLED_AT\tSETTLED_BY\tSETTLE_REASON")
	for _, p := range settled {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			p.WindowStart.Format(time.RFC3339), p.Kind, p.Reason, p.AuthID, ownerLabel(p.OwnerType, p.OwnerID),
			p.ResourceID, p.ModelID, p.ServingMode, p.EventCount, p.UsageLabel(),
			p.SettledAt.UTC().Format(time.RFC3339), p.SettledBy, p.SettleReason)
	}
	_ = tw.Flush()
	fmt.Fprintf(w, "%d parked identity-hour(s) settled by hand (kept as the record; they do not page)\n", len(settled))
}

// ownerLabel is the OWNER column: "<type>/<id>", or "-" for the empty owner.
func ownerLabel(ownerType, ownerID string) string {
	if ownerType == "" {
		return "-"
	}
	return ownerType + "/" + ownerID
}
