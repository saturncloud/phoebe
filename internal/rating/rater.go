package rating

import (
	"context"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/prune"
)

// ParkedRescanReach is how far back the aggregator re-checks parked hours on
// its own. billing_event is pruned on created_at, never sooner than the ruled
// hard floor (prune.MinBillingEventRetentionDays, 7 days), and an event's
// created_at is never before its own hour. So every event of an hour that
// starts inside this reach is guaranteed present, and re-aggregating that hour
// cannot overwrite real usage with a partial sum. One hour of margin covers
// clock skew between the pruner and the rater. Older parked hours stay parked
// and listed until an operator re-runs them with --since/--until.
const ParkedRescanReach = time.Duration(prune.MinBillingEventRetentionDays)*24*time.Hour - time.Hour

// Aggregator turns billing_event into hourly rated_usage rollups. It holds no
// prices and computes no money: the manager rates (billing-design.md D1). It
// runs one statement per window (see store.go), then logs and reports what that
// statement kept out of the rollups and why.
type Aggregator struct {
	store Store
	log   *logging.Logger
	now   func() time.Time
}

// New constructs an Aggregator over a Store.
func New(store Store, log *logging.Logger) *Aggregator {
	return &Aggregator{store: store, log: log, now: time.Now}
}

// Result summarises one aggregation run. It is returned to the caller AND
// logged, so cmd/rater can turn it into an exit code.
type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	AggregateResult
	// RescannedParkedHours counts parked hours outside the window that this run
	// re-aggregated (within ParkedRescanReach) to see whether they resolved.
	RescannedParkedHours int64
	// Parked is every unresolved parked row after this run, any age.
	Parked []Parked
}

// HasParked reports unbilled, parked usage. cmd/rater exits exitParked (4) on it
// every run until it is resolved, so a Job alert keeps firing.
func (r Result) HasParked() bool { return len(r.Parked) > 0 }

// HasUnexplainedMissingUsage: a successful response with no usage block, work
// that may have been served and cannot be billed. The routine share (client
// aborts, upstream failures) is reported, never paged (ratified with Hugo,
// 2026-09-21): it fires on every install with traffic and would bury the rare
// signals below.
func (r Result) HasUnexplainedMissingUsage() bool { return r.UnexplainedMissingUsageEvents > 0 }

// HasAnomaly reports evidence this run could not turn into any record and does
// not park: it needs a person. cmd/rater exits 2 on it. CONFLICTS (owner
// conflict, ambiguous org, ambiguous base, missing org) are not here: they are
// parked and reported through HasParked, which persists across runs.
func (r Result) HasAnomaly() bool {
	return r.UnattributableEvents > 0 ||
		r.HasUnexplainedMissingUsage() ||
		r.InvalidUsageEvents > 0 ||
		r.InvalidServingModeEvents > 0
}

// Run aggregates [windowStart, windowEnd).
//
// HOUR-ALIGNED ONLY: rollups bucket by the UTC hour and the upsert REPLACES a
// bucket, so a partial-hour window would overwrite a complete rollup with a
// partial sum, and its reconcile would delete rows of an hour it only half
// read. Unaligned bounds are refused, not clamped.
//
// RECONCILE: re-running a window converges rated_usage to exactly this run's
// output (store.go). windowExplicit is whether the operator named the window
// (--since/--until). On a ROUTINE run a reconcile-delete means usage that
// existed before has vanished (data loss, or an upstream regression that now
// withholds it), so it is logged at ERROR; on an explicit backfill it is the
// convergence the operator asked for, logged at INFO. cmd/rater turns the same
// distinction into the exit code.
func (a *Aggregator) Run(ctx context.Context, windowStart, windowEnd time.Time, windowExplicit bool) (Result, error) {
	windowStart = windowStart.UTC()
	windowEnd = windowEnd.UTC()
	res := Result{WindowStart: windowStart, WindowEnd: windowEnd}
	w := fmt.Sprintf("[%s,%s)", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))

	if !windowStart.Before(windowEnd) {
		return res, fmt.Errorf("rating: empty/inverted window %s", w)
	}
	if !windowStart.Truncate(time.Hour).Equal(windowStart) || !windowEnd.Truncate(time.Hour).Equal(windowEnd) {
		return res, fmt.Errorf("rating: window %s is not hour-aligned; rollups bucket by the hour and an upsert REPLACES a bucket, so a partial hour would overwrite a complete rollup", w)
	}

	ar, err := a.aggregateAndLog(ctx, windowStart, windowEnd, windowExplicit)
	if err != nil {
		return res, err
	}
	res.AggregateResult = ar

	// Parked hours outside the window: re-check the ones whose raw events are
	// guaranteed present, so a conflict that resolved (or that an operator fixed
	// in billing_event) is aggregated and sent however old the hour is.
	hours, err := a.store.ParkedHours(ctx, a.now().UTC().Add(-ParkedRescanReach).Truncate(time.Hour).Add(time.Hour))
	if err != nil {
		return res, err
	}
	for _, h := range hours {
		if !h.Before(windowStart) && h.Before(windowEnd) {
			continue // already re-aggregated above
		}
		hr, err := a.aggregateAndLog(ctx, h, h.Add(time.Hour), windowExplicit)
		if err != nil {
			return res, err
		}
		res.RescannedParkedHours++
		res.ParkedResolved += hr.ParkedResolved
		res.ParkedWritten += hr.ParkedWritten
		res.ReconciledDeletions += hr.ReconciledDeletions
		res.ParkedDeletions += hr.ParkedDeletions
	}

	res.Parked, err = a.store.ListParked(ctx)
	if err != nil {
		return res, err
	}
	reach := a.now().UTC().Add(-ParkedRescanReach)
	for _, p := range res.Parked {
		note := ""
		if p.WindowStart.Before(reach) {
			note = "; older than the automatic re-check reach (raw events may be pruned): fix the data and re-run this hour with --since/--until"
		}
		a.log.Error.Printf("rating: PARKED (unbilled) usage hour=%s auth_id=%s owner=%s/%s resource_id=%s model_id=%s serving_mode=%s reason=%s events=%d prompt=%d cached=%d completion=%d first_parked=%s%s",
			p.WindowStart.Format(time.RFC3339), p.AuthID, p.OwnerType, p.OwnerID, p.ResourceID, p.ModelID, p.ServingMode,
			p.Reason, p.EventCount, p.PromptTokens, p.CachedTokens, p.CompletionTokens, p.FirstParkedAt.UTC().Format(time.RFC3339), note)
	}
	if len(res.Parked) > 0 {
		a.log.Error.Printf("rating: %d parked identity-hour(s) hold unbilled usage; list them with `phoebe-rater -list-parked`", len(res.Parked))
	}
	return res, nil
}

// aggregateAndLog runs one aggregation statement and logs what it withheld or
// parked.
func (a *Aggregator) aggregateAndLog(ctx context.Context, windowStart, windowEnd time.Time, windowExplicit bool) (AggregateResult, error) {
	w := fmt.Sprintf("[%s,%s)", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	ar, err := a.store.AggregateWindow(ctx, windowStart, windowEnd)
	if err != nil {
		return ar, err
	}

	if ar.UnknownRevisionRollups > 0 {
		a.log.Warn.Printf("rating: window %s has %d rollup(s) naming a graph with topology revision 0; the quantities and payer are right, but margin analysis cannot attribute that slice of the graph's hour. Expected briefly after an interceptor restart or a graph start; persistent counts mean Atlas's tf-graph-topology entries are not reaching phoebe", w, ar.UnknownRevisionRollups)
	}
	if ar.UnattributableEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d UNATTRIBUTABLE events (no auth_id, resource_id or model_id); they cannot name who used what and are not sent. The interceptor's billing gate should refuse them before metering", w, ar.UnattributableEvents)
	}
	if ar.InvalidServingModeEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d INVALID-SERVING-MODE events (neither \"shared\" nor \"dedicated\"); the serving mode is part of the grain and a pricing fact, so they are not sent. NULL or '' rows mean migration 0007's backfill was missed (see docs/billing-reconciliation.md); anything else means the proxy's serving-mode gate is broken", w, ar.InvalidServingModeEvents)
	}
	if ar.OwnerConflictEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d OWNER-CONFLICT events (both X-Saturn-User-Id and X-Saturn-Group-Id); an identity is a user XOR a group, so the owner cannot be determined; they are parked, not sent, and the raw events stay in billing_event. Fix the header producer; the next run sends them once resolved", w, ar.OwnerConflictEvents)
	}
	if ar.EmptyBaseModelEvents > 0 {
		// WARN, not ERROR, and not withheld: a dedicated endpoint may serve a model
		// with no catalog base (D3). The manager prices dedicated + empty base at
		// the plan's default rate and reports shared + empty base as unpriced.
		a.log.Warn.Printf("rating: window %s aggregated %d events with no base_model (sent as \"\"); the manager prices dedicated ones at the plan's default dedicated rate and reports shared ones as unpriced. Expected for a dedicated endpoint serving a model outside the catalog; on a shared model it means X-Saturn-Base-Model is not propagating", w, ar.EmptyBaseModelEvents)
	}
	if ar.AmbiguousBaseEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d events under AMBIGUOUS-BASE identities (one auth/owner/resource/model/serving mode in one hour named more than one base_model or adapter, or a value on some events and none on others); a record carries one of each and the manager prices by them, so they are parked, not sent. Fix base_model/adapter propagation; the next run sends them once resolved", w, ar.AmbiguousBaseEvents)
	}
	if ar.AmbiguousOrgEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d events under AMBIGUOUS-ORG identities (more than one non-NULL org_id); the org pays, so a guess would bill the wrong customer; they are parked, not sent. Fix X-Saturn-Org-Id propagation; the next run sends them once resolved", w, ar.AmbiguousOrgEvents)
	}
	if ar.UnexplainedMissingUsageEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d UNEXPLAINED MISSING-USAGE attempts (not aborted, not failed, no usage block); work may have been served that cannot be billed. Reconcile against engine logs", w, ar.UnexplainedMissingUsageEvents)
	}
	if ar.ExpectedMissingUsageEvents > 0 {
		a.log.Info.Printf("rating: window %s has %d expected missing-usage attempts (client aborted or upstream failed); retained with zero usage, not aggregated, no action required", w, ar.ExpectedMissingUsageEvents)
	}
	if ar.InvalidUsageEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d INVALID-USAGE events (negative counts, or cached > prompt); retained as evidence, not aggregated. Repair or quarantine the evidence", w, ar.InvalidUsageEvents)
	}
	if ar.ReconciledDeletions > 0 {
		if windowExplicit {
			a.log.Info.Printf("rating: EXPLICIT window %s reconcile DELETED %d rollup(s) earlier runs wrote and this run no longer produces; intended convergence", w, ar.ReconciledDeletions)
		} else {
			a.log.Error.Printf("rating: ROUTINE window %s reconcile DELETED %d rollup(s) earlier runs wrote; with no operator behind it this means events VANISHED from billing_event or an upstream regression now withholds them. token-push will tell the manager to delete them. Investigate before the next push", w, ar.ReconciledDeletions)
		}
	}

	if ar.MissingOrgEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d events under MISSING-ORG identities (no event named an org); the payer is unknown, so they are parked, not sent. Fix X-Saturn-Org-Id propagation; the next run sends them once an org arrives", w, ar.MissingOrgEvents)
	}
	if ar.ParkedResolved > 0 {
		a.log.Info.Printf("rating: window %s resolved %d parked identity-hour(s); token-push re-sends their hours", w, ar.ParkedResolved)
	}
	a.log.Info.Printf("rating: window %s aggregated %d events into %d rollups; %d identity-hour(s) parked", w, ar.EventsAggregated, ar.RollupsWritten, ar.ParkedWritten)
	return ar, nil
}
