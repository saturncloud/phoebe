package rating

import (
	"context"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// Aggregator turns billing_event into hourly rated_usage rollups. It holds no
// prices and computes no money: the manager rates (billing-design.md D1). It
// runs one statement per window (see store.go), then logs and reports what that
// statement kept out of the rollups and why.
type Aggregator struct {
	store Store
	log   *logging.Logger
}

// New constructs an Aggregator over a Store.
func New(store Store, log *logging.Logger) *Aggregator {
	return &Aggregator{store: store, log: log}
}

// Result summarises one aggregation run. It is returned to the caller AND
// logged, so cmd/rater can turn it into an exit code.
type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	AggregateResult
}

// HasUnexplainedMissingUsage: a successful response with no usage block, work
// that may have been served and cannot be billed. The routine share (client
// aborts, upstream failures) is reported, never paged (ratified with Hugo,
// 2026-09-21): it fires on every install with traffic and would bury the rare
// signals below.
func (r Result) HasUnexplainedMissingUsage() bool { return r.UnexplainedMissingUsageEvents > 0 }

// HasAnomaly reports whether usage was kept out of rated_usage for a reason
// that needs a person: evidence that cannot become a record, or a record that
// would name the wrong payer or the wrong pricing facts. cmd/rater exits 2 on it.
func (r Result) HasAnomaly() bool {
	return r.UnattributableEvents > 0 ||
		r.HasUnexplainedMissingUsage() ||
		r.InvalidUsageEvents > 0 ||
		r.InvalidServingModeEvents > 0 ||
		r.OwnerConflictEvents > 0 ||
		r.AmbiguousBaseEvents > 0 ||
		r.AmbiguousOrgEvents > 0
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

	ar, err := a.store.AggregateWindow(ctx, windowStart, windowEnd)
	if err != nil {
		return res, err
	}
	res.AggregateResult = ar

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
		a.log.Error.Printf("rating: window %s has %d OWNER-CONFLICT events (both X-Saturn-User-Id and X-Saturn-Group-Id); an identity is a user XOR a group, so the owner cannot be determined and they are not sent. The raw events stay in billing_event. Fix the header producer and re-run this window", w, ar.OwnerConflictEvents)
	}
	if ar.EmptyBaseModelEvents > 0 {
		// WARN, not ERROR, and not withheld: a dedicated endpoint may serve a model
		// with no catalog base (D3). The manager prices dedicated + empty base at
		// the plan's default rate and reports shared + empty base as unpriced.
		a.log.Warn.Printf("rating: window %s aggregated %d events with no base_model (sent as \"\"); the manager prices dedicated ones at the plan's default dedicated rate and reports shared ones as unpriced. Expected for a dedicated endpoint serving a model outside the catalog; on a shared model it means X-Saturn-Base-Model is not propagating", w, ar.EmptyBaseModelEvents)
	}
	if ar.AmbiguousBaseEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d events under AMBIGUOUS-BASE identities (one auth/owner/resource/model/serving mode in one hour named more than one base_model or adapter, or a value on some events and none on others); a record carries one of each and the manager prices by them, so these are not sent. Fix base_model/adapter propagation and re-run this window", w, ar.AmbiguousBaseEvents)
	}
	if ar.AmbiguousOrgEvents > 0 {
		a.log.Error.Printf("rating: window %s has %d events under AMBIGUOUS-ORG identities (more than one non-NULL org_id); the org pays, so a guess would bill the wrong customer and these are not sent. Fix X-Saturn-Org-Id propagation and re-run this window", w, ar.AmbiguousOrgEvents)
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

	a.log.Info.Printf("rating: window %s aggregated %d events into %d rollups", w, ar.EventsAggregated, ar.RollupsWritten)
	return res, nil
}
