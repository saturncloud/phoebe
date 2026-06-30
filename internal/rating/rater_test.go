package rating

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// TestResult_HasAmbiguousOrgDrivesAnomaly locks the exit-2 contract for the new E2
// attribution anomaly by name (mirroring the HasAmbiguousBase/HasUnpriced predicate
// assertions): a Result carrying ambiguous-org events must report HasAmbiguousOrg AND
// HasAnomaly (so cmd/rater exits non-zero and the CronJob alerts), and a clean Result
// must report neither. This pins the HasAmbiguousOrg disjunct in HasAnomaly() — a
// refactor that drops it would let an ambiguous-org-only window exit 0 (mis-attributable
// metered usage passing silently), which this test turns RED. The org anomaly is
// otherwise reachable only via live Postgres (the oracle store can't model org), so this
// pure-Go predicate test is the cheapest guard on the loud-path wiring.
func TestResult_HasAmbiguousOrgDrivesAnomaly(t *testing.T) {
	ambiguous := Result{AmbiguousOrgEvents: 1}
	if !ambiguous.HasAmbiguousOrg() {
		t.Fatal("HasAmbiguousOrg() = false with AmbiguousOrgEvents=1, want true")
	}
	if !ambiguous.HasAnomaly() {
		t.Fatal("HasAnomaly() = false for an ambiguous-org Result, want true (must drive exit-nonzero)")
	}
	clean := Result{}
	if clean.HasAmbiguousOrg() {
		t.Fatal("HasAmbiguousOrg() = true for a zero Result, want false")
	}
	if clean.HasAnomaly() {
		t.Fatal("HasAnomaly() = true for a zero Result, want false (no anomaly, no exit-nonzero)")
	}
}

// oracleStore is an in-memory Store that models EXACTLY what the SQL rater does,
// using the production PriceBook (PriceBook.Resolve) + the Rate() oracle. It exists
// so the Rater orchestration AND the money rules can be exercised without Postgres,
// and so the conformance test has a faithful reference of the SQL's resolve→cost→sum.
//
// It models rated_usage as a map keyed by the natural key, REPLACING on upsert
// (mirroring ON CONFLICT DO UPDATE), so idempotency is observable. Like the
// production statement, RateWindow returns the anomaly counts from the SAME resolve
// pass as the rollups (one snapshot). The book passed to RateWindow is ignored in
// favour of the store's own (they are the same book the Rater holds).
type oracleStore struct {
	book   *PriceBook
	events []RatedEvent

	table     map[rollupKey]oracleRollup // natural key → row
	rateCalls int
}

type rollupKey struct {
	authID      string
	resourceID  string
	modelID     string
	windowStart time.Time
}

type oracleRollup struct {
	prompt, cached, completion, billable int64
	cost                                 Dec
	appliedRate                          Rate3
	eventCount                           int
	// derivedBases is the set of distinct base_models the events in this rollup priced
	// THROUGH the derived path (an ft: id not directly in the file). >1 → the E3
	// ft-uniqueness violation: the rollup is ambiguous and must NOT be billed. Mirrors
	// the SQL's COUNT(DISTINCT base_model) FILTER (WHERE via_derived).
	derivedBases map[string]struct{}
}

func newOracleStore(book *PriceBook, events []RatedEvent) *oracleStore {
	return &oracleStore{book: book, events: events, table: map[rollupKey]oracleRollup{}}
}

func (s *oracleStore) Ping(_ context.Context) error { return nil }
func (s *oracleStore) Close() error                 { return nil }

// resolveWindow walks the events in [start,end) exactly as the SQL CTE does:
// unattributable (empty auth/model) and unpriced (no resolvable rate) are counted
// and EXCLUDED; the rest are priced via Rate() and summed, with the applied rate
// frozen onto the rollup.
func (s *oracleStore) resolveWindow(start, end time.Time) (map[rollupKey]oracleRollup, Anomalies) {
	out := map[rollupKey]oracleRollup{}
	var an Anomalies
	for _, e := range s.events {
		if e.At.Before(start) || !e.At.Before(end) {
			continue
		}
		// EMPTY-STRING MODELS A NULL IDENTITY COLUMN. The Go oracle has no separate
		// NULL, so "" stands in for a NULL auth_id/resource_id/model_id; production SQL
		// instead filters `... IS NULL`. The two agree ONLY because of two production
		// guarantees that ensure a literal '' never reaches billing_event: the drainer's
		// nullStr maps ''→NULL on write (internal/drain/store.go) and the proxy billing
		// gate fails closed on an empty ResourceID before metering. With '' impossible in
		// the column, `== ""` here faithfully mirrors SQL's `IS NULL`, so the oracle's
		// unattributable partition matches the rater's `resource_id IS NULL` count. Do not
		// "fix" this to also test '' — '' is out of the modeled domain by construction.
		if e.AuthID == "" || e.ResourceID == "" || e.ModelID == "" {
			an.UnattributableEvents++
			continue
		}
		resolved, err := s.book.ResolveEvent(e.ModelID, e.BaseModel)
		if err != nil {
			an.UnpricedEvents++ // ErrNoPrice: never $0-billed
			continue
		}
		// The rate that ACTUALLY bills is the 9dp-quantized rate — the one projected
		// into the SQL price table (NUMERIC(20,9)) and frozen onto the row. Cost is
		// computed from THAT rate, so the row's cost is reconstructable from its
		// applied rate (self-auditing). Mirrors the production SQL exactly.
		rate := resolved.Quantized()
		cost := rateExact(e, rate)
		hour := e.At.UTC().Truncate(time.Hour)
		k := rollupKey{authID: e.AuthID, resourceID: e.ResourceID, modelID: e.ModelID, windowStart: hour}
		ru := out[k]
		ru.prompt += e.PromptTokens
		ru.cached += e.CachedTokens
		ru.completion += e.CompletionTokens
		ru.billable += BillablePromptTokens(e.PromptTokens, e.CachedTokens)
		ru.cost = ru.cost.Add(cost)
		ru.appliedRate = rate // single-model rollup → one applied rate
		ru.eventCount++
		// Track the base_model of any DERIVED-priced event (an ft: id not directly in
		// the file). >1 distinct → ambiguous (E3 ft-uniqueness violation). Mirrors the
		// SQL via_derived FILTER.
		if _, direct := s.book.Resolve(e.ModelID); direct != nil && e.BaseModel != "" {
			if ru.derivedBases == nil {
				ru.derivedBases = map[string]struct{}{}
			}
			ru.derivedBases[e.BaseModel] = struct{}{}
		}
		out[k] = ru
	}
	// Split out AMBIGUOUS-BASE rollups: a single ft: model_id that resolved through more
	// than one base_model in the window is NOT billed (a blind MIN()-rate would silently
	// under-charge) — its events are counted as the ambiguous-base anomaly and the rollup
	// is dropped. Mirrors store.go's grouped→priced split.
	for k, ru := range out {
		if len(ru.derivedBases) > 1 {
			an.AmbiguousBaseEvents += int64(ru.eventCount)
			delete(out, k)
			continue
		}
		ru.cost = ru.cost.Round(moneyScale)
		out[k] = ru
	}
	return out, an
}

func (s *oracleStore) RateWindow(_ context.Context, _ *PriceBook, start, end time.Time) (RateResult, error) {
	s.rateCalls++
	start, end = start.UTC(), end.UTC()
	rollups, an := s.resolveWindow(start, end)

	// RECONCILE (mirror store.go's `deleted` CTE): DELETE any in-window rollup this run
	// did NOT reproduce in priced — it billed in a prior run but fell out (became
	// ambiguous/unpriced, or its events vanished). "What the latest run says is what
	// bills." The window predicate is the same half-open [start,end) on window_start.
	var deletions int64
	for k := range s.table {
		if k.windowStart.Before(start) || !k.windowStart.Before(end) {
			continue // out of this run's window — untouched
		}
		if _, survives := rollups[k]; !survives {
			delete(s.table, k)
			deletions++
		}
	}

	total := Dec{}
	var rated int64
	for k, ru := range rollups {
		s.table[k] = ru // REPLACE — ON CONFLICT DO UPDATE
		total = total.Add(ru.cost)
		rated += int64(ru.eventCount)
	}
	res := RateResult{
		RollupsWritten:       int64(len(rollups)),
		EventsRated:          rated,
		ReconciledDeletions:  deletions,
		UnpricedEvents:       an.UnpricedEvents,
		UnattributableEvents: an.UnattributableEvents,
		AmbiguousBaseEvents:  an.AmbiguousBaseEvents,
		// The oracle does NOT model org attribution: RatedEvent has no OrgID (org doesn't
		// affect the money rules the oracle exists to mirror — it's carried, not priced).
		// So AmbiguousOrgEvents is always 0 here, set EXPLICITLY (not left as a zero-value
		// gap) so this struct is field-complete vs the production RateResult and the
		// partition sum is honest. The ambiguous_org / partial-NULL SQL behaviors are
		// covered by the live-Postgres TestIntegration_AmbiguousOrgFailsLoud instead.
		AmbiguousOrgEvents: 0,
	}
	if len(rollups) > 0 {
		res.TotalCost = total.String()
	} else {
		res.TotalCost = "0.000000000"
	}
	return res, nil
}

func testLogger() *logging.Logger { return logging.New(logging.ERROR) }

// TestRater_MultiEventAggregation: many events for one (auth,model,hour) sum into
// ONE rollup with summed tokens and summed cost — the aggregation grain.
func TestRater_MultiEventAggregation(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 10, CachedTokens: 0, CompletionTokens: 5, At: at.Add(20 * time.Minute)},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())

	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.EventsRated != 2 || res.RollupsWritten != 1 {
		t.Fatalf("rated=%d rollups=%d, want 2 and 1", res.EventsRated, res.RollupsWritten)
	}
	// event1: 70*0.000003 + 30*0.0000003 + 50*0.00001 = 0.000719
	// event2: 10*0.000003 + 0 + 5*0.00001 = 0.00008
	// total = 0.000799
	if res.TotalCost != "0.000799000" {
		t.Fatalf("total = %s, want 0.000799000", res.TotalCost)
	}
	k := rollupKey{authID: "a", resourceID: "r", modelID: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
	ru := store.table[k]
	if ru.prompt != 110 || ru.cached != 30 || ru.completion != 55 || ru.billable != 80 {
		t.Fatalf("token sums = p%d c%d comp%d bill%d, want 110/30/55/80", ru.prompt, ru.cached, ru.completion, ru.billable)
	}
	if ru.cost.String() != "0.000799000" || ru.eventCount != 2 {
		t.Fatalf("rollup cost=%s count=%d, want 0.000799000/2", ru.cost, ru.eventCount)
	}
}

// TestRater_IdempotentRerunNoDoubling (idempotent-rerun): rating the SAME window
// twice produces identical totals — no doubling.
func TestRater_IdempotentRerunNoDoubling(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")

	res1, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if res1.TotalCost != res2.TotalCost {
		t.Fatalf("re-run total changed: %s → %s (must be identical)", res1.TotalCost, res2.TotalCost)
	}
	if len(store.table) != 1 {
		t.Fatalf("table has %d rows after re-run, want 1 (no duplication)", len(store.table))
	}
	if store.rateCalls != 2 {
		t.Fatalf("RateWindow called %d times, want 2", store.rateCalls)
	}
}

// TestRater_ReRateDeletesSupersededRollup (re-rate-reconciles-deletes-superseded):
// FIX 2 — re-rate RECONCILES, not upsert-only (Hugo's decision). Run A bills an ft:
// rollup CLEAN (one base in the window). Then data mutates so run B makes that same ft:
// id AMBIGUOUS (a second, distinct base_model arrives in the window) — it falls out of
// priced. The stale rollup from run A must be DELETED by run B, not left billing at its
// old cost. "What the latest run says is what bills."
func TestRater_ReRateDeletesSupersededRollup(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	at := mustTime("2026-06-08T10:15:00Z")
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")

	// Run A: a single-base ft: rollup — CLEAN, bills.
	store := newOracleStore(book, []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "ft:dupe", BaseModel: "cheap/base", PromptTokens: 1000, At: at},
	})
	r := New(store, book, testLogger())
	resA, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if resA.RollupsWritten != 1 || resA.ReconciledDeletions != 0 {
		t.Fatalf("run A: rollups=%d deletions=%d, want 1/0", resA.RollupsWritten, resA.ReconciledDeletions)
	}
	dupeKey := rollupKey{authID: "a", resourceID: "r", modelID: "ft:dupe", windowStart: ws}
	if _, ok := store.table[dupeKey]; !ok {
		t.Fatal("run A: clean ft:dupe rollup must exist before re-rate")
	}

	// Mutate data: a SECOND, distinct base_model arrives for the SAME ft: id in the same
	// window → ambiguous. Run B excludes it from priced.
	store.events = append(store.events,
		RatedEvent{AuthID: "a", ResourceID: "r", ModelID: "ft:dupe", BaseModel: "expensive/base", PromptTokens: 1000, At: at.Add(5 * time.Minute)})
	resB, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}

	// THE INVARIANT: the stale ft:dupe rollup is GONE, not stale-billing at its run-A cost.
	if _, ok := store.table[dupeKey]; ok {
		t.Fatal("run B: ft:dupe became ambiguous but its stale rollup SURVIVED — upsert-only leaves it billing forever (reconcile must delete it)")
	}
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("run B: reconciled deletions = %d, want 1 (the superseded rollup)", resB.ReconciledDeletions)
	}
	if !resB.HasAmbiguousBase() {
		t.Fatal("run B: the two-base ft:dupe must be flagged ambiguous")
	}
	if len(store.table) != 0 {
		t.Fatalf("run B: table has %d rows, want 0 (the only rollup was superseded)", len(store.table))
	}
}

// TestRater_ReRateSupersedesOneKeepsAnotherSameWindow: in ONE window, a re-rate must
// DELETE a superseded rollup while UPDATING a surviving one — the delete-set and
// upsert-set are disjoint, so the reconcile must touch only the superseded key. Guards
// the SQL's two modifying CTEs (deleted + upserted) against clobbering a co-window row.
func TestRater_ReRateSupersedesOneKeepsAnotherSameWindow(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	at := mustTime("2026-06-08T10:15:00Z")
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")
	// Run A: TWO clean single-base ft: rollups (ft:keep and ft:gone).
	store := newOracleStore(book, []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "ft:keep", BaseModel: "cheap/base", PromptTokens: 1000, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "ft:gone", BaseModel: "cheap/base", PromptTokens: 1000, At: at},
	})
	r := New(store, book, testLogger())
	if _, err := r.Run(context.Background(), ws, we, false); err != nil {
		t.Fatal(err)
	}
	if len(store.table) != 2 {
		t.Fatalf("run A: table=%d, want 2", len(store.table))
	}
	// Run B: ft:gone gains a second base (ambiguous → superseded); ft:keep unchanged.
	store.events = append(store.events,
		RatedEvent{AuthID: "a", ResourceID: "r", ModelID: "ft:gone", BaseModel: "expensive/base", PromptTokens: 1000, At: at.Add(5 * time.Minute)})
	resB, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("run B: deletions = %d, want 1 (only ft:gone superseded)", resB.ReconciledDeletions)
	}
	if _, gone := store.table[rollupKey{authID: "a", resourceID: "r", modelID: "ft:gone", windowStart: ws}]; gone {
		t.Fatal("ft:gone rollup survived — it became ambiguous and must be deleted")
	}
	if _, keep := store.table[rollupKey{authID: "a", resourceID: "r", modelID: "ft:keep", windowStart: ws}]; !keep {
		t.Fatal("ft:keep rollup was deleted — a surviving co-window rollup must be UPDATED, not clobbered")
	}
	if len(store.table) != 1 {
		t.Fatalf("run B: table=%d, want 1 (ft:keep only)", len(store.table))
	}
}

// TestRater_ReRateDeletesVanishedRollup: the other supersede path — a rollup whose
// events vanish entirely on re-rate (e.g. an event was deleted/corrected upstream) is
// also DELETED, not left billing. Confirms the reconcile keys on "not reproduced by
// this run," not specifically on ambiguity.
func TestRater_ReRateDeletesVanishedRollup(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")
	store := newOracleStore(bookM(), []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},
	})
	r := New(store, bookM(), testLogger())
	if _, err := r.Run(context.Background(), ws, we, false); err != nil {
		t.Fatal(err)
	}
	if len(store.table) != 1 {
		t.Fatalf("run A: table=%d, want 1", len(store.table))
	}
	// All events for the window vanish.
	store.events = nil
	resB, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if resB.ReconciledDeletions != 1 || len(store.table) != 0 {
		t.Fatalf("run B: deletions=%d table=%d, want 1/0 (vanished rollup must be deleted)", resB.ReconciledDeletions, len(store.table))
	}
}

// TestRater_ReRateIdenticalDataIsNoOp (re-rate-identical-data-no-op): the idempotency
// floor of the reconcile. Re-running with IDENTICAL data deletes NOTHING (every prior
// rollup is reproduced in priced) and upserts the same rows — the reconcile must not
// spuriously delete-then-reinsert or churn. Also guards a SECOND window's rollup from
// being touched by a re-rate scoped to the FIRST window.
func TestRater_ReRateIdenticalDataIsNoOp(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")
	store := newOracleStore(bookM(), []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "b", ResourceID: "r", ModelID: "m", PromptTokens: 200, At: at.Add(10 * time.Minute)},
	})
	r := New(store, bookM(), testLogger())

	res1, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if res1.ReconciledDeletions != 0 || res2.ReconciledDeletions != 0 {
		t.Fatalf("identical re-run deleted rows: A=%d B=%d, want 0/0 (no spurious deletes)", res1.ReconciledDeletions, res2.ReconciledDeletions)
	}
	if res1.TotalCost != res2.TotalCost {
		t.Fatalf("identical re-run total changed: %s → %s", res1.TotalCost, res2.TotalCost)
	}
	if len(store.table) != 2 {
		t.Fatalf("table has %d rows after identical re-run, want 2 (no churn)", len(store.table))
	}

	// A rollup in a DIFFERENT (adjacent) window must NOT be touched by a re-rate scoped to
	// [ws,we): the reconcile's window predicate is half-open and hour-scoped.
	otherHour := mustTime("2026-06-08T12:00:00Z")
	store.events = append(store.events,
		RatedEvent{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 5, At: otherHour.Add(5 * time.Minute)})
	if _, err := r.Run(context.Background(), otherHour, otherHour.Add(time.Hour), false); err != nil {
		t.Fatal(err)
	}
	// Now re-rate ONLY the first window again: the 12:00 rollup must survive untouched.
	resFirst, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if resFirst.ReconciledDeletions != 0 {
		t.Fatalf("re-rate of [10:00,11:00) deleted %d rows, want 0 (must not touch the 12:00 rollup)", resFirst.ReconciledDeletions)
	}
	otherKey := rollupKey{authID: "a", resourceID: "r", modelID: "m", windowStart: otherHour}
	if _, ok := store.table[otherKey]; !ok {
		t.Fatal("a re-rate of the first window deleted an out-of-window (12:00) rollup — the window predicate leaked")
	}
}

// captureLogger builds a Logger whose INFO and ERROR streams are redirected to the
// returned buffers, so a test can assert WHICH severity a reconcile-delete fired at.
// It starts at DEBUG so neither stream is discarded by the level filter — the test
// is asserting the call site's chosen level, not the logger's threshold.
func captureLogger() (*logging.Logger, *bytes.Buffer, *bytes.Buffer) {
	log := logging.New(logging.DEBUG)
	var infoBuf, errBuf bytes.Buffer
	log.Info.SetOutput(&infoBuf)
	log.Error.SetOutput(&errBuf)
	return log, &infoBuf, &errBuf
}

// supersededReconcileStore returns an oracleStore whose first Run produces ONE clean
// rollup and whose second Run (after the events vanish) reconcile-DELETES it with NO
// other anomaly — the minimal reconcile-delete (HasAnomaly stays false), so a test can
// attribute the resulting log line purely to the reconcile path, not the anomaly path.
func supersededReconcileStore() (*oracleStore, time.Time, time.Time) {
	at := mustTime("2026-06-08T10:15:00Z")
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")
	store := newOracleStore(bookM(), []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},
	})
	return store, ws, we
}

// TestRater_RoutineReconcileDeleteLogsError pins the LOUD half of the reconcile
// observability contract (option (c)): a ROUTINE run (windowExplicit == false) that
// reconcile-DELETES a previously-billed rollup must emit an ERROR line (page someone),
// even with NO other anomaly. It uses captureLogger() (which buffers BOTH the INFO and
// ERROR streams, so the assertion is on WHICH severity the line was emitted at — not on
// a level filter discarding it). RED before FIX 1: the old code logged the
// reconcile-delete UNCONDITIONALLY at INFO, so the captured ERROR stream stayed empty
// and the errBuf.Len()==0 assertion failed — the page never fired on a routine bill
// rewrite.
func TestRater_RoutineReconcileDeleteLogsError(t *testing.T) {
	store, ws, we := supersededReconcileStore()
	log, infoBuf, errBuf := captureLogger()
	r := New(store, bookM(), log)

	// Run A: one clean rollup, no reconcile-delete.
	if _, err := r.Run(context.Background(), ws, we, false); err != nil {
		t.Fatal(err)
	}
	infoBuf.Reset()
	errBuf.Reset()

	// Run B (routine): the events vanish → the rollup is reconcile-deleted with no anomaly.
	store.events = nil
	resB, err := r.Run(context.Background(), ws, we, false)
	if err != nil {
		t.Fatal(err)
	}
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("setup: deletions=%d, want 1", resB.ReconciledDeletions)
	}
	if resB.HasAnomaly() {
		t.Fatalf("setup: a clean reconcile-delete must NOT flip HasAnomaly (got %+v) — else the ERROR could come from the anomaly path, not the reconcile path", resB)
	}
	// THE INVARIANT: a routine reconcile-delete fires an ERROR (the page).
	if errBuf.Len() == 0 {
		t.Fatal("routine reconcile-delete emitted NO ERROR line — a prior bill was rewritten with no operator behind it (data loss / upstream regression); it MUST log at ERROR so a CronJob pages")
	}
	if !strings.Contains(errBuf.String(), "ROUTINE") || !strings.Contains(errBuf.String(), "reconcile") {
		t.Fatalf("routine reconcile-delete ERROR line lacks the routine-rewrite wording: %q", errBuf.String())
	}
	// The deletion count and window must be present in the loud line.
	if !strings.Contains(errBuf.String(), "[2026-06-08T10:00:00Z,2026-06-08T11:00:00Z)") {
		t.Fatalf("ERROR line is missing the window: %q", errBuf.String())
	}
}

// TestRater_BackfillReconcileDeleteLogsInfoNoError pins the QUIET half: an EXPLICIT
// operator backfill (windowExplicit == true) that reconcile-deletes the SAME rollup is
// intended convergence — it logs at INFO and emits NO ERROR (no page). The flag flips
// ONLY the reconcile-delete severity; identical data, identical reconcile, opposite level.
func TestRater_BackfillReconcileDeleteLogsInfoNoError(t *testing.T) {
	store, ws, we := supersededReconcileStore()
	log, infoBuf, errBuf := captureLogger()
	r := New(store, bookM(), log)

	// Run A (explicit backfill): one clean rollup.
	if _, err := r.Run(context.Background(), ws, we, true); err != nil {
		t.Fatal(err)
	}
	infoBuf.Reset()
	errBuf.Reset()

	// Run B (explicit backfill): the events vanish → reconcile-delete, but operator-chosen.
	store.events = nil
	resB, err := r.Run(context.Background(), ws, we, true)
	if err != nil {
		t.Fatal(err)
	}
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("setup: deletions=%d, want 1", resB.ReconciledDeletions)
	}
	// THE INVARIANT: a backfill reconcile-delete is INFO, never ERROR.
	if errBuf.Len() != 0 {
		t.Fatalf("explicit backfill reconcile-delete emitted an ERROR line (it must be INFO — the operator asked for this convergence): %q", errBuf.String())
	}
	if !strings.Contains(infoBuf.String(), "reconcile DELETED") {
		t.Fatalf("explicit backfill reconcile-delete did not log the convergence INFO line: %q", infoBuf.String())
	}
	if !strings.Contains(infoBuf.String(), "[2026-06-08T10:00:00Z,2026-06-08T11:00:00Z)") {
		t.Fatalf("INFO line is missing the window: %q", infoBuf.String())
	}
}

// TestRater_LateArrivalRatedByTrailingWindow: an event whose event_ts falls in hour
// H but which is DRAINED only after H was rated is picked up by a later run whose
// window still covers H — the trailing-window contract. The re-rate REPLACES H's
// bucket (never doubles).
func TestRater_LateArrivalRatedByTrailingWindow(t *testing.T) {
	hourH := mustTime("2026-06-08T10:00:00Z")
	store := newOracleStore(bookM(), nil)
	r := New(store, bookM(), testLogger())

	res1, err := r.Run(context.Background(), hourH, hourH.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if res1.EventsRated != 0 || res1.RollupsWritten != 0 {
		t.Fatalf("run1 rated=%d rollups=%d, want 0/0 (event not drained yet)", res1.EventsRated, res1.RollupsWritten)
	}

	store.events = append(store.events, RatedEvent{
		AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: hourH.Add(15 * time.Minute),
	})

	res2, err := r.Run(context.Background(), hourH.Add(-23*time.Hour), hourH.Add(2*time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if res2.EventsRated != 1 || res2.RollupsWritten != 1 {
		t.Fatalf("run2 rated=%d rollups=%d, want 1/1 (late event caught by trailing window)", res2.EventsRated, res2.RollupsWritten)
	}
	k := rollupKey{authID: "a", resourceID: "r", modelID: "m", windowStart: hourH}
	if _, ok := store.table[k]; !ok {
		t.Fatal("no rollup for hour H — the late-drained event was lost")
	}
}

// TestRater_MissingPriceFailsLoudNotZero (missing-price-fails-loud-not-zero): an
// event for a model absent from the price file is counted unpriced and EXCLUDED from
// the rollups — never a $0 row.
func TestRater_MissingPriceFailsLoudNotZero(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},        // priced
		{AuthID: "a", ResourceID: "r", ModelID: "unpriced", PromptTokens: 100, CompletionTokens: 50, At: at}, // NO price
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnpricedEvents != 1 {
		t.Fatalf("unpriced = %d, want 1", res.UnpricedEvents)
	}
	if res.EventsRated != 1 || res.RollupsWritten != 1 || len(store.table) != 1 {
		t.Fatalf("rated=%d rollups=%d table=%d, want 1/1/1 (no $0 row for unpriced)", res.EventsRated, res.RollupsWritten, len(store.table))
	}
	if !res.HasUnpriced() {
		t.Fatal("HasUnpriced() = false, want true (lost-revenue signal)")
	}
	for k := range store.table {
		if k.modelID == "unpriced" {
			t.Fatal("a rollup exists for the unpriced model — must NOT be $0-billed")
		}
	}
}

// TestRater_UnattributableCountedNotSilent: rows with NULL auth_id, NULL resource_id,
// and/or NULL model_id must NOT be rated, MUST be counted, and MUST trigger the loud
// anomaly / exit-2 path. resource_id joins the key columns (E2): a row that can't name
// its deployment/org is unattributable exactly like a NULL auth_id/model_id.
func TestRater_UnattributableCountedNotSilent(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at}, // ok
		{AuthID: "", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL auth_id
		{AuthID: "a", ResourceID: "", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL resource_id
		{AuthID: "a", ResourceID: "r", ModelID: "", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL model_id
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnattributableEvents != 3 {
		t.Fatalf("unattributable = %d, want 3 (NULL auth_id, NULL resource_id, NULL model_id)", res.UnattributableEvents)
	}
	if res.EventsRated != 1 || res.RollupsWritten != 1 || len(store.table) != 1 {
		t.Fatalf("rated=%d rollups=%d table=%d, want 1/1/1", res.EventsRated, res.RollupsWritten, len(store.table))
	}
	if !res.HasUnattributable() || !res.HasAnomaly() {
		t.Fatal("HasUnattributable/HasAnomaly = false, want true (drives exit 2)")
	}
}

// TestRater_DistinctDeploymentsBillSeparately (E2 grain — the negative invariant):
// TWO deployments (distinct resource_id) of the SAME model by the SAME auth in the
// SAME hour produce TWO distinct rated_usage rows, NOT one summed row. resource_id is
// part of the grain precisely because the two deployments may bill to different orgs
// (resource_id→org_id), so collapsing them would mis-attribute revenue. The two rows
// share (auth_id, model_id, window_start) and differ ONLY in resource_id.
func TestRater_DistinctDeploymentsBillSeparately(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	ws := mustTime("2026-06-08T10:00:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "deploy-1", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},
		{AuthID: "a", ResourceID: "deploy-2", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at.Add(5 * time.Minute)},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
	res, err := r.Run(context.Background(), ws, mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	// TWO rollups, one per deployment — NOT one summed row.
	if res.EventsRated != 2 || res.RollupsWritten != 2 || len(store.table) != 2 {
		t.Fatalf("rated=%d rollups=%d table=%d, want 2/2/2 (distinct resource_id must NOT collapse into one row)",
			res.EventsRated, res.RollupsWritten, len(store.table))
	}
	// Each deployment has its OWN row under the same (auth, model, hour).
	for _, rid := range []string{"deploy-1", "deploy-2"} {
		k := rollupKey{authID: "a", resourceID: rid, modelID: "m", windowStart: ws}
		ru, ok := store.table[k]
		if !ok {
			t.Fatalf("no rollup for deployment %q — distinct deployments must each bill", rid)
		}
		if ru.eventCount != 1 {
			t.Fatalf("deployment %q event_count = %d, want 1 (one event per deployment)", rid, ru.eventCount)
		}
	}
}

// TestRater_NullResourceIdIsUnattributable (fail-closed attribution): an event with a
// NULL resource_id CANNOT name its deployment/org (E2 resolves the org via
// resource_id→org_id), so it must be counted UNATTRIBUTABLE and EXCLUDED from billing —
// never $0-billed, never billed to a NULL org. It pins the partition invariant with
// resource_id in the mix: rated + unpriced + unattributable + ambiguous_base +
// ambiguous_org == total (the oracle sets ambiguous_org to 0; it's a true partition cell).
func TestRater_NullResourceIdIsUnattributable(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at}, // attributable
		{AuthID: "a", ResourceID: "", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL resource_id → unattributable
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnattributableEvents != 1 {
		t.Fatalf("unattributable = %d, want 1 (the NULL-resource_id event must be counted, never billed)", res.UnattributableEvents)
	}
	if res.EventsRated != 1 || res.RollupsWritten != 1 || len(store.table) != 1 {
		t.Fatalf("rated=%d rollups=%d table=%d, want 1/1/1 (no row for the unattributable event)", res.EventsRated, res.RollupsWritten, len(store.table))
	}
	// No rollup carries an empty resource_id — the fail-closed row is never written.
	for k := range store.table {
		if k.resourceID == "" {
			t.Fatal("a rollup exists with an empty resource_id — a row that can't name its deployment/org must NEVER be billed")
		}
	}
	if !res.HasUnattributable() || !res.HasAnomaly() {
		t.Fatal("HasUnattributable/HasAnomaly = false, want true (NULL resource_id must drive exit-nonzero)")
	}
	// PARTITION with resource_id in the mix.
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents + res.AmbiguousBaseEvents + res.AmbiguousOrgEvents; got != int64(len(events)) {
		t.Fatalf("rated(%d)+unpriced(%d)+unattr(%d)+ambiguous(%d) = %d, want %d",
			res.EventsRated, res.UnpricedEvents, res.UnattributableEvents, res.AmbiguousBaseEvents, got, len(events))
	}
}

// TestRater_DerivedFineTuneRatedViaPolicy: end-to-end, a fine-tune (derived_from a
// base) is rated at base × premium — exercising derivation in the run.
func TestRater_DerivedFineTuneRatedViaPolicy(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"base": rate3("0.000004", "0", "0")},
		map[string]string{"ft": "base"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "ft", PromptTokens: 1000, At: mustTime("2026-06-08T10:15:00Z")},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	// 1000 * (0.000004 * 1.5) = 0.006
	if res.TotalCost != "0.006000000" {
		t.Fatalf("total = %s, want 0.006000000 (base×1.5 fine-tune)", res.TotalCost)
	}
}

// TestRater_FineTunePricesViaBaseModelOnEvent (fine-tune-prices-via-base-model-on-event):
// a real fine-tune event — an ft:<checkpoint> model_id the file never names, carrying
// its base_model (E3, stamped by Atlas at deploy) — prices at base × premium by
// resolving through the event's base_model. This is the primary production fine-tune
// path: the file declares only the BASE; the ft: checkpoint id is minted per deployment.
func TestRater_FineTunePricesViaBaseModelOnEvent(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"meta-llama/Llama-3.1-8B-Instruct": rate3("0.000004", "0", "0")},
		nil, // NO in-file fine-tune linkage — the ft: id is unknown at file-authoring
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	events := []RatedEvent{{
		AuthID: "a", ResourceID: "r",
		ModelID:      "ft:9f8e7d6c5b4a", // a checkpoint id the file does not list
		BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
		PromptTokens: 1000,
		At:           mustTime("2026-06-08T10:15:00Z"),
	}}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnpricedEvents != 0 {
		t.Fatalf("unpriced = %d, want 0 (the ft: event prices via its base_model)", res.UnpricedEvents)
	}
	// 1000 * (0.000004 * 1.5) = 0.006 — the fine-tune billed at base × premium.
	if res.EventsRated != 1 || res.RollupsWritten != 1 || res.TotalCost != "0.006000000" {
		t.Fatalf("result = %+v, want 1 event / 1 rollup / 0.006000000 (base×1.5 via base_model)", res)
	}
	// The applied rate frozen on the row is the derived (base × premium) rate.
	hour := mustTime("2026-06-08T10:00:00Z")
	row := store.table[rollupKey{"a", "r", "ft:9f8e7d6c5b4a", hour}]
	if row.appliedRate.Prompt.String() != "0.000006000" {
		t.Fatalf("ft applied prompt rate = %s, want 0.000006000 (0.000004 × 1.5)", row.appliedRate.Prompt)
	}
}

// TestRater_FineTuneWithoutBaseModelFailsLoud (fine-tune-without-base-model-fails-loud):
// the fail-closed invariant. An ft:<checkpoint> model_id with an EMPTY base_model is a
// PROPAGATION BUG (Atlas guarantees base_model is present to deploy a fine-tune), NOT a
// free model. It must be counted UNPRICED (ErrNoPrice) and EXCLUDED from the rollups —
// never silently mis-priced or $0-billed. A silent $0 here would lose all fine-tune
// revenue the moment the base_model header stopped propagating.
func TestRater_FineTuneWithoutBaseModelFailsLoud(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"meta-llama/Llama-3.1-8B-Instruct": rate3("0.000004", "0", "0")},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	events := []RatedEvent{{
		AuthID: "a", ResourceID: "r",
		ModelID:      "ft:9f8e7d6c5b4a",
		BaseModel:    "", // THE BUG: base_model never propagated to the event
		PromptTokens: 1000,
		At:           mustTime("2026-06-08T10:15:00Z"),
	}}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.UnpricedEvents != 1 {
		t.Fatalf("unpriced = %d, want 1 (ft: with empty base_model must scream)", res.UnpricedEvents)
	}
	if res.EventsRated != 0 || res.RollupsWritten != 0 || len(store.table) != 0 {
		t.Fatalf("result = %+v, table=%d — an ft: with no base_model must NEVER be rated or $0-billed", res, len(store.table))
	}
	if !res.HasUnpriced() || !res.HasAnomaly() {
		t.Fatal("HasUnpriced/HasAnomaly = false, want true (the propagation bug must drive exit-nonzero)")
	}
}

// TestRater_FineTuneAmbiguousBaseModelFailsLoud (fine-tune-ambiguous-base-model-fails-loud):
// the E3 ft-uniqueness invariant (FIX 2). E3 mints ft:<checkpoint_artifact_id> as a
// globally-unique uuid4, so a single ft: model_id can NEVER legitimately carry two
// different base_models. If it does (a base_model propagation bug), the derived rates
// differ and a blind MIN()-applied-rate would silently bill the rollup at the CHEAPER
// base — under-billing, counted as rated. The invariant converts that into a SCREAM:
// the ambiguous rollup is NOT billed, its events are counted ambiguous, and the run
// fails loud. (The same ft: id with the SAME base across events is fine — one rollup.)
func TestRater_FineTuneAmbiguousBaseModelFailsLoud(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		// SAME ft: model_id, TWO different base_models in the same window → ambiguous.
		{AuthID: "a", ResourceID: "r", ModelID: "ft:dupe", BaseModel: "cheap/base", PromptTokens: 1000, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "ft:dupe", BaseModel: "expensive/base", PromptTokens: 1000, At: at.Add(5 * time.Minute)},
		// THE LEGITIMATE CASE the gate must NOT trip: the same ft: id with the SAME base
		// across MULTIPLE events is one clean rollup (COUNT(DISTINCT base_model)=1), and
		// MUST still rate normally alongside the ambiguous one. Two events prove the gate
		// keys on DISTINCT bases, not on event count.
		{AuthID: "a", ResourceID: "r", ModelID: "ft:clean", BaseModel: "cheap/base", PromptTokens: 1000, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "ft:clean", BaseModel: "cheap/base", PromptTokens: 1000, At: at.Add(7 * time.Minute)},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false)
	if err != nil {
		t.Fatal(err)
	}

	// The two ambiguous events are counted, NOT rated, NOT $0-billed.
	if res.AmbiguousBaseEvents != 2 {
		t.Fatalf("ambiguous-base events = %d, want 2 (the two-base ft: rollup must scream)", res.AmbiguousBaseEvents)
	}
	if !res.HasAmbiguousBase() || !res.HasAnomaly() {
		t.Fatal("HasAmbiguousBase/HasAnomaly = false, want true (the uniqueness violation must drive exit-nonzero)")
	}
	// The clean ft: rollup (both same-base events) still rated as ONE rollup; the
	// ambiguous one did not. 2 events rated into 1 rollup.
	if res.EventsRated != 2 || res.RollupsWritten != 1 {
		t.Fatalf("rated=%d rollups=%d, want 2/1 (the same-ft-same-base multi-event rollup must rate as one)", res.EventsRated, res.RollupsWritten)
	}
	for k := range store.table {
		if k.modelID == "ft:dupe" {
			t.Fatal("a rollup exists for the ambiguous ft: id — it must NOT be billed (silent MIN-rate under-charge)")
		}
	}
	// SINGLE-SNAPSHOT PARTITION: rated + unpriced + unattributable + ambiguous_base +
	// ambiguous_org == total (org bucket is 0 in the oracle; still a true partition cell).
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents + res.AmbiguousBaseEvents + res.AmbiguousOrgEvents; got != int64(len(events)) {
		t.Fatalf("rated(%d)+unpriced(%d)+unattr(%d)+ambiguous(%d) = %d, want %d",
			res.EventsRated, res.UnpricedEvents, res.UnattributableEvents, res.AmbiguousBaseEvents, got, len(events))
	}
}

// TestRater_RollupCostSelfAudits (rollup-cost-self-audits): the persona-pass invariant —
// every written rollup's cost EXACTLY equals its applied per-token rate × its token
// counts (cost == applied_prompt_rate × billable + applied_cached_rate × cached +
// applied_completion_rate × completion). Because the applied rate is the 9dp value
// frozen on the row, the cost is fully reconstructable from the row ALONE — no hidden
// second rate.
//
// SCOPE: the fixture has a base rollup and a DERIVED rollup, which carry DIFFERENT
// applied rates, and asserts EACH reconstructs from its OWN frozen rate — so the
// self-audit is exercised across heterogeneous rate kinds, not just one rate. It does
// NOT exercise MIN() masking a divergent rate WITHIN a rollup: that can only arise when
// one ft: id resolves through >1 base in a window, which the ambiguity gate excludes
// from billing entirely — proven by TestRater_FineTuneAmbiguousBaseModelFailsLoud (and
// its live-PG twin), where the ambiguous rollup is NOT written at all. So within any
// WRITTEN rollup the rate is uniform by construction and MIN() has nothing to mask.
func TestRater_RollupCostSelfAudits(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"base": rate3("0.000004", "0.0000004", "0.00001")},
		map[string]string{"ft": "base"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "base", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "base", PromptTokens: 200, CachedTokens: 0, CompletionTokens: 10, At: at.Add(10 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "ft", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: at},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false); err != nil {
		t.Fatal(err)
	}
	if len(store.table) == 0 {
		t.Fatal("no rollups written")
	}
	for k, ru := range store.table {
		want := ru.appliedRate.Prompt.MulInt(ru.billable).
			Add(ru.appliedRate.Cached.MulInt(ru.cached)).
			Add(ru.appliedRate.Completion.MulInt(ru.completion)).
			Round(moneyScale)
		if ru.cost.String() != want.String() {
			t.Errorf("rollup (%s,%s) cost = %s, but applied_rate × tokens = %s (self-audit broken)",
				k.authID, k.modelID, ru.cost, want)
		}
	}
}

// TestRater_AppliedRateStoredOnRow (applied-rate-stored-on-row): the rollup carries
// the exact per-token rate it was billed at, for both base and derived models.
func TestRater_AppliedRateStoredOnRow(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"base": rate3("0.000004", "0.0000004", "0.00001")},
		map[string]string{"ft": "base"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "base", PromptTokens: 10, At: at},
		{AuthID: "a", ResourceID: "r", ModelID: "ft", PromptTokens: 10, At: at},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"), false); err != nil {
		t.Fatal(err)
	}
	hour := mustTime("2026-06-08T10:00:00Z")
	baseRow := store.table[rollupKey{"a", "r", "base", hour}]
	if baseRow.appliedRate.Prompt.String() != "0.000004000" {
		t.Fatalf("base applied prompt rate = %s, want 0.000004000", baseRow.appliedRate.Prompt)
	}
	ftRow := store.table[rollupKey{"a", "r", "ft", hour}]
	// premium applied: 0.000004 × 1.5 = 0.000006
	if ftRow.appliedRate.Prompt.String() != "0.000006000" {
		t.Fatalf("ft applied prompt rate = %s, want 0.000006000 (premium frozen on row)", ftRow.appliedRate.Prompt)
	}
}

// TestRater_InvertedWindow rejects an empty/inverted window before any SQL.
func TestRater_InvertedWindow(t *testing.T) {
	store := newOracleStore(bookM(), nil)
	r := New(store, bookM(), testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T11:00:00Z"), mustTime("2026-06-08T10:00:00Z"), false); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestOracleModel_SelfConsistent checks the in-Go oracle's INTERNAL consistency — it
// runs NO SQL. Over a fixture mixing base, derived (multiplier), cached-subset, and
// unpriced/unattributable rows, the rollup the oracleStore builds
// (resolve→quantize→exact-cost→sum→round-once) equals an independent recomputation,
// row-for-row. The REAL SQL-vs-oracle conformance is the //go:build integration test
// TestIntegration_RateWindow_ConformsToOracle in store_integration_test.go; this one
// only pins that the oracle agrees with itself (so a faithful reference exists to pin
// the SQL against).
func TestOracleModel_SelfConsistent(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0.0000005", "0.00002")},
		map[string]string{"f": "b"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	hour := mustTime("2026-06-08T10:00:00Z")
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "b", PromptTokens: 200, CachedTokens: 0, CompletionTokens: 10, At: hour.Add(40 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "b", ResourceID: "r", ModelID: "b", PromptTokens: 1000, CachedTokens: 1000, CompletionTokens: 0, At: hour.Add(20 * time.Minute)}, // all-cached
		{AuthID: "a", ResourceID: "r", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},                                       // unpriced
		{AuthID: "", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},                                                                // unattributable
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), hour, hour.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}

	type key struct{ auth, model string }
	want := map[key]Dec{}
	for _, e := range events {
		if e.AuthID == "" || e.ModelID == "" {
			continue
		}
		resolved, err := book.Resolve(e.ModelID)
		if err != nil {
			continue
		}
		k := key{e.AuthID, e.ModelID}
		want[k] = want[k].Add(Rate(e, resolved.Quantized()))
	}
	if res.UnpricedEvents != 1 || res.UnattributableEvents != 1 {
		t.Fatalf("anomalies = unpriced %d / unattr %d, want 1/1", res.UnpricedEvents, res.UnattributableEvents)
	}
	// SINGLE-SNAPSHOT ACCOUNTING INVARIANT: rated + unpriced + unattributable must
	// PARTITION the in-window events.
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents; got != int64(len(events)) {
		t.Fatalf("rated(%d) + unpriced(%d) + unattributable(%d) = %d, want %d",
			res.EventsRated, res.UnpricedEvents, res.UnattributableEvents, got, len(events))
	}
	if res.RollupsWritten != int64(len(want)) {
		t.Fatalf("rollups = %d, want %d", res.RollupsWritten, len(want))
	}
	for k, w := range want {
		rk := rollupKey{authID: k.auth, resourceID: "r", modelID: k.model, windowStart: hour}
		got := store.table[rk].cost
		if got.String() != w.String() {
			t.Errorf("rollup (%s,%s) cost = %s, oracle wants %s", k.auth, k.model, got, w)
		}
	}
}
