package rating

import (
	"context"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// oracleStore is an in-memory Store that models EXACTLY what the SQL rater does,
// using the Go oracle (PriceBook.ResolvePrice + Rate). It exists so the Rater
// orchestration AND the money rules can be exercised without Postgres, and so the
// conformance test has a faithful reference of the SQL's resolve→cost→sum.
//
// It models rated_usage as a map keyed by the natural key, REPLACING on upsert
// (mirroring ON CONFLICT DO UPDATE), so idempotency is observable.
type oracleStore struct {
	book   *PriceBook
	events []RatedEvent

	table      map[rollupKey]oracleRollup // natural key → row
	rateCalls  int
	countCalls int
}

type rollupKey struct {
	authID      string
	modelID     string
	windowStart time.Time
}

type oracleRollup struct {
	prompt, cached, completion, billable int64
	cost                                 Dec
	eventCount                           int
}

func newOracleStore(book *PriceBook, events []RatedEvent) *oracleStore {
	return &oracleStore{book: book, events: events, table: map[rollupKey]oracleRollup{}}
}

func (s *oracleStore) Ping(_ context.Context) error { return nil }
func (s *oracleStore) Close() error                 { return nil }

// resolveWindow walks the events in [start,end) exactly as the SQL CTE does:
// unattributable (empty auth/model) and unpriced (no resolvable rate, or a >1-hop
// chain) are counted and EXCLUDED; the rest are priced via Rate() and summed.
func (s *oracleStore) resolveWindow(start, end time.Time) (map[rollupKey]oracleRollup, Anomalies) {
	out := map[rollupKey]oracleRollup{}
	var an Anomalies
	for _, e := range s.events {
		if e.At.Before(start) || !e.At.Before(end) {
			continue
		}
		if e.AuthID == "" || e.ModelID == "" {
			an.UnattributableEvents++
			continue
		}
		rate, err := s.book.ResolvePrice(e.ModelID, e.At)
		if err != nil {
			an.UnpricedEvents++ // ErrNoPrice or ErrDerivationChain: never $0-billed
			continue
		}
		// Accumulate the EXACT (unrounded) per-event cost. Rounding happens ONCE
		// per rollup below, mirroring the SQL's SUM(...) → NUMERIC(20,9). Summing
		// pre-rounded per-event values here would be round-then-sum and could
		// diverge from production by up to ~1 nano/event on sub-nano residues.
		cost := rateExact(e, rate)
		hour := e.At.UTC().Truncate(time.Hour)
		k := rollupKey{authID: e.AuthID, modelID: e.ModelID, windowStart: hour}
		ru := out[k]
		ru.prompt += e.PromptTokens
		ru.cached += e.CachedTokens
		ru.completion += e.CompletionTokens
		ru.billable += BillablePromptTokens(e.PromptTokens, e.CachedTokens)
		ru.cost = ru.cost.Add(cost)
		ru.eventCount++
		out[k] = ru
	}
	// Round each rollup's exact summed cost ONCE — the quantize the DB applies
	// when the SUM lands in the NUMERIC(20,9) column.
	for k, ru := range out {
		ru.cost = ru.cost.Round(moneyScale)
		out[k] = ru
	}
	return out, an
}

func (s *oracleStore) RateWindow(_ context.Context, start, end time.Time) (RateResult, error) {
	s.rateCalls++
	rollups, _ := s.resolveWindow(start.UTC(), end.UTC())
	total := Dec{}
	var rated int
	for k, ru := range rollups {
		s.table[k] = ru // REPLACE — ON CONFLICT DO UPDATE
		total = total.Add(ru.cost)
		rated += ru.eventCount
	}
	res := RateResult{RollupsWritten: len(rollups), EventsRated: rated}
	if len(rollups) > 0 {
		res.TotalCost = total.String()
	} else {
		res.TotalCost = "0.000000000"
	}
	return res, nil
}

func (s *oracleStore) CountAnomalies(_ context.Context, start, end time.Time) (Anomalies, error) {
	s.countCalls++
	_, an := s.resolveWindow(start.UTC(), end.UTC())
	return an, nil
}

func testLogger() *logging.Logger { return logging.New(logging.ERROR) }

// bookM is a one-base-model price book ("m": prompt 0.000003 / cached 0.0000003 /
// completion 0.00001), open-ended.
func bookM() *PriceBook {
	return NewPriceBook([]PriceRow{baseRow("m", "0.000003", "0.0000003", "0.00001")}, nil)
}

// TestRater_MultiEventAggregation: many events for one (auth,model,hour) sum into
// ONE rollup with summed tokens and summed cost — the aggregation grain. Money is
// summed in the store (modeling SQL SUM), surfaced to the Rater as NUMERIC text.
func TestRater_MultiEventAggregation(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "a", ModelID: "m", PromptTokens: 10, CachedTokens: 0, CompletionTokens: 5, At: at.Add(20 * time.Minute)},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, testLogger())

	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.EventsRated != 2 || res.RollupsWritten != 1 {
		t.Fatalf("rated=%d rollups=%d, want 2 and 1", res.EventsRated, res.RollupsWritten)
	}
	// event1: 70*0.000003 + 30*0.0000003 + 50*0.00001 = 0.00021 + 0.000009 + 0.0005 = 0.000719
	// event2: 10*0.000003 + 0 + 5*0.00001 = 0.00003 + 0.00005 = 0.00008
	// total = 0.000799
	if res.TotalCost != "0.000799000" {
		t.Fatalf("total = %s, want 0.000799000", res.TotalCost)
	}
	k := rollupKey{authID: "a", modelID: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
	ru := store.table[k]
	if ru.prompt != 110 || ru.cached != 30 || ru.completion != 55 || ru.billable != 80 {
		t.Fatalf("token sums = p%d c%d comp%d bill%d, want 110/30/55/80", ru.prompt, ru.cached, ru.completion, ru.billable)
	}
	if ru.cost.String() != "0.000799000" || ru.eventCount != 2 {
		t.Fatalf("rollup cost=%s count=%d, want 0.000799000/2", ru.cost, ru.eventCount)
	}
}

// TestRater_IdempotentRerunNoDoubling: rating the SAME window twice produces
// identical totals — no doubling. The load-bearing re-runnability invariant.
func TestRater_IdempotentRerunNoDoubling(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, testLogger())
	ws, we := mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")

	res1, err := r.Run(context.Background(), ws, we)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := r.Run(context.Background(), ws, we)
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

// TestRater_MissingPriceFailsLoudNotZero: an event for a model with no price is
// counted unpriced and EXCLUDED from the rollups — never a $0 row.
func TestRater_MissingPriceFailsLoudNotZero(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},        // priced
		{AuthID: "a", ModelID: "unpriced", PromptTokens: 100, CompletionTokens: 50, At: at}, // NO price
	}
	store := newOracleStore(bookM(), events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
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

// TestRater_UnattributableCountedNotSilent: rows with NULL auth_id and/or model_id
// must NOT be rated, MUST be counted, and MUST trigger the loud anomaly / exit-2
// path — never silently dropped.
func TestRater_UnattributableCountedNotSilent(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at}, // ok
		{AuthID: "", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL auth_id
		{AuthID: "a", ModelID: "", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL model_id
	}
	store := newOracleStore(bookM(), events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.UnattributableEvents != 2 {
		t.Fatalf("unattributable = %d, want 2", res.UnattributableEvents)
	}
	if res.EventsRated != 1 || res.RollupsWritten != 1 || len(store.table) != 1 {
		t.Fatalf("rated=%d rollups=%d table=%d, want 1/1/1", res.EventsRated, res.RollupsWritten, len(store.table))
	}
	if !res.HasUnattributable() || !res.HasAnomaly() {
		t.Fatal("HasUnattributable/HasAnomaly = false, want true (drives exit 2)")
	}
}

// TestRater_EffectiveDatedSelectionInRun: end-to-end, an old event is rated with
// the old price even when a newer price exists — no retroactive repricing.
func TestRater_EffectiveDatedSelectionInRun(t *testing.T) {
	book := NewPriceBook([]PriceRow{
		{ModelID: "m", HasRate: true, Prompt: MustDec("0.000003"), Cached: MustDec("0"), Completion: MustDec("0"),
			EffectiveFrom: mustTime("2026-01-01T00:00:00Z"), EffectiveTo: mustTime("2026-06-01T00:00:00Z")},
		{ModelID: "m", HasRate: true, Prompt: MustDec("0.000005"), Cached: MustDec("0"), Completion: MustDec("0"),
			EffectiveFrom: mustTime("2026-06-01T00:00:00Z")},
	}, nil)
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, At: mustTime("2026-03-10T10:15:00Z")},
	}
	store := newOracleStore(book, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-03-10T10:00:00Z"), mustTime("2026-03-10T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	// 100 * 0.000003 (old price) = 0.0003, NOT 100*0.000005.
	if res.TotalCost != "0.000300000" {
		t.Fatalf("total = %s, want 0.000300000 (old effective price)", res.TotalCost)
	}
}

// TestRater_DerivedFineTuneRatedViaPolicy: end-to-end, a fine-tune (rate=null,
// derived_from base) is rated at base × policy — exercising derivation in the run.
func TestRater_DerivedFineTuneRatedViaPolicy(t *testing.T) {
	book := NewPriceBook(
		[]PriceRow{
			baseRow("base", "0.000004", "0", "0"),
			derivedRow("ft", "base"),
		},
		[]PolicyRow{{Func: PolicyMultiplier, Factor: MustDec("1.5"), EffectiveFrom: mustTime("2026-01-01T00:00:00Z")}},
	)
	events := []RatedEvent{
		{AuthID: "a", ModelID: "ft", PromptTokens: 1000, At: mustTime("2026-06-08T10:15:00Z")},
	}
	store := newOracleStore(book, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	// 1000 * (0.000004 * 1.5) = 1000 * 0.000006 = 0.006
	if res.TotalCost != "0.006000000" {
		t.Fatalf("total = %s, want 0.006000000 (base×1.5 fine-tune)", res.TotalCost)
	}
}

// TestRater_InvertedWindow rejects an empty/inverted window before any SQL.
func TestRater_InvertedWindow(t *testing.T) {
	store := newOracleStore(bookM(), nil)
	r := New(store, testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T11:00:00Z"), mustTime("2026-06-08T10:00:00Z")); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestConformance_SQLModelMatchesRateOracle checks the in-Go model's INTERNAL
// consistency: over a fixture mixing base, derived (multiplier), effective-dated,
// cached-subset, and unpriced/unattributable rows, the rollup the oracleStore
// builds (resolve→exact-cost→sum→round-once) equals an independent recomputation,
// row-for-row. NOTE: this does NOT exercise the production SQL — both sides are
// pure Go. The SQL is pinned to the oracle by the //go:build integration tests in
// store_integration_test.go, which run the REAL rateWindowSQL against a live
// Postgres (TestConformance_SQLModelMatchesRateOracle conformance + the sub-nano
// rounding-order guard). Those run in CI's integration-test job (Postgres
// service). This in-Go test is the fast inner check; the integration tests are the
// real pin.
func TestConformance_SQLModelMatchesRateOracle(t *testing.T) {
	book := NewPriceBook(
		[]PriceRow{
			// base "b" effective-dated: 0.000003 until June, 0.000005 after.
			{ModelID: "b", HasRate: true, Prompt: MustDec("0.000003"), Cached: MustDec("0.0000003"), Completion: MustDec("0.00001"),
				EffectiveFrom: mustTime("2026-01-01T00:00:00Z"), EffectiveTo: mustTime("2026-06-01T00:00:00Z")},
			{ModelID: "b", HasRate: true, Prompt: MustDec("0.000005"), Cached: MustDec("0.0000005"), Completion: MustDec("0.00002"),
				EffectiveFrom: mustTime("2026-06-01T00:00:00Z")},
			// fine-tune "f" derives from "b".
			derivedRow("f", "b"),
		},
		[]PolicyRow{{Func: PolicyMultiplier, Factor: MustDec("1.5"), EffectiveFrom: mustTime("2026-01-01T00:00:00Z")}},
	)
	hour := mustTime("2026-06-08T10:00:00Z") // after June → base uses 0.000005 rate
	events := []RatedEvent{
		{AuthID: "a", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ModelID: "b", PromptTokens: 200, CachedTokens: 0, CompletionTokens: 10, At: hour.Add(40 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "b", ModelID: "b", PromptTokens: 1000, CachedTokens: 1000, CompletionTokens: 0, At: hour.Add(20 * time.Minute)}, // all-cached
		{AuthID: "a", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},                                       // unpriced
		{AuthID: "", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},                                               // unattributable
	}
	store := newOracleStore(book, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	// Independent oracle recomputation: group the PRICED, ATTRIBUTABLE events and
	// sum Rate() per (auth,model,hour). This deliberately re-derives the expectation
	// rather than trusting the store, so it is a true cross-check.
	type key struct{ auth, model string }
	want := map[key]Dec{}
	for _, e := range events {
		if e.AuthID == "" || e.ModelID == "" {
			continue
		}
		rate, err := book.ResolvePrice(e.ModelID, e.At)
		if err != nil {
			continue
		}
		k := key{e.AuthID, e.ModelID}
		want[k] = want[k].Add(Rate(e, rate))
	}
	if res.UnpricedEvents != 1 || res.UnattributableEvents != 1 {
		t.Fatalf("anomalies = unpriced %d / unattr %d, want 1/1", res.UnpricedEvents, res.UnattributableEvents)
	}
	if res.RollupsWritten != len(want) {
		t.Fatalf("rollups = %d, want %d", res.RollupsWritten, len(want))
	}
	for k, w := range want {
		rk := rollupKey{authID: k.auth, modelID: k.model, windowStart: hour}
		got := store.table[rk].cost
		if got.String() != w.String() {
			t.Errorf("rollup (%s,%s) cost = %s, oracle wants %s", k.auth, k.model, got, w)
		}
	}
}
