package rating

import (
	"context"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

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
	modelID     string
	windowStart time.Time
}

type oracleRollup struct {
	prompt, cached, completion, billable int64
	cost                                 Dec
	appliedRate                          Rate3
	eventCount                           int
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
		if e.AuthID == "" || e.ModelID == "" {
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
		k := rollupKey{authID: e.AuthID, modelID: e.ModelID, windowStart: hour}
		ru := out[k]
		ru.prompt += e.PromptTokens
		ru.cached += e.CachedTokens
		ru.completion += e.CompletionTokens
		ru.billable += BillablePromptTokens(e.PromptTokens, e.CachedTokens)
		ru.cost = ru.cost.Add(cost)
		ru.appliedRate = rate // single-model rollup → one applied rate
		ru.eventCount++
		out[k] = ru
	}
	for k, ru := range out {
		ru.cost = ru.cost.Round(moneyScale)
		out[k] = ru
	}
	return out, an
}

func (s *oracleStore) RateWindow(_ context.Context, _ *PriceBook, start, end time.Time) (RateResult, error) {
	s.rateCalls++
	rollups, an := s.resolveWindow(start.UTC(), end.UTC())
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
		UnpricedEvents:       an.UnpricedEvents,
		UnattributableEvents: an.UnattributableEvents,
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
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "a", ModelID: "m", PromptTokens: 10, CachedTokens: 0, CompletionTokens: 5, At: at.Add(20 * time.Minute)},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())

	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
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
	k := rollupKey{authID: "a", modelID: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
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
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
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

// TestRater_LateArrivalRatedByTrailingWindow: an event whose event_ts falls in hour
// H but which is DRAINED only after H was rated is picked up by a later run whose
// window still covers H — the trailing-window contract. The re-rate REPLACES H's
// bucket (never doubles).
func TestRater_LateArrivalRatedByTrailingWindow(t *testing.T) {
	hourH := mustTime("2026-06-08T10:00:00Z")
	store := newOracleStore(bookM(), nil)
	r := New(store, bookM(), testLogger())

	res1, err := r.Run(context.Background(), hourH, hourH.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if res1.EventsRated != 0 || res1.RollupsWritten != 0 {
		t.Fatalf("run1 rated=%d rollups=%d, want 0/0 (event not drained yet)", res1.EventsRated, res1.RollupsWritten)
	}

	store.events = append(store.events, RatedEvent{
		AuthID: "a", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: hourH.Add(15 * time.Minute),
	})

	res2, err := r.Run(context.Background(), hourH.Add(-23*time.Hour), hourH.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if res2.EventsRated != 1 || res2.RollupsWritten != 1 {
		t.Fatalf("run2 rated=%d rollups=%d, want 1/1 (late event caught by trailing window)", res2.EventsRated, res2.RollupsWritten)
	}
	k := rollupKey{authID: "a", modelID: "m", windowStart: hourH}
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
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},        // priced
		{AuthID: "a", ModelID: "unpriced", PromptTokens: 100, CompletionTokens: 50, At: at}, // NO price
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
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
// must NOT be rated, MUST be counted, and MUST trigger the loud anomaly / exit-2 path.
func TestRater_UnattributableCountedNotSilent(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at}, // ok
		{AuthID: "", ModelID: "m", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL auth_id
		{AuthID: "a", ModelID: "", PromptTokens: 100, CompletionTokens: 50, At: at},  // NULL model_id
	}
	store := newOracleStore(bookM(), events)
	r := New(store, bookM(), testLogger())
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

// TestRater_DerivedFineTuneRatedViaPolicy: end-to-end, a fine-tune (derived_from a
// base) is rated at base × premium — exercising derivation in the run.
func TestRater_DerivedFineTuneRatedViaPolicy(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"base": rate3("0.000004", "0", "0")},
		map[string]string{"ft": "base"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	events := []RatedEvent{
		{AuthID: "a", ModelID: "ft", PromptTokens: 1000, At: mustTime("2026-06-08T10:15:00Z")},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
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
		AuthID:       "a",
		ModelID:      "ft:9f8e7d6c5b4a", // a checkpoint id the file does not list
		BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
		PromptTokens: 1000,
		At:           mustTime("2026-06-08T10:15:00Z"),
	}}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
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
	row := store.table[rollupKey{"a", "ft:9f8e7d6c5b4a", hour}]
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
		AuthID:       "a",
		ModelID:      "ft:9f8e7d6c5b4a",
		BaseModel:    "", // THE BUG: base_model never propagated to the event
		PromptTokens: 1000,
		At:           mustTime("2026-06-08T10:15:00Z"),
	}}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
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
		{AuthID: "a", ModelID: "base", PromptTokens: 10, At: at},
		{AuthID: "a", ModelID: "ft", PromptTokens: 10, At: at},
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")); err != nil {
		t.Fatal(err)
	}
	hour := mustTime("2026-06-08T10:00:00Z")
	baseRow := store.table[rollupKey{"a", "base", hour}]
	if baseRow.appliedRate.Prompt.String() != "0.000004000" {
		t.Fatalf("base applied prompt rate = %s, want 0.000004000", baseRow.appliedRate.Prompt)
	}
	ftRow := store.table[rollupKey{"a", "ft", hour}]
	// premium applied: 0.000004 × 1.5 = 0.000006
	if ftRow.appliedRate.Prompt.String() != "0.000006000" {
		t.Fatalf("ft applied prompt rate = %s, want 0.000006000 (premium frozen on row)", ftRow.appliedRate.Prompt)
	}
}

// TestRater_InvertedWindow rejects an empty/inverted window before any SQL.
func TestRater_InvertedWindow(t *testing.T) {
	store := newOracleStore(bookM(), nil)
	r := New(store, bookM(), testLogger())
	if _, err := r.Run(context.Background(), mustTime("2026-06-08T11:00:00Z"), mustTime("2026-06-08T10:00:00Z")); err == nil {
		t.Fatal("expected error for inverted window")
	}
}

// TestConformance_SQLModelMatchesRateOracle checks the in-Go model's INTERNAL
// consistency: over a fixture mixing base, derived (multiplier), cached-subset, and
// unpriced/unattributable rows, the rollup the oracleStore builds
// (resolve→exact-cost→sum→round-once) equals an independent recomputation,
// row-for-row. The production SQL is pinned to this oracle by the //go:build
// integration tests in store_integration_test.go.
func TestConformance_SQLModelMatchesRateOracle(t *testing.T) {
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0.0000005", "0.00002")},
		map[string]string{"f": "b"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	hour := mustTime("2026-06-08T10:00:00Z")
	events := []RatedEvent{
		{AuthID: "a", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ModelID: "b", PromptTokens: 200, CachedTokens: 0, CompletionTokens: 10, At: hour.Add(40 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "b", ModelID: "b", PromptTokens: 1000, CachedTokens: 1000, CompletionTokens: 0, At: hour.Add(20 * time.Minute)}, // all-cached
		{AuthID: "a", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},                                       // unpriced
		{AuthID: "", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},                                               // unattributable
	}
	store := newOracleStore(book, events)
	r := New(store, book, testLogger())
	res, err := r.Run(context.Background(), hour, hour.Add(time.Hour))
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
		rk := rollupKey{authID: k.auth, modelID: k.model, windowStart: hour}
		got := store.table[rk].cost
		if got.String() != w.String() {
			t.Errorf("rollup (%s,%s) cost = %s, oracle wants %s", k.auth, k.model, got, w)
		}
	}
}
