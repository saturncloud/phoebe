package rating

import (
	"context"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// fakeStore is an in-memory Store for testing the Rater end-to-end without
// Postgres. It models the rated_usage table as a map keyed by the natural key, so
// UpsertRollups REPLACES on conflict exactly like the DB's ON CONFLICT DO UPDATE
// — letting us assert the idempotency invariant (re-run → no doubling).
type fakeStore struct {
	prices []Price
	events []RatedEvent

	// table mirrors rated_usage: natural key → row. Reset-and-replace per upsert
	// key, never appended, so re-runs reconcile rather than duplicate.
	table map[rollupKey]Rollup
	// upsertCalls counts UpsertRollups invocations (to prove re-run behaviour).
	upsertCalls int
}

func newFakeStore(prices []Price, events []RatedEvent) *fakeStore {
	return &fakeStore{prices: prices, events: events, table: map[rollupKey]Rollup{}}
}

func (f *fakeStore) LoadPrices(_ context.Context) ([]Price, error) { return f.prices, nil }

func (f *fakeStore) ReadWindow(_ context.Context, start, end time.Time) ([]RatedEvent, error) {
	var out []RatedEvent
	for _, e := range f.events {
		at := e.At
		if !at.Before(start) && at.Before(end) {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeStore) UpsertRollups(_ context.Context, rollups []Rollup) error {
	f.upsertCalls++
	for _, r := range rollups {
		k := rollupKey{authID: r.AuthID, model: r.Model, windowStart: r.WindowStart.UTC()}
		f.table[k] = r // REPLACE, mirroring ON CONFLICT DO UPDATE
	}
	return nil
}

func (f *fakeStore) Ping(_ context.Context) error { return nil }
func (f *fakeStore) Close() error                 { return nil }

func testLogger() *logging.Logger { return logging.New(logging.ERROR) }

func priceM() Price {
	return Price{
		Model: "m", PromptPriceMicro: 3, CachedPriceMicro: 1, CompletionPriceMicro: 10,
		EffectiveFrom: mustTime("2000-01-01T00:00:00Z"),
	}
}

// TestRater_MultiEventAggregation: many events for one (auth,model,hour) sum into
// ONE rollup with summed tokens and summed cost — the aggregation grain.
func TestRater_MultiEventAggregation(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", Model: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
		{AuthID: "a", Model: "m", PromptTokens: 10, CachedTokens: 0, CompletionTokens: 5, At: at.Add(20 * time.Minute)},
	}
	store := newFakeStore([]Price{priceM()}, events)
	r := New(store, testLogger())

	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.EventsRated != 2 || res.RollupsWritten != 1 {
		t.Fatalf("rated=%d rollups=%d, want 2 and 1", res.EventsRated, res.RollupsWritten)
	}
	// event1: (100-30)*3 + 30*1 + 50*10 = 210+30+500 = 740
	// event2: 10*3 + 0 + 5*10 = 30+50 = 80
	wantCost := int64(740 + 80)
	if res.TotalCostMicro != wantCost {
		t.Fatalf("total cost = %d, want %d", res.TotalCostMicro, wantCost)
	}
	k := rollupKey{authID: "a", model: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
	ru := store.table[k]
	if ru.PromptTokens != 110 || ru.CachedTokens != 30 || ru.CompletionTokens != 55 {
		t.Fatalf("token sums = p%d c%d comp%d, want 110/30/55", ru.PromptTokens, ru.CachedTokens, ru.CompletionTokens)
	}
	if ru.BillablePromptTokens != 80 { // 70 + 10
		t.Fatalf("billable_prompt = %d, want 80", ru.BillablePromptTokens)
	}
	if ru.CostMicroUSD != wantCost || ru.EventCount != 2 {
		t.Fatalf("rollup cost=%d count=%d, want %d/2", ru.CostMicroUSD, ru.EventCount, wantCost)
	}
}

// TestRater_SeparateRollupsPerKey: distinct auth/model/hour → distinct rollups,
// never merged.
func TestRater_SeparateRollupsPerKey(t *testing.T) {
	events := []RatedEvent{
		{AuthID: "a", Model: "m", PromptTokens: 10, At: mustTime("2026-06-08T10:05:00Z")},
		{AuthID: "b", Model: "m", PromptTokens: 10, At: mustTime("2026-06-08T10:05:00Z")}, // diff auth
		{AuthID: "a", Model: "m", PromptTokens: 10, At: mustTime("2026-06-08T10:30:00Z")}, // same hour as #1 → merges with #1
	}
	store := newFakeStore([]Price{priceM()}, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	// (a,m,10:00) and (b,m,10:00) → 2 rollups; #1 and #3 merge.
	if res.RollupsWritten != 2 {
		t.Fatalf("rollups = %d, want 2", res.RollupsWritten)
	}
	ka := rollupKey{authID: "a", model: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
	if store.table[ka].EventCount != 2 {
		t.Fatalf("auth a event_count = %d, want 2", store.table[ka].EventCount)
	}
}

// TestRater_IdempotentRerunNoDoubling: rating the SAME window twice produces
// identical totals — no doubling. The load-bearing re-runnability invariant.
func TestRater_IdempotentRerunNoDoubling(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", Model: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: at},
	}
	store := newFakeStore([]Price{priceM()}, events)
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

	if res1.TotalCostMicro != res2.TotalCostMicro {
		t.Fatalf("re-run total changed: %d → %d (must be identical)", res1.TotalCostMicro, res2.TotalCostMicro)
	}
	if len(store.table) != 1 {
		t.Fatalf("table has %d rows after re-run, want 1 (no duplication)", len(store.table))
	}
	k := rollupKey{authID: "a", model: "m", windowStart: mustTime("2026-06-08T10:00:00Z")}
	if store.table[k].CostMicroUSD != res1.TotalCostMicro {
		t.Fatalf("stored cost = %d after re-run, want %d (no doubling)", store.table[k].CostMicroUSD, res1.TotalCostMicro)
	}
	if store.upsertCalls != 2 {
		t.Fatalf("upsert called %d times, want 2 (one per run)", store.upsertCalls)
	}
}

// TestRater_MissingPriceFailsLoudNotZero: an event for a model with no price is
// counted as unpriced and DROPPED from the rollups — never a $0 row.
func TestRater_MissingPriceFailsLoudNotZero(t *testing.T) {
	at := mustTime("2026-06-08T10:15:00Z")
	events := []RatedEvent{
		{AuthID: "a", Model: "m", PromptTokens: 100, CompletionTokens: 50, At: at},        // priced
		{AuthID: "a", Model: "unpriced", PromptTokens: 100, CompletionTokens: 50, At: at}, // NO price
	}
	store := newFakeStore([]Price{priceM()}, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.UnpricedEvents != 1 {
		t.Fatalf("unpriced = %d, want 1", res.UnpricedEvents)
	}
	if res.EventsRated != 1 {
		t.Fatalf("rated = %d, want 1 (the unpriced one must NOT be rated)", res.EventsRated)
	}
	if !res.HasUnpriced() {
		t.Fatal("HasUnpriced() = false, want true (must surface lost-revenue signal)")
	}
	// Exactly ONE rollup (for the priced model). NO $0 rollup for "unpriced".
	if res.RollupsWritten != 1 || len(store.table) != 1 {
		t.Fatalf("rollups written=%d table=%d, want 1/1 (no $0 row for unpriced model)", res.RollupsWritten, len(store.table))
	}
	for k := range store.table {
		if k.model == "unpriced" {
			t.Fatal("a rollup exists for the unpriced model — it must NOT be $0-billed")
		}
	}
}

// TestRater_EffectiveDatedSelectionInRun: end-to-end, an old event is rated with
// the old price even when a newer price exists — no retroactive repricing.
func TestRater_EffectiveDatedSelectionInRun(t *testing.T) {
	prices := []Price{
		{Model: "m", PromptPriceMicro: 3, EffectiveFrom: mustTime("2026-01-01T00:00:00Z"), EffectiveTo: mustTime("2026-06-01T00:00:00Z")},
		{Model: "m", PromptPriceMicro: 5, EffectiveFrom: mustTime("2026-06-01T00:00:00Z")},
	}
	// Event in March → must use prompt=3, not 5.
	events := []RatedEvent{
		{AuthID: "a", Model: "m", PromptTokens: 100, At: mustTime("2026-03-10T10:15:00Z")},
	}
	store := newFakeStore(prices, events)
	r := New(store, testLogger())
	res, err := r.Run(context.Background(), mustTime("2026-03-10T10:00:00Z"), mustTime("2026-03-10T11:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if res.TotalCostMicro != 300 { // 100 * 3 (old price), NOT 100*5
		t.Fatalf("total = %d, want 300 (old effective price, not retroactively repriced)", res.TotalCostMicro)
	}
}

// TestRater_InvertedWindow rejects an empty/inverted window.
func TestRater_InvertedWindow(t *testing.T) {
	store := newFakeStore(nil, nil)
	r := New(store, testLogger())
	_, err := r.Run(context.Background(), mustTime("2026-06-08T11:00:00Z"), mustTime("2026-06-08T10:00:00Z"))
	if err == nil {
		t.Fatal("expected error for inverted window")
	}
}
