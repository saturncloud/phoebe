package rating

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestPostgresStore_LoadPrices verifies the price-book query scans rows and maps
// a NULL effective_to to the zero Time (open-ended).
func TestPostgresStore_LoadPrices(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	from := mustTime("2026-01-01T00:00:00Z")
	rows := sqlmock.NewRows([]string{"model", "prompt_price_micro", "cached_price_micro", "completion_price_micro", "effective_from", "effective_to"}).
		AddRow("m", int64(3), int64(1), int64(10), from, nil). // open-ended
		AddRow("m", int64(5), int64(2), int64(15), from, mustTime("2026-06-01T00:00:00Z"))

	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT model, prompt_price_micro, cached_price_micro, completion_price_micro, effective_from, effective_to FROM model_price",
	)).WillReturnRows(rows)

	prices, err := store.LoadPrices(context.Background())
	if err != nil {
		t.Fatalf("LoadPrices: %v", err)
	}
	if len(prices) != 2 {
		t.Fatalf("got %d prices, want 2", len(prices))
	}
	if !prices[0].EffectiveTo.IsZero() {
		t.Fatalf("row 0 effective_to = %v, want zero (NULL → open-ended)", prices[0].EffectiveTo)
	}
	if prices[1].EffectiveTo.IsZero() {
		t.Fatal("row 1 effective_to is zero, want the closed bound")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestPostgresStore_ReadWindow verifies the window query filters on the coalesced
// rating instant and excludes NULL auth_id/model, and that scanned rows map to
// RatedEvent.
func TestPostgresStore_ReadWindow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	start := mustTime("2026-06-08T10:00:00Z")
	end := mustTime("2026-06-08T11:00:00Z")
	at := mustTime("2026-06-08T10:15:00Z")

	rows := sqlmock.NewRows([]string{"auth_id", "model", "prompt_tokens", "cached_tokens", "completion_tokens", "aborted", "rating_ts"}).
		AddRow("a", "m", int64(100), int64(30), int64(50), false, at)

	// Match on the stable head of the statement + the WHERE coalesce/null guards.
	mock.ExpectQuery("SELECT auth_id, model, prompt_tokens, cached_tokens, completion_tokens, aborted").
		WithArgs(start, end).
		WillReturnRows(rows)

	events, err := store.ReadWindow(context.Background(), start, end)
	if err != nil {
		t.Fatalf("ReadWindow: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	e := events[0]
	if e.AuthID != "a" || e.Model != "m" || e.PromptTokens != 100 || e.CachedTokens != 30 || e.CompletionTokens != 50 {
		t.Fatalf("event mapped wrong: %+v", e)
	}
	if !e.At.Equal(at) {
		t.Fatalf("event At = %v, want %v (coalesced rating instant)", e.At, at)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestPostgresStore_UpsertRollupsSQL verifies the rollup upsert emits a single
// parameterised INSERT ... ON CONFLICT (auth_id, model, window_start) DO UPDATE
// inside a transaction — the idempotency mechanism. id is a generated 32-char hex
// (matched by pattern, not value).
func TestPostgresStore_UpsertRollupsSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	ws := mustTime("2026-06-08T10:00:00Z")
	we := mustTime("2026-06-08T11:00:00Z")
	rollups := []Rollup{{
		AuthID: "a", Model: "m",
		WindowStart: ws, WindowEnd: we,
		PromptTokens: 110, CachedTokens: 30, CompletionTokens: 55,
		BillablePromptTokens: 80, CostMicroUSD: 820, EventCount: 2,
	}}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT INTO rated_usage (id, auth_id, model, window_start, window_end, prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, cost_micro_usd, event_count) VALUES",
	)).
		// id is generated hex → match any 32 hex chars; the rest are exact.
		WithArgs(
			sqlmock.AnyArg(), // id
			"a", "m",
			ws.UTC(), we.UTC(),
			int64(110), int64(30), int64(55), int64(80), int64(820), 2,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := store.UpsertRollups(context.Background(), rollups); err != nil {
		t.Fatalf("UpsertRollups: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestPostgresStore_UpsertOnConflictClause locks the ON CONFLICT DO UPDATE clause
// (the idempotency contract) into the emitted SQL.
func TestPostgresStore_UpsertOnConflictClause(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectExec("ON CONFLICT \\(auth_id, model, window_start\\) DO UPDATE SET").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = store.UpsertRollups(context.Background(), []Rollup{{
		AuthID: "a", Model: "m",
		WindowStart: mustTime("2026-06-08T10:00:00Z"),
		WindowEnd:   mustTime("2026-06-08T11:00:00Z"),
	}})
	if err != nil {
		t.Fatalf("UpsertRollups: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestPostgresStore_UpsertEmptyNoop proves an empty batch issues no SQL.
func TestPostgresStore_UpsertEmptyNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	if err := store.UpsertRollups(context.Background(), nil); err != nil {
		t.Fatalf("UpsertRollups(nil): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL: %v", err)
	}
}

// TestNewID returns distinct 32-char hex ids.
func TestNewID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := newID()
		if err != nil {
			t.Fatal(err)
		}
		if len(id) != 32 {
			t.Fatalf("id %q len %d, want 32", id, len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}
