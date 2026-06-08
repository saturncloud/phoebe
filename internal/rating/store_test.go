package rating

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestPostgresStore_RateWindowSQL asserts the rate-and-sum statement: it carries
// the effective-dated resolution CTE, the LATERAL own/base/policy joins, the
// billable-prompt formula, the SUM into rollups, and the idempotent upsert. We
// match on stable fragments rather than the whole (large) statement.
func TestPostgresStore_RateWindowSQL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	start := mustTime("2026-06-08T10:00:00Z")
	end := mustTime("2026-06-08T11:00:00Z")

	rows := sqlmock.NewRows([]string{"rollups_written", "events_rated", "total_cost"}).
		AddRow(2, 5, "0.001234500")
	mock.ExpectQuery(`INSERT INTO rated_usage`).
		WithArgs(start.UTC(), end.UTC()).
		WillReturnRows(rows)

	res, err := store.RateWindow(context.Background(), start, end)
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 2 || res.EventsRated != 5 || res.TotalCost != "0.001234500" {
		t.Fatalf("result = %+v, want 2/5/0.001234500", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestRateWindowSQL_Shape locks the load-bearing SQL fragments into the statement
// constant itself (no DB needed): the half-open effective-dating predicate, the
// one-hop derived-from resolution, the policy CASE, the billable-prompt clamp +
// formula, and the idempotent ON CONFLICT.
func TestRateWindowSQL_Shape(t *testing.T) {
	wantFragments := []string{
		// window membership on the coalesced rating instant
		"COALESCE(event_ts, created_at) >= $1",
		"COALESCE(event_ts, created_at) <  $2",
		// half-open effective-dating predicate (appears for own, derived, base)
		"effective_from <= ev.ev_ts",
		"ev.ev_ts < mp.effective_to",
		// own-rate escape hatch (carries a rate) + derived (no own rate)
		"mp.prompt_price IS NOT NULL",
		"mp.prompt_price IS NULL",
		"mp.derived_from IS NOT NULL",
		// one-hop resolution: base is looked up by the model's derived_from
		"mp.model_id = der.derived_from",
		// derivation policy applied in SQL (CASE on function)
		"WHEN 'multiplier' THEN base.prompt_price * pol.factor",
		"WHEN 'markup'     THEN base.prompt_price + pol.markup",
		// the global policy lookup (no per-base scope)
		"FROM derivation_policy dp",
		// billable-prompt clamp + the cost formula (cached charged once)
		"GREATEST(ev.prompt_tokens - ev.cached_tokens, 0)",
		"billable_prompt   * prompt_price",
		"cached_tokens     * cached_price",
		"completion_tokens * completion_price",
		// priced + attributable filter (never $0-bill unpriced/unattributable)
		"WHERE prompt_price IS NOT NULL",
		"AND auth_id  IS NOT NULL",
		"AND model_id IS NOT NULL",
		// idempotent upsert on the natural key
		"ON CONFLICT (auth_id, model_id, window_start) DO UPDATE SET",
	}
	for _, f := range wantFragments {
		if !strings.Contains(rateWindowSQL, f) {
			t.Errorf("rateWindowSQL missing fragment: %q", f)
		}
	}
}

// TestCountAnomaliesSQL_Shape locks the anomaly-count query: it shares the SAME
// resolution CTE (so "unpriced" means the same thing as in the insert) and counts
// unpriced (priced=NULL, attributable) vs unattributable (NULL auth/model) without
// double-attributing a row.
func TestCountAnomaliesSQL_Shape(t *testing.T) {
	wantFragments := []string{
		"COUNT(*) FILTER (",
		"WHERE prompt_price IS NULL",
		"AND auth_id  IS NOT NULL",
		"AND model_id IS NOT NULL",
		"WHERE auth_id IS NULL OR model_id IS NULL",
		"FROM resolved",
	}
	for _, f := range wantFragments {
		if !strings.Contains(countAnomaliesSQL, f) {
			t.Errorf("countAnomaliesSQL missing fragment: %q", f)
		}
	}
	// Both statements must be built on the identical resolution CTE.
	if !strings.Contains(rateWindowSQL, "WITH ev AS (") || !strings.Contains(countAnomaliesSQL, "WITH ev AS (") {
		t.Fatal("rate/anomaly statements must both build on resolvedEventsCTE")
	}
}

// TestPostgresStore_CountAnomalies scans the two counts.
func TestPostgresStore_CountAnomalies(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	start := mustTime("2026-06-08T10:00:00Z")
	end := mustTime("2026-06-08T11:00:00Z")

	mock.ExpectQuery(`COUNT\(\*\) FILTER`).
		WithArgs(start.UTC(), end.UTC()).
		WillReturnRows(sqlmock.NewRows([]string{"unpriced", "unattributable"}).AddRow(3, 1))

	a, err := store.CountAnomalies(context.Background(), start, end)
	if err != nil {
		t.Fatalf("CountAnomalies: %v", err)
	}
	if a.UnpricedEvents != 3 || a.UnattributableEvents != 1 {
		t.Fatalf("anomalies = %+v, want 3/1", a)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}
