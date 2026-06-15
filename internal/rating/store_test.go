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

	rows := sqlmock.NewRows([]string{"rollups_written", "events_rated", "total_cost", "unpriced_events", "unattributable_events"}).
		AddRow(2, 5, "0.001234500", 3, 1)
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
	if res.UnpricedEvents != 3 || res.UnattributableEvents != 1 {
		t.Fatalf("anomaly counts = %d/%d, want 3/1 (must ride the same statement)", res.UnpricedEvents, res.UnattributableEvents)
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
		// session-TZ-independent hour bucket (date_trunc on a tstz truncates in the
		// session TZ; a fractional-offset session would shift ON CONFLICT keys)
		"date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'",
		// deterministic natural-key surrogate id (re-runs regenerate the same id);
		// epoch keeps the hash input session-TZ-independent
		"md5(auth_id || '|' || model_id || '|' || extract(epoch FROM window_start)::bigint::text)",
		// deterministic lock order across concurrent raters (no ABBA deadlock)
		"ORDER BY auth_id, model_id, window_start",
		// idempotent upsert on the natural key
		"ON CONFLICT (auth_id, model_id, window_start) DO UPDATE SET",
		// the anomaly counts ride the SAME statement (one snapshot) as the upsert
		"AS unpriced_events",
		"AS unattributable_events",
	}
	// The session-TZ-DEPENDENT bucket must be gone everywhere (window_start,
	// window_end, GROUP BY): a bare date_trunc('hour', ev_ts) anywhere would let a
	// fractional-offset session write off-boundary buckets.
	if strings.Contains(rateWindowSQL, "date_trunc('hour', ev_ts)") {
		t.Error("rateWindowSQL still contains the session-TZ-dependent date_trunc('hour', ev_ts)")
	}
	for _, f := range wantFragments {
		if !strings.Contains(rateWindowSQL, f) {
			t.Errorf("rateWindowSQL missing fragment: %q", f)
		}
	}
}

// TestEnsureUTCTimeZone: the belt-and-braces DSN pin appends timezone=UTC unless
// the operator already chose a TZ (we never fight an explicit DSN). The bucketing
// expression in rateWindowSQL is the load-bearing TZ fix; this is defense in depth.
func TestEnsureUTCTimeZone(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgres://u:p@h/db", "postgres://u:p@h/db?timezone=UTC"},
		{"postgres://u:p@h/db?sslmode=disable", "postgres://u:p@h/db?sslmode=disable&timezone=UTC"},
		{"host=h dbname=db", "host=h dbname=db timezone=UTC"},
		// already pinned (either case) → untouched
		{"postgres://u:p@h/db?timezone=America/New_York", "postgres://u:p@h/db?timezone=America/New_York"},
		{"host=h TimeZone=UTC", "host=h TimeZone=UTC"},
	}
	for _, c := range cases {
		if got := ensureUTCTimeZone(c.in); got != c.want {
			t.Errorf("ensureUTCTimeZone(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
