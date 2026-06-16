package rating

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestPostgresStore_RateWindowSQL asserts the rate-and-sum flow: it projects the
// price book into a TEMP table inside a transaction, then runs the single resolve →
// sum → upsert statement and reports the priced totals AND the anomaly counts from
// the same row. We match on stable fragments rather than the whole statement.
func TestPostgresStore_RateWindowSQL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)

	book := newTestBook(
		map[string]Rate3{"m": rate3("0.000003", "0.0000003", "0.00001")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	start := mustTime("2026-06-08T10:00:00Z")
	end := mustTime("2026-06-08T11:00:00Z")

	mock.ExpectBegin()
	mock.ExpectExec(`CREATE TEMP TABLE rating_price`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TEMP TABLE rating_derived`).WillReturnResult(sqlmock.NewResult(0, 0))
	// The projected price row is bound as NUMERIC strings (money never a Go float).
	mock.ExpectExec(`INSERT INTO rating_price`).
		WithArgs("m", "0.000003000", "0.000000300", "0.000010000").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// The DERIVED price row (base x premium) — identity premium here, so same rate as
	// the base, keyed on the base_model "m".
	mock.ExpectExec(`INSERT INTO rating_derived`).
		WithArgs("m", "0.000003000", "0.000000300", "0.000010000").
		WillReturnResult(sqlmock.NewResult(0, 1))
	rows := sqlmock.NewRows([]string{"rollups_written", "events_rated", "total_cost", "reconciled_deletions", "unpriced_events", "unattributable_events", "ambiguous_base_events"}).
		AddRow(2, 5, "0.001234500", 0, 3, 1, 4)
	// The statement binds $3 = the ft: LIKE pattern (single-sourced from fineTunePrefix).
	mock.ExpectQuery(`INSERT INTO rated_usage`).
		WithArgs(start.UTC(), end.UTC(), ftLikePattern).
		WillReturnRows(rows)
	mock.ExpectCommit()

	res, err := store.RateWindow(context.Background(), book, start, end)
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 2 || res.EventsRated != 5 || res.TotalCost != "0.001234500" {
		t.Fatalf("result = %+v, want 2/5/0.001234500", res)
	}
	if res.UnpricedEvents != 3 || res.UnattributableEvents != 1 || res.AmbiguousBaseEvents != 4 {
		t.Fatalf("anomaly counts = %d/%d/%d, want 3/1/4 (must ride the same statement)", res.UnpricedEvents, res.UnattributableEvents, res.AmbiguousBaseEvents)
	}
	if res.ReconciledDeletions != 0 {
		t.Fatalf("reconciled deletions = %d, want 0 (the projected count must scan into the result)", res.ReconciledDeletions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestRateWindow_NilBookFailsClosed: a nil price book is rejected before any DB work
// — the rater must load a price file first (never rate at $0).
func TestRateWindow_NilBookFailsClosed(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	if _, err := store.RateWindow(context.Background(), nil, mustTime("2026-06-08T10:00:00Z"), mustTime("2026-06-08T11:00:00Z")); err == nil {
		t.Fatal("nil price book accepted; want a fail-closed error")
	}
}

// TestRateWindowSQL_Shape locks the load-bearing SQL fragments into the statement
// constants (no DB needed): the window predicate, the YAML-projected price join, the
// billable-prompt clamp + formula, the applied-rate-on-row columns, the
// session-TZ-independent hour bucket, the deterministic id, and the idempotent
// upsert + anomaly counts.
func TestRateWindowSQL_Shape(t *testing.T) {
	wantFragments := []string{
		// window membership on the coalesced rating instant
		"COALESCE(event_ts, created_at) >= $1",
		"COALESCE(event_ts, created_at) <  $2",
		// the YAML-projected price table is joined (no model_price/derivation_policy)
		"LEFT JOIN rating_price rp ON rp.model_id = ev.model_id",
		// the DERIVED price table prices an ft: model_id via its event-carried
		// base_model (E3); direct-over-derived precedence + the ft: prefix guard
		"LEFT JOIN rating_derived rd",
		"rd.base_model = ev.base_model",
		"rp.model_id IS NULL",
		// ft: marker is single-sourced from the Go fineTunePrefix constant, bound as $3
		"ev.model_id LIKE $3",
		// the effective rate COALESCEs direct over derived
		"COALESCE(rp.prompt_price,     rd.prompt_price)",
		// billable-prompt clamp + the cost formula (cached charged once)
		"GREATEST(ev.prompt_tokens - ev.cached_tokens, 0)",
		"billable_prompt   * prompt_price",
		"cached_tokens     * cached_price",
		"completion_tokens * completion_price",
		// applied per-token rates frozen onto the rated_usage row (self-auditing)
		"applied_prompt_rate",
		"applied_cached_rate",
		"applied_completion_rate",
		// priced + attributable filter (never $0-bill unpriced/unattributable). A NULL
		// resource_id can't name the deployment/org (E2) → excluded + counted, never billed.
		"WHERE prompt_price IS NOT NULL",
		"AND auth_id     IS NOT NULL",
		"AND model_id    IS NOT NULL",
		// Anchor the grouped filter's resource_id guard to its GROUP BY (which uniquely
		// follows it), so this pins the priced/grouped clause specifically — not the bare
		// substring, which would also match the unpriced-count guard below.
		"AND resource_id IS NOT NULL\n    GROUP BY auth_id, resource_id, model_id",
		// session-TZ-independent hour bucket
		"date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'",
		// deterministic natural-key surrogate id (re-runs regenerate the same id),
		// LENGTH-PREFIXED so a '|' in a field can never collide two keys; resource_id is
		// part of the key, in fixed order (auth_id, resource_id, model_id, window_start)
		"md5(length(auth_id)::text || ':' || auth_id",
		"|| '|' || length(resource_id)::text || ':' || resource_id",
		"|| '|' || length(model_id)::text || ':' || model_id",
		// deterministic lock order across concurrent raters (no ABBA deadlock)
		"ORDER BY auth_id, resource_id, model_id, window_start",
		// idempotent upsert on the natural key
		"ON CONFLICT (auth_id, resource_id, model_id, window_start) DO UPDATE SET",
		// RE-RATE RECONCILES (FIX 2): the `deleted` CTE removes any in-window rollup this
		// run did NOT reproduce in priced, atomically with the upsert — so a superseded
		// rollup cannot keep billing at its stale cost. Window-scoped + NOT EXISTS priced.
		"DELETE FROM rated_usage ru",
		"ru.window_start >= $1",
		"ru.window_start <  $2",
		"NOT EXISTS (",
		"FROM priced p",
		// the reconcile anti-join keys on the FULL grain incl. resource_id, so it matches
		// the new unique constraint exactly (a deployment that fell out is reconciled)
		"p.resource_id  = ru.resource_id",
		"AS reconciled_deletions",
		// the anomaly counts ride the SAME statement (one snapshot) as the upsert; a NULL
		// resource_id is counted UNATTRIBUTABLE (fail closed), never billed.
		// PARTITION EXCLUSIVITY: the unpriced count must require FULL attribution
		// (auth_id, resource_id, model_id all NON-NULL) so a NULL-resource_id unpriced row
		// is counted ONLY as unattributable, never double-counted. Pin the whole
		// contiguous WHERE so the resource_id guard is anchored to THIS count clause — a
		// bare "AND resource_id IS NOT NULL" would also match the grouped/priced filter and
		// wouldn't catch the guard being dropped from the unpriced count.
		"WHERE prompt_price  IS NULL\n        AND auth_id     IS NOT NULL\n        AND resource_id IS NOT NULL\n        AND model_id    IS NOT NULL)",
		"AS unpriced_events",
		"OR resource_id IS NULL OR model_id IS NULL) AS unattributable_events",
		// the E3 ft-uniqueness gate: an ft: rollup spanning >1 base_model is split out
		"COUNT(DISTINCT base_model) FILTER (WHERE via_derived) > 1 AS ambiguous_base",
		"WHERE NOT ambiguous_base",
		"AS ambiguous_base_events",
	}
	// The price tables are GONE (prices are YAML now): no reference to model_price,
	// derivation_policy, effective-dating, or a derivation CASE may remain.
	for _, gone := range []string{"model_price", "derivation_policy", "effective_from", "effective_to", "der.derived_from", "pol.factor"} {
		if strings.Contains(rateWindowSQL, gone) {
			t.Errorf("rateWindowSQL still references removed price-table machinery: %q", gone)
		}
	}
	// The session-TZ-DEPENDENT bucket must be gone everywhere.
	if strings.Contains(rateWindowSQL, "date_trunc('hour', ev_ts)") {
		t.Error("rateWindowSQL still contains the session-TZ-dependent date_trunc('hour', ev_ts)")
	}
	for _, f := range wantFragments {
		if !strings.Contains(rateWindowSQL, f) {
			t.Errorf("rateWindowSQL missing fragment: %q", f)
		}
	}
}

// TestEnsureUTCTimeZone: the belt-and-braces DSN pin appends timezone=UTC unless the
// operator already chose a TZ. The bucketing expression is the load-bearing fix; this
// is defense in depth.
func TestEnsureUTCTimeZone(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgres://u:p@h/db", "postgres://u:p@h/db?timezone=UTC"},
		{"postgres://u:p@h/db?sslmode=disable", "postgres://u:p@h/db?sslmode=disable&timezone=UTC"},
		{"host=h dbname=db", "host=h dbname=db timezone=UTC"},
		{"postgres://u:p@h/db?timezone=America/New_York", "postgres://u:p@h/db?timezone=America/New_York"},
		{"host=h TimeZone=UTC", "host=h TimeZone=UTC"},
	}
	for _, c := range cases {
		if got := ensureUTCTimeZone(c.in); got != c.want {
			t.Errorf("ensureUTCTimeZone(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
