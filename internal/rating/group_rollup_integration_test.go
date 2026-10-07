//go:build integration

package rating

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestIntegration_GroupUsageAttribution runs the REAL rating SQL against a live
// Postgres and pins the group attribution rollup (group_usage, migration 0008)
// the admission group spend check reads:
//
//   - an event is attributed to its token's own group (group-token case) PLUS
//     every group in member_group_ids (membership case): a user in two groups
//     contributes to both rows;
//   - an event whose own group also appears in member_group_ids contributes
//     ONCE to that group (dedupe per (event, group));
//   - WITHHELD (non-money) events contribute nothing to any group row;
//   - the rollup's cost is the summed per-event cost at the applied rate
//     (asserted against the Rate() oracle), and the same event cost
//     legitimately appears under every attributed group (attribution, not
//     money — rated_total must not double-count it);
//   - re-running the window is idempotent (same sums, zero reconcile
//     deletions), and deleting an event re-rates: its groups' rows shrink or
//     are reconcile-deleted.
func TestIntegration_GroupUsageAttribution(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_group_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0.0000005", "0.00002")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	rate, err := book.Resolve("b")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	billed := rate.Quantized()

	const (
		g1 = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
		g2 = "00112233445566778899aabbccddeeff"
	)

	// groupEvent is a seeded billing_event with group attribution set.
	type groupEvent struct {
		auth       string
		group      string   // billing_event.group_id (the token's own group)
		members    []string // billing_event.member_group_ids
		prompt     int
		cached     int
		completion int
		usage      bool
	}
	events := []groupEvent{
		// 0: a GROUP TOKEN's traffic attributes to the token's own group.
		{auth: "a-grp", group: g1, prompt: 100, cached: 30, completion: 50, usage: true},
		// 1: a USER TOKEN belonging to two groups attributes to BOTH.
		{auth: "a-user", members: []string{g1, g2}, prompt: 10, usage: true},
		// 2: WITHHELD (no authoritative usage): money excludes it, so must the
		// attribution — even though it names g1.
		{auth: "a-dead", members: []string{g1}, usage: false},
		// 3: an event whose own group ALSO appears in its membership list
		// attributes to that group exactly ONCE.
		{auth: "a-both", group: g1, members: []string{g1}, prompt: 5, usage: true},
	}
	for i, e := range events {
		_, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, group_id, member_group_ids, resource_id, model, serving_mode, usage_found, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			fmt.Sprintf("req-%d", i), nullableStr(e.auth), nullableStr(e.group), groupIDsOrNil(e.members),
			"r", "b", "dedicated", e.usage, e.prompt, e.cached, e.completion, hour.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	// Oracle per-event costs (the same events the money rollup bills).
	moneyEvents := []RatedEvent{
		{AuthID: events[0].auth, ResourceID: "r", ModelID: "b", ServingMode: "dedicated", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50},
		{AuthID: events[1].auth, ResourceID: "r", ModelID: "b", ServingMode: "dedicated", PromptTokens: 10},
		{AuthID: events[3].auth, ResourceID: "r", ModelID: "b", ServingMode: "dedicated", PromptTokens: 5},
	}
	costs := make([]Dec, len(moneyEvents))
	for i, e := range moneyEvents {
		costs[i] = Rate(e, billed)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// Money is written exactly once per window, untouched by the attribution:
	// three priced events → three rated_usage rows, and the window total is the
	// sum of the three per-event costs (no group double-count in the money).
	if res.RollupsWritten != 3 {
		t.Fatalf("money rollups = %d, want 3 (the withheld event never enters money)", res.RollupsWritten)
	}
	wantTotal := costs[0].Add(costs[1]).Add(costs[2]).String()
	if MustDec(res.TotalCost).String() != wantTotal {
		t.Fatalf("total cost = %s, want %s (attribution must not touch the money)", res.TotalCost, wantTotal)
	}
	if res.EventsRated != 3 || res.MissingUsageEvents != 1 {
		t.Fatalf("rated/missing = %d/%d, want 3/1", res.EventsRated, res.MissingUsageEvents)
	}
	if res.GroupRollupsWritten != 2 {
		t.Fatalf("group rollups = %d, want 2 (groups %s and %s)", res.GroupRollupsWritten, g1[:8], g2[:8])
	}

	// Attribution rows: g1 carries events 0, 1, 3 (event 2 is withheld, event 3
	// dedupes); g2 carries event 1 only.
	type groupRow struct {
		prompt, cached, completion, billable, count int64
		cost                                        string
	}
	readRow := func(gid string) (groupRow, bool) {
		var r groupRow
		err := db.QueryRowContext(ctx,
			`SELECT prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, event_count, cost::text
			   FROM group_usage WHERE group_id=$1 AND window_start=$2`, gid, hour).
			Scan(&r.prompt, &r.cached, &r.completion, &r.billable, &r.count, &r.cost)
		if errors.Is(err, sql.ErrNoRows) {
			return groupRow{}, false
		}
		if err != nil {
			t.Fatalf("read group_usage %s: %v", gid, err)
		}
		return r, true
	}

	g1WantCost := costs[0].Add(costs[1]).Add(costs[2]).String()
	g2WantCost := costs[1].String()
	r1, ok := readRow(g1)
	if !ok {
		t.Fatalf("no group_usage row for %s", g1)
	}
	if r1.count != 3 || r1.prompt != 115 || r1.cached != 30 || r1.completion != 50 || r1.billable != 85 {
		t.Fatalf("group %s row = %+v, want count 3 / prompt 115 / cached 30 / completion 50 / billable 85", g1, r1)
	}
	if MustDec(r1.cost).String() != g1WantCost {
		t.Fatalf("group %s cost = %s, want %s (oracle)", g1, r1.cost, g1WantCost)
	}
	r2, ok := readRow(g2)
	if !ok {
		t.Fatalf("no group_usage row for %s", g2)
	}
	if r2.count != 1 || r2.prompt != 10 || MustDec(r2.cost).String() != g2WantCost {
		t.Fatalf("group %s row = %+v, want count 1 / prompt 10 / cost %s (oracle)", g2, r2, g2WantCost)
	}

	// Idempotent re-run: same rows, same sums, nothing reconciled away.
	res2, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if res2.GroupRollupsWritten != 2 || res2.GroupReconciledDeletions != 0 {
		t.Fatalf("re-run group rollups/deletions = %d/%d, want 2/0 (idempotent)", res2.GroupRollupsWritten, res2.GroupReconciledDeletions)
	}
	if r1b, _ := readRow(g1); MustDec(r1b.cost).String() != g1WantCost || r1b.count != 3 {
		t.Fatalf("re-run changed the %s row: %+v", g1, r1b)
	}

	// Reconcile: the membership event (req-1) vanishes (data-loss drill). Its
	// groups' rows shrink accordingly and g2's row is deleted outright.
	exec(t, db, `DELETE FROM billing_event WHERE request_id='req-1'`)
	res3, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("re-rate after deletion: %v", err)
	}
	if _, ok := readRow(g2); ok {
		t.Fatalf("group %s row survived the reconcile, want deleted (its only event vanished)", g2)
	}
	if res3.GroupReconciledDeletions != 1 {
		t.Fatalf("group reconciled deletions = %d, want 1 (the stale %s row)", res3.GroupReconciledDeletions, g2[:8])
	}
	r1c, ok := readRow(g1)
	if !ok {
		t.Fatalf("group %s row vanished, want it retained (events 0 and 3 remain)", g1)
	}
	g1AfterCost := costs[0].Add(costs[2]).String()
	if r1c.count != 2 || MustDec(r1c.cost).String() != g1AfterCost {
		t.Fatalf("group %s row after reconcile = %+v, want count 2 / cost %s", g1, r1c, g1AfterCost)
	}
}

// groupIDsOrNil binds a membership list as a driver value (nil for none —
// mirrors the drainer's nullGroupIDs so the fixture writes what production
// writes).
func groupIDsOrNil(ids []string) any {
	if len(ids) == 0 {
		return nil
	}
	return ids
}
