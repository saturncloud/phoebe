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

// TestIntegration_GroupUsageWithheldGatesSharedGrain pins the per-event gates
// the attributed CTE re-applies on the resolved side (store.go:819-826) against
// real Postgres. The sibling test above withholds only a usage_found event, and
// every seeded event has its own auth_id, so every event sits in its own money
// grain. That leaves the other gates unexercised: the attributed CTE joins
// priced rollups back onto resolved events on the full grain, and with no
// withheld event sharing a priced rollup's grain, dropping any one of those
// gates would change nothing — the test stays green while a withheld event
// rides its priced twin's rollup into group_usage and inflates the group spend
// the admission check reads.
//
// So every withheld event below SHARES its (auth, resource, model,
// serving_mode, hour) money grain with a priced event:
//
//   - unpriced twin: model 'm-unp' prices through base_model 'b' for one event
//     (C4 rung (c)); its twin names an unknown base, so its prompt_price is
//     NULL and only the prompt_price gate keeps it out of the gA row;
//   - owner-conflict twin: one event carries user_id AND group_id, collapses
//     to the same user/group owner bucket as its clean twin, and is dropped per
//     event — only the owner_conflict gate keeps its memberships (and its own
//     group_id) out of group_usage;
//   - ambiguous_org: one resource carrying two distinct orgs is withheld at
//     the rollup level, so neither clean event may attribute to gC at all;
//   - identical pair: two genuinely different but identical-looking billable
//     events in one rollup must BOTH attribute to gD — the DISTINCT ON
//     (request_id, gid) dedupe must not collapse them into one.
//
// For each withheld case the group's group_usage row must carry exactly its
// priced event(s), and the money rollup must be what a baseline WITHOUT the
// withheld events would write: same rollups, same events rated, same total.
func TestIntegration_GroupUsageWithheldGatesSharedGrain(t *testing.T) {
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

	const sch = "phoebe_rating_group_gates_it"
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
		gA = "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"
		gB = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b"
		gC = "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c"
		gD = "d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d"
	)

	// Every event is dedicated with authoritative usage; each withheld event is
	// kept out of money by exactly ONE classification (its price, its owner
	// conflict, or its rollup's two orgs) and shares its money grain with the
	// priced event that must still attribute.
	type gateEvent struct {
		req      string
		auth     string
		user     string   // billing_event.user_id ('' writes NULL)
		group    string   // billing_event.group_id (the token's own group)
		members  []string // billing_event.member_group_ids
		resource string
		org      string // billing_event.org_id ('' writes NULL)
		model    string
		base     string // billing_event.base_model ('' writes NULL)
		prompt   int
	}
	events := []gateEvent{
		// (a) the unpriced twin: 'ga-ok' prices model 'm-unp' through base 'b'
		// (C4 rung (c)); 'ga-bad' names base 'zz-unpriced-base', which has no
		// price row, so its prompt_price is NULL and it is withheld as unpriced.
		{req: "ga-ok", auth: "a-unp", members: []string{gA}, resource: "r-unp", model: "m-unp", base: "b", prompt: 40},
		{req: "ga-bad", auth: "a-unp", members: []string{gA}, resource: "r-unp", model: "m-unp", base: "zz-unpriced-base", prompt: 999},
		// (b) the owner-conflict twin: 'gb-bad' carries user_id AND group_id,
		// which collapses its owner to ''/'' — the same bucket as 'gb-ok' — and
		// drops it from money per event. Its memberships and its own group must
		// not reach group_usage.
		{req: "gb-ok", auth: "a-conf", members: []string{gB}, resource: "r-conf", model: "b", prompt: 25},
		{req: "gb-bad", auth: "a-conf", user: "u-x", group: "g-conflicted", members: []string{gB}, resource: "r-conf", model: "b", prompt: 777},
		// (c) ambiguous_org: one resource, two distinct orgs — the rollup is
		// withheld as a whole, so neither clean event may attribute to gC.
		{req: "gc-1", auth: "a-org", members: []string{gC}, resource: "r-org", org: "org-x", model: "b", prompt: 50},
		{req: "gc-2", auth: "a-org", members: []string{gC}, resource: "r-org", org: "org-y", model: "b", prompt: 50},
		// (d) the identical pair: same grain, same counts, same group — both
		// events must attribute to gD; dropping request_id from the DISTINCT ON
		// key would collapse them into one.
		{req: "gd-1", auth: "a-twin", members: []string{gD}, resource: "r-twin", model: "b", prompt: 60},
		{req: "gd-2", auth: "a-twin", members: []string{gD}, resource: "r-twin", model: "b", prompt: 60},
	}
	for i, e := range events {
		_, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, user_id, group_id, member_group_ids, resource_id, org_id, model, base_model, serving_mode, usage_found, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'dedicated',true,$10,0,0,$11)`,
			e.req, nullableStr(e.auth), nullableStr(e.user), nullableStr(e.group), groupIDsOrNil(e.members),
			nullableStr(e.resource), nullableStr(e.org), nullableStr(e.model), nullableStr(e.base),
			e.prompt, hour.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatalf("seed event %d (%s): %v", i, e.req, err)
		}
	}

	// Oracle for the money baseline WITHOUT the withheld events: 'ga-ok' prices
	// at the base 'b' rate through C4 rung (c), the others price model 'b'
	// directly — all four resolve to the same quantized rate.
	moneyEvents := []RatedEvent{
		{AuthID: "a-unp", ResourceID: "r-unp", ModelID: "m-unp", BaseModel: "b", ServingMode: "dedicated", PromptTokens: 40},
		{AuthID: "a-conf", ResourceID: "r-conf", ModelID: "b", ServingMode: "dedicated", PromptTokens: 25},
		{AuthID: "a-twin", ResourceID: "r-twin", ModelID: "b", ServingMode: "dedicated", PromptTokens: 60},
		{AuthID: "a-twin", ResourceID: "r-twin", ModelID: "b", ServingMode: "dedicated", PromptTokens: 60},
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

	// Money is exactly the priced baseline: the three priced rollups (one per
	// grain), four rated events, and a total equal to the sum of the four
	// oracle costs. The withheld events changed nothing.
	if res.RollupsWritten != 3 || res.EventsRated != 4 {
		t.Fatalf("rollups/events = %d/%d, want 3/4 (the priced baseline only)", res.RollupsWritten, res.EventsRated)
	}
	wantTotal := costs[0].Add(costs[1]).Add(costs[2]).Add(costs[3]).String()
	if MustDec(res.TotalCost).String() != wantTotal {
		t.Fatalf("total cost = %s, want %s (the priced baseline only)", res.TotalCost, wantTotal)
	}
	// The withheld events are classified, not silently dropped: the anomaly
	// partition accounts for all 8 seeded events exactly once.
	if res.UnpricedEvents != 1 || res.OwnerConflictEvents != 1 || res.AmbiguousOrgEvents != 2 {
		t.Fatalf("unpriced/conflict/ambig-org = %d/%d/%d, want 1/1/2",
			res.UnpricedEvents, res.OwnerConflictEvents, res.AmbiguousOrgEvents)
	}
	if got := res.EventsRated + res.MissingUsageEvents + res.InvalidUsageEvents +
		res.UnpricedEvents + res.UnattributableEvents + res.InvalidServingModeEvents +
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents + res.OwnerConflictEvents; got != 8 {
		t.Fatalf("anomaly partition = %d, want 8 (all seeded events accounted exactly once)", got)
	}
	if res.GroupRollupsWritten != 3 {
		t.Fatalf("group rollups = %d, want 3 (groups %s, %s, %s)",
			res.GroupRollupsWritten, gA[:8], gB[:8], gD[:8])
	}

	type groupRow struct {
		prompt, billable, count int64
		cost                    string
	}
	readRow := func(gid string) (groupRow, bool) {
		var r groupRow
		err := db.QueryRowContext(ctx,
			`SELECT prompt_tokens, billable_prompt_tokens, event_count, cost::text
			   FROM group_usage WHERE group_id=$1 AND window_start=$2`, gid, hour).
			Scan(&r.prompt, &r.billable, &r.count, &r.cost)
		if errors.Is(err, sql.ErrNoRows) {
			return groupRow{}, false
		}
		if err != nil {
			t.Fatalf("read group_usage %s: %v", gid, err)
		}
		return r, true
	}

	// (a) gA carries 'ga-ok' alone: dropping the prompt_price gate would let
	// 'ga-bad' ride its twin's rollup in (count 2, prompt 1039).
	rA, ok := readRow(gA)
	if !ok {
		t.Fatalf("no group_usage row for %s", gA)
	}
	if rA.count != 1 || rA.prompt != 40 || rA.billable != 40 || MustDec(rA.cost).String() != costs[0].String() {
		t.Fatalf("group %s row = %+v, want count 1 / prompt 40 / billable 40 / cost %s (the priced twin only)",
			gA, rA, costs[0])
	}

	// (b) gB carries 'gb-ok' alone: dropping the owner_conflict gate would add
	// 'gb-bad' (count 2, prompt 802) and create a row for its own group.
	rB, ok := readRow(gB)
	if !ok {
		t.Fatalf("no group_usage row for %s", gB)
	}
	if rB.count != 1 || rB.prompt != 25 || MustDec(rB.cost).String() != costs[1].String() {
		t.Fatalf("group %s row = %+v, want count 1 / prompt 25 / cost %s (the clean twin only)", gB, rB, costs[1])
	}
	if _, ok := readRow("g-conflicted"); ok {
		t.Fatalf("the conflicted event's own group reached group_usage; the owner_conflict twin must attribute nowhere")
	}

	// (c) the ambiguous-org rollup has no money row and no attribution row.
	if _, ok := readRow(gC); ok {
		t.Fatalf("group %s row exists, want none (its rollup is ambiguous-org withheld)", gC)
	}
	var nOrgRollups int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rated_usage WHERE resource_id='r-org'`).Scan(&nOrgRollups); err != nil {
		t.Fatalf("count r-org rollups: %v", err)
	}
	if nOrgRollups != 0 {
		t.Fatalf("rated_usage has %d rows for r-org, want 0 (ambiguous-org withheld)", nOrgRollups)
	}

	// (d) gD carries BOTH identical events: per-event dedupe would show count 1.
	rD, ok := readRow(gD)
	if !ok {
		t.Fatalf("no group_usage row for %s", gD)
	}
	twinCost := costs[2].Add(costs[3]).String()
	if rD.count != 2 || rD.prompt != 120 || MustDec(rD.cost).String() != twinCost {
		t.Fatalf("group %s row = %+v, want count 2 / prompt 120 / cost %s (both events attribute)",
			gD, rD, twinCost)
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
