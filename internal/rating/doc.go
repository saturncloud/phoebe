// Package rating is phoebe's REVENUE path. It turns the raw token counts in
// billing_event into money: per (auth_id, model_id, hour) cost rollups in
// rated_usage, computed by joining an effective-dated price book and a global
// fine-tune derivation policy.
//
// MONEY CORRECTNESS is the entire product. The invariants enforced here, each of
// which is a way to silently get a customer's bill wrong:
//
//   - Money is NUMERIC (exact base-10 decimal) in Postgres, NEVER float and NEVER
//     an integer micro/nano scalar. All money MATH happens in SQL, not Go — the
//     rater computes per-event cost AND sums it in one statement; Go never holds a
//     running money total. See store.go.
//   - Prices are per-token NUMERIC, keyed on a STABLE model_id (not a deployment
//     id, not a name), from the price book.
//   - cached tokens are a DISTINCT rate and a SUBSET of prompt tokens — the
//     billable-prompt formula must not double-count them. This is the single
//     highest-risk line in the codebase.
//   - Prices are effective-dated: an event is rated with the price in effect at
//     the event's time, never retroactively repriced.
//   - A fine-tune (no own rate, derived_from set) inherits the base's effective
//     rate transformed by the global derivation policy — a POINTER, not a copy.
//   - A model with no resolvable price FAILS LOUD (ErrNoPrice) — it is NEVER
//     silently billed $0 (that is lost revenue). This is the fail-closed rule.
//     The anomaly counts come from the SAME SQL statement (same snapshot) as the
//     rollup upsert, so a row committed mid-run can never be excluded from the
//     rollups yet missed by the counts.
//   - LATE-DRAINED EVENTS ARE RE-CAUGHT by the trailing default window: cmd/rater
//     re-rates the trailing N complete hours (default 24) every run, and the
//     upsert REPLACES each hour bucket with a recomputed total, so an event that
//     lands in an already-rated hour (Valkey outage → WAL recovery) is folded in
//     by a later run without double-counting. RESIDUAL RISK: an event arriving
//     more than N hours late still slips the default window — the reconciliation
//     backstop sketched in DESIGN.md is the eventual answer; until then, widen
//     rateTrailingHours or re-rate the hour explicitly via --since/--until.
//   - Hour bucketing is SESSION-TZ-INDEPENDENT (date_trunc over a UTC wall-clock
//     timestamp), so rollup keys can never disagree across sessions and re-rates
//     can never create overlapping buckets.
//
// PRODUCTION vs ORACLE — where the money math lives:
//
// The production rater is PURE SQL (store.go): one INSERT…SELECT joins the price
// book and derivation policy, computes per-event cost, and SUMs it, all in
// Postgres. No Go code multiplies a price or holds a money total on the
// production path.
//
// The Go cost formula — Rate(), the Dec exact-decimal type, the in-memory
// PriceBook resolver, ApplyPolicy — is an ORACLE: a reference reimplementation
// used ONLY to pin the SQL. It lives entirely in _test.go files (oracle_test.go,
// resolve_test.go) so the compiler guarantees it never ships in a binary. The
// //go:build integration tests in store_integration_test.go run the REAL
// rateWindowSQL against a live Postgres and assert it matches the oracle
// row-for-row — including the sum-then-round behavior (the oracle sums the EXACT
// per-event costs and rounds ONCE, exactly as the SQL's SUM→NUMERIC(20,9) does).
// Those run in CI's integration-test job. If you are looking for "what bills a
// customer," it is the SQL in store.go — not the Go.
package rating
