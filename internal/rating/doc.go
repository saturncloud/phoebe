// Package rating is phoebe's REVENUE path. It turns the raw token counts in
// billing_event into money: per (auth_id, model_id, hour) cost rollups in
// rated_usage, priced from a YAML PRICE FILE (E1) — not a DB price table.
//
// THE PRICE FILE IS THE CONTRACT (E1). An operator authors a versioned YAML file
// (see config/prices.example.yaml) carrying, per E1/E3:
//
//   - base model prices keyed on the Hugging Face model id (e.g.
//     "meta-llama/Llama-3.1-8B-Instruct"), each a prompt/cached/completion per-token
//     rate written as an EXACT DECIMAL STRING (parsed to the exact Dec, never float);
//   - the single global fine-tune premium policy (identity | multiplier | markup);
//   - per-GPU floor rates keyed on GPU type (parsed + validated now for completeness,
//     consumed by the uptime meter later — the token rater does not yet use them).
//
// The file's version history IS the price audit trail: there is no price table, no
// effective-dating, no GiST exclusion constraint, no operator-writes-to-DB authz
// surface. LoadPriceBook reads and validates the file, FAILING CLOSED on anything
// malformed (missing file, bad YAML, unknown version, a float-shaped or negative
// rate, an inconsistent premium) — the rater refuses to run rather than rate at $0.
//
// MONEY CORRECTNESS is the entire product. The invariants enforced here, each a way
// to silently get a customer's bill wrong:
//
//   - Money is NUMERIC (exact base-10 decimal) in Postgres, NEVER float and NEVER an
//     integer micro/nano scalar. Per-token rates parse from the YAML into the exact
//     Dec type (big.Rat); the production cost MATH happens in SQL — the rater projects
//     the rates into a transient TEMP table and computes per-event cost AND sums it in
//     one statement. Go never holds a running money total. See store.go.
//   - Prices are per-token, keyed on a STABLE model_id (HF base id, or ft:<checkpoint>
//     for a fine-tune — never a deployment id, never a mutable display name).
//   - cached tokens are a DISTINCT rate and a SUBSET of prompt tokens — the
//     billable-prompt formula must not double-count them. The single highest-risk line.
//   - A fine-tune (ft: id) inherits its base's rate transformed by the global premium
//     — a POINTER, not a copy (change a base price and every fine-tune re-prices).
//   - The APPLIED per-token rate is FROZEN onto each rated_usage row
//     (applied_prompt_rate / applied_cached_rate / applied_completion_rate). The row is
//     then immutable and self-auditing; re-rating is a deliberate, audited re-run, never
//     a silent consequence of editing the file. "We never reprice traffic you've already
//     served" holds by construction.
//   - A model with no resolvable price FAILS LOUD (ErrNoPrice / the unpriced count) —
//     NEVER silently billed $0 (that is lost revenue). The E4 create-time price gate
//     should prevent unpriced traffic from ever being served, but the rater keeps this
//     fail-loud backstop. The anomaly counts come from the SAME SQL statement (same
//     snapshot) as the rollup upsert, so a row committed mid-run can never be excluded
//     from the rollups yet missed by the counts.
//   - LATE-DRAINED EVENTS ARE RE-CAUGHT by the trailing default window: cmd/rater
//     re-rates the trailing N complete hours (default 24) every run, and the upsert
//     REPLACES each hour bucket with a recomputed total, so an event landing in an
//     already-rated hour is folded in without double-counting. RESIDUAL RISK: an event
//     arriving more than N hours late still slips the window; widen rateTrailingHours or
//     re-rate explicitly via --since/--until.
//   - Hour bucketing is SESSION-TZ-INDEPENDENT (date_trunc over a UTC wall-clock
//     timestamp), so rollup keys can never disagree across sessions and re-rates can
//     never create overlapping buckets.
//
// FINE-TUNE BASE-LINKAGE GAP (flagged): billing_event today carries only the
// engine-reported model NAME (migrations/0001_billing_event.sql has no derived_from /
// base_model column). So a fine-tune's base is NOT plumbed through to the rater. A
// base-direct model (model_id IS a base_models key in the file) prices fully. An
// ft:<checkpoint> id prices ONLY if the file declares its derived_from (or its own
// rate); otherwise it is UNPRICED — fail loud, never $0. Closing the gap means the
// metering path stamping the base (from saturn.io/...base_model) onto the event, OR
// shipping a fine-tune→base map in the file. The pricing/premium machinery is complete
// and tested; only the linkage source is pending.
//
// PRODUCTION vs ORACLE — where the money math lives:
//
// The production rater is PURE SQL (store.go): one INSERT…SELECT joins the
// YAML-projected price table, computes per-event cost, and SUMs it, all in Postgres.
// No Go code multiplies a price or holds a money total on the production path (the
// fine-tune premium is the one exception — it is applied in exact Dec when the prices
// are projected, then handed to SQL as a NUMERIC string).
//
// The Go cost formula — Rate(), rateExact, BillablePromptTokens — is an ORACLE: a
// reference reimplementation used ONLY to pin the SQL. It lives in _test.go files
// (oracle_test.go) so the compiler guarantees it never ships in a binary. The
// //go:build integration tests in store_integration_test.go run the REAL rateWindowSQL
// against a live Postgres and assert it matches the oracle row-for-row, including the
// sum-then-round behavior. If you are looking for "what bills a customer," it is the
// SQL in store.go and the prices in the YAML file — not the Go.
package rating
