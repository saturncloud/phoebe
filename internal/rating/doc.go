// Package rating aggregates phoebe's raw metering evidence into hourly token
// usage. Despite the name (kept so the binary, chart and table names stay
// stable), it no longer rates: since the billing unification (billing-design.md
// D1, ruled by Hugo 2026-10-22) the manager owns every price and rates each
// hour as of that hour. phoebe sends quantities and the facts a price depends
// on, never money.
//
// WHAT IT DOES. cmd/rater runs Aggregator.Run hourly over the trailing N hours.
// One SQL statement (store.go) reads billing_event, keeps the events that can
// become a record, sums them into rated_usage at the token grain of
// billing-design.md section 3:
//
//	(auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
//	 graph_k8s_name, topology_revision, hour)
//
// carrying org_id, region, base_model and adapter on each rollup, deletes
// in-window rollups it no longer produces, and counts every event it kept out.
// cmd/token-push then sends each hour's rollups to the manager
// (internal/meteredusage).
//
// THE CHECKS THAT NEED RAW EVENTS STAY HERE (D1). Only the install sees single
// events, so only it can tell a clean rollup from one assembled out of
// contradictory evidence. All fail closed, in two ways:
//
//   - Evidence that cannot become any record (unattributable, invalid serving
//     mode, invalid usage) is withheld per event, counted, logged at ERROR, and
//     drives cmd/rater's exit 2. The raw events stay in billing_event.
//   - CONFLICTS are PARKED per identity (auth, owner, resource, model, serving
//     mode) and hour in parked_usage, with their token counts: an owner conflict
//     (per event), an ambiguous org or ambiguous base model / adapter (across
//     graph revisions), and a missing org. Only the conflicted identity is held
//     back; the rest of the hour is aggregated and sent on time. Parked usage is
//     never dropped: it is logged at ERROR every run (cmd/rater exit 4), parked
//     hours from the last ParkedRescanReach are re-checked every run, and a
//     resolved one is aggregated and re-sent by token-push however old.
//
// An absent base model is NOT withheld: a dedicated endpoint may serve a model
// with no catalog base (D3). It is sent as "" with a warning, and the manager
// prices it.
//
// IDEMPOTENT. Re-running any window converges rated_usage to that run's output,
// and a re-sent hour replaces the manager's copy (usage_id is deterministic), so
// cron overlap or a manual backfill never double-counts.
package rating
