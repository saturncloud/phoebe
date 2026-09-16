# Token Factory billing reconciliation and recovery

This runbook describes the deployed `release-2026.08.01` path and the boundary
between guarantees Phoebe owns and guarantees that require the edge, chart, or
central billing service.

## Request-to-invoice data flow

1. Traefik ForwardAuth strips caller-supplied `X-Saturn-*` headers and injects
   authenticated identity, organization, resource, upstream, base-model, adapter,
   and serving-mode headers. Saturn creates those route headers when the endpoint
   is created.
2. Phoebe rejects requests missing the mandatory attribution fields, stores the
   caller's `X-Request-Id` only as `client_request_id`, and mints a cryptographically
   random `request_id` for the billable execution attempt. Each client retry is a
   new attempt; redelivery of one metering event retains the same attempt id.
3. Phoebe forces streaming usage on, forwards the request, and records the serving
   engine's prompt, cached-prompt, and completion counts. It never re-tokenizes.
   Completed, streamed, aborted, and failed upstream attempts all produce one raw
   event. Missing usage is an explicit `usage_found=false` zero-charge row, not a
   silently dropped event or guessed token count.
4. The emitter delivers through Valkey Streams, falling back to its fsync'd WAL and
   then a structured `METERING_FLOOR` log. The drainer acknowledges Valkey only after
   the Postgres transaction commits and deduplicates redelivery on the server-minted
   attempt id.
5. The rater resolves prices and writes hourly `rated_usage` rows. Cached input is
   charged once: `fresh = prompt - cached`. The first applied rate for each
   natural-key/hour is retained in append-only `rating_price_lock`; it survives
   rated-row reconciliation deletion, so late or recovered events use the original
   rate even after the current price book changes. Attempts without authoritative
   engine usage are excluded from money and make the rater exit non-zero.
6. `token-push` sends authoritative hourly snapshots keyed by `rated_usage.id` to
   the central manager. Replays are deterministic; an unattributable window is
   withheld in full instead of partially deleting or mis-attributing prior billing.
   The central manager owns invoice-line creation and must expose the same ids for
   the final rated-row-to-invoice comparison.

## Routine checks and alert conditions

Run these against Phoebe Postgres for every closed hour before treating an invoice
as settled:

```sql
SELECT *
FROM billing_reconciliation_hourly
WHERE window_start >= :start AND window_start < :end
  AND (
      missing_usage_attempts <> 0
      OR invalid_usage_attempts <> 0
      OR missing_org_attempts <> 0
      OR distinct_org_ids > 1
      OR attempt_delta <> missing_usage_attempts
      OR prompt_token_delta <> 0
      OR fresh_input_token_delta <> 0
      OR cached_token_delta <> 0
      OR completion_token_delta <> 0
  )
ORDER BY window_start, org_id, resource_id, model_id;
```

`fresh_input_tokens` is the checked `prompt_tokens - cached_tokens` subset; all
three billable classes remain independently reconcilable. `missing_usage_attempts`
is not automatically revenue loss: it means the engine did
not provide authoritative counts, so Phoebe charged zero and requires engine-log
review. Page on any new missing-usage row, rater anomaly/non-zero exit, reconcile
deletion during a routine run, drainer poison row, `METERING_FLOOR`, WAL corruption,
token-push withheld window, or push failure. Alert separately when the oldest Valkey
pending entry, oldest WAL entry, or oldest unpushed rated hour exceeds two job
periods. The deployment/chart owns those queue-age metrics.

The view groups raw evidence at the exact rated natural key and exposes
`missing_org_attempts` plus `distinct_org_ids` separately. A rollout-era mix of one
NULL and one real org therefore reconciles to one rated row without false token
deltas, while conflicting non-NULL orgs remain explicit.

For the invoice boundary, export `rated_usage.id`, `window_start`, `org_id`, and
`cost` for the same interval and compare it to the central manager's received-rollup
and invoice-line exports. Require set equality on id and exact NUMERIC equality on
cost; compare totals only after the row-level set matches. A total can accidentally
balance while two customers are mis-attributed.

## Repair and replay

1. Quiesce the affected invoice window in the central manager. Do not delete local
   raw events or edit rated money manually.
2. Recover any `METERING_FLOOR` JSON or quarantined/imported WAL entries into the
   configured Valkey stream with the original `request_id`. Duplicate recovery is
   safe; changing the id is not.
3. Run the drainer until the consumer group has no pending/lagging entries. Confirm
   the recovered attempt ids exist once in `billing_event`.
4. Restore the price-book version or attribution headers if the natural-key/hour
   was never locked. For a previously rated key, `rating_price_lock` is the authority
   and a normal re-rate cannot overwrite it, even if reconciliation deleted the
   corresponding `rated_usage` row.
5. Run the rater explicitly for the complete affected half-open window. Investigate
   every unpriced, unattributable, ambiguous, missing-usage, or reconcile-deletion
   signal before proceeding.
6. Re-run the hourly reconciliation view. Then replay `token-push` for every affected
   hour, including empty hours within a non-empty authoritative run.
7. Compare central received rollups and invoice lines by `rated_usage.id` and exact
   cost, then release the invoice window.

Endpoint deletion does not remove `billing_event` or `rated_usage`; attribution and
prices are captured before teardown. Never reconstruct historical ownership by
joining the current endpoint table.

Migration 0005 backfills `usage_found=true` for legacy rows except aborted rows
whose three counts are zero, the only old shape known to represent missing usage.
Token-validity constraints are installed `NOT VALID`: they protect every new row
without making deployment fail on legacy bad evidence. Repair or quarantine any
`invalid_usage_attempts`, then validate both constraints explicitly.

## Failure-injection checklist

The Make suite covers client-id replay, streaming/non-streaming usage, pre/post-header
abort, upstream failure, duplicate delivery, Valkey outage and recovery, WAL restart
and corruption, drainer transaction failure/poison isolation, re-rating, price
change, org ambiguity, and push retry/delete-by-absence behavior. Before a release,
also run the live-Postgres lane and perform chart-level pod-kill tests while Valkey is
unavailable.

## Guarantees outside Phoebe

- Header authenticity depends on auth-server and the saturn-k8s ForwardAuth
  allowlist being deployed before this release.
- WAL survival across pod or node loss is **not guaranteed by Phoebe code**. The
  current chart mounts `emptyDir`; invoice-grade recovery requires a persistent
  volume (or a remote durable append before response completion). Until that chart
  change is deployed, simultaneous Valkey outage and pod death is a declared loss
  window and engine-log reconciliation is mandatory.
- Exact counts depend on the deployed engine emitting the OpenAI usage block,
  including cached prompt tokens. Phoebe records missing/invalid usage but never
  invents it.
- A brand-new rollup whose raw events arrive only after a price change cannot infer
  an old price from the current YAML file. Effective-dated price-book retention in
  the price distribution component is required for that case. Existing rollups are
  protected by their frozen applied rates.
- Final invoice equality depends on the central manager preserving
  `rated_usage.id`, exact decimal cost, authoritative snapshot semantics, and an
  invoice-line export/API. Phoebe cannot prove or repair that external state.
