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
   caller's `X-Request-Id` only as `client_request_id` (printable ASCII, fewer
   than 255 bytes; invalid values are rejected before forwarding), and mints a cryptographically
   random `request_id` for the billable execution attempt. Each client retry is a
   new attempt; redelivery of one metering event retains the same attempt id.
3. Phoebe forces streaming usage on, forwards the request, and records the serving
   engine's prompt, cached-prompt, and completion counts. It never re-tokenizes.
   Completed, streamed, aborted, and failed upstream attempts all produce one raw
   event. Missing usage is an explicit `usage_found=false` zero-charge row, not a
   silently dropped event or guessed token count. A client disconnect never
   erases served work: an aborted attempt with authoritative engine usage is
   rated normally; without authoritative usage it remains a zero-charge raw row.
4. The emitter delivers through Valkey Streams, falling back to its fsync'd WAL and
   then a structured `METERING_FLOOR` log. The drainer acknowledges Valkey only after
   the Postgres transaction commits and deduplicates redelivery on the server-minted
   attempt id.
5. The rater resolves prices and writes hourly `rated_usage` rows. Cached input is
   charged once: `fresh = prompt - cached`. Prices come from the central manager,
   which owns the effective-dated series: the rater asks for the prices effective
   during EACH HOUR it rates, so a late or recovered event bills at its own hour's
   rate however many times the price book has changed since — and a re-rate of that
   hour is idempotent. phoebe keeps no local price history. An install that cannot
   egress to the central manager runs its own manager instance seeded with that
   deployment's prices. The applied rates are
   frozen onto each `rated_usage` row, so the row stays self-auditing. Attempts
   without authoritative engine usage are excluded from money, and an UNEXPLAINED
   missing-usage attempt makes the rater exit non-zero.
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
review.

"Failed" means `status_code >= 400` everywhere — the reconciliation view's
`failed_attempts` and the rater's routine-vs-alarming split use the SAME
threshold, so the view you audit and the decision to page can never disagree
about the same rows.

**Missing usage is paged by CAUSE, not by count.** A client disconnect or an
upstream failure legitimately produces a zero-usage attempt, so those are routine
on any install with traffic: they are reported (INFO, and counted in
`missing_usage_attempts`) and reviewed here, never paged. What pages is an attempt
that was NOT aborted and did NOT fail — a response the engine reported as
SUCCESSFUL while supplying no usage block, meaning work may have been served that
cannot be billed. The rater exits non-zero only on that unexplained subset, so
exit 2 stays reserved for rare, wrong conditions (unpriced, unattributable,
invalid-usage, ambiguous base/org) rather than firing every hour.

Page on an unexplained missing-usage attempt, rater anomaly/non-zero exit, reconcile
deletion during a routine run, drainer poison row, `METERING_FLOOR`, WAL corruption,
token-push withheld window, or push failure. Alert separately when the oldest Valkey
pending entry, oldest WAL entry, or oldest unpushed rated hour exceeds two job
periods. The deployment/chart owns those queue-age metrics.

The view groups raw evidence at the exact rated natural key and exposes
`missing_org_attempts` plus `distinct_org_ids` separately. A rollout-era mix of one
NULL and one real org therefore reconciles to one rated row without false token
deltas, while conflicting non-NULL orgs remain explicit.

**`rated_attempts = 0` with a non-zero `raw_attempts` is NOT by itself lost
rating.** It has two very different causes and the view alone cannot tell them
apart, so never treat the row as a drainer/rating incident before ruling out the
first: either (a) the rater deliberately WITHHELD the rollup at an ambiguity gate
— check `distinct_org_ids > 1` on the row for org-ambiguity, and the same run's
`ambiguous_base_events` count for base-ambiguity, which the view does not surface
at all (the base gate keys on `rating_price`/`rating_derived` join outcomes that
exist only inside the rater, not on `billing_event`) — or (b) the rater has not
yet run for that hour. Check the rater's run report for the window before
escalating.

For the invoice boundary, export `rated_usage.id`, `window_start`, `org_id`, and
`cost` for the same interval and compare it to the central manager's received-rollup
and invoice-line exports. Require set equality on id and exact NUMERIC equality on
cost; compare totals only after the row-level set matches. A total can accidentally
balance while two customers are mis-attributed.

## Repair and replay

1. Quiesce the affected invoice window in the central manager. Do not delete local
   raw events or edit rated money manually.
2. Copy the recovery artifact away from any live writer, then validate it without
   making changes.

   **Recover every interceptor ordinal, not just one.** Each StatefulSet ordinal
   owns its OWN retained WAL PVC (the chart defaults to two interceptor
   replicas), and an ordinal's WAL holds only the attempts that ordinal served.
   Recovering `phoebe-interceptor-0` and stopping leaves every attempt buffered
   on `phoebe-interceptor-1` unbilled, with nothing in the reconciliation view
   to indicate that a whole replica's evidence was skipped. Enumerate the claims
   (they are named `phoebe-wal-phoebe-interceptor-<ordinal>`:
   `kubectl -n saturn get pvc | grep '^phoebe-wal-'`) and run the dry-run/apply
   pair below once per ordinal, reviewing each digest separately. Claims are
   retained, so an ordinal scaled away still has evidence to recover.

   `phoebe-recover` accepts a tidwall WAL directory, legacy/imported
   JSONL, or logs containing `METERING_FLOOR` records. It opens a temporary copy of
   WAL directories so the forensic source is not mutated:

   ```console
   /app/phoebe-recover -input /evidence/phoebe-metering-wal
   ```

   Review the reported record count, duplicate count, and `event_set_sha256`.
   That digest covers the COMPLETE validated event set — every field of every
   event, not just the request ids — so it changes if any token count, org
   attribution, or id differs from what you reviewed.

   Replay only that validated set into the configured Valkey stream by repeating
   BOTH the reported unique count and the reported digest verbatim:

   ```console
   /app/phoebe-recover -input /evidence/phoebe-metering-wal \
     -valkey-addr valkey:6379 -stream phoebe:metering \
     -apply -expected-count 42 \
     -expected-digest 9f2c1d...a71b
   ```

   Both guards are required for `-apply` and are checked before any Valkey
   connection is opened, so the artifact you reviewed is provably the artifact
   that gets replayed: a different set with the same count, or the same ids with
   altered payloads, is refused without writing anything. Do not copy a digest
   from an older run — re-read it from the dry-run you are actually approving.

   The command preserves every original `request_id`, refuses conflicting
   duplicates, schema-poisoning values, and status codes outside the database's
   `100..599` range, and is dry-run-only without both apply guards. A retry
   after a partial write is safe because the drainer deduplicates on
   `request_id`. Never replay a live WAL directory while the interceptor is
   appending or auto-draining it.
3. Run the drainer until the consumer group has no pending/lagging entries. Confirm
   the recovered attempt ids exist once in `billing_event`.
4. Restore attribution headers if they were the problem. Prices need no restoring:
   the rater re-resolves each hour against the manager's effective-dated series, so
   a re-rate of an already-billed hour reproduces its original rates even if
   reconciliation deleted the corresponding `rated_usage` row.
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

Migration 0005 is supported as a verified-empty clean cutover with no legacy
database rows, buffered events, or mixed-version drainers; see
`migrations/README.md`. Its defensive development-database backfill sets
`usage_found=true` for legacy rows except aborted rows whose three counts are
zero, but it is not a rolling-upgrade compatibility guarantee.
The raw ledger deliberately accepts invalid engine counts so evidence is never
discarded merely because it cannot become money. The rater excludes those rows
and reports `invalid_usage_attempts`; repair or explicitly quarantine them before
settling the invoice window.

### Upgrading an install that already carries billing traffic

The interceptor moved from a Deployment with an `emptyDir` WAL to a StatefulSet
with one retained PVC per ordinal. Helm performs that replacement by DELETING
the old Deployment and its pod, which destroys that pod's `emptyDir` — so any
metering event still buffered in the old WAL is lost, not migrated. Rolling back
to the Deployment has the mirror-image problem: the restored pod does not mount
the retained StatefulSet PVCs, so evidence written after the upgrade is stranded
on those claims (retained, but invisible until an operator mounts them
deliberately).

Neither direction is a problem on an install with no billing traffic to lose,
which is the supported cutover today. Before performing this upgrade on an
install that IS carrying billing traffic, quiesce inference to the interceptor
first, then confirm the old WAL is empty and Valkey has no pending or lagging
entries, and only then replace the workload. Treat a non-empty WAL at that point
as evidence to recover (see Repair and replay) rather than something the upgrade
will carry across for you.

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
- WAL survival across pod or node loss depends on deployment storage, not Phoebe
  code alone. The official chart gives every interceptor StatefulSet ordinal a
  retained `ReadWriteOnce` PVC, defaulting to the same `saturn-default-storage`
  class as Atlas Postgres. Custom manifests must provide equivalent durable
  storage; engine-log reconciliation remains the backstop for misconfiguration,
  storage failure, or quarantined corrupt WAL data.
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
