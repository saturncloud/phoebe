# phoebe migrations — raw events, rated usage, reconciliation, and I/O logs

phoebe **owns its billing schema in its OWN Postgres** (deployed by the phoebe
Helm chart), applied by **`cmd/migrate`** (golang-migrate). phoebe is
**self-contained**: no query joins any Atlas-owned table (org is captured at meter
time from the `X-Saturn-Org-Id` header and carried on the rollup — see
`internal/rating` — so there is no `resource_id → resource_name.org_id` push-time
join). Because nothing joins Atlas, the tables do **not** need to be co-located
with the Atlas schema; they live in phoebe's own database.

## The tables

- **`billing_event`** — system-of-record for raw metering records. Written by the
  Postgres drainer (`cmd/drainer`) as it consumes the Valkey metering stream.
  **Retention is finite, by ruled policy (2026-09-30):** `cmd/prune` deletes rows
  older than 30 days on `created_at` (configurable per install, hard floor 7 days
  — inside the floor a later rater re-rate could reconcile-delete billed money).
  ONE horizon for every row class, withheld/invalid rows included; the durable
  records are the manager's money rollup (stored upstream) and the rater's hourly
  anomaly counts/logs, not the raw rows. `rated_usage` below is never pruned.
- **`rated_usage`** — the rating (E1) revenue rollup: per-(auth_id, resource_id,
  model_id, hour) cost, carrying the applied per-token rates frozen onto each row.
  `org_id` is carried from `billing_event` so `cmd/token-push` reads org straight
  off the rollup. **Money is `NUMERIC(20,9)` — exact decimal, never float; all
  money math happens in SQL, not Go.**
- **`io_log`** — optional, sampled, short-retention request/response body capture
  (M5 I/O logging). Written by the interceptor's iolog sink; OFF by default.
  `cmd/prune` enforces the short retention: rows older than 7 days on `created_at`
  are deleted (its OWN period, independent of billing_event's 30 days) — the
  retention job this table's 0003 migration index always promised.

**The price catalog is not a DB table here (E1).** There is no `model_price` table
and no local price history. The manager owns the effective-dated price series; the
rater obtains the prices effective during EACH HOUR it rates, projects that book
into a transient TEMP table, and persists the applied rates onto the self-auditing
`rated_usage` row. "Never reprice served traffic" therefore holds because the price
series is a function of TIME, not because phoebe froze a rate locally: re-rating an
old hour resolves the same rates it originally did.

## The migration files

golang-migrate up/down pairs, applied in version order:

| Version | Files | Creates |
|---|---|---|
| 0001 | `0001_billing_event.{up,down}.sql` | `billing_event` (+ `org_id`, `base_model`, indexes) |
| 0002 | `0002_rating.{up,down}.sql` | `rated_usage` (+ `org_id`, indexes) + the billing_event rating-instant index |
| 0003 | `0003_io_log.{up,down}.sql` | `io_log` (+ GIN body index, retention indexes) |
| 0004 | `0004_billing_event_serving_mode.{up,down}.sql` | `billing_event.serving_mode` (the serving-mode SKU axis; NULL meant dedicated until 0007) |
| 0005 | `0005_invoice_grade_attempts.{up,down}.sql` | trusted/client request identity, attempt outcome and usage evidence, invalid-usage reconciliation, and the hourly reconciliation view at the rated natural key (exposing missing/conflicting org evidence) |
| 0006 | `0006_rollup_grain.{up,down}.sql` | widens the `rated_usage` grain with `serving_mode`, the owner pair and `graph_k8s_name` evidence |
| 0007 | `0007_serving_mode_explicit.{up,down}.sql` | `rated_usage.serving_mode` becomes `'shared'`/`'dedicated'` only (CHECK, no default; dedicated rows renamed from `''` with their ids recomputed) |
| 0008 | `0008_group_scopes.{up,down}.sql` | `billing_event.member_group_ids` (membership evidence) + `group_usage`, the group attribution rollup the admission group spend check reads |

`embed.go` embeds these into the `migrations` package; `cmd/migrate` applies them.

## How it is applied

`cmd/migrate` reads `DATABASE_URL` (the same DSN convention as the
drainer/rater/token-push) and runs the embedded migrations:

```
migrate            # or "migrate up" — apply all pending migrations (default)
migrate down       # roll back one step
migrate version    # print the current applied version
```

### Rollout order for migration 0008

Migration 0008 adds `billing_event.member_group_ids` and the `group_usage`
table, both of which the rater's single rating statement reads/writes. A
pre-0008 rater fails against this schema and a post-0008 rater fails against
the old one (SQLSTATE 42703), so roll code and schema together, in the
migration-0007 order: run `cmd/migrate up`, then deploy the new drainer and
rater (the drainer's INSERT gains the column; an old drainer against schema
0008 simply writes NULL membership). The interceptor's group scope enforcement
is envelope-driven, so it is safe across the cutover: no stamped envelope, no
group check.

`group_usage` rows for already-rated hours exist only after a re-rate (the
rollup is written by the rater, not backfilled). Until a window is re-rated,
the admission spend check sums an empty rollup for that group's hours — group
rate limits still enforce from the envelope; spend caps enforce once rating
has covered the month.

Rolling back 0007 is not exact. The 0007 down migration maps every
`billing_event.serving_mode = 'dedicated'` back to NULL. It also renames the
`rated_usage` `'dedicated'` rows back to `''` and recomputes their ids, so a
rolled-back install stays on one spelling, and re-rating produces `''` rollups
that match the restored rows. The rollback is lossy: afterwards, dedicated events
written after the cutover can no longer be told apart from NULL events written
before the cutover. Ledger item 6 ratifies this loss; after a rollback, NULL means
dedicated.

Roll the code back before the schema. Stop the post-0007 interceptors and
drainers before running `migrate down`. Any event they write as `'dedicated'`
after the down migration would make the pre-0007 rater produce a separate
`'dedicated'` rollup next to the `''` rollup for the same grain.

A rollback is acceptable only while there are no production rows (while
`rated_usage` rows are disposable). See the header of
`0007_serving_mode_explicit.down.sql`.

It adapts `DATABASE_URL`'s `postgres://` scheme to golang-migrate's `pgx5://`
driver scheme internally, so one `DATABASE_URL` serves every phoebe component.

In the phoebe chart, `cmd/migrate up` runs as a one-shot Job / init-container
against phoebe's own Postgres **before** the drainer starts. A serving-only /
spoke install that runs the interceptor ONLY (no drainer/rater/token-push, no DB)
does not run the migrate Job.

### Invoice-grade cutover for migration 0005

Migration 0005 is a coordinated clean cutover, not a mixed-version rolling
migration. Phoebe has no production `billing_event` rows or legacy Valkey/WAL
backlog to preserve. Before applying it, stop old interceptors and billing jobs,
verify `billing_event` and the configured Valkey/WAL buffers are empty, then
apply the migration and deploy the new interceptor, drainer, rater, and push job
as one release. Do not run an old drainer against schema 0005 or replay an old
event encoding after the cutover.

If any installation has legacy rows or buffered events, stop: that installation
does not satisfy this migration's preconditions and needs a separate expand /
contract migration before upgrading.

### Rollout order for migration 0007

Migration 0007 adds the CHECK constraint `rated_usage_serving_mode_ck`
(`serving_mode IN ('shared','dedicated')`). A pre-0007 rater still writes
`serving_mode = ''` for dedicated events and for events whose serving mode is
NULL, so it must not run against schema 0007. Roll out in this order:

1. Before applying 0007, suspend the rater CronJob (and the token-push CronJob if
   you want the push paused too), or let any in-flight rater run finish.
2. Run `cmd/migrate up`, deploy the new rater, proxy and token-push images, then
   resume the rater.
3. Once every interceptor and drainer pod runs the new image, and no old drainer
   pod is left draining the queue, run `UPDATE billing_event SET serving_mode =
   'dedicated' WHERE serving_mode IS NULL` to cover dedicated events an old
   drainer stored as NULL during the rollout. Unlike 0007's one-time backfill,
   this statement does not touch `''` rows: after the cutover a `''` row is a
   producer bug and must not be rewritten. You can bound the statement by
   `created_at` (when the drainer wrote the row) to the rollout window. If an
   old drainer pod might have stored events after the statement ran, run it
   again; it is idempotent.
   The new drainer stores an event whose `serving_mode` key is ABSENT (only an
   old pod emits one, including one replayed later from an on-disk spool or the
   drain queue) as `'dedicated'`, so those events need no backfill. Only an
   absent key gets this default. An explicit `"serving_mode":""` or `null` comes
   from a producer bug after the cutover. The new drainer stores it as `''`, and
   the rater withholds it as an invalid serving mode
   (`invalid_serving_mode_events`). Investigate those rows instead of
   backfilling them, as described in `docs/billing-reconciliation.md`.
   See "Serving-mode cutover (migration 0007)"
   in `docs/billing-reconciliation.md`.
4. Re-rate the hours whose `''` rollup 0007 deleted. Atlas #6709 ships first and
   stamps `X-Saturn-Serving-Mode: dedicated` while the old phoebe is still
   running, so the old rater writes two rollups for each hour that has both
   pre-stamp (NULL) and stamped events: `''` and `'dedicated'`. 0007 deletes the
   `''` row of each such pair and prints a NOTICE with the earliest affected
   `window_start`. The surviving `'dedicated'` row covers only part of its hour
   until that hour is re-rated, and the routine rater re-rates only its trailing
   window (24h by default). Whenever the hour the Atlas #6709 rollout started (or
   the earliest `window_start` in the 0007 twin NOTICE, whichever is earlier) is
   older than that window, run
   `rater --since <that hour> --until <start of the current hour>` and then
   `token-push --since <that hour> --until <start of the current hour>`, so
   saturn-aws-manager replaces the partial rows. Without this, those hours stay
   under-counted in `rated_usage` and are pushed under-billed.
5. Deploy saturn-aws-manager #310 in the same window. Until both sides run the
   new code, pushes are rejected with 400 in whichever direction is mismatched,
   nothing is written, and token-push retries the window on its next run.

If an old rater does run against schema 0007, it writes `serving_mode = ''` for
dedicated or NULL events. The CHECK constraint rejects that write, and the whole
`RateWindow` transaction rolls back, shared rollups included. Nothing is written
for that window, and the next run of the new rater re-rates it. The failure is
closed and heals itself; the order above only avoids the delayed windows.

## Local dev

```
docker run -d -e POSTGRES_PASSWORD=test -e POSTGRES_DB=phoebe -p 5432:5432 postgres:16
DATABASE_URL="postgres://postgres:test@127.0.0.1:5432/phoebe?sslmode=disable" go run ./cmd/migrate up
```

## History

These tables were previously created in the **shared Atlas Postgres** by
copy-into-Atlas Alembic artifacts (`migrations/atlas/*.py`). That coupling was
removed once it was confirmed phoebe performs no cross-joins to Atlas: phoebe now
owns its own database and its own migrator, so the Alembic artifacts and the
`atlas/` directory were deleted. phoebe was not deployed in production anywhere at
the time of the cutover, so there was no data to migrate.
