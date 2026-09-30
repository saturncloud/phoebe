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
- **`rated_usage`** — the rating (E1) revenue rollup: per-(auth_id, resource_id,
  model_id, hour) cost, carrying the applied per-token rates frozen onto each row.
  `org_id` is carried from `billing_event` so `cmd/token-push` reads org straight
  off the rollup. **Money is `NUMERIC(20,9)` — exact decimal, never float; all
  money math happens in SQL, not Go.**
- **`io_log`** — optional, sampled, short-retention request/response body capture
  (M5 I/O logging). Written by the interceptor's iolog sink; OFF by default.

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

`embed.go` embeds these into the `migrations` package; `cmd/migrate` applies them.

## How it is applied

`cmd/migrate` reads `DATABASE_URL` (the same DSN convention as the
drainer/rater/token-push) and runs the embedded migrations:

```
migrate            # or "migrate up" — apply all pending migrations (default)
migrate down       # roll back one step
migrate version    # print the current applied version
```

Rolling back 0007 is not exact for windows metered after its cutover: the
pre-0007 rater keeps the explicit `'dedicated'` recorded in `billing_event`, so
re-rating those windows after a rollback produces `'dedicated'` rollups beside the
`''` rows the down migration restored. That is acceptable only while
`rated_usage` rows are disposable (pre-production). See the header of
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
