# phoebe migrations — `billing_event` + `rated_usage` + `io_log`

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

**Prices are a YAML config file, NOT a DB table (E1).** There is no `model_price`
table. The hourly rater loads the current price YAML, projects it into a transient
TEMP table, rates the last complete hour, and freezes the applied rate onto each
`rated_usage` row.

## The migration files

golang-migrate up/down pairs, applied in version order:

| Version | Files | Creates |
|---|---|---|
| 0001 | `0001_billing_event.{up,down}.sql` | `billing_event` (+ `org_id`, `base_model`, indexes) |
| 0002 | `0002_rating.{up,down}.sql` | `rated_usage` (+ `org_id`, indexes) + the billing_event rating-instant index |
| 0003 | `0003_io_log.{up,down}.sql` | `io_log` (+ GIN body index, retention indexes) |
| 0004 | `0004_billing_event_serving_mode.{up,down}.sql` | `billing_event.serving_mode` (the serving-mode SKU axis; NULL = dedicated) |

`embed.go` embeds these into the `migrations` package; `cmd/migrate` applies them.

## How it is applied

`cmd/migrate` reads `DATABASE_URL` (the same DSN convention as the
drainer/rater/token-push) and runs the embedded migrations:

```
migrate            # or "migrate up" — apply all pending migrations (default)
migrate down       # roll back one step
migrate version    # print the current applied version
```

It adapts `DATABASE_URL`'s `postgres://` scheme to golang-migrate's `pgx5://`
driver scheme internally, so one `DATABASE_URL` serves every phoebe component.

In the phoebe chart, `cmd/migrate up` runs as a one-shot Job / init-container
against phoebe's own Postgres **before** the drainer starts. A serving-only /
spoke install that runs the interceptor ONLY (no drainer/rater/token-push, no DB)
does not run the migrate Job.

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
