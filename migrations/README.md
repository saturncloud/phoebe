# phoebe migrations — `billing_event`, rating (`model_price`, `rated_usage`), `io_log`

This directory holds the schema for phoebe's tables, which live in the **shared
Atlas Postgres** alongside the rest of the Saturn schema:

- `billing_event` — the system-of-record table that phoebe's Postgres drainer
  (`cmd/drainer`) writes raw, pre-rating metering records into.
- `model_price` + `rated_usage` — the **rating** (revenue) schema: the
  effective-dated price book and the per-(auth_id, model, hour) cost rollup that
  the rater batch job (`cmd/rater`) joins and writes. Money is stored as INTEGER
  micro-USD (1e-6 USD), never float.
- `io_log` — the M5 I/O-logging store: the table the I/O-logging `PostgresSink`
  (`internal/iolog`) writes captured request/response **bodies** into. A
  **separate table** from `billing_event`, with its own short retention and
  access posture.

## Why the schema lives here but is *applied* by Atlas (migration ownership)

The drainer/rater are Go; Atlas owns its database schema through an **Alembic
(Python)** migration chain. Putting a second migration tool (a Go migrator) on
the same shared database would mean two systems racing to stamp the same revision
history — a recipe for split-brain DDL. So:

- **One migration system on the shared DB: Atlas Alembic.** The real apply path
  is the Atlas chain.
- The Go services **do not run migrations at startup.** They assume the table
  exists; if it does not, the first query fails with a clear, semantic error
  (e.g. `relation "billing_event" does not exist`) rather than silently creating
  schema out of band.

What's in this directory is therefore **version-controlled, reviewable DDL** plus
**ready-to-copy Alembic files**, not a migrator phoebe executes:

| File | Purpose |
| --- | --- |
| `0001_billing_event.sql` | Plain DDL. Reference + local-dev (`psql -f`). Not the production apply path. |
| `atlas/b1f0c2d3e4a5_add_billing_event.py` | Real Alembic `upgrade()`/`downgrade()`, Atlas conventions. The production artifact. |
| `0002_rating.sql` | Plain DDL for `model_price` + `rated_usage` (rating tables). Reference + local-dev. |
| `atlas/c2f1a3b4d5e6_add_rating.py` | Alembic artifact for the rating tables. Chains after `billing_event`. |
| `0002_io_log.sql` | Plain DDL for `io_log` (M5 I/O-logging). Reference + local-dev. |
| `atlas/c2e1d3f4a5b6_add_io_log.py` | Alembic artifact for `io_log`. Chains after `billing_event` (re-point when landing alongside rating — see below). |
| `seed_example_prices.sql` | **PLACEHOLDER, non-binding** example price book for local dev. NOT a schema migration and NOT for prod — Hugo sets real prices as data. |

### Migration chain (IMPORTANT when landing these together)

Each phoebe Alembic file was authored on its own branch, so both `rating` and
`io_log` pin `down_revision = "b1f0c2d3e4a5"` (billing_event). That's a **fork**,
which Alembic rejects as two heads. When applying more than one of these to the
real Atlas chain, **linearize them** — e.g.:

```
<current Atlas head> → b1f0c2d3e4a5 (billing_event)
                     → c2f1a3b4d5e6 (rating)
                     → c2e1d3f4a5b6 (io_log)   ← re-point its down_revision to rating
```

Re-point `down_revision` at copy time so there is exactly one head. Order among
rating/io_log doesn't matter functionally (different tables); only that the graph
stays linear.

## How to apply it to the shared Atlas Postgres

1. Copy the `atlas/*.py` files you're applying into `saturn/alembic/versions/`.
2. Set the **first** one's `down_revision` to the **current Atlas head** at copy
   time (pinned values are authoring-time placeholders), and chain the rest after
   it so there's a single head. Find the head with:
   ```
   cd ~/work/saturn && python -c "from alembic.config import Config; \
     from alembic.script import ScriptDirectory; \
     print(ScriptDirectory.from_config(Config('alembic.ini')).get_current_head())"
   ```
3. Run the Atlas migration as usual (`alembic upgrade head`).

## Schema conventions (verified against Atlas)

- Table names snake_case, singular (`billing_event`, `model_price`,
  `rated_usage`, `io_log`).
- PK constraints named `pk_<table>` via `op.f(...)`; indexes `<table>_<cols>_ix`
  (matching e.g. `artifact_org_id_created_at_ix` in
  `saturn/alembic/versions/9529189423ff_add_artifact_registry.py`).
- `request_id` is `varchar(255)` (an engine/OpenAI request id, **not** an Atlas
  32-char hex). In `billing_event` it is the PK / idempotency key. In `io_log` it
  is **not** a PK (append-only best-effort) — the PK is a generated `id` and
  `request_id` is indexed for joins back to `billing_event`.
- `io_log.body_tsv` is a `TSVECTOR` with a **GIN** index for full-text "grep
  inside bodies", populated at INSERT via `to_tsvector(...)`. `pgvector` /
  semantic search is intentionally **NOT** added yet — a later milestone.

## Best-effort vs durable (why io_log differs from billing_event)

`billing_event` is durable money data (upsert `ON CONFLICT (request_id) DO
NOTHING`, long retention); `rated_usage` is its durable rollup. `io_log` is
**best-effort, sampled, short-retention** debug telemetry: the sink **drops +
counts** on overflow and rows are pruned aggressively by a retention scan over
`created_at`. A dropped `io_log` row is acceptable; a dropped `billing_event`
row is not.

## Keeping the .sql, Alembic, and Go column lists in sync

Each `.sql` and its Alembic file describe the *same* table(s). The Go column
lists must also match — a column added without a matching entry simply won't be
written:

- `billing_event`: `0001_billing_event.sql` ↔ `atlas/b1f0c2d3e4a5_…` ↔
  `internal/drain/store.go` (`upsertColumns`).
- `rated_usage`: `0002_rating.sql` ↔ `atlas/c2f1a3b4d5e6_…` ↔
  `internal/rating/store.go` (`upsertColumns`).
- `model_price` is read-only to phoebe (rating SELECTs it; Hugo writes prices as
  data), so it has no Go upsert column list — only the `.sql`/Alembic pair.
- `io_log`: `0002_io_log.sql` ↔ `atlas/c2e1d3f4a5b6_…` ↔
  `internal/iolog/postgres.go` (`insertQuery` / `insertArgs`).
