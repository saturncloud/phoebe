# phoebe migrations — `billing_event` + rating (`model_price`, `rated_usage`)

This directory holds the schema for phoebe's billing tables, which live in the
**shared Atlas Postgres** alongside the rest of the Saturn schema:

- `billing_event` — the system-of-record table that phoebe's Postgres drainer
  (`cmd/drainer`) writes raw, pre-rating metering records into.
- `model_price` + `rated_usage` — the **rating** (revenue) schema: the
  effective-dated price book and the per-(auth_id, model, hour) cost rollup that
  the rater batch job (`cmd/rater`) joins and writes. Money is stored as INTEGER
  micro-USD (1e-6 USD), never float.

## Why the schema lives here but is *applied* by Atlas (migration ownership)

The drainer is Go; Atlas owns its database schema through an **Alembic (Python)**
migration chain. Putting a second migration tool (a Go migrator) on the same
shared database would mean two systems racing to stamp the same revision history
— a recipe for split-brain DDL. So:

- **One migration system on the shared DB: Atlas Alembic.** The real apply path
  is the Atlas chain.
- The Go drainer **does not run migrations at startup.** It assumes the table
  exists; if it does not, the first upsert fails with a clear, semantic error
  (`relation "billing_event" does not exist`) rather than silently creating
  schema out of band.

What's in this directory is therefore **version-controlled, reviewable DDL** plus
a **ready-to-copy Alembic file**, not a migrator phoebe executes:

| File | Purpose |
| --- | --- |
| `0001_billing_event.sql` | Plain DDL. Reference + local-dev convenience (`psql -f`). Not the production apply path. |
| `atlas/b1f0c2d3e4a5_add_billing_event.py` | A real Alembic `upgrade()`/`downgrade()` following Atlas conventions. The production artifact. |
| `0002_rating.sql` | Plain DDL for `model_price` + `rated_usage` (the rating tables). Reference + local-dev. |
| `atlas/c2f1a3b4d5e6_add_rating.py` | The Alembic artifact for the rating tables. `down_revision = "b1f0c2d3e4a5"` — it chains **after** `billing_event`, so the phoebe revision graph is linear: `…current Atlas head… → billing_event → rating`. |
| `seed_example_prices.sql` | **PLACEHOLDER, non-binding** example price book for local dev. NOT a schema migration and NOT for prod — Hugo sets real prices as data. |

### Migration chain

The two phoebe Alembic files form a linear chain:

```
<current Atlas head>  →  b1f0c2d3e4a5 (billing_event)  →  c2f1a3b4d5e6 (rating)
```

When copying them into `saturn/alembic/versions/`, only `billing_event`'s
`down_revision` needs re-pointing to the then-current Atlas head; `rating` always
chains off `billing_event` via its pinned `down_revision = "b1f0c2d3e4a5"`.

## How to apply it to the shared Atlas Postgres

1. Copy `atlas/b1f0c2d3e4a5_add_billing_event.py` into `saturn/alembic/versions/`.
2. Set its `down_revision` to the **current Atlas head**. It is pinned to
   `c1d2e3f4a5b6` (the head at authoring time); if the head has moved since,
   re-point `down_revision` to the new head so the revision graph stays linear.
   Find the head with:
   ```
   cd ~/work/saturn && python -c "from alembic.config import Config; \
     from alembic.script import ScriptDirectory; \
     print(ScriptDirectory.from_config(Config('alembic.ini')).get_current_head())"
   ```
3. Run the Atlas migration as usual (`alembic upgrade head`).

## Schema conventions (verified against Atlas)

- Table name `billing_event` — snake_case, singular.
- PK constraint named `pk_billing_event` via `op.f(...)`.
- Indexes named `<table>_<cols>_ix` (matching e.g. `artifact_org_id_created_at_ix`
  in `saturn/alembic/versions/9529189423ff_add_artifact_registry.py`).
- `request_id` is `varchar(255)` (an engine/OpenAI request id, **not** an Atlas
  32-char hex), and is the primary key / idempotency key.

## Keeping the .sql and Alembic in sync

Each `.sql` and its Alembic file describe the *same* table(s). If you change one,
change the other. The Go column lists must also match — a column added to a table
without a matching entry there simply won't be written:

- `billing_event`: `0001_billing_event.sql` ↔ `atlas/b1f0c2d3e4a5_…` ↔
  `internal/drain/store.go` (`upsertColumns`).
- `rated_usage`: `0002_rating.sql` ↔ `atlas/c2f1a3b4d5e6_…` ↔
  `internal/rating/store.go` (`upsertColumns`).
- `model_price` is read-only to phoebe (rating SELECTs it; Hugo writes prices as
  data), so it has no Go upsert column list — only the `.sql`/Alembic pair.
