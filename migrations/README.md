# phoebe migrations — `billing_event` + rating (`model_price`, `rated_usage`)

This directory holds the schema for phoebe's billing tables, which live in the
**shared Atlas Postgres** alongside the rest of the Saturn schema:

- `billing_event` — the system-of-record table that phoebe's Postgres drainer
  (`cmd/drainer`) writes raw, pre-rating metering records into.
- `model_price` + `derivation_policy` + `rated_usage` — the **rating v2** (revenue)
  schema: the effective-dated price book (keyed on a stable `model_id`), the single
  global fine-tune derivation policy, and the per-(auth_id, model_id, hour) cost
  rollup that the rater batch job (`cmd/rater`) joins and writes. **Money is stored
  as `NUMERIC(20,9)` — exact decimal, never float and never an integer micro/nano
  scalar — and ALL money math happens in SQL, not Go.**

### Rating v2 money model (read before touching a number)

- Every money column is `NUMERIC(20,9)`: 9 fractional digits (nano-USD), 11
  integer digits. A sub-$1/1M price like `$0.15/1M = 0.000000150` USD/token is
  exact; an integer micro/nano unit would round it or coarsen it.
- The rater **computes per-event cost AND sums it in a single `INSERT … SELECT`**.
  Go never holds a running money total; it only carries `NUMERIC` values as text.
- `model_price` is keyed on `model_id` (a stable model identity, **not** a
  deployment id or display name). A fine-tune with a NULL rate sets `derived_from`
  to its base's `model_id` and inherits the base's effective rate transformed by
  `derivation_policy` — a pointer, not a copy (a base price change auto-propagates).
  One hop only (a chain > 1 hop is treated as unpriced).
- `derivation_policy` is the **single global** rule (`identity | multiplier |
  markup`), effective-dated. Per-base override is a deliberate v1 non-goal.
- Effective-dating is **forward-only, non-overlapping**, enforced by GiST `EXCLUDE`
  constraints (`btree_gist`): at most one price/policy row matches per instant, so
  the rating join can never fan out and silently over-bill.
- **Audit:** `model_price`/`derivation_policy` are append-only-effective-dated
  (never UPDATE a price — insert a new effective row and close the old); that
  history IS the audit trail, and `created_by` records who set it. The write-path
  authz ("operator-only") is an Atlas/control-plane concern — **out of scope for
  phoebe**; the DB merely records `created_by`.

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
| `0002_rating.sql` | Plain DDL for `model_price` + `derivation_policy` + `rated_usage` (the rating v2 tables, NUMERIC money, GiST exclusion constraints). Reference + local-dev. Notes `CREATE EXTENSION btree_gist`. |
| `atlas/c2f1a3b4d5e6_add_rating.py` | The Alembic artifact for the rating tables. Chains after `billing_event`. |
| `0002_io_log.sql` | Plain DDL for `io_log` (M5 per-tenant I/O-logging store: request/response bodies + `body_tsv` GIN full-text). Reference + local-dev. |
| `atlas/c2e1d3f4a5b6_add_io_log.py` | The Alembic artifact for `io_log`. Chains after `billing_event` (re-point when landing alongside rating — see below). |
| `seed_example_prices.sql` | **PLACEHOLDER, non-binding** example price book for local dev (a base model, a derived fine-tune, and a global derivation policy). NOT a schema migration and NOT for prod — an operator sets real prices as data. |

### Migration chain (IMPORTANT when landing these together)

Both `rating` and `io_log` were authored on their own branches and pin
`down_revision = "b1f0c2d3e4a5"` (billing_event). That's a **fork**, which Alembic
rejects as two heads. When applying more than one, **linearize them** so there's
exactly one head:

```
<current Atlas head> → b1f0c2d3e4a5 (billing_event)
                     → c2f1a3b4d5e6 (rating)
                     → c2e1d3f4a5b6 (io_log)   ← re-point its down_revision to rating
```

Order among rating/io_log doesn't matter functionally (different tables); only
that the graph stays linear. `billing_event`'s own `down_revision` re-points to
the then-current Atlas head at copy time.

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
- `rated_usage`: `0002_rating.sql` ↔ `atlas/c2f1a3b4d5e6_…` ↔ the `INSERT INTO
  rated_usage (…)` column list in `internal/rating/store.go` (`rateWindowSQL`).
- `io_log`: `0002_io_log.sql` ↔ `atlas/c2e1d3f4a5b6_…` ↔ the insert column list in
  `internal/iolog/postgres.go`.
- `model_price` and `derivation_policy` are read-only to phoebe (rating SELECTs
  them; an operator writes prices/policy as data), so they have no Go upsert column
  list — only the `.sql`/Alembic pair.

> Backfill note (empty-model fix): drainer rows written before the
> `nullStr(e.Model)` fix stored `model = ''` instead of NULL when the upstream
> reported no model, hiding them from the rater's `model_id IS NULL`
> unattributable bucket. Reconcile once with:
> `UPDATE billing_event SET model = NULL WHERE model = '';`

> Note: the rater filters `billing_event` on `model_id` (the v2 price key). The
> drainer's `billing_event` carries the metering identity verbatim; whichever
> column the rating join keys on (`model_id`) must be populated by the upstream
> metering path for an event to be attributable — an absent `model_id` is counted
> as **unattributable** and surfaced loudly, never silently dropped.
