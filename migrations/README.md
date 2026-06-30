# phoebe migrations — `billing_event` + rating (`rated_usage`)

This directory holds the schema for phoebe's billing tables, which live in the
**shared Atlas Postgres** alongside the rest of the Saturn schema:

- `billing_event` — the system-of-record table that phoebe's Postgres drainer
  (`cmd/drainer`) writes raw, pre-rating metering records into.
- `rated_usage` — the **rating (E1)** (revenue) rollup: per-(auth_id, resource_id,
  model_id, hour) cost, carrying the applied per-token rates frozen onto each row.
  `resource_id` (the deployment id) is part of the grain for **E2 customer
  attribution**, and `org_id` (the deployment-owning org) is carried onto the rollup
  for the same purpose — captured at **meter time** from the `X-Saturn-Org-Id` header
  and threaded `billing_event`→`rated_usage`, so push reads org straight off the
  rollup rather than re-joining the Atlas-owned `resource_name` table (which raced
  deployment teardown). A NULL-`resource_id` event fails closed (counted
  unattributable, never billed); a NULL-`org_id` rollup is likewise held + screamed at
  push, never billed to a guessed org.
  **Money is
  stored as `NUMERIC(20,9)` — exact decimal, never float and never an integer
  micro/nano scalar — and ALL money math happens in SQL, not Go.**

**PRICES ARE A YAML CONFIG FILE, NOT A DB TABLE (E1).** There is no `model_price`
and no `derivation_policy` table. The operator authors a versioned price YAML
(`config/prices.example.yaml`): base per-token rates keyed on the HF model id, the
single global fine-tune premium policy, and per-GPU floor rates. The file's version
history IS the price audit trail. The hourly rater loads the **current** file,
projects the rates into a transient TEMP table, rates the last complete hour, and
**freezes the applied rate onto each `rated_usage` row** (self-auditing, immutable).

### Rating money model (read before touching a number)

- Every money column is `NUMERIC(20,9)`: 9 fractional digits (nano-USD), 11
  integer digits. A sub-$1/1M price like `$0.15/1M = 0.000000150` USD/token is
  exact; an integer micro/nano unit would round it or coarsen it.
- The rater **computes per-event cost AND sums it in a single `INSERT … SELECT`**
  over the YAML-projected price table. Go never holds a running money total; it only
  carries `NUMERIC` values as text. (The fine-tune premium is the one exception —
  applied in exact decimal when the prices are projected, then handed to SQL.)
- Prices are keyed on `model_id` (a stable model identity, **not** a deployment id
  or display name): an HF base id, or `ft:<checkpoint>` for a fine-tune. A fine-tune
  inherits its base's rate transformed by the global premium — a pointer, not a copy
  (a base price change auto-propagates). One hop only.
- **`rated_usage` carries the applied rates** (`applied_prompt_rate` /
  `applied_cached_rate` / `applied_completion_rate`): the exact per-token rates the
  rollup was billed at. The row is then immutable and self-auditing — "we never
  reprice traffic you've already served" holds by construction.
- **Audit:** the price file's git/version history IS the audit trail; re-rating is a
  deliberate, audited re-run, never a silent side effect of editing the file. The
  write-path authz ("operator-only") is an out-of-band concern (who can edit the
  file), not a phoebe DB concern.

> **Fine-tune base linkage (closed):** `billing_event` now carries a `base_model`
> column — the HF base id a fine-tune derives from (E3), stamped by Atlas at deploy and
> injected on the `X-Saturn-Base-Model` header. The rater prices an `ft:<checkpoint>`
> id (which the price file never names) at base × premium by resolving through this
> column. A base-direct model still prices off its own rate. An `ft:` id with a NULL
> `base_model` is a propagation bug, not a free model: it is **unpriced** (fail loud,
> never $0). The column is declared in the `billing_event` create migration and added
> idempotently in the rating migration (`ADD COLUMN IF NOT EXISTS`) for any
> already-applied `billing_event`.

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
| `0002_rating.sql` | Plain DDL for `rated_usage` (the rating rollup; NUMERIC money + applied-rate columns). Reference + local-dev. Prices are a YAML file, not a table. |
| `atlas/c2f1a3b4d5e6_add_rating.py` | The Alembic artifact for `rated_usage`. Chains after `billing_event`. |
| `0002_io_log.sql` | Plain DDL for `io_log` (M5 per-tenant I/O-logging store: request/response bodies + `body_tsv` GIN full-text). Reference + local-dev. |
| `atlas/c2e1d3f4a5b6_add_io_log.py` | The Alembic artifact for `io_log`. Chains after `billing_event` (re-point when landing alongside rating — see below). |
| `atlas/d3a2b4c5e6f7_add_org_id.py` | Follow-up Alembic that adds `org_id` (E2 attribution) idempotently to `billing_event` AND `rated_usage` (`ADD COLUMN IF NOT EXISTS`, nullable). A follow-up — not an edit to the create migrations — because those already shipped on `release-2026.02.01`/`release-2026.06.01`. Chains after `rating`. |
| `../config/prices.example.yaml` | The **operator-facing price file** (E1): base per-token rates keyed on the HF model id, the global fine-tune premium policy, per-GPU floor rates. The contract the rater prices from. Not a migration. |

### Migration chain (IMPORTANT when landing these together)

Both `rating` and `io_log` were authored on their own branches and pin
`down_revision = "b1f0c2d3e4a5"` (billing_event). That's a **fork**, which Alembic
rejects as two heads. When applying more than one, **linearize them** so there's
exactly one head:

```
<current Atlas head> → b1f0c2d3e4a5 (billing_event)
                     → c2f1a3b4d5e6 (rating)
                     → c2e1d3f4a5b6 (io_log)     ← re-point its down_revision to rating
                     → d3a2b4c5e6f7 (org_id)     ← chains after the last phoebe rev
```

`org_id` is a LATER follow-up: `billing_event`/`rating` have already shipped to the
release branches, so `d3a2b4c5e6f7` adds the column idempotently rather than editing
the (already-applied) create migrations. At copy time, point its `down_revision` at
whatever the last phoebe revision applied to the shared DB is.

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

- `billing_event`: `0001_billing_event.sql` ↔ `atlas/b1f0c2d3e4a5_…` (+ `org_id`
  added by `atlas/d3a2b4c5e6f7_…`) ↔ `internal/drain/store.go` (`upsertColumns`).
- `rated_usage`: `0002_rating.sql` ↔ `atlas/c2f1a3b4d5e6_…` (+ `org_id` added by
  `atlas/d3a2b4c5e6f7_…`) ↔ the `INSERT INTO rated_usage (…)` column list in
  `internal/rating/store.go` (`rateWindowSQL`), including the `applied_*_rate` and
  `org_id` columns.
- `io_log`: `0002_io_log.sql` ↔ `atlas/c2e1d3f4a5b6_…` ↔ the insert column list in
  `internal/iolog/postgres.go`.
- Prices are NOT in the DB: they live in the YAML price file
  (`config/prices.example.yaml`), parsed by `internal/rating/pricebook.go`. There is
  no `model_price`/`derivation_policy` table to keep in sync.

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
