# phoebe migrations — `io_log` (M5 I/O-logging store)

This directory holds the schema for `io_log`, the table phoebe's M5 I/O-logging
`PostgresSink` (`internal/iolog`) writes captured request/response **bodies**
into. It lives in the **shared Atlas Postgres**, alongside the rest of the
Saturn schema — but is a **separate table from `billing_event`** (the metering
system-of-record), with its own short retention and access posture.

> Note: the `billing_event` migration (`0001_billing_event.sql` +
> `atlas/b1f0c2d3e4a5_add_billing_event.py`) is owned by the Postgres-drainer
> work on its own branch. This M5 branch adds **only** the `io_log` artifacts and
> chains its Alembic revision after that one (`down_revision = b1f0c2d3e4a5`).
> If the two land out of order, re-point `down_revision` at copy time (below).

## Why the schema lives here but is *applied* by Atlas (migration ownership)

Same model as the drainer: **one migration system on the shared DB — Atlas
Alembic.** Phoebe does **not** run migrations at startup; the interceptor assumes
`io_log` exists and the first INSERT fails with a clear, semantic error
(`relation "io_log" does not exist`) rather than creating schema out of band.
What's in this directory is therefore **version-controlled, reviewable DDL** plus
a **ready-to-copy Alembic file**, not a migrator phoebe executes:

| File | Purpose |
| --- | --- |
| `0002_io_log.sql` | Plain DDL. Reference + local-dev convenience (`psql -f`). Not the production apply path. |
| `atlas/c2e1d3f4a5b6_add_io_log.py` | A real Alembic `upgrade()`/`downgrade()` following Atlas conventions. The production artifact. |

## How to apply it to the shared Atlas Postgres

1. Copy `atlas/c2e1d3f4a5b6_add_io_log.py` into `saturn/alembic/versions/`.
2. **Set its `down_revision` to the current Atlas head AT COPY TIME.** It is
   pinned to `b1f0c2d3e4a5` (the phoebe `billing_event` migration) as the natural
   predecessor; if that revision isn't applied yet, or the head has moved,
   re-point `down_revision` to the actual head so the revision graph stays linear.
   Find the head with:
   ```
   cd ~/work/saturn && python -c "from alembic.config import Config; \
     from alembic.script import ScriptDirectory; \
     print(ScriptDirectory.from_config(Config('alembic.ini')).get_current_head())"
   ```
3. Run the Atlas migration as usual (`alembic upgrade head`).

## Schema conventions (verified against Atlas / the drainer's billing_event)

- Table name `io_log` — snake_case, singular.
- Indexes named `<table>_<cols>_ix`.
- `request_id` is `varchar(255)` (an engine/OpenAI request id, **not** an Atlas
  32-char hex). It is **NOT** a primary key here — `io_log` is append-only
  best-effort, so the PK is a generated `id` and `request_id` is indexed for
  joins back to `billing_event`.
- `body_tsv` is a `TSVECTOR` with a **GIN** index for full-text "grep inside
  bodies". The sink populates it at INSERT time via `to_tsvector(...)`.
- `pgvector` / semantic search is intentionally **NOT** added — a later milestone.

## Best-effort vs durable (why this differs from billing_event)

`billing_event` is durable money data (upsert `ON CONFLICT (request_id) DO
NOTHING`, long retention). `io_log` is **best-effort, sampled, short-retention**
debug telemetry: the sink **drops + counts** on overflow and rows are pruned
aggressively by a retention scan over `created_at`. A dropped `io_log` row is
acceptable; a dropped `billing_event` row is not.

## Keeping things in sync

`0002_io_log.sql`, `atlas/c2e1d3f4a5b6_add_io_log.py`, and the sink's INSERT
column list in `internal/iolog/postgres.go` (`insertQuery` / `insertArgs`)
describe the *same* table. If you change one, change the others.
