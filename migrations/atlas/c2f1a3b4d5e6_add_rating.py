# pylint: disable=no-member
"""add rating (E1) table (phoebe revenue path: rated_usage)

Revision ID: c2f1a3b4d5e6
Revises: b1f0c2d3e4a5
Create Date: 2026-06-08 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/. down_revision is pinned to
b1f0c2d3e4a5 (phoebe's billing_event migration), so the phoebe chain is linear:
billing_event -> rating. See migrations/README.md for the rationale.

Rating (E1) is the REVENUE path. Money correctness is the entire product:

  * PRICES ARE A YAML CONFIG FILE, NOT A DB TABLE. There is no model_price and no
    derivation_policy table, no GiST exclusion constraint, no effective-dating. The
    operator authors a versioned price YAML (base per-token rates keyed on the HF
    model id, the single global fine-tune premium policy, per-GPU floor rates); the
    file's history IS the price audit trail. See config/prices.example.yaml.
  * MONEY is NUMERIC(20,9) — exact base-10 decimal, NEVER float and NEVER an integer
    micro/nano scalar. The rater projects the YAML prices into a transient TEMP
    table and computes AND sums per-event cost in one SQL statement; Go never holds
    a money total. The fine-tune premium is applied in exact decimal at projection.
  * rated_usage is the per-(auth_id, model_id, hour) rollup, idempotently upserted on
    the natural key. It carries the APPLIED per-token rates (applied_prompt_rate /
    applied_cached_rate / applied_completion_rate) so the row is self-auditing and
    immutable: "we never reprice traffic you've already served" holds by construction.

CLEAN REWRITE (no prod data): an earlier draft of this revision created model_price
+ derivation_policy. E1 moved prices to a YAML file, so those tables are gone and
rated_usage gained the applied-rate columns. Because no environment had this
revision applied (it had not been copied into saturn/alembic), this file is rewritten
in place rather than chained behind a drop-and-alter follow-up. If you have ALREADY
applied a model_price/derivation_policy version of this revision anywhere, do NOT use
this file as-is — add a follow-up migration that DROPs those tables and ALTERs
rated_usage instead (see migrations/README.md).
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2f1a3b4d5e6"
down_revision = "b1f0c2d3e4a5"
branch_labels = None
depends_on = None

# NUMERIC(20,9): 9 fractional digits (nano-USD resolution), 11 integer digits.
MONEY = sa.Numeric(precision=20, scale=9)


def upgrade():
    # --- rated_usage: the per-(auth_id, model_id, hour) cost rollup ---
    op.create_table(
        "rated_usage",
        sa.Column("id", sa.Unicode(length=32), nullable=False),
        sa.Column("auth_id", sa.Unicode(length=64), nullable=False),
        sa.Column("model_id", sa.Unicode(length=255), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        # Raw token sums (audit trail behind the cost).
        sa.Column("prompt_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cached_tokens", sa.BigInteger(), nullable=False),
        sa.Column("completion_tokens", sa.BigInteger(), nullable=False),
        sa.Column("billable_prompt_tokens", sa.BigInteger(), nullable=False),
        # The money, exact NUMERIC, computed and summed in SQL.
        sa.Column("cost", MONEY, nullable=False),
        # The APPLIED per-token rates frozen onto the row (self-auditing rollup).
        # server_default=0 so an ALTER on an existing table is backfill-free; the
        # rater always writes them explicitly.
        sa.Column("applied_prompt_rate", MONEY, nullable=False, server_default="0"),
        sa.Column("applied_cached_rate", MONEY, nullable=False, server_default="0"),
        sa.Column("applied_completion_rate", MONEY, nullable=False, server_default="0"),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column(
            "rated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rated_usage")),
        # Idempotency key: one rollup row per (auth_id, model_id, hour).
        sa.UniqueConstraint(
            "auth_id",
            "model_id",
            "window_start",
            name=op.f("rated_usage_auth_model_window_uq"),
        ),
    )
    op.create_index(
        "rated_usage_auth_id_window_start_ix",
        "rated_usage",
        ["auth_id", "window_start"],
        unique=False,
    )

    # The rater filters billing_event on its RATING INSTANT, COALESCE(event_ts,
    # created_at). The index must be on that EXACT expression: Postgres matches index
    # expressions structurally, so an index on bare (event_ts) can never serve the
    # COALESCE predicate and the rater would seq-scan a table that only grows. Raw SQL
    # because expression indexes are clumsy through op.create_index.
    op.execute(
        "CREATE INDEX billing_event_rating_instant_ix "
        "ON billing_event ((COALESCE(event_ts, created_at)))"
    )

    # base_model on billing_event (E3 fine-tune linkage): the HF base id a fine-tune
    # derives from, stamped by Atlas at deploy. The rater prices an ft:<checkpoint>
    # model at base x premium via this column. The billing_event create migration
    # (b1f0c2d3e4a5) now declares it directly, but this revision was authored after
    # billing_event shipped, so add it idempotently here too — a billing_event table
    # created before the column was added still gets it, and a fresh DB (where the
    # create already has it) is a harmless no-op. NULL for a base-model deployment.
    op.execute(
        "ALTER TABLE billing_event ADD COLUMN IF NOT EXISTS base_model VARCHAR(255)"
    )


def downgrade():
    op.execute("ALTER TABLE billing_event DROP COLUMN IF EXISTS base_model")
    op.execute("DROP INDEX billing_event_rating_instant_ix")
    op.drop_index("rated_usage_auth_id_window_start_ix", table_name="rated_usage")
    op.drop_table("rated_usage")
