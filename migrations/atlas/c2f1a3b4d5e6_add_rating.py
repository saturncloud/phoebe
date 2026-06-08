# pylint: disable=no-member
"""add rating tables (phoebe revenue path: model_price + rated_usage)

Revision ID: c2f1a3b4d5e6
Revises: b1f0c2d3e4a5
Create Date: 2026-06-08 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/. down_revision is pinned to
b1f0c2d3e4a5 (phoebe's billing_event migration), so the phoebe chain is linear:
billing_event -> rating. If billing_event's down_revision was re-pointed to a
newer Atlas head when it was copied in, this file still chains correctly after it.
See migrations/README.md for the rationale.

Rating v1 is the REVENUE path. model_price is the effective-dated price book;
rated_usage is the per-(auth_id, model, hour) cost rollup. All money is stored as
INTEGER micro-USD (1e-6 USD) — never float. 1 Atlas hourly_usage_record unit
(1e-4 USD) == 100 micro-USD.

PLACEHOLDER PRICES: this migration creates the TABLES only. No prices ship in the
schema — prices are DATA set by Hugo. See migrations/seed_example_prices.sql for a
clearly-labelled, non-binding example seed.
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2f1a3b4d5e6"
down_revision = "b1f0c2d3e4a5"
branch_labels = None
depends_on = None


def upgrade():
    # --- model_price: the effective-dated price book ---
    op.create_table(
        "model_price",
        sa.Column("id", sa.Unicode(length=32), nullable=False),
        sa.Column("model", sa.Unicode(length=255), nullable=False),
        # Per-token prices in micro-USD (1e-6 USD), stored as integers (BIGINT).
        # cached tokens are charged at a DISTINCT (usually discounted) rate from
        # prompt tokens; the billable-prompt formula lives in internal/rating.
        sa.Column("prompt_price_micro", sa.BigInteger(), nullable=False),
        sa.Column("cached_price_micro", sa.BigInteger(), nullable=False),
        sa.Column("completion_price_micro", sa.BigInteger(), nullable=False),
        # effective_to NULL == open-ended (current price). An event is rated with
        # the price whose window contains the event's event_ts.
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_price")),
    )
    # Price lookups resolve (model, at-time) → newest price with effective_from <= at.
    op.create_index(
        "model_price_model_effective_from_ix",
        "model_price",
        ["model", "effective_from"],
        unique=False,
    )

    # --- rated_usage: the per-(auth_id, model, hour) cost rollup ---
    op.create_table(
        "rated_usage",
        sa.Column("id", sa.Unicode(length=32), nullable=False),
        sa.Column("auth_id", sa.Unicode(length=64), nullable=False),
        sa.Column("model", sa.Unicode(length=255), nullable=False),
        # [window_start, window_end) is the hour this rollup covers.
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        # Raw token sums (audit trail behind the cost).
        sa.Column("prompt_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cached_tokens", sa.BigInteger(), nullable=False),
        sa.Column("completion_tokens", sa.BigInteger(), nullable=False),
        # billable_prompt_tokens = prompt_tokens - cached_tokens.
        sa.Column("billable_prompt_tokens", sa.BigInteger(), nullable=False),
        sa.Column("cost_micro_usd", sa.BigInteger(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column(
            "rated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rated_usage")),
        # Idempotency key: one rollup row per (auth_id, model, hour). A re-run
        # upserts ON CONFLICT on this constraint instead of inserting duplicates.
        sa.UniqueConstraint(
            "auth_id",
            "model",
            "window_start",
            name=op.f("rated_usage_auth_model_window_uq"),
        ),
    )
    # Per-API-key billing queries scan by auth_id over a time window.
    op.create_index(
        "rated_usage_auth_id_window_start_ix",
        "rated_usage",
        ["auth_id", "window_start"],
        unique=False,
    )


def downgrade():
    op.drop_index("rated_usage_auth_id_window_start_ix", table_name="rated_usage")
    op.drop_table("rated_usage")
    op.drop_index("model_price_model_effective_from_ix", table_name="model_price")
    op.drop_table("model_price")
