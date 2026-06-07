# pylint: disable=no-member
"""add billing_event (phoebe metering system-of-record)

Revision ID: b1f0c2d3e4a5
Revises: c1d2e3f4a5b6
Create Date: 2026-06-07 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/ and set down_revision to the THEN
-current Atlas head (it is c1d2e3f4a5b6 as of authoring; re-point it if the head
has since moved). See migrations/README.md for the rationale.

billing_event is the system-of-record for RAW (pre-rating) metering records,
written by phoebe's Postgres drainer. request_id is the idempotency key; the
drainer upserts ON CONFLICT (request_id) DO NOTHING.
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b1f0c2d3e4a5"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "billing_event",
        # request_id is an engine/OpenAI request id, not an Atlas 32-char hex —
        # so varchar(255), not Unicode(32).
        sa.Column("request_id", sa.Unicode(length=255), nullable=False),
        # Identity, captured verbatim from atlas-auth headers (nullable).
        sa.Column("auth_id", sa.Unicode(length=64), nullable=True),
        sa.Column("user_id", sa.Unicode(length=32), nullable=True),
        sa.Column("group_id", sa.Unicode(length=32), nullable=True),
        sa.Column("resource_id", sa.Unicode(length=64), nullable=True),
        sa.Column("resource_type", sa.Unicode(length=64), nullable=True),
        # Workload.
        sa.Column("model", sa.Unicode(length=255), nullable=True),
        sa.Column("adapter", sa.Unicode(length=255), nullable=True),
        # Raw token counts (NOT NULL, default 0).
        sa.Column("prompt_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cached_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completion_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("finish_reason", sa.Unicode(length=64), nullable=True),
        sa.Column("gpu_type", sa.Unicode(length=64), nullable=True),
        sa.Column("aborted", sa.Boolean(), nullable=False, server_default=sa.false()),
        # event_ts: interceptor stamp time; created_at: drain write time.
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("request_id", name=op.f("pk_billing_event")),
    )
    # Per-API-key billing queries scan by auth_id over a time window.
    op.create_index(
        "billing_event_auth_id_created_at_ix",
        "billing_event",
        ["auth_id", "created_at"],
        unique=False,
    )
    # Time-range scans for batch rating.
    op.create_index(
        "billing_event_created_at_ix",
        "billing_event",
        ["created_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("billing_event_created_at_ix", table_name="billing_event")
    op.drop_index("billing_event_auth_id_created_at_ix", table_name="billing_event")
    op.drop_table("billing_event")
