# pylint: disable=no-member
"""add io_log (phoebe M5 I/O-logging store)

Revision ID: c2e1d3f4a5b6
Revises: b1f0c2d3e4a5
Create Date: 2026-06-07 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/ and set down_revision to the THEN
-current Atlas head. It is pinned to b1f0c2d3e4a5 (the phoebe billing_event
migration) as the natural predecessor; if that migration is not yet applied, or
the Atlas head has since moved, RE-POINT down_revision to the current head so the
revision graph stays linear. See migrations/README.md for the rationale and the
head-discovery command.

io_log is the M5 store for request/response BODIES — a SEPARATE table from
billing_event with its own (short) retention and access posture. Capture is
opt-in + sampled and OFF by default (bodies are sensitive); this table only holds
what the iolog policy gate let through. The body_tsv + GIN index provides
full-text "grep inside bodies". pgvector/semantic search is a later milestone and
is intentionally NOT added here.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c2e1d3f4a5b6"
down_revision = "b1f0c2d3e4a5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "io_log",
        # Surrogate key: io_log is append-only, so a generated id is cleaner than
        # overloading request_id (which is NOT unique here — a request yields 0
        # or 1 sampled rows, never upserted).
        sa.Column(
            "id",
            sa.BigInteger(),
            sa.Identity(always=True),
            nullable=False,
        ),
        # request_id is an engine/OpenAI request id, not an Atlas 32-char hex —
        # varchar(255). Indexed (below) for joins back to billing_event.
        sa.Column("request_id", sa.Unicode(length=255), nullable=False),
        # Identity, captured verbatim from atlas-auth headers (nullable).
        sa.Column("auth_id", sa.Unicode(length=64), nullable=True),
        sa.Column("user_id", sa.Unicode(length=32), nullable=True),
        sa.Column("group_id", sa.Unicode(length=32), nullable=True),
        sa.Column("resource_id", sa.Unicode(length=64), nullable=True),
        sa.Column("resource_type", sa.Unicode(length=64), nullable=True),
        # Workload.
        sa.Column("model", sa.Unicode(length=255), nullable=True),
        # Captured bodies. TEXT at the column level; buffered size is capped in
        # the sink (default 256 KiB response) so rows stay bounded.
        sa.Column("request_body", sa.UnicodeText(), nullable=True),
        sa.Column("response_body", sa.UnicodeText(), nullable=True),
        sa.Column(
            "response_truncated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("streamed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("latency_ms", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # Full-text vector over the bodies, populated at INSERT time by the sink.
        sa.Column("body_tsv", postgresql.TSVECTOR(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_io_log")),
    )
    # GIN index over the body tsvector: the "grep inside bodies" capability.
    op.create_index(
        "io_log_body_tsv_ix",
        "io_log",
        ["body_tsv"],
        unique=False,
        postgresql_using="gin",
    )
    # Per-API-key troubleshooting: "this key's recent requests".
    op.create_index(
        "io_log_auth_id_created_at_ix",
        "io_log",
        ["auth_id", "created_at"],
        unique=False,
    )
    # Retention scans (prune rows past the short retention window).
    op.create_index(
        "io_log_created_at_ix",
        "io_log",
        ["created_at"],
        unique=False,
    )
    # Join back to billing_event by request id.
    op.create_index(
        "io_log_request_id_ix",
        "io_log",
        ["request_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("io_log_request_id_ix", table_name="io_log")
    op.drop_index("io_log_created_at_ix", table_name="io_log")
    op.drop_index("io_log_auth_id_created_at_ix", table_name="io_log")
    op.drop_index("io_log_body_tsv_ix", table_name="io_log")
    op.drop_table("io_log")
