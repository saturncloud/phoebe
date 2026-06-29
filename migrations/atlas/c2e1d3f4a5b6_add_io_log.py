# pylint: disable=no-member
"""add io_log (phoebe M5 I/O-logging store)

Revision ID: c2e1d3f4a5b6
Revises: c2f1a3b4d5e6
Create Date: 2026-06-07 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/. Its down_revision is c2f1a3b4d5e6
(the phoebe RATING migration), so the three phoebe migrations form a single
LINEAR chain — billing_event -> rating -> io_log — with exactly one head, never a
fork. (rating and io_log were both originally authored off billing_event; io_log
is re-pointed AFTER rating so the graph stays linear, as we always require.) If
the Atlas head moved under billing_event, re-point BILLING_EVENT's down_revision
(not this one) to the new head; this file always chains after rating. See
migrations/README.md.

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
down_revision = "c2f1a3b4d5e6"
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
        # the sink (default 256 KiB) so rows stay bounded. The cap applies to BOTH
        # bodies — request_body flows into the body_tsv to_tsvector, which Postgres
        # rejects past ~1 MiB, so an uncapped long-context prompt would fail the
        # whole INSERT. request_truncated / response_truncated flag a cut body.
        sa.Column("request_body", sa.UnicodeText(), nullable=True),
        sa.Column(
            "request_truncated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
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
