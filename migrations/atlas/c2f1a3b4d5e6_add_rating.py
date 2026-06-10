# pylint: disable=no-member
"""add rating v2 tables (phoebe revenue path: model_price + derivation_policy + rated_usage)

Revision ID: c2f1a3b4d5e6
Revises: b1f0c2d3e4a5
Create Date: 2026-06-08 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/. down_revision is pinned to
b1f0c2d3e4a5 (phoebe's billing_event migration), so the phoebe chain is linear:
billing_event -> rating. If billing_event's down_revision was re-pointed to a
newer Atlas head when it was copied in, this file still chains correctly after it.
See migrations/README.md for the rationale.

Rating v2 is the REVENUE path. Money correctness is the entire product:

  * MONEY is NUMERIC(20,9) — exact base-10 decimal, NEVER float and NEVER an
    integer micro/nano scalar. All money MATH happens in SQL (the rater computes
    AND sums per-event cost in one statement); Go never holds a money total.
  * model_price is the effective-dated price book keyed on a STABLE model_id (not
    a deployment id, not a name). A fine-tune with no own rate points at its base
    via derived_from and inherits base_rate transformed by the global
    derivation_policy (a POINTER, not a copy: a base price change auto-propagates).
  * derivation_policy is the SINGLE GLOBAL rule (identity | multiplier | markup),
    effective-dated. Operators never set per-fine-tune prices. Per-base override is
    a deliberate v1 NON-GOAL.
  * Effective-dating is FORWARD-ONLY, NON-OVERLAPPING, enforced by GiST EXCLUDE
    constraints (needs btree_gist): at most ONE price/policy row matches per
    instant, so the rating join can never fan out and silently over-bill.
  * rated_usage is the per-(auth_id, model_id, hour) rollup, idempotently upserted
    on the natural key.
  * AUDIT: model_price / derivation_policy are append-only-effective-dated (you
    never UPDATE a price; you insert a new effective row and close the old). That
    history IS the audit trail; created_by records WHO set it. The write-path
    authz ("operator-only") is an Atlas/control-plane concern — OUT OF SCOPE for
    phoebe; the DB merely records created_by.

PLACEHOLDER PRICES: this migration creates the TABLES only. No prices ship in the
schema — prices are DATA set by an operator. See migrations/seed_example_prices.sql
for a clearly-labelled, non-binding example seed.
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
    # btree_gist lets a GiST exclusion constraint mix an equality column with a
    # range-overlap (&&) operator — the mechanism behind the no-overlap constraints
    # below. Without it the && opclass for the scalar key is unavailable.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    # --- model_price: the effective-dated price book, keyed on model_id ---
    op.create_table(
        "model_price",
        sa.Column("id", sa.Unicode(length=32), nullable=False),
        # model_id is the STABLE price key (a model identity, not a deployment id).
        sa.Column("model_id", sa.Unicode(length=255), nullable=False),
        # derived_from: NULL for a base; else the base model_id this inherits from
        # (one hop). A self-reference in price-key space, resolved AS-OF the event.
        sa.Column("derived_from", sa.Unicode(length=255), nullable=True),
        # Per-token prices as NUMERIC, NULLABLE (a derived model inherits via
        # derived_from + derivation_policy). cached_price is a DISTINCT discounted
        # rate; cached_tokens are the SUBSET of prompt_tokens served from cache.
        sa.Column("prompt_price", MONEY, nullable=True),
        sa.Column("cached_price", MONEY, nullable=True),
        sa.Column("completion_price", MONEY, nullable=True),
        # effective_to NULL == open-ended (current price).
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        # AUDIT: who set this price (write-path authz is Atlas's concern), and when.
        sa.Column("created_by", sa.Unicode(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_price")),
        # A row must carry EITHER an own rate OR a derived_from; a row with neither
        # is a dead "no price" entry that would silently make the model unpriced.
        sa.CheckConstraint(
            "derived_from IS NOT NULL OR prompt_price IS NOT NULL",
            name="model_price_rate_or_derived_ck",
        ),
        # An own rate is ALL-OR-NOTHING across the three components — a partially
        # NULL rate would make a cost term NULL in the SQL sum and silently
        # under-bill. (Charge $0 for a component via 0, never NULL.)
        sa.CheckConstraint(
            "(prompt_price IS NULL     AND cached_price IS NULL     AND completion_price IS NULL) OR "
            "(prompt_price IS NOT NULL AND cached_price IS NOT NULL AND completion_price IS NOT NULL)",
            name="model_price_rate_all_or_none_ck",
        ),
        # effective_from must strictly precede effective_to. An equal-bound row is
        # an EMPTY tstzrange that overlaps nothing (so the no-overlap exclusion
        # wouldn't catch it) yet the rater's [from, to) predicate never matches it
        # — a silently inert dead price row. Reject at write time.
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_from < effective_to",
            name="model_price_effective_order_ck",
        ),
    )
    op.create_index(
        "model_price_model_id_effective_from_ix",
        "model_price",
        ["model_id", "effective_from"],
        unique=False,
    )
    # FORWARD-ONLY, NON-OVERLAPPING effective-dating: at most ONE price row matches
    # per (model_id, instant). Two overlapping rows would let the rating join FAN
    # OUT and silently OVER-bill; this GiST exclusion makes that data IMPOSSIBLE.
    # ExcludeConstraint is Postgres-specific; create it via raw SQL so this file has
    # no dialect-import requirement.
    op.execute(
        "ALTER TABLE model_price ADD CONSTRAINT model_price_no_overlap "
        "EXCLUDE USING gist (model_id WITH =, "
        "tstzrange(effective_from, effective_to) WITH &&)"
    )

    # --- derivation_policy: the single GLOBAL fine-tune derivation rule ---
    op.create_table(
        "derivation_policy",
        sa.Column("id", sa.Unicode(length=32), nullable=False),
        sa.Column("function", sa.Unicode(length=32), nullable=False),
        # 'multiplier' factor (dimensionless) / 'markup' additive per-token amount.
        sa.Column("factor", MONEY, nullable=True),
        sa.Column("markup", MONEY, nullable=True),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.Unicode(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_derivation_policy")),
        sa.CheckConstraint(
            "function IN ('identity', 'multiplier', 'markup')",
            name="derivation_policy_function_ck",
        ),
        # Each function carries exactly its own parameter (and only it).
        sa.CheckConstraint(
            "(function = 'identity'   AND factor IS NULL     AND markup IS NULL) OR "
            "(function = 'multiplier' AND factor IS NOT NULL AND markup IS NULL) OR "
            "(function = 'markup'     AND markup IS NOT NULL AND factor IS NULL)",
            name="derivation_policy_params_ck",
        ),
    )
    # Single global policy per instant (no per-base scope in v1): the constant 0 is
    # the "all rows one group" equality key, so any two overlapping policy windows
    # are rejected — exactly one policy in effect at any instant.
    op.execute(
        "ALTER TABLE derivation_policy ADD CONSTRAINT derivation_policy_no_overlap "
        "EXCLUDE USING gist ((0) WITH =, "
        "tstzrange(effective_from, effective_to) WITH &&)"
    )

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
    # created_at). The index must be on that EXACT expression: Postgres matches
    # index expressions structurally, so an index on bare (event_ts) — partial or
    # not — can never serve the COALESCE predicate and the rater would seq-scan a
    # table that only grows. Raw SQL because expression indexes are clumsy through
    # op.create_index.
    op.execute(
        "CREATE INDEX billing_event_rating_instant_ix "
        "ON billing_event ((COALESCE(event_ts, created_at)))"
    )


def downgrade():
    op.execute("DROP INDEX billing_event_rating_instant_ix")
    op.drop_index("rated_usage_auth_id_window_start_ix", table_name="rated_usage")
    op.drop_table("rated_usage")
    op.execute("ALTER TABLE derivation_policy DROP CONSTRAINT derivation_policy_no_overlap")
    op.drop_table("derivation_policy")
    op.execute("ALTER TABLE model_price DROP CONSTRAINT model_price_no_overlap")
    op.drop_index("model_price_model_id_effective_from_ix", table_name="model_price")
    op.drop_table("model_price")
