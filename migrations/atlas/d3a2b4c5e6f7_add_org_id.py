# pylint: disable=no-member
"""add org_id to billing_event + rated_usage (E2 customer attribution at meter time)

Revision ID: d3a2b4c5e6f7
Revises: c2f1a3b4d5e6
Create Date: 2026-06-30 00:00:00.000000

This is a READY-TO-COPY artifact maintained in the phoebe repo. To apply it,
copy this file into saturn/alembic/versions/. down_revision is pinned to
c2f1a3b4d5e6 (phoebe's rating migration) so the phoebe chain stays contiguous:
billing_event -> rating -> io_log -> org_id. At copy time, re-point down_revision
to whatever the LAST phoebe revision already applied to the shared DB is (so the
graph stays linear). See migrations/README.md.

WHY THIS IS A FOLLOW-UP MIGRATION, NOT AN EDIT TO THE CREATE MIGRATIONS:
billing_event (b1f0c2d3e4a5) and rated_usage (c2f1a3b4d5e6) have ALREADY shipped on
release-2026.02.01 / release-2026.06.01 and are in Atlas's applied chain, so their
create migrations cannot be edited in place. This revision adds org_id idempotently
to both — ADD COLUMN IF NOT EXISTS, nullable, backfill-free — exactly the pattern the
rating migration used for billing_event.base_model.

WHAT org_id IS (and why it is captured, not reconstructed):
org_id is the org that OWNS the served deployment (E2 customer attribution). Atlas
knows it at deploy time (the saturncloud.io/org-id label) and injects it as a
per-deployment Traefik header (X-Saturn-Org-Id) on the inference route. phoebe
captures it at METER time onto billing_event, and the rater carries it onto
rated_usage. This REPLACES the prior push-time reconstruction (token-push LEFT
JOINing the Atlas-owned resource_name table on resource_id), which raced deployment
teardown: a deployment deleted between meter and push lost its resource_name row,
making already-metered usage unattributable. With org captured at meter time the org
is frozen onto the event and a later teardown cannot un-attribute it.

NULLABLE on BOTH tables (deliberately, and unlike rated_usage.resource_id which is
NOT NULL): the producer header rolls out per-install, so during the rollout an event
may legitimately arrive with no org. A NOT NULL would make the drainer's batch INSERT
fail (poisoning at-least-once redelivery) and would force the rater to drop or
sentinel those rows. Keeping it nullable preserves "held, not lost": a NULL-org
rollup is withheld from the push (counted + screamed), never billed to a guessed org,
and re-pushed clean once the mapping/header is restored.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "d3a2b4c5e6f7"
down_revision = "c2f1a3b4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    # org_id on billing_event: captured verbatim from the X-Saturn-Org-Id header at
    # meter time. NULLABLE — an absent producer header must not fail the drainer's
    # batch INSERT. Idempotent (IF NOT EXISTS) so a fresh DB whose create migration
    # may later declare it directly is a harmless no-op.
    op.execute(
        "ALTER TABLE billing_event ADD COLUMN IF NOT EXISTS org_id VARCHAR(64)"
    )
    # org_id on rated_usage: carried from billing_event by the rater so token-push
    # reads the org straight off the rollup (no resource_name join). NULLABLE, unlike
    # resource_id on this table — a NULL org is the rater's fail-closed signal that
    # push must withhold + scream, not delete-by-absence and not bill a guessed org.
    op.execute(
        "ALTER TABLE rated_usage ADD COLUMN IF NOT EXISTS org_id VARCHAR(64)"
    )


def downgrade():
    # This revision OWNS both org_id columns (the create migrations predate it and do
    # not declare org_id), so it reverses its own additions. IF EXISTS for symmetry
    # with the idempotent upgrade — a partially-applied or re-run downgrade must not
    # error on an already-dropped column. Keeps up/down/up idempotent.
    op.execute("ALTER TABLE rated_usage DROP COLUMN IF EXISTS org_id")
    op.execute("ALTER TABLE billing_event DROP COLUMN IF EXISTS org_id")
