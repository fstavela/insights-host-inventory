"""add gin for sap sids

Revision ID: 911b5b30db35
Revises: 7c97d8464b6b
Create Date: 2024-04-17 14:51:27.007430

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "911b5b30db35"
down_revision = "7c97d8464b6b"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.create_index(
            "idxsap_sids",
            "hosts",
            [sa.text("(system_profile_facts->'sap_sids') jsonb_path_ops")],
            postgresql_using="gin",
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.drop_index("idxsap_sids", table_name="hosts", postgresql_concurrently=True, if_exists=True)
