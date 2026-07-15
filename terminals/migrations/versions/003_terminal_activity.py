"""Cross-worker terminal activity tracking.

Revision ID: 003_terminal_activity
Revises: 002_policy_lifecycles
"""

from alembic import op
import sqlalchemy as sa

revision = "003_terminal_activity"
down_revision = "002_policy_lifecycles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Write-heavy table accessed only by primary key — no secondary indexes.
    op.create_table(
        "terminal_activity",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("policy_id", sa.String(), nullable=False),
        sa.Column("last_active_at", sa.Float(), nullable=False),
        sa.UniqueConstraint(
            "user_id",
            "policy_id",
            name="uq_terminal_activity_user_policy",
        ),
    )


def downgrade() -> None:
    op.drop_table("terminal_activity")
