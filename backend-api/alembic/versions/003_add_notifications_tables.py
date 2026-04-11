"""Add notifications and notification rules tables

Revision ID: 003_add_notifications_tables
Revises: 002_add_news_articles_table
Create Date: 2026-04-09
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "003_add_notifications_tables"
down_revision = "002_add_news_articles_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_settings",
        sa.Column(
            "notification_price_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "user_settings",
        sa.Column(
            "notification_news_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "user_settings",
        sa.Column(
            "notification_portfolio_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )

    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("type", sa.String(length=50), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default=sa.text("'unread'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_notifications_id", "notifications", ["id"], unique=False)
    op.create_index(
        "ix_notifications_user_id",
        "notifications",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_notifications_user_status_created_at_desc",
        "notifications",
        ["user_id", "status", sa.text("created_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_notifications_user_created_at_desc",
        "notifications",
        ["user_id", sa.text("created_at DESC")],
        unique=False,
    )

    op.create_table(
        "notification_rules",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rule_type", sa.String(length=50), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=True),
        sa.Column("operator", sa.String(length=20), nullable=True),
        sa.Column("threshold_value", sa.Numeric(18, 6), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            "cooldown_minutes",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("60"),
        ),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_index(
        "ix_notification_rules_id", "notification_rules", ["id"], unique=False
    )
    op.create_index(
        "ix_notification_rules_user_id",
        "notification_rules",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_notification_rules_user_active",
        "notification_rules",
        ["user_id", "is_active"],
        unique=False,
    )
    op.create_index(
        "ix_notification_rules_user_type",
        "notification_rules",
        ["user_id", "rule_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_notification_rules_user_type", table_name="notification_rules")
    op.drop_index("ix_notification_rules_user_active", table_name="notification_rules")
    op.drop_index("ix_notification_rules_user_id", table_name="notification_rules")
    op.drop_index("ix_notification_rules_id", table_name="notification_rules")
    op.drop_table("notification_rules")

    op.drop_index("ix_notifications_user_created_at_desc", table_name="notifications")
    op.drop_index(
        "ix_notifications_user_status_created_at_desc", table_name="notifications"
    )
    op.drop_index("ix_notifications_user_id", table_name="notifications")
    op.drop_index("ix_notifications_id", table_name="notifications")
    op.drop_table("notifications")

    op.drop_column("user_settings", "notification_portfolio_enabled")
    op.drop_column("user_settings", "notification_news_enabled")
    op.drop_column("user_settings", "notification_price_enabled")
