"""Add news_articles table

Revision ID: 002_add_news_articles_table
Revises: 001_initial_schema
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "002_add_news_articles_table"
down_revision = "001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_articles",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("source", sa.String(length=50), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("category", sa.String(length=100), nullable=True),
        sa.Column(
            "symbols",
            postgresql.ARRAY(sa.String()),
            nullable=False,
            server_default=sa.text("'{}'::character varying[]"),
        ),
        sa.Column(
            "fetched_at", sa.DateTime(), server_default=sa.func.now(), nullable=True
        ),
    )

    op.create_index("ix_news_articles_url", "news_articles", ["url"], unique=True)
    op.create_index(
        "ix_news_articles_source", "news_articles", ["source"], unique=False
    )
    op.create_index(
        "ix_news_articles_published_at", "news_articles", ["published_at"], unique=False
    )
    op.create_index(
        "ix_news_published_at_desc",
        "news_articles",
        [sa.text("published_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_news_symbols",
        "news_articles",
        ["symbols"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_news_symbols", table_name="news_articles")
    op.drop_index("ix_news_published_at_desc", table_name="news_articles")
    op.drop_index("ix_news_articles_published_at", table_name="news_articles")
    op.drop_index("ix_news_articles_source", table_name="news_articles")
    op.drop_index("ix_news_articles_url", table_name="news_articles")
    op.drop_table("news_articles")
