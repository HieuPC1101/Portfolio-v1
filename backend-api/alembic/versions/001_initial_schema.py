"""Initial schema

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("username", sa.String(length=50), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("hashed_password", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "is_verified", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_users_id", "users", ["id"], unique=False)
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.create_table(
        "user_settings",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("default_investment", sa.Numeric(15, 2), nullable=True),
        sa.Column("preferred_market", sa.String(length=20), nullable=True),
        sa.Column("risk_tolerance", sa.String(length=20), nullable=True),
        sa.Column("notifications_enabled", sa.Boolean(), nullable=True),
        sa.Column("theme", sa.String(length=20), nullable=True),
        sa.Column("investment_horizon", sa.String(length=30), nullable=True),
        sa.Column("preferred_sectors", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.UniqueConstraint("user_id", name="uq_user_settings_user_id"),
    )
    op.create_index("ix_user_settings_id", "user_settings", ["id"], unique=False)

    op.create_table(
        "user_sessions",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("refresh_token", sa.String(length=512), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_user_sessions_id", "user_sessions", ["id"], unique=False)
    op.create_index(
        "ix_user_sessions_refresh_token",
        "user_sessions",
        ["refresh_token"],
        unique=True,
    )

    op.create_table(
        "company_info",
        sa.Column("symbol", sa.String(length=20), primary_key=True, nullable=False),
        sa.Column("organ_name", sa.String(length=500), nullable=True),
        sa.Column("icb_name", sa.String(length=200), nullable=True),
        sa.Column("exchange", sa.String(length=20), nullable=True),
    )
    op.create_index("ix_company_info_symbol", "company_info", ["symbol"], unique=False)

    op.create_table(
        "portfolios",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("total_investment", sa.Numeric(15, 2), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_portfolios_id", "portfolios", ["id"], unique=False)

    op.create_table(
        "portfolio_stocks",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "portfolio_id",
            sa.Integer(),
            sa.ForeignKey("portfolios.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(length=10), nullable=False),
        sa.Column("shares", sa.Integer(), nullable=False),
        sa.Column("purchase_price", sa.Numeric(12, 2), nullable=True),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_portfolio_stocks_id", "portfolio_stocks", ["id"], unique=False)

    op.create_table(
        "watchlists",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_watchlists_id", "watchlists", ["id"], unique=False)

    op.create_table(
        "watchlist_stocks",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "watchlist_id",
            sa.Integer(),
            sa.ForeignKey("watchlists.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(length=10), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbol"),
    )
    op.create_index("ix_watchlist_stocks_id", "watchlist_stocks", ["id"], unique=False)

    op.create_table(
        "optimization_runs",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "portfolio_id",
            sa.Integer(),
            sa.ForeignKey("portfolios.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("model_name", sa.String(length=50), nullable=False),
        sa.Column("input_symbols", postgresql.ARRAY(sa.String()), nullable=False),
        sa.Column("total_investment", sa.Numeric(15, 2), nullable=False),
        sa.Column("expected_return", sa.Numeric(8, 4), nullable=True),
        sa.Column("risk_volatility", sa.Numeric(8, 4), nullable=True),
        sa.Column("sharpe_ratio", sa.Numeric(8, 4), nullable=True),
        sa.Column("weights", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("shares", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("leftover_cash", sa.Numeric(12, 2), nullable=True),
        sa.Column("extra_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_optimization_runs_id", "optimization_runs", ["id"], unique=False
    )

    op.create_table(
        "backtest_results",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "optimization_run_id",
            sa.Integer(),
            sa.ForeignKey("optimization_runs.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("initial_capital", sa.Numeric(15, 2), nullable=True),
        sa.Column("final_value", sa.Numeric(15, 2), nullable=True),
        sa.Column("total_return", sa.Numeric(8, 4), nullable=True),
        sa.Column("annualized_return", sa.Numeric(8, 4), nullable=True),
        sa.Column("volatility", sa.Numeric(8, 4), nullable=True),
        sa.Column("max_drawdown", sa.Numeric(8, 4), nullable=True),
        sa.Column("sharpe_ratio", sa.Numeric(8, 4), nullable=True),
        sa.Column("win_rate", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "backtest_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_backtest_results_id", "backtest_results", ["id"], unique=False)

    op.create_table(
        "stock_prices_cache",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("symbol", sa.String(length=10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(12, 2), nullable=True),
        sa.Column("high", sa.Numeric(12, 2), nullable=True),
        sa.Column("low", sa.Numeric(12, 2), nullable=True),
        sa.Column("close", sa.Numeric(12, 2), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.UniqueConstraint("symbol", "date", name="uq_symbol_date"),
    )
    op.create_index(
        "ix_stock_prices_cache_id", "stock_prices_cache", ["id"], unique=False
    )
    op.create_index(
        "ix_stock_prices_cache_symbol", "stock_prices_cache", ["symbol"], unique=False
    )
    op.create_index(
        "ix_stock_prices_cache_date", "stock_prices_cache", ["date"], unique=False
    )
    op.create_index(
        "idx_symbol_date_desc", "stock_prices_cache", ["symbol", "date"], unique=False
    )

    op.create_table(
        "fundamentals_cache",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("symbol", sa.String(length=10), nullable=False),
        sa.Column("pe_ratio", sa.Numeric(8, 2), nullable=True),
        sa.Column("pb_ratio", sa.Numeric(8, 2), nullable=True),
        sa.Column("eps", sa.Numeric(12, 2), nullable=True),
        sa.Column("roe", sa.Numeric(8, 4), nullable=True),
        sa.Column("roa", sa.Numeric(8, 4), nullable=True),
        sa.Column("profit_margin", sa.Numeric(8, 4), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "fundamentals_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_fundamentals_cache_id", "fundamentals_cache", ["id"], unique=False
    )
    op.create_index(
        "ix_fundamentals_cache_symbol", "fundamentals_cache", ["symbol"], unique=True
    )

    op.create_table(
        "chat_conversations",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_chat_conversations_user_id", "chat_conversations", ["user_id"], unique=False
    )

    op.create_table(
        "chat_messages",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column(
            "conversation_id",
            sa.String(length=36),
            sa.ForeignKey("chat_conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(length=20), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("sources", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "suggested_actions", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_chat_messages_conversation_id",
        "chat_messages",
        ["conversation_id"],
        unique=False,
    )

    op.create_table(
        "chat_feedback",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "conversation_id",
            sa.String(length=36),
            sa.ForeignKey("chat_conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "message_id",
            sa.String(length=36),
            sa.ForeignKey("chat_messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rating", sa.Integer(), nullable=False),
        sa.Column("feedback_text", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.create_index("ix_chat_feedback_id", "chat_feedback", ["id"], unique=False)
    op.create_index(
        "ix_chat_feedback_conversation_id",
        "chat_feedback",
        ["conversation_id"],
        unique=False,
    )
    op.create_index(
        "ix_chat_feedback_user_id", "chat_feedback", ["user_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_chat_feedback_user_id", table_name="chat_feedback")
    op.drop_index("ix_chat_feedback_conversation_id", table_name="chat_feedback")
    op.drop_index("ix_chat_feedback_id", table_name="chat_feedback")
    op.drop_table("chat_feedback")

    op.drop_index("ix_chat_messages_conversation_id", table_name="chat_messages")
    op.drop_table("chat_messages")

    op.drop_index("ix_chat_conversations_user_id", table_name="chat_conversations")
    op.drop_table("chat_conversations")

    op.drop_index("ix_fundamentals_cache_symbol", table_name="fundamentals_cache")
    op.drop_index("ix_fundamentals_cache_id", table_name="fundamentals_cache")
    op.drop_table("fundamentals_cache")

    op.drop_index("idx_symbol_date_desc", table_name="stock_prices_cache")
    op.drop_index("ix_stock_prices_cache_date", table_name="stock_prices_cache")
    op.drop_index("ix_stock_prices_cache_symbol", table_name="stock_prices_cache")
    op.drop_index("ix_stock_prices_cache_id", table_name="stock_prices_cache")
    op.drop_table("stock_prices_cache")

    op.drop_index("ix_backtest_results_id", table_name="backtest_results")
    op.drop_table("backtest_results")

    op.drop_index("ix_optimization_runs_id", table_name="optimization_runs")
    op.drop_table("optimization_runs")

    op.drop_index("ix_watchlist_stocks_id", table_name="watchlist_stocks")
    op.drop_table("watchlist_stocks")

    op.drop_index("ix_watchlists_id", table_name="watchlists")
    op.drop_table("watchlists")

    op.drop_index("ix_portfolio_stocks_id", table_name="portfolio_stocks")
    op.drop_table("portfolio_stocks")

    op.drop_index("ix_portfolios_id", table_name="portfolios")
    op.drop_table("portfolios")

    op.drop_index("ix_company_info_symbol", table_name="company_info")
    op.drop_table("company_info")

    op.drop_index("ix_user_sessions_refresh_token", table_name="user_sessions")
    op.drop_index("ix_user_sessions_id", table_name="user_sessions")
    op.drop_table("user_sessions")

    op.drop_index("ix_user_settings_id", table_name="user_settings")
    op.drop_table("user_settings")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_index("ix_users_id", table_name="users")
    op.drop_table("users")
