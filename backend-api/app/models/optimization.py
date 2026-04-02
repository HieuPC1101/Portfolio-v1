"""
Optimization run models.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Numeric,
    ForeignKey,
    Date,
    Index,
    UniqueConstraint,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class OptimizationRun(Base):
    """Optimization run results."""

    __tablename__ = "optimization_runs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    portfolio_id = Column(
        Integer, ForeignKey("portfolios.id", ondelete="SET NULL"), nullable=True
    )

    # Model configuration
    model_name = Column(String(50), nullable=False)  # Markowitz, Max_Sharpe, etc.
    input_symbols = Column(ARRAY(String), nullable=False)  # ["VCB", "FPT", ...]
    total_investment = Column(Numeric(15, 2), nullable=False)

    # Results
    expected_return = Column(Numeric(8, 4), nullable=True)
    risk_volatility = Column(Numeric(8, 4), nullable=True)
    sharpe_ratio = Column(Numeric(8, 4), nullable=True)
    weights = Column(JSONB, nullable=False)  # {"VCB": 0.25, "FPT": 0.35, ...}
    shares = Column(JSONB, nullable=False)  # {"VCB": 10, "FPT": 15, ...}
    leftover_cash = Column(Numeric(12, 2), nullable=True)

    # Additional optimization-specific data
    extra_data = Column(JSONB, nullable=True)  # For model-specific results

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="optimization_runs")
    portfolio = relationship("Portfolio", back_populates="optimization_runs")
    backtest_results = relationship(
        "BacktestResult",
        back_populates="optimization_run",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<OptimizationRun(id={self.id}, model='{self.model_name}', user_id={self.user_id})>"


class BacktestResult(Base):
    """Backtest results for an optimization run."""

    __tablename__ = "backtest_results"

    id = Column(Integer, primary_key=True, index=True)
    optimization_run_id = Column(
        Integer, ForeignKey("optimization_runs.id", ondelete="CASCADE"), nullable=False
    )

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    # Performance metrics
    total_return = Column(Numeric(8, 4), nullable=True)
    max_drawdown = Column(Numeric(8, 4), nullable=True)
    sharpe_ratio = Column(Numeric(8, 4), nullable=True)

    # Full time series data
    results_data = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    optimization_run = relationship(
        "OptimizationRun", back_populates="backtest_results"
    )

    def __repr__(self):
        return f"<BacktestResult(id={self.id}, run_id={self.optimization_run_id})>"


class StockPriceCache(Base):
    """Cache for historical stock prices."""

    __tablename__ = "stock_prices_cache"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)

    open = Column(Numeric(12, 2), nullable=True)
    high = Column(Numeric(12, 2), nullable=True)
    low = Column(Numeric(12, 2), nullable=True)
    close = Column(Numeric(12, 2), nullable=True)
    volume = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Unique constraint
    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uq_symbol_date"),
        Index("idx_symbol_date_desc", "symbol", desc("date")),
    )

    def __repr__(self):
        return f"<StockPriceCache(symbol='{self.symbol}', date={self.date})>"


class FundamentalsCache(Base):
    """Cache for stock fundamental data."""

    __tablename__ = "fundamentals_cache"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, nullable=False, index=True)

    pe_ratio = Column(Numeric(8, 2), nullable=True)
    pb_ratio = Column(Numeric(8, 2), nullable=True)
    eps = Column(Numeric(12, 2), nullable=True)
    roe = Column(Numeric(8, 4), nullable=True)
    roa = Column(Numeric(8, 4), nullable=True)
    profit_margin = Column(Numeric(8, 4), nullable=True)

    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<FundamentalsCache(symbol='{self.symbol}')>"
