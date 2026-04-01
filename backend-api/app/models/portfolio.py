"""
Portfolio and watchlist models.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Numeric,
    ForeignKey,
    ARRAY,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Portfolio(Base):
    """Portfolio model for managing user's stock portfolios."""

    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    total_investment = Column(Numeric(15, 2), default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    owner = relationship("User", back_populates="portfolios")
    stocks = relationship(
        "PortfolioStock", back_populates="portfolio", cascade="all, delete-orphan"
    )
    optimization_runs = relationship("OptimizationRun", back_populates="portfolio")

    def __repr__(self):
        return f"<Portfolio(id={self.id}, name='{self.name}', user_id={self.user_id})>"


class PortfolioStock(Base):
    """Individual stock within a portfolio."""

    __tablename__ = "portfolio_stocks"

    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(
        Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False
    )
    symbol = Column(String(10), nullable=False)
    shares = Column(Integer, nullable=False)
    purchase_price = Column(Numeric(12, 2), nullable=True)
    added_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    portfolio = relationship("Portfolio", back_populates="stocks")

    def __repr__(self):
        return f"<PortfolioStock(symbol='{self.symbol}', shares={self.shares})>"


class Watchlist(Base):
    """Watchlist model for tracking stocks of interest."""

    __tablename__ = "watchlists"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name = Column(String(100), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    owner = relationship("User", back_populates="watchlists")
    stocks = relationship(
        "WatchlistStock", back_populates="watchlist", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Watchlist(id={self.id}, name='{self.name}', user_id={self.user_id})>"


class WatchlistStock(Base):
    """Individual stock in a watchlist."""

    __tablename__ = "watchlist_stocks"

    id = Column(Integer, primary_key=True, index=True)
    watchlist_id = Column(
        Integer, ForeignKey("watchlists.id", ondelete="CASCADE"), nullable=False
    )
    symbol = Column(String(10), nullable=False)
    added_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    watchlist = relationship("Watchlist", back_populates="stocks")

    # Unique constraint
    from sqlalchemy import UniqueConstraint

    __table_args__ = (
        UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbol"),
    )

    def __repr__(self):
        return f"<WatchlistStock(watchlist_id={self.watchlist_id}, symbol='{self.symbol}')>"
