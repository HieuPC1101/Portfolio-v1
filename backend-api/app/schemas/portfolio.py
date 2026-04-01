"""
Portfolio schemas for request/response validation.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime
from decimal import Decimal


# Portfolio Stock Schemas
class PortfolioStockBase(BaseModel):
    """Base portfolio stock schema."""

    symbol: str = Field(..., min_length=3, max_length=10)
    shares: int = Field(..., gt=0)
    purchase_price: Optional[Decimal] = None


class PortfolioStockCreate(PortfolioStockBase):
    """Schema for adding stock to portfolio."""

    pass


class PortfolioStockResponse(PortfolioStockBase):
    """Schema for portfolio stock response."""

    id: int
    portfolio_id: int
    added_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Portfolio Schemas
class PortfolioBase(BaseModel):
    """Base portfolio schema."""

    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None


class PortfolioCreate(PortfolioBase):
    """Schema for creating portfolio."""

    total_investment: Optional[Decimal] = Field(default=0, ge=0)


class PortfolioUpdate(BaseModel):
    """Schema for updating portfolio."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    total_investment: Optional[Decimal] = Field(None, ge=0)


class PortfolioResponse(PortfolioBase):
    """Schema for portfolio response."""

    id: int
    user_id: int
    total_investment: Decimal
    created_at: datetime
    updated_at: datetime
    stocks: List[PortfolioStockResponse] = []

    model_config = ConfigDict(from_attributes=True)


# Watchlist Schemas
class WatchlistStockBase(BaseModel):
    """Base watchlist stock schema."""

    symbol: str = Field(..., min_length=3, max_length=10)


class WatchlistStockCreate(WatchlistStockBase):
    """Schema for adding stock to watchlist."""

    pass


class WatchlistStockResponse(WatchlistStockBase):
    """Schema for watchlist stock response."""

    id: int
    watchlist_id: int
    added_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WatchlistBase(BaseModel):
    """Base watchlist schema."""

    name: str = Field(..., min_length=1, max_length=100)


class WatchlistCreate(WatchlistBase):
    """Schema for creating watchlist."""

    pass


class WatchlistUpdate(BaseModel):
    """Schema for updating watchlist."""

    name: Optional[str] = Field(None, min_length=1, max_length=100)


class WatchlistResponse(WatchlistBase):
    """Schema for watchlist response."""

    id: int
    user_id: int
    created_at: datetime
    stocks: List[WatchlistStockResponse] = []

    model_config = ConfigDict(from_attributes=True)
