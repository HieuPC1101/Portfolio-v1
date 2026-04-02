"""
Optimization schemas for request/response validation.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Optional, Any
from datetime import datetime, date
from decimal import Decimal


# Optimization Request
class OptimizationRequest(BaseModel):
    """Schema for optimization request."""

    symbols: List[str] = Field(..., min_items=2, max_items=50)
    model: str = Field(
        ..., pattern="^(Markowitz|Max_Sharpe|Min_Volatility|HRP|Min_CVaR|Min_CDaR)$"
    )
    investment: Decimal = Field(..., gt=0)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    portfolio_id: Optional[int] = None


class OptimizationResponse(BaseModel):
    """Schema for optimization response."""

    id: int
    user_id: int
    portfolio_id: Optional[int]
    model_name: str
    input_symbols: List[str]
    total_investment: Decimal
    expected_return: Optional[Decimal]
    risk_volatility: Optional[Decimal]
    sharpe_ratio: Optional[Decimal]
    weights: Dict[str, float]
    shares: Dict[str, int]
    leftover_cash: Optional[Decimal]
    extra_data: Optional[Dict[str, Any]] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Backtest Schemas
class BacktestRequest(BaseModel):
    """Schema for backtest request."""

    optimization_run_id: int
    start_date: date
    end_date: date


class BacktestResponse(BaseModel):
    """Schema for backtest response."""

    id: int
    optimization_run_id: int
    start_date: date
    end_date: date
    total_return: Optional[Decimal]
    max_drawdown: Optional[Decimal]
    sharpe_ratio: Optional[Decimal]
    results_data: Optional[Dict[str, Any]]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Market Data Schemas
class StockPriceData(BaseModel):
    """Schema for stock price data."""

    symbol: str
    date: date
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Decimal
    volume: Optional[int]


class FundamentalData(BaseModel):
    """Schema for fundamental data."""

    symbol: str
    pe_ratio: Optional[Decimal]
    pb_ratio: Optional[Decimal]
    eps: Optional[Decimal]
    roe: Optional[Decimal]
    roa: Optional[Decimal]
    profit_margin: Optional[Decimal]
    fetched_at: datetime


# Optimization Comparison
class OptimizationComparison(BaseModel):
    """Schema for comparing multiple optimization results."""

    optimizations: List[OptimizationResponse]
    comparison_metrics: Dict[str, Any]
