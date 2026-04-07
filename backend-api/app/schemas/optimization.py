"""Optimization schemas for request/response validation."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class OptimizationRequest(BaseModel):
    """Schema for optimization request."""

    symbols: List[str] = Field(default_factory=list)
    model: str = Field(
        ...,
        pattern=r"^(Markowitz|Max_Sharpe|Min_Volatility|HRP|Min_CVaR|Min_CDaR|markowitz|max_sharpe|min_volatility|hrp|min_cvar|min_cdar)$",
    )
    investment: Decimal = Field(..., gt=0)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    portfolio_id: Optional[int] = None
    constraints: Optional[Dict[str, Any]] = None


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


class OptimizationRunResponse(OptimizationResponse):
    """Schema for optimization run history rows."""


class BacktestRequest(BaseModel):
    """Schema for backtest request."""

    optimization_id: Optional[int] = None
    weights: Optional[Dict[str, float]] = None
    symbols: Optional[List[str]] = None
    start_date: date
    end_date: date
    initial_capital: Decimal = Field(..., gt=0)
    rebalance_frequency: str = Field(default="monthly")


class BacktestResponse(BaseModel):
    """Schema for backtest response."""

    id: int
    user_id: int
    optimization_run_id: Optional[int]
    start_date: date
    end_date: date
    initial_capital: Optional[Decimal]
    final_value: Optional[Decimal]
    total_return: Optional[Decimal]
    annualized_return: Optional[Decimal]
    volatility: Optional[Decimal]
    max_drawdown: Optional[Decimal]
    sharpe_ratio: Optional[Decimal]
    win_rate: Optional[Decimal]
    backtest_data: Optional[Dict[str, Any]]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


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


class OptimizationComparison(BaseModel):
    """Schema for comparing multiple optimization results."""

    optimizations: List[OptimizationResponse]
    comparison_metrics: Dict[str, Any]
