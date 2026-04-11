from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class StockPriceResponse(BaseModel):
    """Response model for stock price data"""

    symbol: str
    start_date: str
    end_date: str
    data: Dict[str, Any]
    cached: bool = False


class StockInfoResponse(BaseModel):
    """Response model for stock information"""

    symbol: str
    data: Dict[str, Any]
    cached: bool = False
    last_updated: datetime


class MarketIndexData(BaseModel):
    """Individual market index data"""

    name: str
    value: float
    change: float
    change_percent: float
    volume: Optional[float] = None

    class Config:
        from_attributes = True


class MarketIndicesResponse(BaseModel):
    """Response model for market indices"""

    vnindex: MarketIndexData
    vn30: MarketIndexData
    hnx: MarketIndexData
    upcom: MarketIndexData
    timestamp: datetime

    class Config:
        from_attributes = True


class SectorPerformanceResponse(BaseModel):
    """Response model for sector performance"""

    sector_name: str
    change_percent: float
    volume: float
    top_stocks: Optional[List[Dict[str, Any]]] = None

    class Config:
        from_attributes = True


class TopMover(BaseModel):
    """Top gaining/losing stock"""

    symbol: str
    name: Optional[str] = None
    price: float
    change: float
    change_percent: float
    volume: float

    class Config:
        from_attributes = True


class MarketStatistics(BaseModel):
    """Market statistics data"""

    total_stocks: int
    advancing: int
    declining: int
    unchanged: int
    total_volume: float
    total_value: float

    class Config:
        from_attributes = True


class MarketOverviewResponse(BaseModel):
    """Comprehensive market overview response"""

    indices: MarketIndicesResponse
    top_gainers: List[TopMover]
    top_losers: List[TopMover]
    most_active: List[TopMover]
    statistics: MarketStatistics
    timestamp: datetime

    class Config:
        from_attributes = True


class NewsArticle(BaseModel):
    """News article model"""

    title: str
    summary: Optional[str] = None
    url: Optional[str] = None
    source: Optional[str] = None
    published_at: Optional[datetime] = None
    symbols: Optional[List[str]] = None
    category: Optional[str] = None

    class Config:
        from_attributes = True


class StockSearchResult(BaseModel):
    """Stock search result"""

    symbol: str
    name: str
    exchange: Optional[str] = None
    sector: Optional[str] = None

    class Config:
        from_attributes = True


class StockOverviewResponse(BaseModel):
    symbol: str
    company_name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_float: Optional[float] = None
    listing_date: Optional[str] = None
    headquarters: Optional[str] = None
    employee_count: Optional[int] = None
    business_summary: Optional[str] = None
    latest_highlights: List[str] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    source: Optional[str] = None


class StockFinancialPeriod(BaseModel):
    period_label: str
    year: Optional[int] = None
    quarter: Optional[int] = None
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_profit: Optional[float] = None
    net_income: Optional[float] = None
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    equity: Optional[float] = None
    total_debt: Optional[float] = None
    cash_and_cash_equivalents: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    free_cash_flow: Optional[float] = None


class StockFinancialsResponse(BaseModel):
    symbol: str
    period: str
    items: List[StockFinancialPeriod] = Field(default_factory=list)
    as_of_date: Optional[str] = None
    source: Optional[str] = None


class StockRatiosResponse(BaseModel):
    symbol: str
    pe: Optional[float] = None
    pb: Optional[float] = None
    ev_ebitda: Optional[float] = None
    gross_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_to_equity: Optional[float] = None
    as_of_date: Optional[str] = None
    reporting_period: Optional[str] = None
    quality_flags: List[str] = Field(default_factory=list)
    source: Optional[str] = None
