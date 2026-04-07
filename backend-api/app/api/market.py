from typing import Any, Dict, List, Optional
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.models.optimization import StockPriceCache, FundamentalsCache
from app.services.market_service import MarketService
from app.services.news_service import NewsService
from app.services.market_overview_service import MarketOverviewService
from app.schemas.market import (
    StockPriceResponse,
    StockInfoResponse,
    MarketIndicesResponse,
    SectorPerformanceResponse,
    NewsArticle,
    MarketOverviewResponse,
)

router = APIRouter(prefix="/market", tags=["Market Data"])

# Initialize services
market_service = MarketService()
news_service = NewsService()
market_overview_service = MarketOverviewService()


@router.get("/indices", response_model=MarketIndicesResponse)
def get_market_indices() -> Any:
    """
    Get current market indices (VN-Index, VN30, HNX, UPCOM).

    Returns real-time index values and changes.
    """
    try:
        indices_data = market_service.get_market_indices()
        return indices_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch market indices: {str(e)}"
        )


@router.get("/overview", response_model=MarketOverviewResponse)
def get_market_overview() -> Any:
    """
    Get comprehensive market overview including indices, top movers, and statistics.
    """
    try:
        overview_data = market_overview_service.get_market_overview()
        return overview_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch market overview: {str(e)}"
        )


@router.get("/sectors", response_model=List[SectorPerformanceResponse])
def get_sector_performance() -> Any:
    """
    Get sector performance data.

    Returns performance metrics for all market sectors.
    """
    try:
        sector_data = market_service.get_sector_performance()
        return sector_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch sector performance: {str(e)}"
        )


@router.get("/stock/{symbol}/price", response_model=StockPriceResponse)
def get_stock_price(
    symbol: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get historical price data for a stock symbol.

    - **symbol**: Stock ticker symbol (e.g., VNM, VCB, HPG)
    - **start_date**: Optional start date, defaults to 1 year ago
    - **end_date**: Optional end date, defaults to today

    Uses cache to minimize API calls.
    """
    symbol = symbol.upper()
    end_date_str = end_date or datetime.now().strftime("%Y-%m-%d")
    start_date_str = start_date or (datetime.now() - timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )

    try:
        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid date format, expected YYYY-MM-DD"
        )

    cached_rows = (
        db.query(StockPriceCache)
        .filter(
            StockPriceCache.symbol == symbol,
            StockPriceCache.date >= start_date_obj,
            StockPriceCache.date <= end_date_obj,
        )
        .order_by(StockPriceCache.date.asc())
        .all()
    )

    if (
        cached_rows
        and cached_rows[0].date <= start_date_obj
        and cached_rows[-1].date >= end_date_obj
    ):
        return {
            "symbol": symbol,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "data": {
                "prices": [
                    {
                        "date": row.date.isoformat(),
                        "open": float(row.open) if row.open is not None else None,
                        "high": float(row.high) if row.high is not None else None,
                        "low": float(row.low) if row.low is not None else None,
                        "close": float(row.close) if row.close is not None else None,
                        "volume": row.volume,
                    }
                    for row in cached_rows
                ]
            },
            "cached": True,
        }

    try:
        fetched_ohlc = market_service.fetch_ohlc(symbol, start_date_str, end_date_str)
        fetched_prices: Dict[str, Any] = market_service.get_stock_price(
            symbol, start_date_str, end_date_str
        )

        if fetched_ohlc is None or fetched_ohlc.empty:
            if not fetched_prices:
                raise HTTPException(status_code=404, detail="No price data available")
            fetched_ohlc = _dict_prices_to_dataframe(fetched_prices)

        rows_to_insert = []
        for _, item in fetched_ohlc.iterrows():
            item_date = _extract_row_date(item)
            if item_date is None:
                continue

            existing = (
                db.query(StockPriceCache)
                .filter(
                    StockPriceCache.symbol == symbol, StockPriceCache.date == item_date
                )
                .first()
            )
            if existing:
                existing.open = _as_float(item.get("open"))
                existing.high = _as_float(item.get("high"))
                existing.low = _as_float(item.get("low"))
                existing.close = _as_float(item.get("close"))
                existing.volume = _as_int(item.get("volume"))
                rows_to_insert.append(existing)
            else:
                row = StockPriceCache(
                    symbol=symbol,
                    date=item_date,
                    open=_as_float(item.get("open")),
                    high=_as_float(item.get("high")),
                    low=_as_float(item.get("low")),
                    close=_as_float(item.get("close")),
                    volume=_as_int(item.get("volume")),
                )
                db.add(row)
                rows_to_insert.append(row)

        db.commit()

        refreshed_rows = (
            db.query(StockPriceCache)
            .filter(
                StockPriceCache.symbol == symbol,
                StockPriceCache.date >= start_date_obj,
                StockPriceCache.date <= end_date_obj,
            )
            .order_by(StockPriceCache.date.asc())
            .all()
        )

        return {
            "symbol": symbol,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "data": {
                "prices": [
                    {
                        "date": row.date.isoformat(),
                        "open": float(row.open) if row.open is not None else None,
                        "high": float(row.high) if row.high is not None else None,
                        "low": float(row.low) if row.low is not None else None,
                        "close": float(row.close) if row.close is not None else None,
                        "volume": row.volume,
                    }
                    for row in refreshed_rows
                ]
            },
            "cached": False,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock price: {str(e)}"
        )


@router.get("/stock/{symbol}/info", response_model=StockInfoResponse)
def get_stock_info(
    symbol: str,
    db: Session = Depends(get_db),
) -> Any:
    """
    Get detailed information about a stock.

    - **symbol**: Stock ticker symbol

    Includes company info, financial metrics, and fundamentals.
    Uses cache to minimize API calls.
    """
    symbol = symbol.upper()
    cache_entry = (
        db.query(FundamentalsCache).filter(FundamentalsCache.symbol == symbol).first()
    )
    now = datetime.utcnow()

    if cache_entry:
        expires_at = cache_entry.expires_at
        if expires_at is None and cache_entry.fetched_at:
            expires_at = cache_entry.fetched_at + timedelta(hours=24)

        if expires_at and expires_at > now and cache_entry.fundamentals_data:
            return {
                "symbol": symbol,
                "data": cache_entry.fundamentals_data,
                "cached": True,
                "last_updated": cache_entry.fetched_at or now,
            }

    try:
        stock_info = market_service.get_stock_info(symbol)

        if cache_entry:
            cache_entry.fundamentals_data = stock_info
            cache_entry.expires_at = now + timedelta(hours=24)
            cache_entry.fetched_at = now
        else:
            cache_entry = FundamentalsCache(
                symbol=symbol,
                fundamentals_data=stock_info,
                expires_at=now + timedelta(hours=24),
                fetched_at=now,
            )
            db.add(cache_entry)

        db.commit()

        return {
            "symbol": symbol,
            "data": stock_info,
            "cached": False,
            "last_updated": now,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock info: {str(e)}"
        )


def _dict_prices_to_dataframe(prices: Dict[str, Any]):
    import pandas as pd

    rows = []
    for key, value in prices.items():
        rows.append(
            {
                "time": key,
                "close": value,
                "open": value,
                "high": value,
                "low": value,
                "volume": None,
            }
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    frame = frame.dropna(subset=["time"]).sort_values("time")
    return frame


def _extract_row_date(row) -> Optional[date]:
    value = row.get("time")
    if value is None:
        return None
    if hasattr(value, "date"):
        return value.date()
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


@router.get("/stock/{symbol}/fundamentals")
def get_stock_fundamentals(
    symbol: str,
    db: Session = Depends(get_db),
) -> Any:
    """
    Get fundamental analysis data for a stock.

    Includes P/E ratio, P/B ratio, ROE, ROA, debt ratios, etc.
    """
    try:
        fundamentals = market_service.get_stock_fundamentals(symbol)
        return fundamentals
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch fundamentals: {str(e)}"
        )


@router.get("/news", response_model=List[NewsArticle])
def get_market_news(
    limit: int = Query(20, ge=1, le=100, description="Number of news articles to return"),
    category: Optional[str] = Query(None, description="News category filter"),
    refresh: bool = Query(False, description="Bỏ qua cache, lấy tin mới nhất"),
) -> Any:
    """
    Get latest market news articles.

    - **limit**: Number of articles (1-100)
    - **category**: Optional category filter
    - **refresh**: Skip cache and fetch fresh articles
    """
    try:
        news_articles = news_service.get_latest_news(limit=limit, category=category, refresh=refresh)
        return news_articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch news: {str(e)}")


@router.get("/news/{symbol}", response_model=List[NewsArticle])
def get_stock_news(
    symbol: str,
    limit: int = Query(10, ge=1, le=50, description="Number of news articles to return"),
    refresh: bool = Query(False, description="Bỏ qua cache, lấy tin mới nhất"),
) -> Any:
    """
    Get news articles related to a specific stock.

    - **symbol**: Stock ticker symbol
    - **limit**: Number of articles (1-50)
    - **refresh**: Skip cache and fetch fresh articles
    """
    try:
        news_articles = news_service.get_stock_news(symbol=symbol, limit=limit, refresh=refresh)
        return news_articles
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock news: {str(e)}"
        )


@router.get("/search")
def search_stocks(
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Number of results"),
) -> Any:
    """
    Search for stocks by symbol or company name.

    - **query**: Search term (symbol or company name)
    - **limit**: Maximum number of results
    """
    try:
        results = market_service.search_stocks(query=query, limit=limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.post("/cache/clear")
def clear_cache(
    cache_type: Optional[str] = Query(
        None, description="Cache type: 'price' or 'fundamentals'"
    ),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Clear market data cache.

    Admin function to clear cached price or fundamentals data.
    """
    try:
        if cache_type == "price":
            db.query(StockPriceCache).delete()
        elif cache_type == "fundamentals":
            db.query(FundamentalsCache).delete()
        else:
            # Clear all cache
            db.query(StockPriceCache).delete()
            db.query(FundamentalsCache).delete()

        db.commit()
        return {
            "message": f"Cache cleared successfully",
            "cache_type": cache_type or "all",
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")
