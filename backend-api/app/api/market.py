from typing import Any, List, Optional
from datetime import datetime, timedelta

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
def get_market_indices(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Any:
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
def get_market_overview(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Any:
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
def get_sector_performance(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Any:
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
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get historical price data for a stock symbol.

    - **symbol**: Stock ticker symbol (e.g., VNM, VCB, HPG)
    - **start_date**: Optional start date, defaults to 1 year ago
    - **end_date**: Optional end date, defaults to today

    Uses cache to minimize API calls.
    """
    # Set default dates if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    # Check cache first
    cache_key = f"{symbol}_{start_date}_{end_date}"
    cached_data = (
        db.query(StockPriceCache)
        .filter(
            StockPriceCache.cache_key == cache_key,
            StockPriceCache.expires_at > datetime.utcnow(),
        )
        .first()
    )

    if cached_data:
        return {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "data": cached_data.price_data,
            "cached": True,
        }

    # Fetch from service
    try:
        price_data = market_service.get_stock_price(symbol, start_date, end_date)

        # Cache the result (expires in 1 hour for historical data)
        cache_entry = StockPriceCache(
            cache_key=cache_key,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            price_data=price_data,
            expires_at=datetime.utcnow() + timedelta(hours=1),
        )
        db.add(cache_entry)
        db.commit()

        return {
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "data": price_data,
            "cached": False,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock price: {str(e)}"
        )


@router.get("/stock/{symbol}/info", response_model=StockInfoResponse)
def get_stock_info(
    symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get detailed information about a stock.

    - **symbol**: Stock ticker symbol

    Includes company info, financial metrics, and fundamentals.
    Uses cache to minimize API calls.
    """
    # Check cache first
    cached_data = (
        db.query(FundamentalsCache)
        .filter(
            FundamentalsCache.symbol == symbol,
            FundamentalsCache.expires_at > datetime.utcnow(),
        )
        .first()
    )

    if cached_data:
        return {
            "symbol": symbol,
            "data": cached_data.fundamentals_data,
            "cached": True,
            "last_updated": cached_data.created_at,
        }

    # Fetch from service
    try:
        stock_info = market_service.get_stock_info(symbol)

        # Cache the result (expires in 24 hours for fundamentals)
        cache_entry = FundamentalsCache(
            symbol=symbol,
            fundamentals_data=stock_info,
            expires_at=datetime.utcnow() + timedelta(hours=24),
        )
        db.add(cache_entry)
        db.commit()

        return {
            "symbol": symbol,
            "data": stock_info,
            "cached": False,
            "last_updated": datetime.utcnow(),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock info: {str(e)}"
        )


@router.get("/stock/{symbol}/fundamentals")
def get_stock_fundamentals(
    symbol: str,
    current_user: User = Depends(get_current_user),
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
    limit: int = Query(
        20, ge=1, le=100, description="Number of news articles to return"
    ),
    category: Optional[str] = Query(None, description="News category filter"),
    current_user: User = Depends(get_current_user),
) -> Any:
    """
    Get latest market news articles.

    - **limit**: Number of articles (1-100)
    - **category**: Optional category filter
    """
    try:
        news_articles = news_service.get_latest_news(limit=limit, category=category)
        return news_articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch news: {str(e)}")


@router.get("/news/{symbol}", response_model=List[NewsArticle])
def get_stock_news(
    symbol: str,
    limit: int = Query(
        10, ge=1, le=50, description="Number of news articles to return"
    ),
    current_user: User = Depends(get_current_user),
) -> Any:
    """
    Get news articles related to a specific stock.

    - **symbol**: Stock ticker symbol
    - **limit**: Number of articles (1-50)
    """
    try:
        news_articles = news_service.get_stock_news(symbol=symbol, limit=limit)
        return news_articles
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch stock news: {str(e)}"
        )


@router.get("/search")
def search_stocks(
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Number of results"),
    current_user: User = Depends(get_current_user),
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
