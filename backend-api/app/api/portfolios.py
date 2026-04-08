from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.models.portfolio import Portfolio, PortfolioStock, Watchlist, WatchlistStock
from app.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    PortfolioStockCreate,
    PortfolioStockUpdate,
    PortfolioStockResponse,
    WatchlistCreate,
    WatchlistUpdate,
    WatchlistResponse,
    WatchlistStockCreate,
)

router = APIRouter(prefix="/portfolios", tags=["Portfolio Management"])


# ========== Portfolio CRUD ==========


@router.post("", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
def create_portfolio(
    portfolio_data: PortfolioCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Create a new portfolio for the current user.

    - **name**: Portfolio name
    - **description**: Optional description
    - **total_investment**: Starting invested capital amount
    """
    portfolio = Portfolio(
        user_id=current_user.id,
        name=portfolio_data.name,
        description=portfolio_data.description,
        total_investment=portfolio_data.total_investment or 0,
    )

    db.add(portfolio)
    db.commit()
    db.refresh(portfolio)

    return portfolio


@router.get("", response_model=List[PortfolioResponse])
def list_portfolios(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get all portfolios for the current user.

    Returns list of portfolios with pagination.
    """
    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.user_id == current_user.id)
        .offset(skip)
        .limit(limit)
        .all()
    )

    return portfolios


@router.get("/{portfolio_id}", response_model=PortfolioResponse)
def get_portfolio(
    portfolio_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get a specific portfolio by ID.

    Only returns portfolio if it belongs to the current user.
    """
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    return portfolio


@router.put("/{portfolio_id}", response_model=PortfolioResponse)
def update_portfolio(
    portfolio_id: int,
    portfolio_data: PortfolioUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Update a portfolio.

    Can update name, description, and other non-critical fields.
    """
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Update fields
    update_data = portfolio_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(portfolio, field, value)

    db.commit()
    db.refresh(portfolio)

    return portfolio


@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_portfolio(
    portfolio_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """
    Delete a portfolio.

    Also deletes all associated stocks and optimization runs.
    """
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    db.delete(portfolio)
    db.commit()


# ========== Portfolio Stocks ==========


@router.post(
    "/{portfolio_id}/stocks",
    response_model=PortfolioStockResponse,
    status_code=status.HTTP_201_CREATED,
)
def add_stock_to_portfolio(
    portfolio_id: int,
    stock_data: PortfolioStockCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Add a stock to a portfolio.

    - **symbol**: Stock ticker symbol
    - **shares**: Number of shares
    - **purchase_price**: Price per share at purchase
    """
    # Verify portfolio ownership
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Check if stock already exists in portfolio
    existing_stock = (
        db.query(PortfolioStock)
        .filter(
            PortfolioStock.portfolio_id == portfolio_id,
            PortfolioStock.symbol == stock_data.symbol,
        )
        .first()
    )

    if existing_stock:
        # Update existing position
        existing_shares = existing_stock.shares
        incoming_shares = stock_data.shares

        existing_price = float(existing_stock.purchase_price or 0)
        incoming_price = float(stock_data.purchase_price or 0)

        total_shares = existing_shares + incoming_shares

        if total_shares <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Total shares must be greater than 0",
            )

        # Recalculate average purchase price
        total_cost = (existing_shares * existing_price) + (
            incoming_shares * incoming_price
        )
        existing_stock.shares = total_shares
        existing_stock.purchase_price = total_cost / total_shares

        db.commit()
        db.refresh(existing_stock)
        return existing_stock

    # Create new stock position
    stock = PortfolioStock(
        portfolio_id=portfolio_id,
        symbol=stock_data.symbol,
        shares=stock_data.shares,
        purchase_price=stock_data.purchase_price,
    )

    db.add(stock)
    db.commit()
    db.refresh(stock)

    return stock


@router.get("/{portfolio_id}/stocks", response_model=List[PortfolioStockResponse])
def list_portfolio_stocks(
    portfolio_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get all stocks in a portfolio.
    """
    # Verify portfolio ownership
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    stocks = (
        db.query(PortfolioStock)
        .filter(PortfolioStock.portfolio_id == portfolio_id)
        .all()
    )

    return stocks


@router.put("/{portfolio_id}/stocks/{stock_id}", response_model=PortfolioStockResponse)
def update_portfolio_stock(
    portfolio_id: int,
    stock_id: int,
    stock_data: PortfolioStockUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Update a stock position in a portfolio.

    Can update shares and purchase price.
    """
    # Verify portfolio ownership
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Get stock
    stock = (
        db.query(PortfolioStock)
        .filter(
            PortfolioStock.id == stock_id, PortfolioStock.portfolio_id == portfolio_id
        )
        .first()
    )

    if not stock:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stock not found in portfolio"
        )

    # Update fields
    update_data = stock_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(stock, field, value)

    db.commit()
    db.refresh(stock)

    return stock


@router.delete(
    "/{portfolio_id}/stocks/{stock_id}", status_code=status.HTTP_204_NO_CONTENT
)
def remove_stock_from_portfolio(
    portfolio_id: int,
    stock_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """
    Remove a stock from a portfolio.
    """
    # Verify portfolio ownership
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Get and delete stock
    stock = (
        db.query(PortfolioStock)
        .filter(
            PortfolioStock.id == stock_id, PortfolioStock.portfolio_id == portfolio_id
        )
        .first()
    )

    if not stock:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stock not found in portfolio"
        )

    db.delete(stock)
    db.commit()


# ========== Watchlists ==========


@router.post(
    "/watchlists", response_model=WatchlistResponse, status_code=status.HTTP_201_CREATED
)
def create_watchlist(
    watchlist_data: WatchlistCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Create a new watchlist for the current user.

    - **name**: Watchlist name
    - **description**: Optional description
    """
    watchlist = Watchlist(
        user_id=current_user.id,
        name=watchlist_data.name,
        description=watchlist_data.description,
    )

    db.add(watchlist)
    db.commit()
    db.refresh(watchlist)

    return watchlist


@router.get("/watchlists", response_model=List[WatchlistResponse])
def list_watchlists(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get all watchlists for the current user.
    """
    watchlists = (
        db.query(Watchlist)
        .filter(Watchlist.user_id == current_user.id)
        .offset(skip)
        .limit(limit)
        .all()
    )

    return watchlists


@router.get("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
def get_watchlist(
    watchlist_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get a specific watchlist by ID.
    """
    watchlist = (
        db.query(Watchlist)
        .filter(Watchlist.id == watchlist_id, Watchlist.user_id == current_user.id)
        .first()
    )

    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Watchlist not found"
        )

    return watchlist


@router.put("/watchlists/{watchlist_id}", response_model=WatchlistResponse)
def update_watchlist(
    watchlist_id: int,
    watchlist_data: WatchlistUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Update a watchlist.
    """
    watchlist = (
        db.query(Watchlist)
        .filter(Watchlist.id == watchlist_id, Watchlist.user_id == current_user.id)
        .first()
    )

    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Watchlist not found"
        )

    # Update fields
    update_data = watchlist_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(watchlist, field, value)

    db.commit()
    db.refresh(watchlist)

    return watchlist


@router.delete("/watchlists/{watchlist_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_watchlist(
    watchlist_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """
    Delete a watchlist.
    """
    watchlist = (
        db.query(Watchlist)
        .filter(Watchlist.id == watchlist_id, Watchlist.user_id == current_user.id)
        .first()
    )

    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Watchlist not found"
        )

    db.delete(watchlist)
    db.commit()


@router.post("/watchlists/{watchlist_id}/stocks", status_code=status.HTTP_201_CREATED)
def add_stock_to_watchlist(
    watchlist_id: int,
    stock_data: WatchlistStockCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Add a stock to a watchlist.

    - **symbol**: Stock ticker symbol
    """
    # Verify watchlist ownership
    watchlist = (
        db.query(Watchlist)
        .filter(Watchlist.id == watchlist_id, Watchlist.user_id == current_user.id)
        .first()
    )

    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Watchlist not found"
        )

    # Check if stock already in watchlist
    existing = (
        db.query(WatchlistStock)
        .filter(
            WatchlistStock.watchlist_id == watchlist_id,
            WatchlistStock.symbol == stock_data.symbol,
        )
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Stock already in watchlist"
        )

    # Add stock
    stock = WatchlistStock(
        watchlist_id=watchlist_id,
        symbol=stock_data.symbol,
    )

    db.add(stock)
    db.commit()
    db.refresh(stock)

    return stock


@router.delete(
    "/watchlists/{watchlist_id}/stocks/{symbol}", status_code=status.HTTP_204_NO_CONTENT
)
def remove_stock_from_watchlist(
    watchlist_id: int,
    symbol: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """
    Remove a stock from a watchlist.
    """
    # Verify watchlist ownership
    watchlist = (
        db.query(Watchlist)
        .filter(Watchlist.id == watchlist_id, Watchlist.user_id == current_user.id)
        .first()
    )

    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Watchlist not found"
        )

    # Remove stock
    stock = (
        db.query(WatchlistStock)
        .filter(
            WatchlistStock.watchlist_id == watchlist_id, WatchlistStock.symbol == symbol
        )
        .first()
    )

    if not stock:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stock not found in watchlist"
        )

    db.delete(stock)
    db.commit()
