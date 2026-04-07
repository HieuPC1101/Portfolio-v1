from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.optimization import BacktestResult, OptimizationRun
from app.models.portfolio import Portfolio, PortfolioStock
from app.models.user import User
from app.schemas.optimization import (
    BacktestRequest,
    BacktestResponse,
    OptimizationRequest,
    OptimizationResponse,
    OptimizationRunResponse,
)
from app.services.optimization_service import OptimizationService

router = APIRouter(prefix="/optimize", tags=["Portfolio Optimization"])

optimization_service = OptimizationService()


@router.post(
    "/run", response_model=OptimizationResponse, status_code=status.HTTP_201_CREATED
)
def run_optimization(
    request: OptimizationRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Run portfolio optimization and persist the run."""
    portfolio = None
    if request.portfolio_id:
        portfolio = (
            db.query(Portfolio)
            .filter(
                Portfolio.id == request.portfolio_id,
                Portfolio.user_id == current_user.id,
            )
            .first()
        )
        if not portfolio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

    symbols = request.symbols
    if not symbols and portfolio:
        portfolio_stocks = (
            db.query(PortfolioStock)
            .filter(PortfolioStock.portfolio_id == portfolio.id)
            .all()
        )
        symbols = [stock.symbol for stock in portfolio_stocks]

    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No symbols provided and portfolio has no stocks",
        )

    try:
        result = optimization_service.optimize_portfolio(
            symbols=symbols,
            model=request.model,
            start_date=request.start_date,
            end_date=request.end_date,
            constraints=request.constraints,
            investment=float(request.investment),
        )

        optimization_run = OptimizationRun(
            user_id=current_user.id,
            portfolio_id=request.portfolio_id,
            model_name=str(result.get("model") or request.model),
            input_symbols=[symbol.upper() for symbol in symbols],
            total_investment=request.investment,
            expected_return=result.get("expected_return"),
            risk_volatility=result.get("expected_volatility"),
            sharpe_ratio=result.get("sharpe_ratio"),
            weights=result.get("weights", {}),
            shares=result.get("shares", {}),
            leftover_cash=result.get("leftover_cash"),
            extra_data=result.get("raw_result"),
        )

        db.add(optimization_run)
        db.commit()
        db.refresh(optimization_run)
        return optimization_run
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Optimization failed: {exc}",
        )


@router.get("/runs", response_model=List[OptimizationRunResponse])
def list_optimization_runs(
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio ID"),
    model_name: Optional[str] = Query(None, description="Filter by model name"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get optimization run history for the current user."""
    query = db.query(OptimizationRun).filter(OptimizationRun.user_id == current_user.id)

    if portfolio_id:
        query = query.filter(OptimizationRun.portfolio_id == portfolio_id)

    if model_name:
        query = query.filter(OptimizationRun.model_name == model_name)

    return (
        query.order_by(OptimizationRun.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )


@router.get("/runs/{run_id}", response_model=OptimizationRunResponse)
def get_optimization_run(
    run_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get details of a specific optimization run."""
    run = (
        db.query(OptimizationRun)
        .filter(
            OptimizationRun.id == run_id, OptimizationRun.user_id == current_user.id
        )
        .first()
    )
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Optimization run not found"
        )
    return run


@router.delete("/runs/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_optimization_run(
    run_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """Delete an optimization run."""
    run = (
        db.query(OptimizationRun)
        .filter(
            OptimizationRun.id == run_id, OptimizationRun.user_id == current_user.id
        )
        .first()
    )
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Optimization run not found"
        )
    db.delete(run)
    db.commit()


@router.post(
    "/backtest", response_model=BacktestResponse, status_code=status.HTTP_201_CREATED
)
def run_backtest(
    request: BacktestRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Run portfolio backtest and persist the result."""
    weights = request.weights
    symbols = request.symbols
    optimization_run_id = request.optimization_id

    if request.optimization_id:
        optimization_run = (
            db.query(OptimizationRun)
            .filter(
                OptimizationRun.id == request.optimization_id,
                OptimizationRun.user_id == current_user.id,
            )
            .first()
        )
        if not optimization_run:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Optimization run not found",
            )

        weights = optimization_run.weights
        symbols = optimization_run.input_symbols
        optimization_run_id = optimization_run.id

    if not weights or not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Weights and symbols are required",
        )

    try:
        result = optimization_service.backtest_portfolio(
            symbols=symbols,
            weights=weights,
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=float(request.initial_capital),
            rebalance_frequency=request.rebalance_frequency,
        )

        backtest = BacktestResult(
            user_id=current_user.id,
            optimization_run_id=optimization_run_id,
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=request.initial_capital,
            final_value=result.get("final_value"),
            total_return=result.get("total_return"),
            annualized_return=result.get("annualized_return"),
            volatility=result.get("volatility"),
            sharpe_ratio=result.get("sharpe_ratio"),
            max_drawdown=result.get("max_drawdown"),
            win_rate=result.get("win_rate"),
            backtest_data=result.get("backtest_data"),
        )

        db.add(backtest)
        db.commit()
        db.refresh(backtest)
        return backtest
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backtest failed: {exc}",
        )


@router.get("/backtests", response_model=List[BacktestResponse])
def list_backtests(
    optimization_run_id: Optional[int] = Query(
        None, description="Filter by optimization run ID"
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get backtest history for the current user."""
    query = db.query(BacktestResult).filter(BacktestResult.user_id == current_user.id)

    if optimization_run_id:
        query = query.filter(BacktestResult.optimization_run_id == optimization_run_id)

    return (
        query.order_by(BacktestResult.created_at.desc()).offset(skip).limit(limit).all()
    )


@router.get("/backtests/{backtest_id}", response_model=BacktestResponse)
def get_backtest(
    backtest_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get details of a specific backtest."""
    backtest = (
        db.query(BacktestResult)
        .filter(
            BacktestResult.id == backtest_id, BacktestResult.user_id == current_user.id
        )
        .first()
    )
    if not backtest:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Backtest not found"
        )
    return backtest


@router.delete("/backtests/{backtest_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_backtest(
    backtest_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """Delete a backtest result."""
    backtest = (
        db.query(BacktestResult)
        .filter(
            BacktestResult.id == backtest_id, BacktestResult.user_id == current_user.id
        )
        .first()
    )
    if not backtest:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Backtest not found"
        )
    db.delete(backtest)
    db.commit()


@router.get("/models")
def list_optimization_models(current_user: User = Depends(get_current_user)) -> Any:
    """Get list of available optimization models with descriptions."""
    return {
        "models": [
            {
                "id": "markowitz",
                "name": "Mean-Variance Optimization (Markowitz)",
                "description": "Classic portfolio optimization balancing expected return and risk",
                "risk_metric": "Variance/Volatility",
            },
            {
                "id": "max_sharpe",
                "name": "Maximum Sharpe Ratio",
                "description": "Maximize risk-adjusted returns (Sharpe ratio)",
                "risk_metric": "Volatility",
            },
            {
                "id": "min_volatility",
                "name": "Minimum Volatility",
                "description": "Minimize portfolio volatility (conservative approach)",
                "risk_metric": "Volatility",
            },
            {
                "id": "hrp",
                "name": "Hierarchical Risk Parity",
                "description": "Diversification-focused approach using hierarchical clustering",
                "risk_metric": "Risk Parity",
            },
            {
                "id": "min_cvar",
                "name": "Minimum Conditional Value at Risk",
                "description": "Minimize expected loss in worst-case scenarios",
                "risk_metric": "CVaR (Expected Shortfall)",
            },
            {
                "id": "min_cdar",
                "name": "Minimum Conditional Drawdown at Risk",
                "description": "Minimize expected drawdown in worst-case scenarios",
                "risk_metric": "CDaR (Conditional Drawdown)",
            },
        ]
    }
