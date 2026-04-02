from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.models.portfolio import Portfolio
from app.models.optimization import OptimizationRun, BacktestResult
from app.schemas.optimization import (
    OptimizationRequest,
    OptimizationResponse,
    BacktestRequest,
    BacktestResponse,
    OptimizationRunResponse,
)
from app.services.optimization_service import OptimizationService

router = APIRouter(prefix="/optimize", tags=["Portfolio Optimization"])

# Initialize service
optimization_service = OptimizationService()


@router.post(
    "/run", response_model=OptimizationResponse, status_code=status.HTTP_201_CREATED
)
def run_optimization(
    request: OptimizationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Run portfolio optimization.

    Supports multiple optimization models:
    - **markowitz**: Mean-Variance Optimization (Markowitz)
    - **max_sharpe**: Maximum Sharpe Ratio
    - **min_volatility**: Minimum Volatility
    - **hrp**: Hierarchical Risk Parity
    - **min_cvar**: Minimum Conditional Value at Risk
    - **min_cdar**: Minimum Conditional Drawdown at Risk

    Parameters:
    - **portfolio_id**: Optional portfolio ID to optimize
    - **symbols**: List of stock symbols (required if no portfolio_id)
    - **model**: Optimization model to use
    - **start_date**: Historical data start date
    - **end_date**: Historical data end date
    - **constraints**: Optional optimization constraints
    """
    # Verify portfolio if provided
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

    # Validate symbols
    symbols = request.symbols
    if not symbols and portfolio:
        # Get symbols from portfolio
        from app.models.portfolio import PortfolioStock

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
        # Run optimization
        result = optimization_service.optimize_portfolio(
            symbols=symbols,
            model=request.model,
            start_date=request.start_date,
            end_date=request.end_date,
            constraints=request.constraints,
        )

        # Save optimization run to database
        optimization_run = OptimizationRun(
            user_id=current_user.id,
            portfolio_id=request.portfolio_id,
            model_type=request.model,
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            weights=result.get("weights", {}),
            expected_return=result.get("expected_return"),
            expected_volatility=result.get("expected_volatility"),
            sharpe_ratio=result.get("sharpe_ratio"),
            optimization_params=request.constraints or {},
            result_data=result,
        )

        db.add(optimization_run)
        db.commit()
        db.refresh(optimization_run)

        return {
            "optimization_id": optimization_run.id,
            "model": request.model,
            "symbols": symbols,
            "weights": result.get("weights", {}),
            "metrics": {
                "expected_return": result.get("expected_return"),
                "expected_volatility": result.get("expected_volatility"),
                "sharpe_ratio": result.get("sharpe_ratio"),
                "cvar": result.get("cvar"),
                "max_drawdown": result.get("max_drawdown"),
            },
            "efficient_frontier": result.get("efficient_frontier"),
            "created_at": optimization_run.created_at,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Optimization failed: {str(e)}",
        )


@router.get("/runs", response_model=List[OptimizationRunResponse])
def list_optimization_runs(
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio ID"),
    model_type: Optional[str] = Query(None, description="Filter by model type"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get optimization run history for the current user.

    Can filter by portfolio or model type.
    """
    query = db.query(OptimizationRun).filter(OptimizationRun.user_id == current_user.id)

    if portfolio_id:
        query = query.filter(OptimizationRun.portfolio_id == portfolio_id)

    if model_type:
        query = query.filter(OptimizationRun.model_type == model_type)

    runs = (
        query.order_by(OptimizationRun.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return runs


@router.get("/runs/{run_id}", response_model=OptimizationRunResponse)
def get_optimization_run(
    run_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get details of a specific optimization run.
    """
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
    """
    Delete an optimization run.
    """
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
    """
    Run portfolio backtest.

    Tests portfolio performance over historical data.

    Parameters:
    - **optimization_id**: Optional optimization run ID to backtest
    - **weights**: Portfolio weights (required if no optimization_id)
    - **symbols**: Stock symbols (required if no optimization_id)
    - **start_date**: Backtest start date
    - **end_date**: Backtest end date
    - **initial_capital**: Starting capital amount
    - **rebalance_frequency**: How often to rebalance (daily, weekly, monthly)
    """
    # Get weights from optimization run if provided
    weights = request.weights
    symbols = request.symbols

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
        symbols = optimization_run.symbols

    if not weights or not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Weights and symbols required",
        )

    try:
        # Run backtest
        result = optimization_service.backtest_portfolio(
            symbols=symbols,
            weights=weights,
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=request.initial_capital,
            rebalance_frequency=request.rebalance_frequency,
        )

        # Save backtest result
        backtest = BacktestResult(
            user_id=current_user.id,
            optimization_id=request.optimization_id,
            symbols=symbols,
            weights=weights,
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
            backtest_data=result,
        )

        db.add(backtest)
        db.commit()
        db.refresh(backtest)

        return {
            "backtest_id": backtest.id,
            "optimization_id": request.optimization_id,
            "symbols": symbols,
            "weights": weights,
            "performance": {
                "initial_capital": request.initial_capital,
                "final_value": result.get("final_value"),
                "total_return": result.get("total_return"),
                "annualized_return": result.get("annualized_return"),
                "volatility": result.get("volatility"),
                "sharpe_ratio": result.get("sharpe_ratio"),
                "max_drawdown": result.get("max_drawdown"),
                "win_rate": result.get("win_rate"),
            },
            "equity_curve": result.get("equity_curve"),
            "drawdown_curve": result.get("drawdown_curve"),
            "trades": result.get("trades"),
            "created_at": backtest.created_at,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backtest failed: {str(e)}",
        )


@router.get("/backtests", response_model=List[BacktestResponse])
def list_backtests(
    optimization_id: Optional[int] = Query(
        None, description="Filter by optimization ID"
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get backtest history for the current user.
    """
    query = db.query(BacktestResult).filter(BacktestResult.user_id == current_user.id)

    if optimization_id:
        query = query.filter(BacktestResult.optimization_id == optimization_id)

    backtests = (
        query.order_by(BacktestResult.created_at.desc()).offset(skip).limit(limit).all()
    )

    return backtests


@router.get("/backtests/{backtest_id}", response_model=BacktestResponse)
def get_backtest(
    backtest_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get details of a specific backtest.
    """
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
    """
    Delete a backtest result.
    """
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
    """
    Get list of available optimization models with descriptions.
    """
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
