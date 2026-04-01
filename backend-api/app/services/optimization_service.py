"""Portfolio optimization service orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from backend.services.market_service import get_market_data_service
from backend.models.portfolio_models import (
    hrp_model,
    markowitz_optimization,
    max_sharpe,
    min_cdar,
    min_cvar,
    min_volatility,
)


@dataclass(frozen=True)
class OptimizationRequest:
    """Input model for portfolio optimization runs."""

    prices: pd.DataFrame
    total_investment: float
    mode: str = "manual"


class OptimizationService:
    """Facade to execute optimization models with a stable interface."""

    def __init__(self, latest_price_provider):
        self._latest_price_provider = latest_price_provider
        self._model_map = {
            "Mô hình Markowitz": lambda d, ti: markowitz_optimization(d, ti, self._latest_price_provider),
            "Mô hình Max Sharpe Ratio": lambda d, ti: max_sharpe(d, ti, self._latest_price_provider),
            "Mô hình Min Volatility": lambda d, ti: min_volatility(d, ti, self._latest_price_provider),
            "Mô hình HRP": lambda d, ti: hrp_model(d, ti, self._latest_price_provider),
            "Mô hình Min CVaR": lambda d, ti: min_cvar(d, ti, self._latest_price_provider),
            "Mô hình Min CDaR": lambda d, ti: min_cdar(d, ti, self._latest_price_provider),
        }

    def available_models(self) -> Dict[str, object]:
        """Expose supported models for UI binding."""
        return dict(self._model_map)

    def run_model(self, model_name: str, request: OptimizationRequest) -> Optional[dict]:
        """Run a single optimization model by name."""
        model = self._model_map.get(model_name)
        if model is None:
            raise ValueError(f"Unsupported model: {model_name}")
        return model(request.prices, request.total_investment)

    def run_all(self, request: OptimizationRequest) -> Dict[str, dict]:
        """Run all available optimization models."""
        results: Dict[str, dict] = {}
        for model_name, model in self._model_map.items():
            result = model(request.prices, request.total_investment)
            if result:
                results[model_name] = result
        return results


_optimization_service: Optional[OptimizationService] = None


def get_optimization_service() -> OptimizationService:
    """Return singleton optimization service."""
    global _optimization_service
    if _optimization_service is None:
        market_service = get_market_data_service()
        _optimization_service = OptimizationService(market_service.latest_prices)
    return _optimization_service
