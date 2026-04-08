"""Portfolio optimization service orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from app.portfolio_models.portfolio_models import (
    hrp_model,
    markowitz_optimization,
    max_sharpe,
    min_cdar,
    min_cvar,
    min_volatility,
)
from app.services.market_service import PriceQuery, get_market_data_service


@dataclass(frozen=True)
class OptimizationRequest:
    """Input model for portfolio optimization runs."""

    prices: pd.DataFrame
    total_investment: float
    mode: str = "manual"
    constraints: Optional[Dict[str, Any]] = None


class OptimizationService:
    """Facade to execute optimization models with a stable interface."""

    def __init__(self):
        self.market_service = get_market_data_service()
        self._latest_price_provider = self.market_service.latest_prices
        self._model_map = {
            "markowitz": lambda request: markowitz_optimization(
                request.prices,
                request.total_investment,
                self._latest_price_provider,
                target_return=self._constraint_rate(
                    request.constraints, ["target_return", "targetReturn"]
                ),
            ),
            "max_sharpe": lambda request: max_sharpe(
                request.prices,
                request.total_investment,
                self._latest_price_provider,
                risk_free_rate=self._constraint_rate(
                    request.constraints, ["risk_free_rate", "riskFreeRate"]
                ),
            ),
            "min_volatility": lambda request: min_volatility(
                request.prices, request.total_investment, self._latest_price_provider
            ),
            "hrp": lambda request: hrp_model(
                request.prices, request.total_investment, self._latest_price_provider
            ),
            "min_cvar": lambda request: min_cvar(
                request.prices, request.total_investment, self._latest_price_provider
            ),
            "min_cdar": lambda request: min_cdar(
                request.prices, request.total_investment, self._latest_price_provider
            ),
        }
        self._api_model_alias = {
            "markowitz": "markowitz",
            "max_sharpe": "max_sharpe",
            "min_volatility": "min_volatility",
            "hrp": "hrp",
            "min_cvar": "min_cvar",
            "min_cdar": "min_cdar",
            "Markowitz": "markowitz",
            "Max_Sharpe": "max_sharpe",
            "Min_Volatility": "min_volatility",
            "HRP": "hrp",
            "Min_CVaR": "min_cvar",
            "Min_CDaR": "min_cdar",
            "Mô hình Markowitz": "markowitz",
            "Mô hình Max Sharpe Ratio": "max_sharpe",
            "Mô hình Min Volatility": "min_volatility",
            "Mô hình HRP": "hrp",
            "Mô hình Min CVaR": "min_cvar",
            "Mô hình Min CDaR": "min_cdar",
        }

    def available_models(self) -> Dict[str, object]:
        """Expose supported models for UI binding."""
        return dict(self._model_map)

    def run_model(
        self, model_name: str, request: OptimizationRequest
    ) -> Optional[dict]:
        """Run a single optimization model by canonical name."""
        model = self._model_map.get(model_name)
        if model is None:
            raise ValueError(f"Unsupported model: {model_name}")
        return model(request)

    def optimize_portfolio(
        self,
        symbols,
        model,
        start_date,
        end_date,
        constraints: Optional[Dict[str, Any]] = None,
        investment: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Run optimization from API input and return normalized result."""
        constraints = constraints or {}
        total_investment = float(
            investment
            if investment is not None
            else constraints.get("investment", constraints.get("total_investment", 0))
        )
        if total_investment <= 0:
            raise ValueError("investment must be greater than 0")

        resolved_end_date = end_date or date.today()
        resolved_start_date = start_date or (resolved_end_date - timedelta(days=365))

        price_data, skipped = self.market_service.fetch_prices(
            PriceQuery(
                symbols=[str(symbol).upper() for symbol in symbols],
                start_date=str(resolved_start_date),
                end_date=str(resolved_end_date),
                verbose=False,
            )
        )
        if price_data.empty:
            raise ValueError("No historical price data returned for selected symbols")

        canonical_model = self._api_model_alias.get(model, str(model).lower())
        model_result = self.run_model(
            canonical_model,
            OptimizationRequest(
                prices=price_data,
                total_investment=total_investment,
                constraints=constraints,
            ),
        )
        if not model_result:
            raise ValueError("Optimization returned empty result")

        weights = self._as_float_dict(model_result.get("Trọng số danh mục", {}))
        shares = self._as_int_dict(model_result.get("Số mã cổ phiếu cần mua", {}))
        normalized_extra_data = self._build_extra_data(model_result, shares)

        return {
            "model": canonical_model,
            "weights": weights,
            "shares": shares,
            "expected_return": self._as_float(model_result.get("Lợi nhuận kỳ vọng")),
            "expected_volatility": self._as_float(
                model_result.get("Rủi ro (Độ lệch chuẩn)")
            ),
            "sharpe_ratio": self._as_float(model_result.get("Tỷ lệ Sharpe")),
            "leftover_cash": self._as_float(model_result.get("Số tiền còn lại")),
            "skipped_symbols": skipped,
            "extra_data": normalized_extra_data,
            "raw_result": self._to_jsonable(model_result),
        }

    def backtest_portfolio(
        self,
        symbols,
        weights,
        start_date: date,
        end_date: date,
        initial_capital: float,
        rebalance_frequency: str = "monthly",
    ) -> Dict[str, Any]:
        """Run a simple weighted historical backtest."""
        if initial_capital <= 0:
            raise ValueError("initial_capital must be greater than 0")

        symbol_list = [str(symbol).upper() for symbol in symbols]
        weight_dict = {str(k).upper(): float(v) for k, v in (weights or {}).items()}
        if not weight_dict:
            raise ValueError("weights are required")

        total_weight = sum(weight_dict.values())
        if total_weight <= 0:
            raise ValueError("sum of weights must be greater than 0")
        weight_dict = {k: v / total_weight for k, v in weight_dict.items()}

        price_data, _ = self.market_service.fetch_prices(
            PriceQuery(
                symbols=symbol_list,
                start_date=str(start_date),
                end_date=str(end_date),
                verbose=False,
            )
        )
        if price_data.empty:
            raise ValueError("No historical price data returned for backtest")

        selected_columns = [
            symbol for symbol in symbol_list if symbol in price_data.columns
        ]
        if not selected_columns:
            raise ValueError(
                "None of the requested symbols are present in returned price data"
            )

        returns = price_data[selected_columns].pct_change().dropna(how="all")
        returns = returns.fillna(0.0)

        aligned_weights = np.array(
            [weight_dict.get(symbol, 0.0) for symbol in selected_columns], dtype=float
        )
        if aligned_weights.sum() <= 0:
            raise ValueError("No matching weights for available symbols")
        aligned_weights = aligned_weights / aligned_weights.sum()

        portfolio_returns = returns.dot(aligned_weights)
        equity_curve = (1 + portfolio_returns).cumprod() * float(initial_capital)

        final_value = (
            float(equity_curve.iloc[-1])
            if not equity_curve.empty
            else float(initial_capital)
        )
        total_return = (final_value / float(initial_capital)) - 1
        trading_days = max(len(portfolio_returns), 1)
        annualized_return = (
            (1 + total_return) ** (252 / trading_days) - 1 if trading_days > 0 else 0.0
        )
        volatility = (
            float(portfolio_returns.std() * np.sqrt(252))
            if not portfolio_returns.empty
            else 0.0
        )
        risk_free_rate = 0.02
        sharpe_ratio = (
            (annualized_return - risk_free_rate) / volatility
            if volatility and np.isfinite(volatility) and volatility > 0
            else 0.0
        )

        running_max = equity_curve.cummax()
        drawdown_series = (
            (equity_curve / running_max) - 1
            if not equity_curve.empty
            else pd.Series(dtype=float)
        )
        max_drawdown = (
            float(drawdown_series.min()) if not drawdown_series.empty else 0.0
        )
        win_rate = (
            float((portfolio_returns > 0).mean())
            if not portfolio_returns.empty
            else 0.0
        )

        return {
            "initial_capital": float(initial_capital),
            "final_value": final_value,
            "total_return": float(total_return),
            "annualized_return": float(annualized_return),
            "volatility": float(volatility),
            "sharpe_ratio": float(sharpe_ratio),
            "max_drawdown": float(max_drawdown),
            "win_rate": float(win_rate),
            "equity_curve": self._series_to_dict(equity_curve),
            "drawdown_curve": self._series_to_dict(drawdown_series),
            "trades": [],
            "backtest_data": {
                "rebalance_frequency": rebalance_frequency,
                "weights": {
                    symbol: float(weight)
                    for symbol, weight in zip(selected_columns, aligned_weights)
                },
                "portfolio_returns": self._series_to_dict(portfolio_returns),
            },
        }

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _as_float_dict(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        output: Dict[str, float] = {}
        for key, item in value.items():
            try:
                output[str(key)] = float(item)
            except Exception:
                continue
        return output

    @staticmethod
    def _as_int_dict(value: Any) -> Dict[str, int]:
        if not isinstance(value, dict):
            return {}
        output: Dict[str, int] = {}
        for key, item in value.items():
            try:
                output[str(key)] = int(item)
            except Exception:
                continue
        return output

    @classmethod
    def _constraint_rate(
        cls, constraints: Optional[Dict[str, Any]], keys: list[str]
    ) -> Optional[float]:
        if not isinstance(constraints, dict):
            return None

        for key in keys:
            value = constraints.get(key)
            if value is None:
                continue

            numeric_value = cls._as_float(value)
            if numeric_value is None or not np.isfinite(numeric_value):
                continue

            return numeric_value / 100 if abs(numeric_value) > 1 else numeric_value

        return None

    @classmethod
    def _build_extra_data(
        cls, model_result: Dict[str, Any], shares: Dict[str, int]
    ) -> Dict[str, Any]:
        latest_prices = cls._as_float_dict(model_result.get("Giá mã cổ phiếu", {}))
        allocation_amounts = {
            symbol: float(quantity * latest_prices[symbol])
            for symbol, quantity in shares.items()
            if symbol in latest_prices
        }

        extra_data: Dict[str, Any] = {
            "latest_prices": latest_prices,
            "allocation_amounts": allocation_amounts,
        }

        cvar = cls._as_float(model_result.get("Rủi ro CVaR"))
        if cvar is not None:
            extra_data["cvar"] = cvar

        cdar = cls._as_float(model_result.get("Rủi ro CDaR"))
        if cdar is not None:
            extra_data["cdar"] = cdar

        target_return = cls._as_float(model_result.get("target_return"))
        if target_return is not None:
            extra_data["target_return"] = target_return

        risk_free_rate = cls._as_float(model_result.get("risk_free_rate"))
        if risk_free_rate is not None:
            extra_data["risk_free_rate"] = risk_free_rate

        return extra_data

    @classmethod
    def _series_to_dict(cls, series: pd.Series) -> Dict[str, float]:
        return {
            str(index.date() if hasattr(index, "date") else index): float(value)
            for index, value in series.items()
        }

    @classmethod
    def _to_jsonable(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): cls._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._to_jsonable(v) for v in value]
        if isinstance(value, tuple):
            return [cls._to_jsonable(v) for v in value]
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, pd.Series):
            return {str(k): cls._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, pd.DataFrame):
            return value.to_dict(orient="records")
        if isinstance(value, (np.generic,)):
            return value.item()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (pd.Timestamp,)):
            return value.isoformat()
        return value


_optimization_service: Optional[OptimizationService] = None


def get_optimization_service() -> OptimizationService:
    """Return singleton optimization service."""
    global _optimization_service
    if _optimization_service is None:
        _optimization_service = OptimizationService()
    return _optimization_service
