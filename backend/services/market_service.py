"""Centralized market data service.

This module provides a stable interface so UI/chatbot layers do not import
low-level data fetching helpers directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from data_process.data_loader import (
    fetch_data_from_csv,
    calculate_metrics,
    fetch_ohlc_data,
    fetch_stock_data2,
    get_index_history,
    get_latest_prices,
)
from data_process.fundamentals import fetch_fundamental_data


@dataclass
class PriceQuery:
    """Input model for historical price requests."""

    symbols: List[str]
    start_date: str
    end_date: str
    verbose: bool = True


class MarketDataService:
    """Facade over market data providers and processors."""

    def fetch_prices(self, query: PriceQuery) -> Tuple[pd.DataFrame, List[str]]:
        """Fetch historical close prices for symbols."""
        return fetch_stock_data2(
            query.symbols,
            str(query.start_date),
            str(query.end_date),
            verbose=query.verbose,
        )

    def load_company_info(self, file_path: str) -> pd.DataFrame:
        """Load static company metadata from CSV."""
        return fetch_data_from_csv(file_path)

    def fetch_ohlc(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch OHLC data for a single symbol."""
        return fetch_ohlc_data(ticker, start_date, end_date)

    def latest_prices(self, tickers: Iterable[str]) -> Dict[str, float]:
        """Fetch latest prices for a list of symbols."""
        return get_latest_prices(list(tickers))

    def index_history(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        months: int = 6,
        source: str = "MSN",
    ) -> pd.DataFrame:
        """Fetch historical market index data."""
        return get_index_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            months=months,
            source=source,
        )

    def return_volatility_metrics(self, prices: pd.DataFrame):
        """Compute return and volatility metrics from price history."""
        return calculate_metrics(prices)

    def fundamental_data(self, symbol: str):
        """Fetch fundamental metrics for a ticker."""
        return fetch_fundamental_data(symbol)


_market_data_service: Optional[MarketDataService] = None


def get_market_data_service() -> MarketDataService:
    """Return singleton market data service."""
    global _market_data_service
    if _market_data_service is None:
        _market_data_service = MarketDataService()
    return _market_data_service
