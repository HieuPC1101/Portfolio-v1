"""Centralized market data service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.data_process.data_loader import (
    calculate_metrics,
    fetch_ohlc_data,
    fetch_stock_data2,
    get_index_history,
    get_latest_prices,
    get_market_indices_metrics,
    get_sector_snapshot,
    summarize_sector_performance,
)
from app.data_process.fundamentals import fetch_fundamental_data


@dataclass
class PriceQuery:
    """Input model for historical price requests."""

    symbols: List[str]
    start_date: str
    end_date: str
    verbose: bool = True


class MarketService:
    """Facade over market data providers and processors."""

    def fetch_prices(self, query: PriceQuery) -> Tuple[pd.DataFrame, List[str]]:
        """Fetch historical close prices for symbols."""
        return fetch_stock_data2(
            query.symbols,
            str(query.start_date),
            str(query.end_date),
            verbose=query.verbose,
        )

    def get_stock_price(
        self, symbol: str, start_date: str, end_date: str
    ) -> Dict[str, Dict[str, float]]:
        """Fetch historical close prices for one symbol."""
        prices, _ = self.fetch_prices(
            PriceQuery(
                symbols=[symbol.upper()],
                start_date=start_date,
                end_date=end_date,
                verbose=False,
            )
        )
        if prices.empty or symbol.upper() not in prices.columns:
            return {}
        series = prices[symbol.upper()].dropna()
        return {
            str(idx.date() if hasattr(idx, "date") else idx): float(value)
            for idx, value in series.items()
        }

    def load_company_info(self) -> pd.DataFrame:
        """Load static company metadata from database."""
        from app.database import SessionLocal
        from app.models.company_info import CompanyInfo

        db = SessionLocal()
        try:
            rows = db.query(CompanyInfo).all()
            return pd.DataFrame(
                [
                    {
                        "symbol": r.symbol,
                        "organ_name": r.organ_name,
                        "icb_name": r.icb_name,
                        "exchange": r.exchange,
                    }
                    for r in rows
                ]
            )
        finally:
            db.close()

    def search_stocks(self, query: str, limit: int = 10) -> List[dict]:
        """Search stock symbols and company names from local metadata."""
        query_norm = (query or "").strip().upper()
        if not query_norm:
            return []

        company_df = self.load_company_info()
        if company_df.empty:
            return []

        filtered = company_df[
            company_df["symbol"]
            .astype(str)
            .str.upper()
            .str.contains(query_norm, na=False)
            | company_df["organ_name"]
            .astype(str)
            .str.upper()
            .str.contains(query_norm, na=False)
        ]
        filtered = filtered.head(limit)

        return [
            {
                "symbol": str(row["symbol"]).upper(),
                "name": row.get("organ_name"),
                "exchange": row.get("exchange"),
                "sector": row.get("icb_name"),
            }
            for _, row in filtered.iterrows()
        ]

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
        source: str = "VCI",
    ) -> pd.DataFrame:
        """Fetch historical market index data."""
        return get_index_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            months=months,
            source=source,
        )

    def get_market_indices(self) -> Dict[str, dict]:
        """Return headline market indices formatted for API response."""
        metrics = get_market_indices_metrics(
            symbols=("VNINDEX", "VN30", "HNXINDEX", "UPCOMINDEX")
        )
        mapped = {
            "VNINDEX": "vnindex",
            "VN30": "vn30",
            "HNXINDEX": "hnx",
            "UPCOMINDEX": "upcom",
        }
        response: Dict[str, dict] = {
            "vnindex": {
                "name": "VN-Index",
                "value": 0.0,
                "change": 0.0,
                "change_percent": 0.0,
                "volume": None,
            },
            "vn30": {
                "name": "VN30",
                "value": 0.0,
                "change": 0.0,
                "change_percent": 0.0,
                "volume": None,
            },
            "hnx": {
                "name": "HNX-Index",
                "value": 0.0,
                "change": 0.0,
                "change_percent": 0.0,
                "volume": None,
            },
            "upcom": {
                "name": "UPCoM",
                "value": 0.0,
                "change": 0.0,
                "change_percent": 0.0,
                "volume": None,
            },
            "timestamp": datetime.utcnow(),
        }
        for item in metrics:
            key = mapped.get(str(item.get("symbol", "")).upper())
            if not key:
                continue
            response[key] = {
                "name": item.get("label", key.upper()),
                "value": float(item.get("value") or 0),
                "change": float(item.get("change") or 0),
                "change_percent": float(item.get("pct_change") or 0),
                "volume": None,
            }
        return response

    def get_sector_performance(self) -> List[dict]:
        """Return aggregated sector performance."""
        snapshot = get_sector_snapshot(exchange="HOSE,HNX,UPCOM", size=400)
        if snapshot.empty:
            return []

        sector_df = summarize_sector_performance(snapshot, top_n=20)
        if sector_df.empty:
            return []

        output: List[dict] = []
        for _, row in sector_df.iterrows():
            output.append(
                {
                    "sector_name": row.get("industry"),
                    "change_percent": float(row.get("avg_growth_1w") or 0),
                    "volume": float(row.get("avg_liquidity") or 0),
                    "top_stocks": None,
                }
            )
        return output

    def return_volatility_metrics(self, prices: pd.DataFrame):
        """Compute return and volatility metrics from price history."""
        return calculate_metrics(prices)

    def fundamental_data(self, symbol: str):
        """Fetch fundamental metrics for a ticker."""
        return fetch_fundamental_data(symbol)

    def get_stock_fundamentals(self, symbol: str):
        """Compatibility wrapper for API layer."""
        return self.fundamental_data(symbol) or {}

    def get_stock_info(self, symbol: str) -> Dict[str, object]:
        """Build a stock info payload from fundamentals and metadata."""
        symbol_upper = symbol.upper()
        fundamentals = self.fundamental_data(symbol_upper) or {}

        company_name = None
        company_df = self.load_company_info()
        if not company_df.empty:
            matched = company_df[
                company_df["symbol"].astype(str).str.upper() == symbol_upper
            ]
            if not matched.empty:
                company_name = matched.iloc[0].get("organ_name")

        return {
            "symbol": symbol_upper,
            "company_name": company_name,
            "fundamentals": fundamentals,
        }


MarketDataService = MarketService

_market_data_service: Optional[MarketService] = None


def get_market_data_service() -> MarketService:
    """Return singleton market data service."""
    global _market_data_service
    if _market_data_service is None:
        _market_data_service = MarketService()
    return _market_data_service
