"""Centralized market data service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
import time
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from app.config import settings
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


logger = logging.getLogger(__name__)


@dataclass
class PriceQuery:
    """Input model for historical price requests."""

    symbols: List[str]
    start_date: str
    end_date: str
    verbose: bool = True


class MarketService:
    """Facade over market data providers and processors."""

    def __init__(self) -> None:
        self._fundamental_cache_ttl = max(0, int(settings.vnstock_cache_expire_seconds))
        self._fundamental_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._fundamental_cache_lock = Lock()

    def _get_cached_fundamentals(self, symbol: str) -> Tuple[bool, Dict[str, Any]]:
        if self._fundamental_cache_ttl <= 0:
            return False, {}

        now = time.monotonic()
        with self._fundamental_cache_lock:
            cached_entry = self._fundamental_cache.get(symbol)
            if cached_entry is None:
                return False, {}

            expires_at, payload = cached_entry
            if expires_at <= now:
                self._fundamental_cache.pop(symbol, None)
                return False, {}

            return True, dict(payload)

    def _set_cached_fundamentals(self, symbol: str, payload: Dict[str, Any]) -> None:
        if self._fundamental_cache_ttl <= 0:
            return

        expires_at = time.monotonic() + self._fundamental_cache_ttl
        with self._fundamental_cache_lock:
            self._fundamental_cache[symbol] = (expires_at, dict(payload))

    @staticmethod
    def _is_missing_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            normalized = value.strip().lower()
            return len(normalized) == 0 or normalized in {
                "nan",
                "none",
                "null",
                "n/a",
                "na",
            }
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return True
        return False

    @classmethod
    def _to_optional_text(cls, value: Any) -> Optional[str]:
        if cls._is_missing_value(value):
            return None
        text = str(value).strip()
        if cls._is_missing_value(text):
            return None
        return text

    @staticmethod
    def _to_optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(parsed):
            return None
        return parsed

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed

    @staticmethod
    def _to_optional_share_count(value: Any) -> Optional[float]:
        parsed = MarketService._to_optional_float(value)
        if parsed is None or parsed <= 0:
            return None

        # Some providers return listed volume in millions of shares.
        if parsed < 10_000_000:
            return parsed * 1_000_000

        return parsed

    @classmethod
    def _pick_value(cls, source: Dict[str, Any], keys: Sequence[str]) -> Any:
        for key in keys:
            if key in source and not cls._is_missing_value(source[key]):
                return source[key]
        return None

    @classmethod
    def _pick_value_flexible(cls, source: Dict[str, Any], keys: Sequence[str]) -> Any:
        if not source:
            return None

        normalized_source: Dict[str, Any] = {}
        for key, value in source.items():
            if cls._is_missing_value(value):
                continue
            normalized_source[str(key).strip().lower()] = value

        for key in keys:
            if key in source and not cls._is_missing_value(source[key]):
                return source[key]
            matched = normalized_source.get(str(key).strip().lower())
            if not cls._is_missing_value(matched):
                return matched

        return None

    def _fetch_company_profile(self, symbol: str) -> Dict[str, Any]:
        try:
            from vnstock import Vnstock

            for source in ("KBS", "VCI"):
                try:
                    stock = Vnstock().stock(symbol=symbol, source=source)
                except Exception:
                    continue

                company = getattr(stock, "company", None)
                if company is None:
                    continue

                for method_name in ("profile", "overview"):
                    method = getattr(company, method_name, None)
                    if not callable(method):
                        continue

                    try:
                        payload = method()
                    except SystemExit as exc:
                        logger.warning(
                            "vnstock rate limit while fetching profile for %s: %s",
                            symbol,
                            exc,
                        )
                        continue
                    except Exception:
                        continue

                    if payload is None:
                        continue

                    if isinstance(payload, pd.DataFrame):
                        if payload.empty:
                            continue
                        row = payload.iloc[0].to_dict()
                    elif isinstance(payload, dict):
                        row = payload
                    else:
                        continue

                    normalized = {str(key): value for key, value in row.items()}
                    if normalized:
                        return normalized

            return {}
        except SystemExit as exc:
            logger.warning(
                "vnstock rate limit while loading company profile for %s: %s",
                symbol,
                exc,
            )
            return {}
        except Exception:
            return {}

    @staticmethod
    def _safe_divide(
        numerator: Optional[float], denominator: Optional[float]
    ) -> Optional[float]:
        if numerator is None or denominator is None or denominator <= 0:
            return None
        return numerator / denominator

    @staticmethod
    def _normalize_period(period: str) -> str:
        normalized = (period or "").strip().lower()
        if normalized in {"quarter", "quarterly", "q"}:
            return "quarterly"
        if normalized in {"year", "yearly", "y", "annual"}:
            return "yearly"
        raise ValueError("Invalid period")

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
        symbol_upper = symbol.upper()
        cache_hit, cached_payload = self._get_cached_fundamentals(symbol_upper)
        if cache_hit:
            return cached_payload

        try:
            payload = fetch_fundamental_data(symbol_upper) or {}
        except SystemExit as exc:
            logger.warning(
                "vnstock rate limit while fetching fundamentals for %s: %s",
                symbol_upper,
                exc,
            )
            payload = {}

        self._set_cached_fundamentals(symbol_upper, payload)
        return payload

    def _fetch_financial_ratio_frame(self, symbol: str, period: str) -> pd.DataFrame:
        from vnstock import Vnstock

        period_value = "quarter" if period == "quarterly" else "year"

        for source in ("KBS", "VCI"):
            try:
                stock = Vnstock().stock(symbol=symbol, source=source)
                try:
                    frame = stock.finance.ratio(period=period_value, lang="vi")
                except TypeError:
                    frame = stock.finance.ratio(period=period_value)

                if frame is None or frame.empty:
                    continue
                return frame.copy()
            except SystemExit as exc:
                logger.warning(
                    "vnstock rate limit while fetching ratio frame for %s (%s): %s",
                    symbol,
                    period,
                    exc,
                )
                continue
            except Exception:
                continue

        return pd.DataFrame()

    def get_stock_overview(self, symbol: str) -> Dict[str, Any]:
        symbol_upper = symbol.upper()
        company_name: Optional[str] = None
        exchange: Optional[str] = None
        sector: Optional[str] = None

        company_df = self.load_company_info()
        if not company_df.empty:
            matched = company_df[
                company_df["symbol"].astype(str).str.upper() == symbol_upper
            ]
            if not matched.empty:
                row = matched.iloc[0]
                company_name = self._to_optional_text(row.get("organ_name"))
                exchange = self._to_optional_text(row.get("exchange"))
                sector = self._to_optional_text(row.get("icb_name"))

        fundamentals = self.fundamental_data(symbol_upper) or {}

        market_cap = self._to_optional_float(
            self._pick_value(fundamentals, ["marketCap", "market_cap"])
        )
        shares_outstanding = self._to_optional_float(
            self._pick_value(fundamentals, ["sharesOutstanding", "shares_outstanding"])
        )
        free_float = self._to_optional_float(
            self._pick_value(fundamentals, ["freeFloat", "free_float"])
        )

        highlights: List[str] = []
        pe = self._to_optional_float(
            self._pick_value(fundamentals, ["pe", "priceToEarning"])
        )
        pb = self._to_optional_float(
            self._pick_value(fundamentals, ["pb", "priceToBook"])
        )
        roe = self._to_optional_float(self._pick_value(fundamentals, ["roe"]))
        if pe is not None:
            highlights.append(f"P/E hien tai: {pe:.2f}")
        if pb is not None:
            highlights.append(f"P/B hien tai: {pb:.2f}")
        if roe is not None:
            highlights.append(f"ROE gan nhat: {roe:.2%}")

        business_summary = self._to_optional_text(
            self._pick_value(
                fundamentals, ["businessSummary", "business_summary", "description"]
            )
        )

        profile_data = self._fetch_company_profile(symbol_upper)
        if self._is_missing_value(company_name):
            company_name = self._to_optional_text(
                self._pick_value_flexible(
                    profile_data,
                    ["company_name", "organ_name", "companyName", "name"],
                )
            )
        if self._is_missing_value(exchange):
            exchange = self._to_optional_text(
                self._pick_value_flexible(
                    profile_data,
                    ["exchange", "exchange_name", "listed_exchange"],
                )
            )
        if self._is_missing_value(sector):
            sector = self._to_optional_text(
                self._pick_value_flexible(
                    profile_data,
                    ["icb_name", "icb_name3", "industry", "industry_name", "sector"],
                )
            )
        if self._is_missing_value(business_summary):
            business_summary = self._to_optional_text(
                self._pick_value_flexible(
                    profile_data,
                    [
                        "business_model",
                        "company_profile",
                        "companyProfile",
                        "business_summary",
                        "businessSummary",
                        "company_description",
                        "description",
                        "summary",
                        "overview",
                    ],
                )
            )

        if shares_outstanding is None:
            shares_outstanding = self._to_optional_share_count(
                self._pick_value_flexible(
                    profile_data,
                    [
                        "shares_outstanding",
                        "outstanding_share",
                        "listed_volume",
                        "listed_shares",
                    ],
                )
            )

        return {
            "symbol": symbol_upper,
            "company_name": company_name,
            "exchange": exchange,
            "sector": sector,
            "industry": sector,
            "market_cap": market_cap,
            "shares_outstanding": shares_outstanding,
            "free_float": free_float,
            "listing_date": self._pick_value(
                fundamentals, ["listingDate", "listing_date"]
            ),
            "headquarters": self._pick_value(fundamentals, ["headquarters"]),
            "employee_count": self._to_optional_int(
                self._pick_value(fundamentals, ["employeeCount", "employee_count"])
            ),
            "business_summary": business_summary,
            "latest_highlights": highlights,
            "as_of_date": self._pick_value(fundamentals, ["asOfDate", "as_of_date"]),
            "source": self._pick_value(fundamentals, ["source"]) or "vnstock",
        }

    def get_stock_financials(
        self,
        symbol: str,
        period: str = "quarterly",
        limit: int = 8,
    ) -> Dict[str, Any]:
        symbol_upper = symbol.upper()
        normalized_period = self._normalize_period(period)
        safe_limit = max(1, limit)

        frame = self._fetch_financial_ratio_frame(symbol_upper, normalized_period)
        if frame.empty:
            return {
                "symbol": symbol_upper,
                "period": normalized_period,
                "items": [],
                "as_of_date": None,
                "source": "vnstock",
            }

        working = frame.copy()
        sort_columns: List[str] = []
        if "year" in working.columns:
            sort_columns.append("year")
        if normalized_period == "quarterly" and "quarter" in working.columns:
            sort_columns.append("quarter")
        if sort_columns:
            working = working.sort_values(sort_columns, ascending=False)

        items: List[Dict[str, Any]] = []
        for _, row in working.head(safe_limit).iterrows():
            row_data = row.to_dict()
            year = self._to_optional_int(row_data.get("year"))
            quarter = self._to_optional_int(row_data.get("quarter"))

            revenue = self._to_optional_float(
                self._pick_value(row_data, ["revenue", "saleRevenue", "sales"])
            )
            gross_profit = self._to_optional_float(
                self._pick_value(row_data, ["grossProfit", "gross_profit"])
            )
            operating_profit = self._to_optional_float(
                self._pick_value(
                    row_data, ["operatingProfit", "operating_profit", "operationProfit"]
                )
            )
            net_income = self._to_optional_float(
                self._pick_value(
                    row_data, ["netIncome", "net_income", "postTaxProfit", "profit"]
                )
            )

            total_assets = self._to_optional_float(
                self._pick_value(row_data, ["totalAssets", "total_assets"])
            )
            total_liabilities = self._to_optional_float(
                self._pick_value(row_data, ["totalLiabilities", "total_liabilities"])
            )
            equity = self._to_optional_float(
                self._pick_value(row_data, ["equity", "ownerEquity", "owner_equity"])
            )
            total_debt = self._to_optional_float(
                self._pick_value(row_data, ["totalDebt", "total_debt"])
            )
            cash_and_cash_equivalents = self._to_optional_float(
                self._pick_value(
                    row_data,
                    ["cashAndCashEquivalents", "cash_and_cash_equivalents", "cash"],
                )
            )

            operating_cash_flow = self._to_optional_float(
                self._pick_value(row_data, ["operatingCashFlow", "operating_cash_flow"])
            )
            investing_cash_flow = self._to_optional_float(
                self._pick_value(row_data, ["investingCashFlow", "investing_cash_flow"])
            )
            financing_cash_flow = self._to_optional_float(
                self._pick_value(row_data, ["financingCashFlow", "financing_cash_flow"])
            )
            free_cash_flow = (
                operating_cash_flow + investing_cash_flow
                if operating_cash_flow is not None and investing_cash_flow is not None
                else None
            )

            if (
                normalized_period == "quarterly"
                and year is not None
                and quarter is not None
            ):
                period_label = f"Q{quarter}/{year}"
            elif year is not None:
                period_label = str(year)
            else:
                period_label = str(
                    self._pick_value(row_data, ["period", "date"]) or "N/A"
                )

            items.append(
                {
                    "period_label": period_label,
                    "year": year,
                    "quarter": quarter,
                    "revenue": revenue,
                    "gross_profit": gross_profit,
                    "operating_profit": operating_profit,
                    "net_income": net_income,
                    "total_assets": total_assets,
                    "total_liabilities": total_liabilities,
                    "equity": equity,
                    "total_debt": total_debt,
                    "cash_and_cash_equivalents": cash_and_cash_equivalents,
                    "operating_cash_flow": operating_cash_flow,
                    "investing_cash_flow": investing_cash_flow,
                    "financing_cash_flow": financing_cash_flow,
                    "free_cash_flow": free_cash_flow,
                }
            )

        as_of_date: Optional[str] = None
        if items:
            latest_period_label = str(items[0].get("period_label") or "").strip()
            if latest_period_label and latest_period_label != "N/A":
                as_of_date = latest_period_label
            elif items[0].get("year"):
                as_of_date = str(items[0]["year"])

        return {
            "symbol": symbol_upper,
            "period": normalized_period,
            "items": items,
            "as_of_date": as_of_date,
            "source": "vnstock",
        }

    def get_stock_ratios(self, symbol: str) -> Dict[str, Any]:
        symbol_upper = symbol.upper()
        fundamentals = self.fundamental_data(symbol_upper) or {}
        financials = self.get_stock_financials(
            symbol_upper, period="quarterly", limit=1
        )
        latest = financials["items"][0] if financials.get("items") else {}
        reporting_period = financials.get("as_of_date") or self._pick_value(
            fundamentals, ["asOfDate", "as_of_date"]
        )

        pe = self._to_optional_float(
            self._pick_value(fundamentals, ["pe", "priceToEarning"])
        )
        pb = self._to_optional_float(
            self._pick_value(fundamentals, ["pb", "priceToBook"])
        )
        ev_ebitda = self._to_optional_float(
            self._pick_value(fundamentals, ["ev_ebitda", "evEbitda", "evToEbitda"])
        )

        revenue = self._to_optional_float(self._pick_value(latest, ["revenue"]))
        gross_profit = self._to_optional_float(
            self._pick_value(latest, ["gross_profit", "grossProfit"])
        )
        net_income = self._to_optional_float(
            self._pick_value(latest, ["net_income", "netIncome"])
        )
        equity = self._to_optional_float(self._pick_value(latest, ["equity"]))
        total_assets = self._to_optional_float(
            self._pick_value(latest, ["total_assets", "totalAssets"])
        )
        total_debt = self._to_optional_float(
            self._pick_value(latest, ["total_debt", "totalDebt"])
        )

        gross_margin = self._safe_divide(gross_profit, revenue)
        net_margin = self._safe_divide(net_income, revenue)
        debt_to_equity = self._safe_divide(total_debt, equity)

        if gross_margin is None:
            gross_margin = self._to_optional_float(
                self._pick_value(
                    fundamentals,
                    [
                        "gross_margin",
                        "grossMargin",
                    ],
                )
            )

        if net_margin is None:
            net_margin = self._to_optional_float(
                self._pick_value(
                    fundamentals,
                    [
                        "net_margin",
                        "netMargin",
                        "profit_margin",
                        "profitMargin",
                        "netProfitMargin",
                    ],
                )
            )

        if debt_to_equity is None:
            debt_to_equity = self._to_optional_float(
                self._pick_value(
                    fundamentals,
                    [
                        "debt_to_equity",
                        "debtToEquity",
                    ],
                )
            )

        roe = self._to_optional_float(self._pick_value(fundamentals, ["roe"]))
        roa = self._to_optional_float(self._pick_value(fundamentals, ["roa"]))
        if roe is None:
            roe = self._safe_divide(net_income, equity)
        if roa is None:
            roa = self._safe_divide(net_income, total_assets)

        quality_flags: List[str] = []
        if gross_margin is None:
            quality_flags.append("gross_margin_unavailable")
        if net_margin is None:
            quality_flags.append("net_margin_unavailable")
        if debt_to_equity is None:
            quality_flags.append("debt_to_equity_unavailable")
        if ev_ebitda is None:
            quality_flags.append("ev_ebitda_unavailable")
        if roe is None:
            quality_flags.append("roe_unavailable")
        if roa is None:
            quality_flags.append("roa_unavailable")

        return {
            "symbol": symbol_upper,
            "pe": pe,
            "pb": pb,
            "ev_ebitda": ev_ebitda,
            "gross_margin": gross_margin,
            "net_margin": net_margin,
            "roe": roe,
            "roa": roa,
            "debt_to_equity": debt_to_equity,
            "as_of_date": datetime.utcnow().date().isoformat(),
            "reporting_period": reporting_period,
            "quality_flags": quality_flags,
            "source": self._pick_value(fundamentals, ["source"]) or "vnstock",
        }

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
