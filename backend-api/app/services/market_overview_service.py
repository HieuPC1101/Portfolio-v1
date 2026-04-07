"""Service layer for market overview tab data assembly."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

import pandas as pd

from app.data_process.data_loader import (
    get_foreign_flow_leaderboard,
    get_index_history,
    get_indices_history,
    get_liquidity_leaders,
    get_market_indices_metrics,
    get_realtime_index_board,
    get_sector_snapshot,
    summarize_market_cap_distribution,
    summarize_sector_performance,
)


class MarketOverviewService:
    """Prepare market-overview datasets consumed by Streamlit UI."""

    def load_company_industries(self) -> pd.DataFrame:
        """Load level-1 industry classification from database."""
        from app.database import SessionLocal
        from app.models.company_info import CompanyInfo

        db = SessionLocal()
        try:
            rows = db.query(CompanyInfo.symbol, CompanyInfo.icb_name).all()
            if not rows:
                return pd.DataFrame(columns=["symbol", "industry_level_1"])
            data = [
                {
                    "symbol": r.symbol.upper(),
                    "industry_level_1": r.icb_name or "Ngành khác",
                }
                for r in rows
            ]
            return pd.DataFrame(data)
        finally:
            db.close()

    def load_overview_data(
        self, analysis_start_date: str, analysis_end_date: str
    ) -> Dict[str, object]:
        """Fetch lightweight data powering KPI cards and index chart."""
        analysis_start = pd.to_datetime(analysis_start_date).strftime("%Y-%m-%d")
        analysis_end = pd.to_datetime(analysis_end_date).strftime("%Y-%m-%d")
        months_span = max(
            1,
            int(
                (pd.to_datetime(analysis_end) - pd.to_datetime(analysis_start)).days
                / 30
            ),
        )

        return {
            "indices_metrics": get_market_indices_metrics(),
            "index_history": get_indices_history(
                start_date=analysis_start, end_date=analysis_end, months=months_span
            ),
        }

    def build_realtime_metrics(
        self, symbols: Iterable[str], labels: Dict[str, str]
    ) -> List[dict]:
        """Build realtime/fallback metric cards for configured index symbols."""
        realtime_symbols = list(symbols)
        symbol_keys = sorted(
            {symbol.upper() for symbol in realtime_symbols}
            | {symbol.upper() for symbol in labels.keys()}
        )

        board = get_realtime_index_board(realtime_symbols)
        metrics: List[dict] = []
        history_cache: Dict[str, pd.DataFrame] = {}

        def _safe_float(value):
            try:
                if pd.isna(value):
                    return None
                return float(value)
            except Exception:
                return None

        def _get_sorted_history(symbol_key):
            if symbol_key not in history_cache:
                history = get_index_history(symbol_key, months=1)
                history_cache[symbol_key] = (
                    history.sort_values("time") if not history.empty else pd.DataFrame()
                )
            return history_cache[symbol_key]

        if board is not None and not board.empty:
            for _, row in board.iterrows():
                symbol_key = str(row["symbol"]).upper()
                price = _safe_float(row.get("gia_khop"))
                reference = _safe_float(row.get("gia_tham_chieu"))
                change = _safe_float(row.get("thay_doi"))
                pct_change = _safe_float(row.get("ty_le_thay_doi"))
                note_ts = row.get("last_updated")
                note_text = None

                if reference in (None, 0) and price is not None and change is not None:
                    reference = price - change

                if price in (None, 0) and reference not in (None, 0):
                    price = reference
                    change = 0.0
                    pct_change = 0.0
                    note_text = "Chưa có khớp · Hiển thị tham chiếu"
                elif price in (None, 0):
                    history = _get_sorted_history(symbol_key)
                    if history.empty:
                        continue
                    last_row = history.iloc[-1]
                    prev_row = history.iloc[-2] if len(history) > 1 else last_row
                    price = float(last_row["close"])
                    reference = (
                        float(prev_row["close"])
                        if pd.notna(prev_row["close"])
                        else price
                    )
                    change = price - reference
                    pct_change = (
                        (change / reference * 100)
                        if reference not in (0, None)
                        else 0.0
                    )
                    note_text = "Dữ liệu cuối phiên"
                    note_ts = pd.to_datetime(last_row["time"]).to_pydatetime()
                else:
                    base_reference = reference if reference not in (None, 0) else None
                    if base_reference is None and change is not None:
                        base_reference = price - change
                    if change is None and base_reference is not None:
                        change = price - base_reference
                    if pct_change is None and base_reference not in (None, 0):
                        pct_change = (
                            (change / base_reference * 100)
                            if change is not None
                            else 0.0
                        )

                if change is None:
                    change = 0.0
                if pct_change is None:
                    pct_change = 0.0
                if note_text is None:
                    note_text = f"Thay đổi {change:+.2f} điểm"

                metrics.append(
                    {
                        "symbol": symbol_key,
                        "label": labels.get(symbol_key, symbol_key),
                        "value": price,
                        "change": change,
                        "pct_change": pct_change,
                        "note": note_text,
                        "timestamp": note_ts,
                    }
                )

        available_symbols = {metric["symbol"] for metric in metrics}
        for symbol_key in symbol_keys:
            if symbol_key in available_symbols:
                continue
            history = _get_sorted_history(symbol_key)
            if history.empty:
                continue

            last_row = history.iloc[-1]
            prev_row = history.iloc[-2] if len(history) > 1 else last_row
            last_close = (
                float(last_row["close"]) if pd.notna(last_row["close"]) else None
            )
            prev_close = (
                float(prev_row["close"]) if pd.notna(prev_row["close"]) else None
            )
            if last_close is None:
                continue

            change = last_close - (prev_close if prev_close is not None else last_close)
            pct_change = (
                (change / prev_close * 100) if prev_close not in (0, None) else 0.0
            )
            timestamp = pd.to_datetime(last_row["time"]).to_pydatetime()
            metrics.append(
                {
                    "symbol": symbol_key,
                    "label": labels.get(symbol_key, symbol_key),
                    "value": last_close,
                    "change": change,
                    "pct_change": pct_change,
                    "note": "Dữ liệu cuối phiên",
                    "timestamp": timestamp,
                }
            )

        return metrics

    def load_sector_snapshot(
        self, companies_df: pd.DataFrame, size: int = 250
    ) -> pd.DataFrame:
        """Load and normalize sector snapshot."""
        snapshot = get_sector_snapshot(exchange="HOSE", size=size)
        if snapshot.empty:
            return snapshot

        snapshot_columns = [
            "ticker",
            "industry",
            "market_cap",
            "price_growth_1w",
            "price_growth_1m",
            "avg_trading_value_20d",
            "foreign_buysell_20s",
        ]
        cols_to_keep = [
            column for column in snapshot_columns if column in snapshot.columns
        ]
        if cols_to_keep:
            snapshot = snapshot[cols_to_keep]

        if not companies_df.empty and "ticker" in snapshot.columns:
            working = snapshot.copy()
            working["ticker"] = working["ticker"].astype(str).str.upper()
            merged = working.merge(
                companies_df, left_on="ticker", right_on="symbol", how="left"
            )
            merged["industry_level_1"] = merged["industry_level_1"].fillna(
                merged.get("industry", "Ngành khác")
            )
            merged["industry"] = merged["industry_level_1"]
            merged = merged.drop(columns=["symbol"], errors="ignore")
            return merged

        return snapshot

    def load_detail_data(self, sector_snapshot: pd.DataFrame) -> Dict[str, object]:
        """Load heavy datasets for secondary charts."""
        return {
            "sector_snapshot": sector_snapshot,
            "sector_perf": summarize_sector_performance(sector_snapshot, top_n=None),
            "market_cap": summarize_market_cap_distribution(sector_snapshot, top_n=8),
            "foreign_flow": get_foreign_flow_leaderboard(sector_snapshot, top_n=6),
            "liquidity": get_liquidity_leaders(sector_snapshot, top_n=40),
        }

    def get_market_overview(self) -> Dict[str, object]:
        """Return a lightweight market overview payload for the API."""
        metrics = get_market_indices_metrics()
        return {
            "indices": {
                "vnindex": self._metric_to_index(metrics, "VNINDEX", "VN-Index"),
                "vn30": self._metric_to_index(metrics, "VN30", "VN30"),
                "hnx": self._metric_to_index(metrics, "HNXINDEX", "HNX-Index"),
                "upcom": self._metric_to_index(metrics, "UPCOMINDEX", "UPCoM"),
                "timestamp": pd.Timestamp.utcnow().to_pydatetime(),
            },
            "top_gainers": [],
            "top_losers": [],
            "most_active": [],
            "statistics": {
                "total_stocks": 0,
                "advancing": 0,
                "declining": 0,
                "unchanged": 0,
                "total_volume": 0,
                "total_value": 0,
            },
            "timestamp": pd.Timestamp.utcnow().to_pydatetime(),
        }

    @staticmethod
    def _metric_to_index(
        metrics: List[dict], symbol: str, label: str
    ) -> Dict[str, object]:
        for metric in metrics:
            if str(metric.get("symbol", "")).upper() == symbol:
                return {
                    "name": metric.get("label", label),
                    "value": float(metric.get("value") or 0),
                    "change": float(metric.get("change") or 0),
                    "change_percent": float(metric.get("pct_change") or 0),
                    "volume": None,
                }
        return {
            "name": label,
            "value": 0.0,
            "change": 0.0,
            "change_percent": 0.0,
            "volume": None,
        }


_market_overview_service: Optional[MarketOverviewService] = None


def get_market_overview_service() -> MarketOverviewService:
    """Return singleton market overview service."""
    global _market_overview_service
    if _market_overview_service is None:
        _market_overview_service = MarketOverviewService()
    return _market_overview_service
