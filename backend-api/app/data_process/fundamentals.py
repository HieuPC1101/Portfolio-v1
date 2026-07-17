"""Helpers dedicated to fetching fundamental (financial) data for tickers."""

from __future__ import annotations

from builtins import print as builtin_print
import re
import unicodedata
from typing import Any, Dict, List, Optional

import pandas as pd
from vnstock import Vnstock


def _safe_print(*args, **kwargs) -> None:
    try:
        builtin_print(*args, **kwargs)
    except UnicodeEncodeError:
        safe_args = [
            str(arg).encode("ascii", "backslashreplace").decode("ascii")
            for arg in args
        ]
        builtin_print(*safe_args, **kwargs)


print = _safe_print


def _normalize_label(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


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


def _to_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _flatten_ratio_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()

    working = frame.copy()
    if isinstance(working.columns, pd.MultiIndex):
        flattened: List[str] = []
        for column in working.columns.to_flat_index():
            if isinstance(column, tuple):
                parts = [str(part).strip() for part in column if str(part).strip()]
                flattened.append(parts[-1] if parts else "")
            else:
                flattened.append(str(column))
        working.columns = flattened
    else:
        working.columns = [str(column) for column in working.columns]

    return working


def _pick_value(payload: Dict[str, Any], aliases: List[str]) -> Any:
    normalized_payload: Dict[str, Any] = {}
    for key, value in payload.items():
        if value is None or pd.isna(value):
            continue
        normalized_key = _normalize_label(key)
        if normalized_key and normalized_key not in normalized_payload:
            normalized_payload[normalized_key] = value

    for alias in aliases:
        matched = normalized_payload.get(_normalize_label(alias))
        if matched is not None:
            return matched

    for alias in aliases:
        alias_key = _normalize_label(alias)
        if not alias_key:
            continue
        for normalized_key, value in normalized_payload.items():
            if alias_key in normalized_key:
                return value

    return None


def _build_as_of_date(year: Optional[int], period: Optional[int]) -> Optional[str]:
    if year is None:
        return None
    if period is None or period < 1 or period > 4:
        return str(year)
    return f"{year}-Q{period}"


def fetch_fundamental_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch normalized fundamental data for a single ticker."""
    try:
        symbol_upper = symbol.upper()
        financial_ratio = None

        for source in ("KBS", "VCI"):
            try:
                stock = Vnstock().stock(symbol=symbol_upper, source=source)
                try:
                    financial_ratio = stock.finance.ratio(period="year", lang="en")
                except TypeError:
                    financial_ratio = stock.finance.ratio(period="year")

                if financial_ratio is not None and not financial_ratio.empty:
                    break
            except Exception:
                financial_ratio = None

        if financial_ratio is None or financial_ratio.empty:
            print(f"Không có dữ liệu phân tích cơ bản cho {symbol}")
            return None

        flattened = _flatten_ratio_frame(financial_ratio)
        latest_data = flattened.iloc[0] if not flattened.empty else None
        if latest_data is None:
            return None

        row = latest_data.to_dict()
        year = _to_optional_int(_pick_value(row, ["yearReport", "year", "Năm"]))
        period = _to_optional_int(_pick_value(row, ["lengthReport", "quarter", "Kỳ"]))

        return {
            "symbol": symbol_upper,
            "marketCap": _to_optional_float(
                _pick_value(
                    row, ["Market Capital (Bn. VND)", "Vốn hóa (Tỷ đồng)", "market_cap"]
                )
            ),
            "sharesOutstanding": _to_optional_float(
                _pick_value(
                    row,
                    [
                        "Outstanding Share (Mil. Shares)",
                        "Số CP lưu hành (Triệu CP)",
                        "shares_outstanding",
                    ],
                )
            ),
            "freeFloat": _to_optional_float(
                _pick_value(row, ["free_float", "freeFloat"])
            ),
            "pe": _to_optional_float(_pick_value(row, ["P/E", "priceToEarning", "pe"])),
            "pb": _to_optional_float(_pick_value(row, ["P/B", "priceToBook", "pb"])),
            "evEbitda": _to_optional_float(
                _pick_value(row, ["EV/EBITDA", "evEbitda", "ev_ebitda"])
            ),
            "eps": _to_optional_float(
                _pick_value(row, ["EPS (VND)", "earningPerShare", "eps"])
            ),
            "roe": _to_optional_float(_pick_value(row, ["ROE (%)", "roe"])),
            "roa": _to_optional_float(_pick_value(row, ["ROA (%)", "roa"])),
            "profit_margin": _to_optional_float(
                _pick_value(
                    row,
                    [
                        "Net Profit Margin (%)",
                        "Biên lợi nhuận ròng (%)",
                        "netProfitMargin",
                    ],
                )
            ),
            "revenue": _to_optional_float(
                _pick_value(row, ["Revenue (Bn. VND)", "revenue"])
            ),
            "profit": _to_optional_float(
                _pick_value(
                    row,
                    [
                        "Attribute to parent company (Bn. VND)",
                        "Lợi nhuận sau thuế CĐ công ty mẹ",
                        "postTaxProfit",
                    ],
                )
            ),
            "asOfDate": _build_as_of_date(year, period),
            "source": "vnstock",
        }
    except SystemExit as exc:  # pragma: no cover - vnstock quota exits process
        print(f"Vượt giới hạn vnstock cho {symbol}: {exc}")
        return None
    except Exception as exc:  # pragma: no cover - network/IO
        print(f"Lỗi khi lấy dữ liệu phân tích cơ bản cho {symbol}: {exc}")
        return None


def fetch_fundamental_data_batch(symbols: List[str]) -> pd.DataFrame:
    """Batch fetch fundamental data for multiple tickers."""
    fundamental_list = []
    print(f"\nĐang lấy dữ liệu phân tích cơ bản cho {len(symbols)} mã cổ phiếu...")
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] Đang xử lý {symbol}...", end=" ")
        data = fetch_fundamental_data(symbol)
        if data:
            fundamental_list.append(data)
            print(" Thành công")
        else:
            print(" Không có dữ liệu")

    if fundamental_list:
        df = pd.DataFrame(fundamental_list)
        print(
            f"\n Hoàn thành! Lấy dữ liệu thành công cho {len(df)}/{len(symbols)} mã cổ phiếu"
        )
        return df

    print(f"\n Không thể lấy dữ liệu phân tích cơ bản cho bất kỳ mã cổ phiếu nào")
    return pd.DataFrame()


__all__ = ["fetch_fundamental_data", "fetch_fundamental_data_batch"]
