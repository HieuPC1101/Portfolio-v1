import pandas as pd

from app.data_process import processors


def test_get_indices_history_keeps_volume(monkeypatch):
    def fake_get_index_history(
        symbol, start_date=None, end_date=None, months=6, source="VCI"
    ):
        del start_date, end_date, months, source
        return pd.DataFrame(
            {
                "time": [pd.Timestamp("2026-04-08"), pd.Timestamp("2026-04-09")],
                "close": [1000.0, 1010.0],
                "volume": [123_000_000, 140_000_000],
                "symbol": [symbol, symbol],
            }
        )

    monkeypatch.setattr(processors, "get_index_history", fake_get_index_history)

    result = processors.get_indices_history(symbols=("VNINDEX", "VN30"), months=1)

    assert "volume" in result.columns
    assert set(result["symbol"]) == {"VN-Index", "VN30"}
    assert result["volume"].notna().all()


def test_get_indices_history_fills_missing_volume(monkeypatch):
    def fake_get_index_history(
        symbol, start_date=None, end_date=None, months=6, source="VCI"
    ):
        del start_date, end_date, months, source
        return pd.DataFrame(
            {
                "time": [pd.Timestamp("2026-04-09")],
                "close": [1000.0],
                "symbol": [symbol],
            }
        )

    monkeypatch.setattr(processors, "get_index_history", fake_get_index_history)

    result = processors.get_indices_history(symbols=("VNINDEX",), months=1)

    assert "volume" in result.columns
    assert result["volume"].isna().all()
