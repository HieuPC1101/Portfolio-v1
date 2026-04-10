import pandas as pd
from datetime import datetime
import sys
import types

from app.services.market_service import MarketService


def test_get_stock_overview_maps_company_profile(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "load_company_info",
        lambda: pd.DataFrame(
            [
                {
                    "symbol": "FPT",
                    "organ_name": "CTCP FPT",
                    "icb_name": "Cong nghe",
                    "exchange": "HOSE",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        service,
        "fundamental_data",
        lambda symbol: {
            "marketCap": 185_000_000_000_000,
            "sharesOutstanding": 1_470_000_000,
            "freeFloat": 0.72,
        },
    )

    overview = service.get_stock_overview("fpt")

    assert overview["symbol"] == "FPT"
    assert overview["company_name"] == "CTCP FPT"
    assert overview["exchange"] == "HOSE"
    assert overview["sector"] == "Cong nghe"
    assert overview["market_cap"] == 185_000_000_000_000
    assert isinstance(overview["latest_highlights"], list)


def test_get_stock_overview_builds_summary_when_source_missing(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "load_company_info",
        lambda: pd.DataFrame(
            [
                {
                    "symbol": "FPT",
                    "organ_name": "CTCP FPT",
                    "icb_name": "Cong nghe",
                    "exchange": "HOSE",
                }
            ]
        ),
    )
    monkeypatch.setattr(service, "fundamental_data", lambda symbol: {})
    monkeypatch.setattr(service, "_fetch_company_profile", lambda symbol: {})

    overview = service.get_stock_overview("FPT")

    assert overview["business_summary"] is not None
    assert "CTCP FPT" in overview["business_summary"]
    assert "HOSE" in overview["business_summary"]
    assert "Cong nghe" in overview["business_summary"]


def test_get_stock_overview_uses_company_profile_summary(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "load_company_info",
        lambda: pd.DataFrame(
            [
                {
                    "symbol": "FPT",
                    "organ_name": "CTCP FPT",
                    "icb_name": "Cong nghe",
                    "exchange": "HOSE",
                }
            ]
        ),
    )
    monkeypatch.setattr(service, "fundamental_data", lambda symbol: {})
    monkeypatch.setattr(
        service,
        "_fetch_company_profile",
        lambda symbol: {
            "company_profile": "FPT tap trung vao cong nghe, vien thong va giao duc.",
        },
    )

    overview = service.get_stock_overview("FPT")

    assert (
        overview["business_summary"]
        == "FPT tap trung vao cong nghe, vien thong va giao duc."
    )


def test_fetch_company_profile_falls_back_to_overview_when_profile_unavailable(
    monkeypatch,
):
    service = MarketService()

    class FakeCompany:
        @staticmethod
        def profile():
            raise AttributeError("profile method is unavailable")

        @staticmethod
        def overview():
            return pd.DataFrame(
                [
                    {
                        "company_profile": "Doanh nghiep cong nghe hang dau.",
                        "icb_name3": "Cong nghe va thong tin",
                    }
                ]
            )

    class FakeStock:
        company = FakeCompany()

    class FakeVnstock:
        @staticmethod
        def stock(symbol: str, source: str):
            return FakeStock()

    fake_vnstock_module = types.SimpleNamespace(Vnstock=FakeVnstock)
    monkeypatch.setitem(sys.modules, "vnstock", fake_vnstock_module)

    profile = service._fetch_company_profile("FPT")

    assert profile["company_profile"] == "Doanh nghiep cong nghe hang dau."
    assert profile["icb_name3"] == "Cong nghe va thong tin"


def test_get_stock_overview_ignores_nan_sector_and_builds_clean_summary(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "load_company_info",
        lambda: pd.DataFrame(
            [
                {
                    "symbol": "FRE",
                    "organ_name": "CTCP Freco Viet Nam",
                    "icb_name": float("nan"),
                    "exchange": "UPCOM",
                }
            ]
        ),
    )
    monkeypatch.setattr(service, "fundamental_data", lambda symbol: {})
    monkeypatch.setattr(
        service,
        "_fetch_company_profile",
        lambda symbol: {
            "industry": "Hang tieu dung",
        },
    )

    overview = service.get_stock_overview("FRE")

    assert overview["sector"] == "Hang tieu dung"
    assert overview["business_summary"] is not None
    assert "NaN" not in overview["business_summary"]


def test_get_stock_financials_builds_statement_rows(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "_fetch_financial_ratio_frame",
        lambda symbol, period: pd.DataFrame(
            [
                {
                    "year": 2025,
                    "quarter": 4,
                    "revenue": 10_000,
                    "grossProfit": 4_000,
                    "operatingProfit": 2_100,
                    "postTaxProfit": 1_600,
                    "totalAssets": 22_000,
                    "totalLiabilities": 9_000,
                    "equity": 13_000,
                    "totalDebt": 5_000,
                    "cashAndCashEquivalents": 2_400,
                    "operatingCashFlow": 1_800,
                    "investingCashFlow": -700,
                    "financingCashFlow": 350,
                }
            ]
        ),
    )

    payload = service.get_stock_financials("FPT", period="quarterly", limit=8)

    assert payload["symbol"] == "FPT"
    assert payload["period"] == "quarterly"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["free_cash_flow"] == 1_100
    assert payload["as_of_date"] == "Q4/2025"


def test_get_stock_ratios_returns_null_when_denominator_non_positive(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "fundamental_data",
        lambda symbol: {
            "pe": 18.4,
            "pb": 4.1,
            "evEbitda": None,
            "roe": None,
            "roa": None,
        },
    )
    monkeypatch.setattr(
        service,
        "get_stock_financials",
        lambda symbol, period="quarterly", limit=8: {
            "symbol": symbol,
            "period": period,
            "items": [
                {
                    "revenue": 0,
                    "gross_profit": 800,
                    "net_income": 500,
                    "equity": 0,
                    "total_assets": 10_000,
                    "total_debt": 1_500,
                    "operating_profit": 900,
                }
            ],
        },
    )

    ratios = service.get_stock_ratios("FPT")

    assert ratios["pe"] == 18.4
    assert ratios["pb"] == 4.1
    assert ratios["gross_margin"] is None
    assert ratios["net_margin"] is None
    assert ratios["debt_to_equity"] is None
    assert "gross_margin_unavailable" in ratios["quality_flags"]
    assert "net_margin_unavailable" in ratios["quality_flags"]
    assert "debt_to_equity_unavailable" in ratios["quality_flags"]


def test_get_stock_ratios_uses_fundamental_fallbacks_when_financials_missing(
    monkeypatch,
):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "fundamental_data",
        lambda symbol: {
            "pe": 14.3,
            "pb": 3.7,
            "evEbitda": 10.4,
            "roe": 0.28,
            "roa": 0.11,
            "grossMargin": 0.35,
            "netMargin": 0.13,
            "debtToEquity": 1.01,
            "asOfDate": "2025",
        },
    )
    monkeypatch.setattr(
        service,
        "get_stock_financials",
        lambda symbol, period="quarterly", limit=8: {
            "symbol": symbol,
            "period": period,
            "items": [],
            "as_of_date": "2025",
        },
    )

    ratios = service.get_stock_ratios("FPT")

    assert ratios["gross_margin"] == 0.35
    assert ratios["net_margin"] == 0.13
    assert ratios["debt_to_equity"] == 1.01
    assert "gross_margin_unavailable" not in ratios["quality_flags"]
    assert "net_margin_unavailable" not in ratios["quality_flags"]
    assert "debt_to_equity_unavailable" not in ratios["quality_flags"]
    assert ratios["as_of_date"] == datetime.utcnow().date().isoformat()
    assert ratios["reporting_period"] == "2025"


def test_get_stock_ratios_prefers_financial_as_of_date(monkeypatch):
    service = MarketService()

    monkeypatch.setattr(
        service,
        "fundamental_data",
        lambda symbol: {
            "pe": 14.3,
            "pb": 3.7,
            "asOfDate": "2025",
        },
    )
    monkeypatch.setattr(
        service,
        "get_stock_financials",
        lambda symbol, period="quarterly", limit=8: {
            "symbol": symbol,
            "period": period,
            "items": [
                {
                    "revenue": 10_000,
                    "gross_profit": 2_400,
                    "net_income": 1_100,
                    "equity": 5_000,
                    "total_assets": 8_000,
                    "total_debt": 2_000,
                }
            ],
            "as_of_date": "Q4/2025",
        },
    )

    ratios = service.get_stock_ratios("FPT")

    assert ratios["as_of_date"] == datetime.utcnow().date().isoformat()
    assert ratios["reporting_period"] == "Q4/2025"


def test_fundamental_data_returns_empty_payload_on_rate_limit(monkeypatch):
    service = MarketService()

    def _raise_system_exit(symbol: str):
        raise SystemExit("Rate limit exceeded")

    monkeypatch.setattr(
        "app.services.market_service.fetch_fundamental_data", _raise_system_exit
    )

    payload = service.fundamental_data("FPT")

    assert payload == {}


def test_fundamental_data_uses_in_memory_cache(monkeypatch):
    service = MarketService()
    calls = {"count": 0}

    def _fake_fetch(symbol: str):
        calls["count"] += 1
        return {"symbol": symbol, "pe": 14.3}

    monkeypatch.setattr(
        "app.services.market_service.fetch_fundamental_data", _fake_fetch
    )

    first = service.fundamental_data("FPT")
    second = service.fundamental_data("FPT")

    assert first == second
    assert calls["count"] == 1


def test_fetch_company_profile_returns_empty_on_rate_limit(monkeypatch):
    service = MarketService()

    class _FakeVnstock:
        @staticmethod
        def stock(symbol: str, source: str):
            raise SystemExit("Rate limit exceeded")

    fake_vnstock_module = types.SimpleNamespace(Vnstock=_FakeVnstock)
    monkeypatch.setitem(sys.modules, "vnstock", fake_vnstock_module)

    profile = service._fetch_company_profile("FPT")

    assert profile == {}


def test_fetch_financial_ratio_frame_returns_empty_on_rate_limit(monkeypatch):
    service = MarketService()

    class _FakeFinance:
        @staticmethod
        def ratio(period: str, lang: str):
            raise SystemExit("Rate limit exceeded")

    class _FakeStock:
        finance = _FakeFinance()

    class _FakeVnstock:
        @staticmethod
        def stock(symbol: str, source: str):
            return _FakeStock()

    fake_vnstock_module = types.SimpleNamespace(Vnstock=_FakeVnstock)
    monkeypatch.setitem(sys.modules, "vnstock", fake_vnstock_module)

    frame = service._fetch_financial_ratio_frame("FPT", "quarterly")

    assert frame.empty
