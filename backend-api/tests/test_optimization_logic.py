import pandas as pd

from app.portfolio_models import portfolio_models as pm
from app.services.optimization_service import OptimizationService


def build_price_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "AAA": [100.0, 101.5, 102.0, 103.0, 104.5],
            "BBB": [50.0, 50.5, 51.0, 51.5, 52.0],
        },
        index=pd.date_range("2025-01-01", periods=5, freq="D"),
    )


def test_markowitz_uses_target_return_constraint(monkeypatch):
    calls = {}

    class FakeEfficientFrontier:
        def __init__(self, mean_returns, cov_matrix):
            calls["mean_returns"] = mean_returns
            calls["cov_matrix"] = cov_matrix

        def efficient_return(self, target_return, market_neutral=False):
            calls["target_return"] = target_return
            calls["market_neutral"] = market_neutral
            return {"AAA": 0.6, "BBB": 0.4}

        def clean_weights(self):
            return {"AAA": 0.6, "BBB": 0.4}

        def portfolio_performance(self, verbose=False):
            calls["verbose"] = verbose
            return (0.14, 0.09, 1.33)

    monkeypatch.setattr(
        pm.expected_returns,
        "mean_historical_return",
        lambda data: pd.Series({"AAA": 0.15, "BBB": 0.11}),
    )
    monkeypatch.setattr(
        pm.risk_models,
        "sample_cov",
        lambda data: pd.DataFrame(
            [[0.04, 0.01], [0.01, 0.03]],
            index=["AAA", "BBB"],
            columns=["AAA", "BBB"],
        ),
    )
    monkeypatch.setattr(pm, "EfficientFrontier", FakeEfficientFrontier)
    monkeypatch.setattr(
        pm,
        "run_integer_programming",
        lambda weights, latest_prices, total_portfolio_value: (
            {"AAA": 5, "BBB": 8},
            5000,
        ),
    )

    result = pm.markowitz_optimization(
        build_price_data(),
        1_000_000,
        lambda tickers: {"AAA": 100_000, "BBB": 50_000},
        target_return=0.12,
    )

    assert calls["target_return"] == 0.12
    assert calls["market_neutral"] is False
    assert result["Trọng số danh mục"] == {"AAA": 0.6, "BBB": 0.4}
    assert result["Lợi nhuận kỳ vọng"] == 0.14
    assert result["Rủi ro (Độ lệch chuẩn)"] == 0.09


def test_max_sharpe_uses_requested_risk_free_rate(monkeypatch):
    calls = {}

    class FakeEfficientFrontier:
        def __init__(self, mean_returns, cov_matrix):
            calls["mean_returns"] = mean_returns
            calls["cov_matrix"] = cov_matrix

        def max_sharpe(self, risk_free_rate=0.02):
            calls["risk_free_rate"] = risk_free_rate
            return {"AAA": 0.55, "BBB": 0.45}

        def clean_weights(self):
            return {"AAA": 0.55, "BBB": 0.45}

        def portfolio_performance(self, verbose=False, risk_free_rate=0.02):
            calls["performance_risk_free_rate"] = risk_free_rate
            return (0.16, 0.1, 1.4)

    monkeypatch.setattr(
        pm.expected_returns,
        "mean_historical_return",
        lambda data: pd.Series({"AAA": 0.17, "BBB": 0.12}),
    )
    monkeypatch.setattr(
        pm.risk_models,
        "sample_cov",
        lambda data: pd.DataFrame(
            [[0.05, 0.02], [0.02, 0.04]],
            index=["AAA", "BBB"],
            columns=["AAA", "BBB"],
        ),
    )
    monkeypatch.setattr(pm, "EfficientFrontier", FakeEfficientFrontier)
    monkeypatch.setattr(
        pm,
        "run_integer_programming",
        lambda weights, latest_prices, total_portfolio_value: (
            {"AAA": 4, "BBB": 9},
            2000,
        ),
    )

    result = pm.max_sharpe(
        build_price_data(),
        1_000_000,
        lambda tickers: {"AAA": 100_000, "BBB": 50_000},
        risk_free_rate=0.05,
    )

    assert calls["risk_free_rate"] == 0.05
    assert calls["performance_risk_free_rate"] == 0.05
    assert result["Tỷ lệ Sharpe"] == 1.4


def test_service_normalizes_tail_risk_and_actual_allocation_amounts(monkeypatch):
    service = OptimizationService()
    price_data = build_price_data()

    monkeypatch.setattr(
        service.market_service,
        "fetch_prices",
        lambda query: (price_data, []),
    )
    monkeypatch.setattr(
        service,
        "run_model",
        lambda model_name, request: {
            "Trọng số danh mục": {"AAA": 0.6, "BBB": 0.4},
            "Số mã cổ phiếu cần mua": {"AAA": 3, "BBB": 2},
            "Giá mã cổ phiếu": {"AAA": 100_000, "BBB": 200_000},
            "Số tiền còn lại": 50_000,
            "Lợi nhuận kỳ vọng": 0.15,
            "Rủi ro (Độ lệch chuẩn)": 0.11,
            "Tỷ lệ Sharpe": 1.2,
            "Rủi ro CVaR": 0.08,
            "Rủi ro CDaR": 0.12,
        },
    )

    result = service.optimize_portfolio(
        symbols=["AAA", "BBB"],
        model="min_cvar",
        start_date=None,
        end_date=None,
        constraints=None,
        investment=1_000_000,
    )

    assert result["expected_return"] == 0.15
    assert result["expected_volatility"] == 0.11
    assert result["extra_data"]["cvar"] == 0.08
    assert result["extra_data"]["cdar"] == 0.12
    assert result["extra_data"]["latest_prices"] == {"AAA": 100_000.0, "BBB": 200_000.0}
    assert result["extra_data"]["allocation_amounts"] == {
        "AAA": 300_000.0,
        "BBB": 400_000.0,
    }
