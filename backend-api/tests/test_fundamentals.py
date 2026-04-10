import pandas as pd

from app.data_process import fundamentals


def test_fetch_fundamental_data_parses_multiindex_ratio_columns(monkeypatch):
    columns = pd.MultiIndex.from_tuples(
        [
            ("Meta", "CP"),
            ("Meta", "Năm"),
            ("Meta", "Kỳ"),
            ("Chỉ tiêu định giá", "Vốn hóa (Tỷ đồng)"),
            ("Chỉ tiêu định giá", "Số CP lưu hành (Triệu CP)"),
            ("Chỉ tiêu định giá", "P/E"),
            ("Chỉ tiêu định giá", "P/B"),
            ("Chỉ tiêu định giá", "EV/EBITDA"),
            ("Chỉ tiêu khả năng sinh lợi", "ROE (%)"),
            ("Chỉ tiêu khả năng sinh lợi", "ROA (%)"),
        ]
    )
    ratio_frame = pd.DataFrame(
        [
            [
                "FPT",
                2025,
                4,
                134_747_413_271_100,
                1_703_507_121,
                14.34,
                3.69,
                10.89,
                0.2829,
                0.1171,
            ]
        ],
        columns=columns,
    )

    class _FakeFinance:
        def ratio(self, period: str, lang: str):
            return ratio_frame

    class _FakeStock:
        finance = _FakeFinance()

    class _FakeVnstock:
        def stock(self, symbol: str, source: str):
            return _FakeStock()

    monkeypatch.setattr(fundamentals, "Vnstock", _FakeVnstock)

    payload = fundamentals.fetch_fundamental_data("FPT")

    assert payload is not None
    assert payload["symbol"] == "FPT"
    assert payload["marketCap"] == 134_747_413_271_100
    assert payload["sharesOutstanding"] == 1_703_507_121
    assert payload["pe"] == 14.34
    assert payload["pb"] == 3.69
    assert payload["evEbitda"] == 10.89
    assert payload["roe"] == 0.2829
    assert payload["roa"] == 0.1171


def test_fetch_fundamental_data_returns_none_on_rate_limit(monkeypatch):
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

    monkeypatch.setattr(fundamentals, "Vnstock", _FakeVnstock)

    payload = fundamentals.fetch_fundamental_data("FPT")

    assert payload is None
