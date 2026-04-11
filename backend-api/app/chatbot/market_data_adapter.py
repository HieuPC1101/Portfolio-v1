"""
Market Data Adapter - Wrapper layer cho chatbot để truy cập dữ liệu thị trường
Sử dụng 100% các hàm có sẵn từ fetchers.py và fundamentals.py
"""

import re
from datetime import datetime, timedelta
from typing import Optional, Set, Dict, List
from app.services.market_service import PriceQuery, get_market_data_service


class MarketDataAdapter:
    """
    Adapter layer để chatbot gọi các hàm data fetching có sẵn
    Không duplicate logic, chỉ wrap và format output
    """

    def __init__(self):
        """Khởi tạo adapter"""
        self.stock_pattern = re.compile(r"\b([A-Z]{3,4})\b")
        self.market_service = get_market_data_service()
        self.common_words = {
            "THE",
            "AND",
            "FOR",
            "NOT",
            "BUT",
            "CAN",
            "ARE",
            "WAS",
            "HAS",
            "HAD",
            "YOU",
            "ALL",
            "HER",
            "ONE",
            "OUR",
            "OUT",
            "DAY",
            "GET",
            "HIM",
            "HIS",
            "HOW",
            "MAN",
            "NEW",
            "NOW",
            "OLD",
            "SEE",
            "TWO",
            "WAY",
            "WHO",
            "BOY",
            "DID",
            "ITS",
            "LET",
            "PUT",
            "SAY",
            "SHE",
            "TOO",
            "USE",
        }

    def extract_stock_symbols(self, query: str) -> Set[str]:
        """
        Trích xuất mã cổ phiếu từ câu hỏi

        Args:
            query: Câu hỏi của người dùng

        Returns:
            Set các mã cổ phiếu tìm thấy
        """
        matches = self.stock_pattern.findall(query.upper())
        symbols = set(matches) - self.common_words
        return symbols

    def is_market_indices_query(self, query: str) -> bool:
        """Kiểm tra có phải câu hỏi về chỉ số thị trường không"""
        keywords = ["chỉ số", "thị trường", "vnindex", "vn30", "hnx", "hnxindex"]
        return any(kw in query.lower() for kw in keywords)

    def is_analysis_query(self, query: str) -> bool:
        """Kiểm tra có phải câu hỏi phân tích không"""
        keywords = ["phân tích", "đánh giá", "nhận xét", "xem xét", "giá", "cổ phiếu"]
        return any(kw in query.lower() for kw in keywords)

    def get_stock_realtime_info(self, symbol: str) -> Optional[Dict]:
        """
        Lấy thông tin realtime của 1 mã cổ phiếu
        Sử dụng: fetch_stock_data2, fetch_ohlc_data

        Args:
            symbol: Mã cổ phiếu

        Returns:
            Dict chứa thông tin giá hoặc None
        """
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            # Gọi hàm có sẵn
            data, _ = self.market_service.fetch_prices(
                PriceQuery(
                    symbols=[symbol],
                    start_date=start_date,
                    end_date=end_date,
                    verbose=False,
                )
            )

            if data.empty or symbol not in data.columns:
                return None

            prices = data[symbol].dropna()
            if len(prices) < 2:
                return None

            latest = float(prices.iloc[-1])
            prev = float(prices.iloc[-2])

            # Lấy OHLC và volume
            ohlc = self.market_service.fetch_ohlc(symbol, start_date, end_date)
            volume = 0
            high = latest
            low = latest

            if not ohlc.empty:
                if "volume" in ohlc.columns:
                    volume = int(ohlc["volume"].iloc[-1])
                if "high" in ohlc.columns:
                    high = float(ohlc["high"].iloc[-1])
                if "low" in ohlc.columns:
                    low = float(ohlc["low"].iloc[-1])

            return {
                "symbol": symbol.upper(),
                "price": latest * 1000,  # Convert từ nghìn VND sang VND
                "high": high * 1000,
                "low": low * 1000,
                "volume": volume,
                "change_percent": ((latest - prev) / prev * 100) if prev != 0 else 0,
                "date": data.index[-1].strftime("%Y-%m-%d")
                if hasattr(data.index[-1], "strftime")
                else str(data.index[-1]),
            }
        except Exception as e:
            print(f"Lỗi get_stock_realtime_info {symbol}: {e}")
            return None

    def get_stock_fundamental_info(self, symbol: str) -> Optional[Dict]:
        """
        Lấy thông tin fundamental của 1 mã
        Sử dụng: fetch_fundamental_data

        Args:
            symbol: Mã cổ phiếu

        Returns:
            Dict chứa thông tin fundamental hoặc None
        """
        try:
            # Gọi hàm có sẵn
            return self.market_service.fundamental_data(symbol)
        except Exception as e:
            print(f"Lỗi get_stock_fundamental_info {symbol}: {e}")
            return None

    def get_stock_history(self, symbol: str, days: int = 30) -> Optional[Dict]:
        """
        Lấy lịch sử giá của 1 mã
        Sử dụng: fetch_stock_data2

        Args:
            symbol: Mã cổ phiếu
            days: Số ngày lịch sử

        Returns:
            Dict chứa thông tin lịch sử hoặc None
        """
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            # Gọi hàm có sẵn
            data, _ = self.market_service.fetch_prices(
                PriceQuery(
                    symbols=[symbol],
                    start_date=start_date,
                    end_date=end_date,
                    verbose=False,
                )
            )

            if data.empty or symbol not in data.columns:
                return None

            prices = data[symbol].dropna().values
            if len(prices) < 2:
                return None

            return {
                "symbol": symbol.upper(),
                "avg_price": float(prices.mean()) * 1000,
                "min_price": float(prices.min()) * 1000,
                "max_price": float(prices.max()) * 1000,
                "current_price": float(prices[-1]) * 1000,
                "change_percent": ((prices[-1] - prices[0]) / prices[0] * 100)
                if prices[0] != 0
                else 0,
            }
        except Exception as e:
            print(f"Lỗi get_stock_history {symbol}: {e}")
            return None

    def get_market_indices(self) -> Optional[Dict]:
        """
        Lấy thông tin các chỉ số thị trường
        Sử dụng: get_index_history

        Returns:
            Dict chứa thông tin các chỉ số hoặc None
        """
        try:
            indices_data = {}
            indices = ["VNINDEX", "VN30", "HNXINDEX"]

            for index_symbol in indices:
                try:
                    # Gọi hàm có sẵn
                    history = self.market_service.index_history(
                        symbol=index_symbol, months=1, source="VCI"
                    )

                    if history is not None and not history.empty:
                        history = history.sort_values("time")
                        latest = history.iloc[-1]
                        prev = history.iloc[-2] if len(history) > 1 else latest

                        indices_data[index_symbol] = {
                            "value": float(latest["close"]),
                            "change": float(latest["close"] - prev["close"]),
                            "change_percent": float(
                                (latest["close"] - prev["close"]) / prev["close"] * 100
                            )
                            if prev["close"] != 0
                            else 0,
                            "volume": int(latest["volume"])
                            if "volume" in latest
                            else 0,
                        }
                except Exception as e:
                    print(f"Lỗi get_market_indices {index_symbol}: {e}")
                    continue

            return indices_data if indices_data else None
        except Exception as e:
            print(f"Lỗi get_market_indices: {e}")
            return None

    def format_stock_analysis(self, symbol: str) -> Optional[str]:
        """
        Phân tích đầy đủ 1 mã cổ phiếu

        Args:
            symbol: Mã cổ phiếu

        Returns:
            Chuỗi text phân tích hoặc None
        """
        try:
            lines = [f"\n=== PHÂN TÍCH MÃ {symbol.upper()} ===\n"]

            # Realtime info
            realtime = self.get_stock_realtime_info(symbol)
            if realtime:
                lines.append(" Dữ liệu giao dịch:")
                lines.append(
                    f"- Giá hiện tại: {realtime['price']:,.0f} VND ({realtime['change_percent']:+.2f}%)"
                )
                lines.append(f"- Khối lượng: {realtime['volume']:,} cp")
                lines.append(
                    f"- Vùng giá: {realtime['low']:,.0f} - {realtime['high']:,.0f} VND\n"
                )

            # Fundamental info
            fundamental = self.get_stock_fundamental_info(symbol)
            if fundamental:
                lines.append(" Chỉ số tài chính:")
                if fundamental.get("pe"):
                    lines.append(f"- P/E: {fundamental['pe']:.2f}")
                if fundamental.get("pb"):
                    lines.append(f"- P/B: {fundamental['pb']:.2f}")
                if fundamental.get("eps"):
                    lines.append(f"- EPS: {fundamental['eps']:,.0f}")
                if fundamental.get("roe"):
                    lines.append(f"- ROE: {fundamental['roe']:.2f}%")
                if fundamental.get("roa"):
                    lines.append(f"- ROA: {fundamental['roa']:.2f}%\n")

            # History info
            history = self.get_stock_history(symbol, 30)
            if history:
                lines.append(" Lịch sử 30 ngày:")
                lines.append(f"- Giá TB: {history['avg_price']:,.0f} VND")
                lines.append(
                    f"- Biên độ: {history['min_price']:,.0f} - {history['max_price']:,.0f} VND"
                )
                lines.append(f"- Thay đổi: {history['change_percent']:+.2f}%")

            lines.append("\n\nDựa vào dữ liệu trên, hãy phân tích chi tiết:")
            return "\n".join(lines)

        except Exception as e:
            print(f"Lỗi format_stock_analysis {symbol}: {e}")
            return None

    def format_stocks_comparison(self, symbols: List[str]) -> Optional[str]:
        """
        So sánh nhiều mã cổ phiếu

        Args:
            symbols: Danh sách mã cổ phiếu (tối đa 3)

        Returns:
            Chuỗi text so sánh hoặc None
        """
        try:
            lines = ["\n DỮ LIỆU REALTIME CÁC MÃ CỔ PHIẾU:\n"]

            for symbol in symbols[:3]:
                realtime = self.get_stock_realtime_info(symbol)
                if realtime:
                    lines.append(f"\n{symbol}:")
                    lines.append(
                        f"  Giá: {realtime['price']:,.0f} VND ({realtime['change_percent']:+.2f}%)"
                    )

                    fundamental = self.get_stock_fundamental_info(symbol)
                    if fundamental:
                        if fundamental.get("pe"):
                            lines.append(f"  P/E: {fundamental['pe']:.2f}")
                        if fundamental.get("roe"):
                            lines.append(f"  ROE: {fundamental['roe']:.2f}%")

            lines.append("\n\nDựa vào dữ liệu trên, hãy so sánh các mã:")
            return "\n".join(lines)

        except Exception as e:
            print(f"Lỗi format_stocks_comparison: {e}")
            return None

    def format_basic_stock_info(self, symbols: List[str]) -> Optional[str]:
        """
        Hiển thị thông tin giá cơ bản

        Args:
            symbols: Danh sách mã cổ phiếu (tối đa 3)

        Returns:
            Chuỗi text thông tin giá hoặc None
        """
        try:
            lines = ["\n DỮ LIỆU REALTIME:\n"]

            # Gọi hàm có sẵn
            prices = self.market_service.latest_prices(symbols[:3])

            for symbol in symbols[:3]:
                if symbol in prices:
                    lines.append(f"{symbol}: {prices[symbol] * 1000:,.0f} VND")

            return "\n".join(lines) + "\n" if len(lines) > 1 else None

        except Exception as e:
            print(f"Lỗi format_basic_stock_info: {e}")
            return None

    def format_market_indices(self) -> Optional[str]:
        """
        Format thông tin chỉ số thị trường

        Returns:
            Chuỗi text thông tin chỉ số hoặc None
        """
        try:
            indices = self.get_market_indices()
            if not indices:
                return None

            lines = [" DỮ LIỆU CHỈ SỐ THỊ TRƯỜNG (REALTIME):"]

            for index_name, data in indices.items():
                lines.append(f"\n{index_name}: {data['value']:,.2f} điểm")
                lines.append(
                    f"  Thay đổi: {data['change']:+,.2f} ({data['change_percent']:+.2f}%)"
                )
                if data["volume"] > 0:
                    lines.append(f"  Khối lượng: {data['volume']:,}")

            return "\n".join(lines)

        except Exception as e:
            print(f"Lỗi format_market_indices: {e}")
            return None

    def get_context_from_query(self, query: str) -> Optional[str]:
        """
        Phát hiện intent và lấy dữ liệu phù hợp từ câu hỏi

        Args:
            query: Câu hỏi của người dùng

        Returns:
            Context string để thêm vào prompt hoặc None
        """
        # Kiểm tra câu hỏi về chỉ số thị trường
        if self.is_market_indices_query(query):
            return self.format_market_indices()

        # Trích xuất mã cổ phiếu
        symbols = self.extract_stock_symbols(query)
        if not symbols:
            return None

        symbols_list = list(symbols)

        # Phân tích chi tiết 1 mã
        if len(symbols_list) == 1 and self.is_analysis_query(query):
            return self.format_stock_analysis(symbols_list[0])

        # So sánh nhiều mã
        if len(symbols_list) > 1:
            return self.format_stocks_comparison(symbols_list)

        # Thông tin giá cơ bản
        if symbols_list:
            return self.format_basic_stock_info(symbols_list)

        return None


# Singleton instance
_adapter_instance = None


def get_market_data_adapter() -> MarketDataAdapter:
    """Lấy singleton instance của MarketDataAdapter"""
    global _adapter_instance
    if _adapter_instance is None:
        _adapter_instance = MarketDataAdapter()
    return _adapter_instance
