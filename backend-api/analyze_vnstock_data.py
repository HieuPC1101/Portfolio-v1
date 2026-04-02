"""
Script phân tích dữ liệu từ vnstock API
Giúp hiểu cấu trúc dữ liệu để thiết kế schema lưu trữ
"""

import pandas as pd
from datetime import datetime, timedelta
import json

try:
    from vnstock3 import Vnstock
except ImportError:
    print("Cài đặt vnstock3: pip install vnstock3")
    exit(1)


class VnstockDataAnalyzer:
    """Analyze data structure from vnstock API"""

    def __init__(self):
        self.stock = Vnstock()

    def print_section(self, title):
        print("\n" + "=" * 70)
        print(f"  {title}")
        print("=" * 70)

    def analyze_stock_price(self, symbol="VNM"):
        """Phân tích cấu trúc dữ liệu giá cổ phiếu"""
        self.print_section(f"1. Stock Price Data - {symbol}")

        try:
            # Lấy dữ liệu giá 3 tháng gần nhất
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)

            df = self.stock.stock(symbol=symbol, source="VCI").quote.history(
                start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d")
            )

            print("\nData Shape:", df.shape)
            print("\nColumns:", list(df.columns))
            print("\nData Types:")
            print(df.dtypes)
            print("\nFirst 5 rows:")
            print(df.head())
            print("\nStatistical Summary:")
            print(df.describe())

            # Analyze for storage
            print("\nStorage Considerations:")
            print(f"- Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
            print(f"- Date range: {df.index.min()} to {df.index.max()}")
            print(f"- Total records: {len(df)}")

            # Sample JSON structure
            print("\nSample JSON Structure:")
            sample = df.head(3).to_dict(orient="records")
            print(json.dumps(sample, indent=2, default=str))

            return df

        except Exception as e:
            print(f"[ERROR] Error: {e}")
            return None

    def analyze_stock_info(self, symbol="VNM"):
        """Phân tích thông tin cơ bản cổ phiếu"""
        self.print_section(f"2. Stock Info - {symbol}")

        try:
            # Lấy thông tin công ty
            info = self.stock.stock(symbol=symbol, source="VCI").company.profile()

            print("\nCompany Profile:")
            print(info)

            print("\nFields to store:")
            if isinstance(info, pd.DataFrame):
                print(f"- Columns: {list(info.columns)}")
                print(f"\nSample data:")
                print(info.to_dict(orient="records"))

            return info

        except Exception as e:
            print(f"[ERROR] Error: {e}")
            return None

    def analyze_fundamentals(self, symbol="VNM"):
        """Phân tích dữ liệu chỉ số tài chính"""
        self.print_section(f"3. Fundamentals - {symbol}")

        try:
            # Lấy các chỉ số tài chính
            ratios = self.stock.stock(symbol=symbol, source="VCI").finance.ratio()

            print("\nFinancial Ratios:")
            print(ratios)

            if isinstance(ratios, pd.DataFrame):
                print("\nAvailable Metrics:")
                print(list(ratios.columns))

                print("\nStorage format (latest record):")
                latest = ratios.head(1).to_dict(orient="records")
                print(json.dumps(latest, indent=2, default=str))

            return ratios

        except Exception as e:
            print(f"[ERROR] Error: {e}")
            return None

    def analyze_market_indices(self):
        """Phân tích dữ liệu chỉ số thị trường"""
        self.print_section("4. Market Indices")

        try:
            indices = ["VNINDEX", "VN30", "HNX", "UPCOM"]

            for index in indices:
                print(f"\n{index}:")
                df = self.stock.stock(symbol=index, source="VCI").quote.history(
                    start=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
                    end=datetime.now().strftime("%Y-%m-%d"),
                )
                print(f"- Columns: {list(df.columns)}")
                print(
                    f"- Latest value: {df['close'].iloc[-1] if not df.empty else 'N/A'}"
                )
                print(
                    f"- 30-day change: {((df['close'].iloc[-1] / df['close'].iloc[0] - 1) * 100):.2f}%"
                )

        except Exception as e:
            print(f"[ERROR] Error: {e}")

    def analyze_sector_data(self):
        """Phân tích dữ liệu ngành"""
        self.print_section("5. Sector Performance")

        try:
            # Lấy danh sách ngành
            industries = self.stock.stock(source="VCI").listing.all_industries()

            print("\nAvailable Sectors/Industries:")
            print(industries)

            if isinstance(industries, pd.DataFrame):
                print(f"\n- Total sectors: {len(industries)}")
                print(f"- Columns: {list(industries.columns)}")

        except Exception as e:
            print(f"[ERROR] Error: {e}")

    def analyze_listing_data(self):
        """Phân tích dữ liệu danh sách cổ phiếu"""
        self.print_section("6. Stock Listing Data")

        try:
            # Lấy tất cả cổ phiếu
            all_stocks = self.stock.stock(source="VCI").listing.all_symbols()

            print("\nAll Listed Stocks:")
            print(f"- Total stocks: {len(all_stocks)}")
            print(f"- Columns: {list(all_stocks.columns)}")
            print(f"\nSample (first 10):")
            print(all_stocks.head(10))

            # Breakdown by exchange
            if "exchange" in all_stocks.columns:
                print("\nBreakdown by Exchange:")
                print(all_stocks["exchange"].value_counts())

            return all_stocks

        except Exception as e:
            print(f"[ERROR] Error: {e}")
            return None

    def estimate_storage_requirements(self):
        """Ước tính nhu cầu lưu trữ"""
        self.print_section("7. Storage Requirements Estimation")

        try:
            # Giả sử có 1000 cổ phiếu
            num_stocks = 1000

            # Price data (1 year = ~252 trading days)
            days_per_year = 252
            price_record_size = 100  # bytes (estimate for JSON)

            price_storage = num_stocks * days_per_year * price_record_size / (1024**2)

            print(f"""
Estimated Storage Requirements:

1. Historical Price Data (1 year):
   - Per stock: {days_per_year * price_record_size / 1024:.2f} KB
   - Total ({num_stocks} stocks): {price_storage:.2f} MB
   
2. Real-time Cache (in memory):
   - Per stock: ~5 KB (recent price + metadata)
   - Total ({num_stocks} stocks): {num_stocks * 5 / 1024:.2f} MB

3. Fundamentals Data:
   - Per stock: ~10 KB (ratios, metrics)
   - Total ({num_stocks} stocks): {num_stocks * 10 / 1024:.2f} MB

4. User Data (10,000 users):
   - Portfolios: ~50 MB
   - Optimization results: ~100 MB
   - Total: ~150 MB

Total Estimated Storage:
   - Database: ~{price_storage + 150 + (num_stocks * 10 / 1024):.2f} MB
   - Redis cache: ~{num_stocks * 5 / 1024:.2f} MB
   - Total: ~{price_storage + 150 + (num_stocks * 15 / 1024):.2f} MB
   
Conclusion: PostgreSQL + Redis is sufficient
            """)

        except Exception as e:
            print(f"[ERROR] Error: {e}")

    def generate_schema_suggestions(self):
        """Đề xuất schema database dựa trên phân tích"""
        self.print_section("8. Database Schema Suggestions")

        schema = """
RECOMMENDED SCHEMA:

1. stock_price_cache:
   - id: SERIAL PRIMARY KEY
   - symbol: VARCHAR(10)
   - date: DATE
   - open, high, low, close: NUMERIC(12, 2)
   - volume: BIGINT
   - created_at, expires_at: TIMESTAMP
   - INDEX: (symbol, date)
   - PARTITION BY: date (monthly partitions)

2. fundamentals_cache:
   - id: SERIAL PRIMARY KEY
   - symbol: VARCHAR(10) UNIQUE
   - data: JSONB  -- Store all ratios as JSON
   - created_at, expires_at: TIMESTAMP
   - INDEX: (symbol)

3. market_indices_cache:
   - id: SERIAL PRIMARY KEY
   - index_name: VARCHAR(20)  -- VNINDEX, VN30, etc.
   - date: DATE
   - value: NUMERIC(12, 2)
   - change_percent: NUMERIC(6, 2)
   - created_at, expires_at: TIMESTAMP

4. stock_listing:
   - symbol: VARCHAR(10) PRIMARY KEY
   - company_name: VARCHAR(255)
   - exchange: VARCHAR(20)
   - industry: VARCHAR(100)
   - sector: VARCHAR(100)
   - updated_at: TIMESTAMP

Tips:
- Use JSONB for flexible data (fundamentals, news)
- Use partitioning for price data (by month/year)
- Create indexes on frequently queried fields
- Use materialized views for aggregations
        """

        print(schema)

    def run_full_analysis(self):
        """Chạy phân tích đầy đủ"""
        print("""
╔══════════════════════════════════════════════════════════════════╗
║          VNSTOCK DATA ANALYSIS FOR DATABASE DESIGN               ║
║                                                                  ║
║  Phân tích cấu trúc dữ liệu từ vnstock API để thiết kế schema   ║
╚══════════════════════════════════════════════════════════════════╝
        """)

        # Run analyses
        self.analyze_stock_price("VNM")
        self.analyze_stock_info("VNM")
        self.analyze_fundamentals("VNM")
        self.analyze_market_indices()
        self.analyze_sector_data()
        self.analyze_listing_data()
        self.estimate_storage_requirements()
        self.generate_schema_suggestions()

        print("\n[OK] Analysis complete!")
        print("\nNext steps:")
        print("1. Review the data structures above")
        print("2. Update database models in app/models/")
        print("3. Create Alembic migration")
        print("4. Implement caching layer")
        print("5. Test with real data")


def main():
    analyzer = VnstockDataAnalyzer()

    try:
        analyzer.run_full_analysis()
    except KeyboardInterrupt:
        print("\n\n[WARNING] Analysis interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
