"""
Test script để kiểm tra API endpoints và dữ liệu từ vnstock
Chạy: python test_manual.py
"""

import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"


class APITester:
    def __init__(self):
        self.base_url = BASE_URL
        self.token = None

    def print_section(self, title):
        print("\n" + "=" * 60)
        print(f"  {title}")
        print("=" * 60)

    def print_response(self, response):
        """Pretty print response"""
        print(f"Status Code: {response.status_code}")
        try:
            data = response.json()
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except:
            print(response.text)

    # ========== Basic Endpoints ==========

    def test_health(self):
        """Test health check endpoint"""
        self.print_section("Test 1: Health Check")
        response = requests.get(f"{self.base_url}/health")
        self.print_response(response)
        return response.status_code == 200

    def test_root(self):
        """Test root endpoint"""
        self.print_section("Test 2: Root Endpoint")
        response = requests.get(f"{self.base_url}/")
        self.print_response(response)
        return response.status_code == 200

    def test_api_info(self):
        """Test API info endpoint"""
        self.print_section("Test 3: API Info")
        response = requests.get(f"{self.base_url}/api/v1/info")
        self.print_response(response)
        return response.status_code == 200

    # ========== Authentication ==========

    def test_register(
        self, username="testuser", email="test@example.com", password="Test@123"
    ):
        """Test user registration"""
        self.print_section("Test 4: User Registration")

        payload = {
            "username": username,
            "email": email,
            "password": password,
            "full_name": "Test User",
        }

        response = requests.post(f"{self.base_url}/api/v1/auth/register", json=payload)
        self.print_response(response)
        return response.status_code in [200, 201]

    def test_login(self, username="testuser", password="Test@123"):
        """Test user login and get token"""
        self.print_section("Test 5: User Login")

        payload = {"username": username, "password": password}

        response = requests.post(f"{self.base_url}/api/v1/auth/login", json=payload)
        self.print_response(response)

        if response.status_code == 200:
            data = response.json()
            self.token = data.get("access_token")
            print(f"\n[OK] Token saved: {self.token[:20]}...")
            return True
        return False

    def get_headers(self):
        """Get authorization headers"""
        if not self.token:
            print("[ERROR] No token available. Please login first.")
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    # ========== Market Data Endpoints ==========

    def test_market_indices(self):
        """Test market indices endpoint"""
        self.print_section("Test 6: Market Indices")

        headers = self.get_headers()
        if not headers:
            return False

        response = requests.get(
            f"{self.base_url}/api/v1/market/indices", headers=headers
        )
        self.print_response(response)
        return response.status_code == 200

    def test_market_overview(self):
        """Test market overview endpoint"""
        self.print_section("Test 7: Market Overview")

        headers = self.get_headers()
        if not headers:
            return False

        response = requests.get(
            f"{self.base_url}/api/v1/market/overview", headers=headers
        )
        self.print_response(response)
        return response.status_code == 200

    def test_sector_performance(self):
        """Test sector performance endpoint"""
        self.print_section("Test 8: Sector Performance")

        headers = self.get_headers()
        if not headers:
            return False

        response = requests.get(
            f"{self.base_url}/api/v1/market/sectors", headers=headers
        )
        self.print_response(response)
        return response.status_code == 200

    def test_stock_price(self, symbol="VNM"):
        """Test stock price endpoint"""
        self.print_section(f"Test 9: Stock Price - {symbol}")

        headers = self.get_headers()
        if not headers:
            return False

        # Get last 3 months of data
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        params = {"start_date": start_date, "end_date": end_date}

        response = requests.get(
            f"{self.base_url}/api/v1/market/stock/{symbol}/price",
            headers=headers,
            params=params,
        )
        self.print_response(response)
        return response.status_code == 200

    def test_stock_info(self, symbol="VNM"):
        """Test stock info endpoint"""
        self.print_section(f"Test 10: Stock Info - {symbol}")

        headers = self.get_headers()
        if not headers:
            return False

        response = requests.get(
            f"{self.base_url}/api/v1/market/stock/{symbol}/info", headers=headers
        )
        self.print_response(response)
        return response.status_code == 200

    def test_stock_fundamentals(self, symbol="VNM"):
        """Test stock fundamentals endpoint"""
        self.print_section(f"Test 11: Stock Fundamentals - {symbol}")

        headers = self.get_headers()
        if not headers:
            return False

        response = requests.get(
            f"{self.base_url}/api/v1/market/stock/{symbol}/fundamentals",
            headers=headers,
        )
        self.print_response(response)
        return response.status_code == 200

    def test_search_stocks(self, query="vinamilk"):
        """Test stock search endpoint"""
        self.print_section(f"Test 12: Search Stocks - '{query}'")

        headers = self.get_headers()
        if not headers:
            return False

        params = {"query": query, "limit": 5}

        response = requests.get(
            f"{self.base_url}/api/v1/market/search", headers=headers, params=params
        )
        self.print_response(response)
        return response.status_code == 200

    def test_market_news(self):
        """Test market news endpoint"""
        self.print_section("Test 13: Market News")

        headers = self.get_headers()
        if not headers:
            return False

        params = {"limit": 5}

        response = requests.get(
            f"{self.base_url}/api/v1/market/news", headers=headers, params=params
        )
        self.print_response(response)
        return response.status_code == 200

    # ========== Run All Tests ==========

    def run_all_tests(self):
        """Run all tests in sequence"""
        print("\n" + "=" * 60)
        print("  STARTING API TESTS")
        print("=" * 60)

        results = {}

        # Basic tests (no auth required)
        results["health"] = self.test_health()
        results["root"] = self.test_root()
        results["api_info"] = self.test_api_info()

        # Authentication
        # Try to register (may fail if user exists)
        self.test_register()

        # Login (required for other tests)
        if not self.test_login():
            print("\n[ERROR] Login failed. Cannot proceed with authenticated tests.")
            return results

        # Market data tests (require auth)
        results["market_indices"] = self.test_market_indices()
        results["market_overview"] = self.test_market_overview()
        results["sector_performance"] = self.test_sector_performance()
        results["stock_price"] = self.test_stock_price("VNM")
        results["stock_info"] = self.test_stock_info("VNM")
        results["stock_fundamentals"] = self.test_stock_fundamentals("VNM")
        results["search_stocks"] = self.test_search_stocks("vinamilk")
        results["market_news"] = self.test_market_news()

        # Print summary
        self.print_summary(results)

        return results

    def print_summary(self, results):
        """Print test results summary"""
        print("\n" + "=" * 60)
        print("  TEST SUMMARY")
        print("=" * 60)

        total = len(results)
        passed = sum(1 for v in results.values() if v)
        failed = total - passed

        for test_name, passed in results.items():
            status = "[PASS]" if passed else "[FAIL]"
            print(f"{status}  {test_name}")

        print("-" * 60)
        print(f"Total: {total} | Passed: {passed} | Failed: {failed}")
        print("=" * 60)


def main():
    """Main test function"""
    tester = APITester()

    print("""
    ╔════════════════════════════════════════════════════════╗
    ║     Portfolio API - Manual Testing Script             ║
    ║                                                        ║
    ║  Đảm bảo server đang chạy tại http://localhost:8000   ║
    ║  Chạy: uvicorn app.main:app --reload                   ║
    ╚════════════════════════════════════════════════════════╝
    """)

    input("Press Enter to start testing...")

    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n[WARNING] Tests interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Error during testing: {e}")


if __name__ == "__main__":
    main()
