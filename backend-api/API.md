# API Documentation

**Base URL:** `http://localhost:8000/api/v1`
**Auth:** `Authorization: Bearer <access_token>` (tất cả endpoint trừ những endpoint đánh dấu ❌)
**Content-Type:** `application/json`
**Docs:** `http://localhost:8000/docs` (Swagger) | `http://localhost:8000/redoc`

---

## Mục lục

- [Root & Health](#root--health)
- [Authentication](#authentication)
- [Portfolio Management](#portfolio-management)
- [Market Data](#market-data)
- [Optimization](#optimization)
- [Chatbot](#chatbot)
- [Testing Guide](#testing-guide)
- [Deployment](#deployment)

---

## Root & Health

| Method | Path | Auth | Mô tả |
|--------|------|------|-------|
| GET | `/` | ❌ | Thông tin app |
| GET | `/health` | ❌ | Health check |
| GET | `/api/v1/info` | ❌ | API version info |

**GET `/health`**
```json
{ "status": "healthy" }
```

---

## Authentication

**Prefix:** `/api/v1/auth`

| Method | Path | Auth | Mô tả |
|--------|------|------|-------|
| POST | `/register` | ❌ | Đăng ký tài khoản |
| POST | `/login` | ❌ | Đăng nhập (OAuth2) |
| POST | `/refresh` | ❌ | Làm mới access token |
| POST | `/logout` | ✅ | Đăng xuất |
| GET | `/me` | ✅ | Thông tin user hiện tại |

### POST `/register`
```json
// Request
{
  "username": "testuser",       // 3-50 ký tự, unique
  "email": "test@example.com",
  "password": "password123",    // tối thiểu 8 ký tự
  "full_name": "Test User"      // optional
}

// Response
{
  "access_token": "eyJhbGc...",
  "refresh_token": "eyJhbGc...",
  "token_type": "bearer"
}
```

### POST `/login`
```json
// Request (form data hoặc JSON)
{ "username": "testuser", "password": "password123" }

// Response
{
  "access_token": "eyJhbGc...",
  "refresh_token": "eyJhbGc...",
  "token_type": "bearer"
}
```

### POST `/refresh`
```json
// Request
{ "refresh_token": "eyJhbGc..." }

// Response
{ "access_token": "eyJhbGc...", "refresh_token": "eyJhbGc...", "token_type": "bearer" }
```

### POST `/logout`
```json
// Request
{ "refresh_token": "eyJhbGc..." }

// Response
{ "message": "Successfully logged out" }
```

### GET `/me`
```json
{
  "id": 1,
  "username": "testuser",
  "email": "test@example.com",
  "full_name": "Test User",
  "is_active": true,
  "created_at": "2024-01-01T00:00:00Z"
}
```

---

## Portfolio Management

**Prefix:** `/api/v1/portfolios` | Tất cả endpoint yêu cầu auth ✅

### Portfolios

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `` | Tạo portfolio mới |
| GET | `` | Danh sách portfolios (`?skip=0&limit=100`) |
| GET | `/{portfolio_id}` | Chi tiết portfolio |
| PUT | `/{portfolio_id}` | Cập nhật portfolio |
| DELETE | `/{portfolio_id}` | Xóa portfolio |

**POST `/portfolios`**
```json
// Request
{
  "name": "Danh mục dài hạn",
  "description": "Mô tả...",     // optional
  "initial_capital": 100000000
}

// Response
{
  "id": 1,
  "user_id": 1,
  "name": "Danh mục dài hạn",
  "description": "Mô tả...",
  "initial_capital": 100000000,
  "current_value": 100000000,
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z"
}
```

### Portfolio Stocks

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/{portfolio_id}/stocks` | Thêm cổ phiếu |
| GET | `/{portfolio_id}/stocks` | Danh sách cổ phiếu trong portfolio |
| PUT | `/{portfolio_id}/stocks/{stock_id}` | Cập nhật vị thế |
| DELETE | `/{portfolio_id}/stocks/{stock_id}` | Xóa cổ phiếu |

**POST `/{portfolio_id}/stocks`**
```json
// Request
{
  "symbol": "VCB",
  "quantity": 100,
  "purchase_price": 85000
}

// Response
{
  "id": 1,
  "portfolio_id": 1,
  "symbol": "VCB",
  "quantity": 100,
  "purchase_price": 85000,
  "added_at": "2024-01-01T00:00:00Z"
}
```

### Watchlists

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/watchlists` | Tạo watchlist |
| GET | `/watchlists` | Danh sách watchlists (`?skip=0&limit=100`) |
| GET | `/watchlists/{watchlist_id}` | Chi tiết watchlist |
| PUT | `/watchlists/{watchlist_id}` | Cập nhật watchlist |
| DELETE | `/watchlists/{watchlist_id}` | Xóa watchlist |
| POST | `/watchlists/{watchlist_id}/stocks` | Thêm cổ phiếu vào watchlist |
| DELETE | `/watchlists/{watchlist_id}/stocks/{symbol}` | Xóa cổ phiếu khỏi watchlist |

**POST `/watchlists`**
```json
// Request
{ "name": "Cổ phiếu theo dõi", "description": "..." }

// Response
{
  "id": 1,
  "user_id": 1,
  "name": "Cổ phiếu theo dõi",
  "created_at": "2024-01-01T00:00:00Z",
  "stocks": []
}
```

---

## Market Data

**Prefix:** `/api/v1/market` | Tất cả endpoint yêu cầu auth ✅

| Method | Path | Mô tả |
|--------|------|-------|
| GET | `/indices` | Chỉ số VN-Index, VN30, HNX, UPCOM |
| GET | `/overview` | Tổng quan thị trường + top movers |
| GET | `/sectors` | Hiệu suất theo ngành |
| GET | `/stock/{symbol}/price` | Lịch sử giá (cache 1h) |
| GET | `/stock/{symbol}/info` | Thông tin công ty |
| GET | `/stock/{symbol}/fundamentals` | P/E, P/B, ROE, ROA... (cache 24h) |
| GET | `/news` | Tin tức thị trường |
| GET | `/news/{symbol}` | Tin tức theo mã cổ phiếu |
| GET | `/search` | Tìm kiếm cổ phiếu |
| POST | `/cache/clear` | Xóa cache dữ liệu |

### GET `/indices`
```json
{
  "vnindex": { "name": "VN-Index", "value": 1234.56, "change": 12.34, "change_percent": 1.01, "volume": 500000000 },
  "vn30": { ... },
  "hnx": { ... },
  "upcom": { ... },
  "timestamp": "2024-12-31T15:00:00Z"
}
```

### GET `/overview`
```json
{
  "indices": { ... },
  "top_gainers": [{ "symbol": "VCB", "price": 85000, "change_percent": 5.5 }],
  "top_losers": [...],
  "top_volume": [...],
  "market_stats": {
    "total_volume": 500000000,
    "total_value": 10000000000000,
    "advancing": 250,
    "declining": 150,
    "unchanged": 50
  }
}
```

### GET `/stock/{symbol}/price`

**Query params:** `start_date` (YYYY-MM-DD, optional), `end_date` (YYYY-MM-DD, optional)
```json
{
  "symbol": "VNM",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "data": {
    "2024-01-01": { "open": 85000, "high": 86000, "low": 84000, "close": 85500, "volume": 1000000 }
  },
  "cached": true
}
```

### GET `/stock/{symbol}/fundamentals`
```json
{
  "symbol": "VNM",
  "pe": 15.5,
  "pb": 3.2,
  "roe": 0.25,
  "roa": 0.15,
  "eps": 5500,
  "debt_to_equity": 0.3
}
```

### GET `/news`

**Query params:** `limit` (1-100, default 20), `category` (optional)
```json
[
  {
    "title": "Thị trường tăng điểm mạnh",
    "summary": "...",
    "url": "https://...",
    "source": "VnExpress",
    "published_at": "2024-12-31T17:00:00Z",
    "symbols": ["VCB", "CTG"],
    "category": "market"
  }
]
```

### GET `/search`

**Query params:** `query` (required), `limit` (1-50, default 10)

**Ví dụ:** `/search?query=vinamilk&limit=5`
```json
[{ "symbol": "VNM", "name": "Vinamilk", "exchange": "HOSE", "industry": "Food & Beverage" }]
```

### POST `/cache/clear`

**Query params:** `cache_type` (optional: `price` | `fundamentals` | bỏ trống = clear all)
```json
{ "message": "Cache cleared successfully", "cache_type": "all" }
```

> ⚠️ Lưu ý: endpoint này chưa có phân quyền admin, bất kỳ user đã đăng nhập nào cũng có thể gọi.

---

## Optimization

**Prefix:** `/api/v1/optimize` | Tất cả endpoint yêu cầu auth ✅

### Optimization Runs

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/run` | Chạy tối ưu hoá danh mục |
| GET | `/runs` | Lịch sử runs (`?portfolio_id=&model_type=&skip=0&limit=50`) |
| GET | `/runs/{run_id}` | Chi tiết run |
| DELETE | `/runs/{run_id}` | Xóa run |
| GET | `/models` | Danh sách mô hình hỗ trợ |

### POST `/run`
```json
// Request
{
  "symbols": ["VCB", "FPT", "VNM", "HPG", "GAS"],  // 2-50 mã
  "model": "Max_Sharpe",
  "start_date": "2023-01-01",   // optional, mặc định 1 năm trước
  "end_date": "2024-12-31",     // optional, mặc định hôm nay
  "portfolio_id": 1,            // optional
  "constraints": {}             // optional
}

// Response
{
  "optimization_id": 1,
  "model": "Max_Sharpe",
  "symbols": ["VCB", "FPT", "VNM", "HPG", "GAS"],
  "weights": { "VCB": 0.25, "FPT": 0.20, "VNM": 0.20, "HPG": 0.20, "GAS": 0.15 },
  "metrics": {
    "expected_return": 0.18,
    "expected_volatility": 0.12,
    "sharpe_ratio": 1.25,
    "cvar": -0.05,
    "max_drawdown": -0.08
  },
  "efficient_frontier": {},
  "created_at": "2024-12-31T10:00:00Z"
}
```

**Các mô hình hỗ trợ:**

| Model | Mô tả |
|-------|-------|
| `Markowitz` | Mean-Variance Optimization |
| `Max_Sharpe` | Tối đa hoá Sharpe Ratio |
| `Min_Volatility` | Tối thiểu hoá rủi ro |
| `HRP` | Hierarchical Risk Parity |
| `Min_CVaR` | Minimum Conditional Value at Risk |
| `Min_CDaR` | Minimum Conditional Drawdown at Risk |

### Backtests

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/backtest` | Chạy backtest |
| GET | `/backtests` | Lịch sử backtests (`?optimization_id=&skip=0&limit=50`) |
| GET | `/backtests/{backtest_id}` | Chi tiết backtest |
| DELETE | `/backtests/{backtest_id}` | Xóa backtest |

### POST `/backtest`
```json
// Request
{
  "optimization_id": 1,           // optional nếu cung cấp weights + symbols
  "weights": { "VCB": 0.5, "FPT": 0.5 },  // required nếu không có optimization_id
  "symbols": ["VCB", "FPT"],      // required nếu không có optimization_id
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "initial_capital": 100000000,
  "rebalance_frequency": "monthly"  // daily | weekly | monthly
}

// Response
{
  "backtest_id": 1,
  "optimization_id": 1,
  "symbols": ["VCB", "FPT"],
  "weights": { "VCB": 0.5, "FPT": 0.5 },
  "performance": {
    "initial_capital": 100000000,
    "final_value": 118000000,
    "total_return": 0.18,
    "annualized_return": 0.18,
    "volatility": 0.12,
    "sharpe_ratio": 1.25,
    "max_drawdown": -0.08,
    "win_rate": 0.62
  },
  "equity_curve": {},
  "drawdown_curve": {},
  "created_at": "2024-12-31T11:00:00Z"
}
```

---

## Chatbot

**Prefix:** `/api/v1/chat` | Tất cả endpoint yêu cầu auth ✅
**Powered by:** Google Gemini API

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/message` | Gửi tin nhắn tới AI |
| GET | `/conversations` | Lịch sử hội thoại (`?skip=0&limit=50`) |
| GET | `/conversations/{conversation_id}` | Chi tiết hội thoại |
| DELETE | `/conversations/{conversation_id}` | Xóa hội thoại |
| POST | `/feedback` | Đánh giá response |
| GET | `/suggestions` | Gợi ý câu hỏi |

### POST `/message`
```json
// Request
{
  "message": "Hôm nay cổ phiếu nào tăng mạnh nhất?",
  "conversation_id": "abc123",        // optional - tiếp tục hội thoại cũ
  "context": {},                      // optional
  "include_portfolio_context": false  // true = đưa data portfolio vào context
}

// Response
{
  "message": "Dựa trên dữ liệu thị trường hôm nay...",
  "conversation_id": "abc123",
  "sources": [],
  "suggested_actions": ["Xem chi tiết VCB", "Chạy optimization"],
  "timestamp": "2024-12-31T10:00:00Z"
}
```

### POST `/feedback`

**Query params:** `conversation_id`, `message_id`, `rating` (1-5), `feedback` (optional text)

> ⚠️ Lưu ý: endpoint này dùng query params thay vì request body — không nhất quán với các endpoint khác.

```json
{ "message": "Feedback submitted", "conversation_id": "abc123", "message_id": "msg1" }
```

### GET `/suggestions`
```json
{
  "suggestions": [
    { "category": "Market", "questions": ["Tổng quan thị trường hôm nay?", "Cổ phiếu nào đang tăng mạnh?"] },
    { "category": "Portfolio", "questions": ["Phân tích danh mục của tôi", "Gợi ý tái cân bằng danh mục"] },
    { "category": "Optimization", "questions": ["Tối ưu danh mục với VCB, FPT, VNM", "So sánh các mô hình tối ưu"] }
  ]
}
```

---

## Testing Guide

### Chạy test thủ công

```bash
# Khởi động server
python run.py

# Mở terminal khác
python test_manual.py
```

`test_manual.py` test 13 endpoints: health, root, info, register, login, market indices, overview, sectors, stock price, stock info, fundamentals, search, news.

### cURL examples

```bash
# Đăng ký
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "test", "email": "test@test.com", "password": "test1234", "full_name": "Test"}'

# Đăng nhập và lưu token
TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "test", "password": "test1234"}' | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Lấy chỉ số thị trường
curl http://localhost:8000/api/v1/market/indices \
  -H "Authorization: Bearer $TOKEN"

# Tối ưu danh mục
curl -X POST http://localhost:8000/api/v1/optimize/run \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["VCB", "FPT", "VNM"], "model": "Max_Sharpe", "start_date": "2023-01-01", "end_date": "2024-12-31"}'
```

### Python requests

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"

# Đăng nhập
res = requests.post(f"{BASE_URL}/auth/login", json={"username": "test", "password": "test1234"})
token = res.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Tối ưu danh mục
res = requests.post(f"{BASE_URL}/optimize/run", headers=headers, json={
    "symbols": ["VCB", "FPT", "VNM", "HPG"],
    "model": "Max_Sharpe",
    "start_date": "2023-01-01",
    "end_date": "2024-12-31"
})
print(res.json())
```

### Pytest

```bash
pip install pytest pytest-asyncio httpx

pytest tests/ -v
pytest tests/ --cov=app --cov-report=html
```

---

## Deployment

### Railway

```bash
npm install -g @railway/cli
railway login && railway init
railway add --plugin postgresql
railway variables set SECRET_KEY=... GEMINI_API_KEY=...
railway up
railway run alembic upgrade head
```

### Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Thêm PostgreSQL database, copy `DATABASE_URL` vào env vars
- Sau deploy: vào Render Shell chạy `alembic upgrade head`

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: portfolio_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data

  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://postgres:password@db:5432/portfolio_db
      SECRET_KEY: your-secret-key
      GEMINI_API_KEY: your-api-key
    depends_on:
      - db

volumes:
  postgres_data:
```

```bash
docker-compose up -d
docker-compose exec api alembic upgrade head
docker-compose logs -f api
```

---

## Production Checklist

- [ ] Đổi `SECRET_KEY` thành giá trị ngẫu nhiên mạnh (`openssl rand -hex 32`)
- [ ] Bật HTTPS
- [ ] Giới hạn CORS origins cụ thể trong `.env`
- [ ] Thêm rate limiting
- [ ] Phân quyền admin cho `POST /market/cache/clear`
- [ ] Bật Redis caching (cấu hình `REDIS_URL`)
- [ ] Thiết lập backup database định kỳ
- [ ] Cấu hình monitoring (Sentry, logging)
