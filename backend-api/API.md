# API Documentation & Testing Guide

Hướng dẫn chi tiết về API endpoints, testing và deployment.

## Mục lục

- [API Overview](#api-overview)
- [Authentication](#authentication)
- [Market Data API](#market-data-api)
- [Portfolio API](#portfolio-api)
- [Optimization API](#optimization-api)
- [Chatbot API](#chatbot-api)
- [Testing Guide](#testing-guide)
- [Deployment](#deployment)
- [Supabase Setup](#supabase-setup)

---

## API Overview

**Base URL:** `http://localhost:8000/api/v1`

**Authentication:** Bearer token (JWT)

**Content-Type:** `application/json`

**Auto-generated docs:**
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## Authentication

### Register User

**Endpoint:** `POST /api/v1/auth/register`

**Request:**
```json
{
  "username": "testuser",
  "email": "test@example.com",
  "password": "password123",
  "full_name": "Test User"
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

**cURL:**
```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "password123",
    "full_name": "Test User"
  }'
```

### Login

**Endpoint:** `POST /api/v1/auth/login`

**Request:**
```json
{
  "username": "testuser",
  "password": "password123"
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### Refresh Token

**Endpoint:** `POST /api/v1/auth/refresh`

**Request:**
```json
{
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### Get Current User

**Endpoint:** `GET /api/v1/auth/me`

**Headers:** `Authorization: Bearer {access_token}`

**Response:**
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

### Logout

**Endpoint:** `POST /api/v1/auth/logout`

**Headers:** `Authorization: Bearer {access_token}`

**Response:**
```json
{
  "message": "Successfully logged out"
}
```

---

## Market Data API

All market endpoints require authentication.

### Get Market Indices

**Endpoint:** `GET /api/v1/market/indices`

**Headers:** `Authorization: Bearer {access_token}`

**Response:**
```json
{
  "vnindex": {
    "value": 1234.56,
    "change": 12.34,
    "change_percent": 1.01,
    "volume": 500000000,
    "time": "2024-12-31T15:00:00Z"
  },
  "vn30": {...},
  "hnx": {...},
  "upcom": {...}
}
```

### Get Market Overview

**Endpoint:** `GET /api/v1/market/overview`

**Response:**
```json
{
  "indices": {...},
  "top_gainers": [
    {"symbol": "VCB", "price": 85000, "change_percent": 5.5},
    ...
  ],
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

### Get Sector Performance

**Endpoint:** `GET /api/v1/market/sectors`

**Response:**
```json
{
  "sectors": [
    {
      "name": "Banking",
      "change_percent": 2.5,
      "volume": 100000000,
      "top_stocks": ["VCB", "CTG", "BID"]
    },
    ...
  ]
}
```

### Get Stock Price

**Endpoint:** `GET /api/v1/market/stock/{symbol}/price`

**Query Parameters:**
- `start_date` (required): YYYY-MM-DD
- `end_date` (required): YYYY-MM-DD

**Example:** `GET /api/v1/market/stock/VNM/price?start_date=2024-01-01&end_date=2024-12-31`

**Response:**
```json
{
  "symbol": "VNM",
  "data": [
    {
      "date": "2024-01-01",
      "open": 85000,
      "high": 86000,
      "low": 84000,
      "close": 85500,
      "volume": 1000000
    },
    ...
  ],
  "from_cache": true,
  "cached_at": "2024-12-31T10:00:00Z"
}
```

### Get Stock Info

**Endpoint:** `GET /api/v1/market/stock/{symbol}/info`

**Response:**
```json
{
  "symbol": "VNM",
  "name": "Vinamilk",
  "exchange": "HOSE",
  "industry": "Food & Beverage",
  "current_price": 85000,
  "market_cap": 100000000000000,
  "shares_outstanding": 1176000000,
  "from_cache": true
}
```

### Get Stock Fundamentals

**Endpoint:** `GET /api/v1/market/stock/{symbol}/fundamentals`

**Response:**
```json
{
  "symbol": "VNM",
  "pe": 15.5,
  "pb": 3.2,
  "roe": 0.25,
  "roa": 0.15,
  "eps": 5500,
  "bvps": 26500,
  "debt_to_equity": 0.3,
  "from_cache": true,
  "cached_at": "2024-12-31T00:00:00Z"
}
```

### Get Market News

**Endpoint:** `GET /api/v1/market/news`

**Query Parameters:**
- `limit` (optional, default: 10): Number of articles
- `offset` (optional, default: 0): Pagination offset

**Response:**
```json
{
  "articles": [
    {
      "title": "Market closes higher on strong banking sector",
      "description": "VN-Index gained 1.5% today...",
      "url": "https://...",
      "published_at": "2024-12-31T17:00:00Z",
      "source": "VnExpress"
    },
    ...
  ],
  "total": 100,
  "limit": 10,
  "offset": 0
}
```

### Search Stocks

**Endpoint:** `GET /api/v1/market/search`

**Query Parameters:**
- `query` (required): Search term (symbol or name)
- `limit` (optional, default: 10): Max results

**Example:** `GET /api/v1/market/search?query=vinamilk&limit=5`

**Response:**
```json
{
  "results": [
    {
      "symbol": "VNM",
      "name": "Vinamilk",
      "exchange": "HOSE",
      "industry": "Food & Beverage"
    }
  ],
  "total": 1
}
```

### Clear Market Cache

**Endpoint:** `POST /api/v1/market/cache/clear`

**Headers:** `Authorization: Bearer {access_token}` (admin only)

**Response:**
```json
{
  "message": "Cache cleared successfully",
  "rows_deleted": 150
}
```

---

## Portfolio API

### List Portfolios

**Endpoint:** `GET /api/v1/portfolios`

**Query Parameters:**
- `skip` (optional, default: 0): Pagination offset
- `limit` (optional, default: 10): Max results

**Response:**
```json
{
  "portfolios": [
    {
      "id": 1,
      "name": "My Portfolio",
      "description": "Long-term investment",
      "total_investment": 100000000,
      "created_at": "2024-01-01T00:00:00Z",
      "stocks_count": 5
    },
    ...
  ],
  "total": 3
}
```

### Create Portfolio

**Endpoint:** `POST /api/v1/portfolios`

**Request:**
```json
{
  "name": "My New Portfolio",
  "description": "Diversified portfolio",
  "total_investment": 50000000
}
```

**Response:**
```json
{
  "id": 2,
  "name": "My New Portfolio",
  "description": "Diversified portfolio",
  "total_investment": 50000000,
  "created_at": "2024-12-31T10:00:00Z"
}
```

### Get Portfolio Details

**Endpoint:** `GET /api/v1/portfolios/{id}`

**Response:**
```json
{
  "id": 1,
  "name": "My Portfolio",
  "description": "Long-term investment",
  "total_investment": 100000000,
  "created_at": "2024-01-01T00:00:00Z",
  "stocks": [
    {
      "id": 1,
      "symbol": "VCB",
      "shares": 100,
      "average_price": 85000,
      "added_at": "2024-01-01T00:00:00Z"
    },
    ...
  ]
}
```

### Update Portfolio

**Endpoint:** `PUT /api/v1/portfolios/{id}`

**Request:**
```json
{
  "name": "Updated Name",
  "description": "Updated description"
}
```

### Delete Portfolio

**Endpoint:** `DELETE /api/v1/portfolios/{id}`

**Response:**
```json
{
  "message": "Portfolio deleted successfully"
}
```

### Add Stock to Portfolio

**Endpoint:** `POST /api/v1/portfolios/{id}/stocks`

**Request:**
```json
{
  "symbol": "VCB",
  "shares": 100,
  "average_price": 85000
}
```

**Response:**
```json
{
  "id": 1,
  "portfolio_id": 1,
  "symbol": "VCB",
  "shares": 100,
  "average_price": 85000,
  "added_at": "2024-12-31T10:00:00Z"
}
```

### Remove Stock from Portfolio

**Endpoint:** `DELETE /api/v1/portfolios/{id}/stocks/{stock_id}`

**Response:**
```json
{
  "message": "Stock removed successfully"
}
```

### Watchlist Endpoints

Similar structure to portfolios:
- `GET /api/v1/portfolios/watchlists` - List watchlists
- `POST /api/v1/portfolios/watchlists` - Create watchlist
- `GET /api/v1/portfolios/watchlists/{id}` - Get watchlist
- `PUT /api/v1/portfolios/watchlists/{id}` - Update watchlist
- `DELETE /api/v1/portfolios/watchlists/{id}` - Delete watchlist
- `POST /api/v1/portfolios/watchlists/{id}/stocks` - Add to watchlist
- `DELETE /api/v1/portfolios/watchlists/{id}/stocks/{symbol}` - Remove from watchlist

---

## Optimization API

### Run Optimization

**Endpoint:** `POST /api/v1/optimize/run`

**Request:**
```json
{
  "symbols": ["VCB", "FPT", "VNM", "HPG", "GAS"],
  "method": "max_sharpe",
  "total_investment": 100000000,
  "start_date": "2023-01-01",
  "end_date": "2024-12-31",
  "risk_free_rate": 0.03
}
```

**Available methods:**
- `markowitz` - Mean-Variance Optimization
- `max_sharpe` - Maximum Sharpe Ratio
- `min_volatility` - Minimum Volatility
- `hrp` - Hierarchical Risk Parity
- `min_cvar` - Minimum CVaR
- `min_cdar` - Minimum CDaR

**Response:**
```json
{
  "id": 1,
  "symbols": ["VCB", "FPT", "VNM", "HPG", "GAS"],
  "method": "max_sharpe",
  "weights": {
    "VCB": 0.25,
    "FPT": 0.20,
    "VNM": 0.20,
    "HPG": 0.20,
    "GAS": 0.15
  },
  "allocation": {
    "VCB": 25000000,
    "FPT": 20000000,
    "VNM": 20000000,
    "HPG": 20000000,
    "GAS": 15000000
  },
  "expected_return": 0.18,
  "volatility": 0.12,
  "sharpe_ratio": 1.25,
  "status": "completed",
  "created_at": "2024-12-31T10:00:00Z"
}
```

### Get Optimization History

**Endpoint:** `GET /api/v1/optimize/runs`

**Query Parameters:**
- `skip` (optional): Pagination offset
- `limit` (optional): Max results

**Response:**
```json
{
  "runs": [
    {
      "id": 1,
      "method": "max_sharpe",
      "symbols": ["VCB", "FPT", "VNM"],
      "sharpe_ratio": 1.25,
      "created_at": "2024-12-31T10:00:00Z"
    },
    ...
  ],
  "total": 10
}
```

### Get Optimization Details

**Endpoint:** `GET /api/v1/optimize/runs/{id}`

**Response:** Same as run optimization response

### Delete Optimization Run

**Endpoint:** `DELETE /api/v1/optimize/runs/{id}`

### Run Backtest

**Endpoint:** `POST /api/v1/optimize/backtest`

**Request:**
```json
{
  "optimization_run_id": 1,
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "initial_investment": 100000000,
  "rebalance_frequency": "monthly"
}
```

**Response:**
```json
{
  "id": 1,
  "optimization_run_id": 1,
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "initial_investment": 100000000,
  "final_value": 118000000,
  "total_return": 0.18,
  "annual_return": 0.18,
  "volatility": 0.12,
  "sharpe_ratio": 1.25,
  "max_drawdown": -0.08,
  "portfolio_value_history": {
    "2024-01-01": 100000000,
    "2024-02-01": 102000000,
    ...
  },
  "created_at": "2024-12-31T11:00:00Z"
}
```

### Get Available Models

**Endpoint:** `GET /api/v1/optimize/models`

**Response:**
```json
{
  "models": [
    {
      "name": "max_sharpe",
      "display_name": "Maximum Sharpe Ratio",
      "description": "Maximizes risk-adjusted returns"
    },
    {
      "name": "min_volatility",
      "display_name": "Minimum Volatility",
      "description": "Minimizes portfolio risk"
    },
    ...
  ]
}
```

---

## Chatbot API

### Send Message

**Endpoint:** `POST /api/v1/chat/message`

**Request:**
```json
{
  "message": "What are the top performing stocks today?",
  "context": {
    "portfolio_id": 1,
    "include_market_data": true
  }
}
```

**Response:**
```json
{
  "response": "Based on today's market data, the top performing stocks are...",
  "conversation_id": "abc123",
  "timestamp": "2024-12-31T10:00:00Z",
  "metadata": {
    "model": "gemini-pro",
    "tokens_used": 150
  }
}
```

### Get Suggestions

**Endpoint:** `GET /api/v1/chat/suggestions`

**Query Parameters:**
- `portfolio_id` (optional): Get suggestions based on portfolio

**Response:**
```json
{
  "suggestions": [
    "Analyze my portfolio performance",
    "What stocks should I buy today?",
    "Compare VCB and CTG",
    "What's the market sentiment?"
  ]
}
```

### Submit Feedback

**Endpoint:** `POST /api/v1/chat/feedback`

**Request:**
```json
{
  "conversation_id": "abc123",
  "rating": 5,
  "comment": "Very helpful response"
}
```

---

## Testing Guide

### 1. Setup Test Environment

```bash
cd backend-api

# Install dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio httpx

# Configure test database
cp .env.example .env.test
# Edit .env.test with test database credentials
```

### 2. Run Manual Test Script

```bash
# Start server
python run.py

# In another terminal
python test_manual.py
```

**test_manual.py** includes 13 test scenarios:
1. Health check
2. Root endpoint
3. API info
4. User registration
5. User login
6. Market indices
7. Market overview
8. Sector performance
9. Stock price data
10. Stock info
11. Stock fundamentals
12. Stock search
13. Market news

### 3. Pytest (Unit Tests)

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_market_api.py -v

# Run with coverage
pytest --cov=app --cov-report=html

# View coverage report
open htmlcov/index.html
```

**Example test file:**
```python
# tests/test_market_api.py
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_market_indices_requires_auth():
    response = client.get("/api/v1/market/indices")
    assert response.status_code == 401

def test_market_indices_with_auth(auth_headers):
    response = client.get(
        "/api/v1/market/indices",
        headers=auth_headers
    )
    assert response.status_code == 200
    data = response.json()
    assert "vnindex" in data
```

### 4. Test with cURL

```bash
# Health check
curl http://localhost:8000/health

# Register user
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "test", "email": "test@test.com", "password": "test123", "full_name": "Test User"}'

# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "test", "password": "test123"}'

# Save token
TOKEN="your_access_token_here"

# Get market indices
curl http://localhost:8000/api/v1/market/indices \
  -H "Authorization: Bearer $TOKEN"

# Get stock price
curl "http://localhost:8000/api/v1/market/stock/VNM/price?start_date=2024-01-01&end_date=2024-12-31" \
  -H "Authorization: Bearer $TOKEN"

# Run optimization
curl -X POST http://localhost:8000/api/v1/optimize/run \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["VCB", "FPT", "VNM"],
    "method": "max_sharpe",
    "total_investment": 100000000,
    "start_date": "2023-01-01",
    "end_date": "2024-12-31"
  }'
```

### 5. Test with Python requests

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"

# Login
login_response = requests.post(
    f"{BASE_URL}/auth/login",
    json={"username": "test", "password": "test123"}
)
token = login_response.json()["access_token"]

# Set headers
headers = {"Authorization": f"Bearer {token}"}

# Get market indices
indices = requests.get(f"{BASE_URL}/market/indices", headers=headers)
print(indices.json())

# Run optimization
optimization = requests.post(
    f"{BASE_URL}/optimize/run",
    headers=headers,
    json={
        "symbols": ["VCB", "FPT", "VNM"],
        "method": "max_sharpe",
        "total_investment": 100000000,
        "start_date": "2023-01-01",
        "end_date": "2024-12-31"
    }
)
print(optimization.json())
```

---

## Deployment

### Option 1: Railway

**1. Setup:**
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Initialize project
railway init
```

**2. Configure:**
```bash
# Add PostgreSQL
railway add --plugin postgresql

# Set environment variables
railway variables set SECRET_KEY=your-secret-key
railway variables set GEMINI_API_KEY=your-api-key
```

**3. Deploy:**
```bash
railway up
```

**4. Run migrations:**
```bash
railway run alembic upgrade head
```

### Option 2: Render

**1. Create Web Service:**
- Connect GitHub repository
- Select `backend-api` directory
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

**2. Add PostgreSQL:**
- Create PostgreSQL database
- Copy internal database URL to environment variables

**3. Environment Variables:**
```
DATABASE_URL=<from Render PostgreSQL>
SECRET_KEY=<generate new>
GEMINI_API_KEY=<your key>
```

**4. Run migrations:**
```bash
# In Render shell
alembic upgrade head
```

### Option 3: Docker

**Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**docker-compose.yml:**
```yaml
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
    ports:
      - "5432:5432"

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

**Deploy:**
```bash
# Build and run
docker-compose up -d

# Run migrations
docker-compose exec api alembic upgrade head

# View logs
docker-compose logs -f api
```

---

## Supabase Setup

### 1. Create Project

1. Truy cập [supabase.com](https://supabase.com)
2. Click "New Project"
3. Điền thông tin:
   - **Name:** portfolio-optimization
   - **Password:** Tạo mật khẩu mạnh
   - **Region:** Singapore
4. Click "Create Project"

### 2. Get Credentials

Vào **Project Settings** → **API**:
```
Project URL: https://xxxxxxxxxxxxx.supabase.co
anon public key: eyJhbGc...
service_role key: eyJhbGc...
```

### 3. Configure Backend

Update `.env`:
```env
USE_SUPABASE=True
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-key
```

### 4. Create Schema

Vào **SQL Editor** và run:
```sql
-- Create tables
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create portfolios
CREATE TABLE portfolios (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    total_investment NUMERIC(15, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_portfolios_user_id ON portfolios(user_id);

-- Enable Row Level Security
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE portfolios ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY "Users can view their own data" ON users
    FOR SELECT USING (auth.uid() = id);

CREATE POLICY "Users can view their own portfolios" ON portfolios
    FOR SELECT USING (auth.uid() = user_id);
```

### 5. Test Connection

```python
from app.supabase_client import supabase

client = supabase()
result = client.table("portfolios").select("*").limit(1).execute()
print(result.data)
```

---

## Production Checklist

### Security
- [ ] Change SECRET_KEY to strong random value
- [ ] Use HTTPS only
- [ ] Enable CORS with specific origins
- [ ] Set up rate limiting
- [ ] Enable database backups
- [ ] Rotate API keys regularly

### Performance
- [ ] Enable Redis caching
- [ ] Set up connection pooling
- [ ] Configure proper indexes
- [ ] Monitor query performance
- [ ] Set up CDN for static assets

### Monitoring
- [ ] Set up error tracking (Sentry)
- [ ] Configure logging (CloudWatch/DataDog)
- [ ] Set up uptime monitoring
- [ ] Configure alerts for errors
- [ ] Monitor API response times

### Documentation
- [ ] API documentation is up-to-date
- [ ] Environment variables documented
- [ ] Deployment process documented
- [ ] Backup/restore procedures documented

---

## Support

### Common Issues

**Issue: Database connection error**
- Check DATABASE_URL in .env
- Verify PostgreSQL is running
- Check firewall/security groups

**Issue: Authentication fails**
- Verify SECRET_KEY is set
- Check token expiration settings
- Ensure user exists in database

**Issue: Market data not loading**
- Check vnstock API is accessible
- Verify cache is working
- Check network connectivity

### Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [vnstock API](https://github.com/thinh-vu/vnstock)
- [Supabase Documentation](https://supabase.com/docs)

### Contact

- Report bugs: GitHub Issues
- Feature requests: GitHub Discussions
- Email: support@example.com
