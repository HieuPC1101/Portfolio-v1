# Portfolio Optimization API

Backend API cho hệ thống tối ưu hóa danh mục đầu tư chứng khoán Việt Nam.

## Tổng quan

Hệ thống tối ưu hóa danh mục đầu tư với FastAPI, PostgreSQL và PyPortfolioOpt, tích hợp dữ liệu thị trường Việt Nam qua vnstock API.

**Tech Stack:**
- **Framework:** FastAPI 0.110+
- **Database:** PostgreSQL 15+ với SQLAlchemy ORM
- **Authentication:** JWT (python-jose) + bcrypt
- **Market Data:** vnstock 2.0 (Vietnamese stock market)
- **Optimization:** PyPortfolioOpt 1.5+
- **AI Chatbot:** Google Gemini AI

**Tính năng:**
- 6 mô hình tối ưu hóa (Markowitz, Max Sharpe, Min Volatility, HRP, CVaR, CDaR)
- Quản lý danh mục đầu tư và watchlist
- Backtesting với rebalancing
- AI chatbot hỗ trợ đầu tư
- Market data caching (1-24 giờ)
- Real-time market indices

## Quick Start

### 1. Cài đặt Dependencies

```bash
cd backend-api
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

# Install packages
pip install -r requirements.txt
```

### 2. Cấu hình môi trường

```bash
# Copy template
cp .env.example .env
```

Chỉnh sửa `.env`:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/portfolio_db

# JWT Secret (generate: openssl rand -hex 32)
SECRET_KEY=your-secret-key-here
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Gemini API
GEMINI_API_KEY=your-gemini-api-key

# Environment
DEBUG=True
ENVIRONMENT=development
```

### 3. Setup Database

**Option A: Local PostgreSQL**
```bash
# Install PostgreSQL 15+
# Create database
createdb portfolio_db

# Run migrations
alembic upgrade head
```

**Option B: Supabase** (Recommended)
1. Tạo project tại [supabase.com](https://supabase.com)
2. Copy credentials vào `.env`:
```env
USE_SUPABASE=True
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-key
```

### 4. Chạy Server

```bash
# Development mode (auto-reload)
python run.py

# hoặc
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Server chạy tại:
- **API:** http://localhost:8000
- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

### 5. Test API

#### Swagger UI (Recommended)
Truy cập http://localhost:8000/docs để test interactive

#### Manual Testing Script
```bash
python test_manual.py
```

#### cURL
```bash
# Health check
curl http://localhost:8000/health

# Register user
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "testuser", "email": "test@example.com", "password": "password123", "full_name": "Test User"}'

# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "testuser", "password": "password123"}'

# Get market indices (requires token)
curl http://localhost:8000/api/v1/market/indices \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

## Tính năng chính

### 1. Authentication & User Management
- JWT access tokens (30 phút)
- JWT refresh tokens (7 ngày)
- Password hashing với bcrypt
- Session management

### 2. Market Data
- VN-Index, VN30, HNX, UPCOM indices
- Historical stock prices với caching
- Fundamental analysis (P/E, P/B, ROE, ROA, EPS)
- Sector performance analysis
- Market news aggregation
- Stock search

### 3. Portfolio Management
- CRUD operations cho portfolios
- Stock position tracking
- Average cost calculation
- Watchlist management
- User data isolation

### 4. Portfolio Optimization
6 thuật toán tối ưu hóa:
1. **Markowitz** - Mean-Variance Optimization
2. **Max Sharpe** - Maximum Sharpe Ratio
3. **Min Volatility** - Minimum Risk
4. **HRP** - Hierarchical Risk Parity
5. **Min CVaR** - Minimum Conditional Value at Risk
6. **Min CDaR** - Minimum Conditional Drawdown at Risk

Tính năng:
- Efficient frontier calculation
- Custom constraints
- Risk-free rate adjustment
- Historical data backtesting
- Rebalancing strategies

### 5. Backtesting
- Portfolio performance analysis
- Multiple time periods
- Rebalancing strategies
- Performance metrics:
  - Total return, Annual return
  - Volatility, Sharpe ratio
  - Maximum drawdown
  - Portfolio value history

### 6. AI Chatbot
- Context-aware investment advice
- Portfolio analysis
- Market insights
- Powered by Google Gemini AI

## API Endpoints

### Authentication
```
POST   /api/v1/auth/register     - Đăng ký user
POST   /api/v1/auth/login        - Đăng nhập (JWT)
POST   /api/v1/auth/refresh      - Refresh access token
POST   /api/v1/auth/logout       - Đăng xuất
GET    /api/v1/auth/me           - Thông tin user
```

### Market Data
```
GET    /api/v1/market/indices                      - Market indices
GET    /api/v1/market/overview                     - Market overview
GET    /api/v1/market/sectors                      - Sector performance
GET    /api/v1/market/stock/{symbol}/price         - Stock prices
GET    /api/v1/market/stock/{symbol}/info          - Stock info
GET    /api/v1/market/stock/{symbol}/fundamentals  - Fundamentals
GET    /api/v1/market/news                         - Market news
GET    /api/v1/market/search                       - Search stocks
```

### Portfolios
```
GET    /api/v1/portfolios                - List portfolios
POST   /api/v1/portfolios                - Create portfolio
GET    /api/v1/portfolios/{id}           - Get portfolio
PUT    /api/v1/portfolios/{id}           - Update portfolio
DELETE /api/v1/portfolios/{id}           - Delete portfolio
POST   /api/v1/portfolios/{id}/stocks    - Add stock
DELETE /api/v1/portfolios/{id}/stocks/{stock_id} - Remove stock
```

### Optimization
```
POST   /api/v1/optimize/run              - Run optimization
GET    /api/v1/optimize/runs             - Optimization history
GET    /api/v1/optimize/runs/{id}        - Run details
DELETE /api/v1/optimize/runs/{id}        - Delete run
POST   /api/v1/optimize/backtest         - Run backtest
GET    /api/v1/optimize/backtests        - Backtest history
GET    /api/v1/optimize/models           - Available models
```

### Chatbot
```
POST   /api/v1/chat/message              - Send message
GET    /api/v1/chat/conversations        - Conversation history
GET    /api/v1/chat/suggestions          - Get suggestions
POST   /api/v1/chat/feedback             - Submit feedback
```

## Database

### Models (11 SQLAlchemy models)
- **User Management:** User, UserSession, UserSettings
- **Portfolio:** Portfolio, PortfolioStock, Watchlist, WatchlistStock
- **Optimization:** OptimizationRun, BacktestResult
- **Caching:** StockPriceCache, FundamentalsCache

### Migrations với Alembic
```bash
# Create migration
alembic revision --autogenerate -m "description"

# Apply migration
alembic upgrade head

# Rollback
alembic downgrade -1

# View history
alembic history
```

## Testing

```bash
# Run manual tests
python test_manual.py

# Run pytest (when available)
pytest tests/ -v

# With coverage
pytest --cov=app --cov-report=html
```

## Project Structure

```
backend-api/
├── app/
│   ├── api/              # API routes (5 routers)
│   ├── models/           # SQLAlchemy models (11 models)
│   ├── schemas/          # Pydantic schemas (18+ schemas)
│   ├── services/         # Business logic
│   ├── data_process/     # Data processing layer
│   ├── chatbot/          # AI chatbot
│   ├── portfolio_models/ # Optimization algorithms
│   ├── utils/            # Utilities
│   ├── config.py         # Settings
│   ├── database.py       # Database setup
│   ├── dependencies.py   # FastAPI dependencies
│   └── main.py           # FastAPI app
├── alembic/              # Database migrations
├── tests/                # Tests
├── requirements.txt      # Dependencies
├── .env.example          # Environment template
├── run.py                # Quick start script
└── test_manual.py        # Manual testing script
```

## Production Deployment

### Requirements
- PostgreSQL 15+
- Python 3.11+
- Redis 7+ (optional, for caching)

### Deployment Options

**Railway / Render:**
1. Create PostgreSQL database
2. Set environment variables
3. Deploy from GitHub
4. Run migrations: `alembic upgrade head`

**Docker:**
```bash
docker-compose up -d
```

**Vercel (Serverless):**
- Deploy với Vercel Postgres
- Configure serverless functions

## Security

- Password hashing với bcrypt
- JWT token-based authentication
- SQL injection prevention (SQLAlchemy ORM)
- Input validation (Pydantic)
- CORS protection
- Row Level Security (RLS) với Supabase

## Documentation

- **README.md** - This file (Setup & Quick Start)
- **DEVELOPMENT.md** - Development guide, architecture details
- **API.md** - Complete API documentation, testing guide
- **/docs** - Auto-generated Swagger UI
- **/redoc** - Auto-generated ReDoc

## Support & Contributing

### Issues
Report bugs hoặc feature requests tại GitHub Issues

### Development
1. Fork repository
2. Create feature branch
3. Write tests
4. Submit pull request

### License
MIT License

---

**Status:** 95% Complete - Ready for testing and deployment

**Next Steps:**
1. Setup database (PostgreSQL hoặc Supabase)
2. Run migrations: `alembic upgrade head`
3. Test API với Swagger UI
4. Deploy to production
