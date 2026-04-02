# Development Guide

Hướng dẫn phát triển và kiến trúc Backend API.

## Mục lục

- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Database Models](#database-models)
- [API Layer](#api-layer)
- [Services Layer](#services-layer)
- [Data Processing](#data-processing)
- [Migration từ Streamlit](#migration-từ-streamlit)
- [Database Migrations](#database-migrations)
- [Storage Strategy](#storage-strategy)
- [Best Practices](#best-practices)

---

## Kiến trúc hệ thống

### 3-Tier Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  FRONTEND (Next.js)                          │
│                   [Chưa implement]                           │
└─────────────────────────────────────────────────────────────┘
                            ↕ REST API (JSON)
┌─────────────────────────────────────────────────────────────┐
│                  BACKEND API (FastAPI)                       │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  API Routes (5 routers)                             │    │
│  │  • /auth - Authentication & authorization           │    │
│  │  • /market - Market data & news                     │    │
│  │  • /portfolios - Portfolio management               │    │
│  │  • /optimize - Portfolio optimization               │    │
│  │  • /chat - AI chatbot                               │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Services Layer                                      │    │
│  │  • market_service - Market data facade              │    │
│  │  • optimization_service - Optimization orchestrator │    │
│  │  • news_service - News aggregation                  │    │
│  │  • market_overview_service - Dashboard data         │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Data Processing Layer                               │    │
│  │  • fetchers - vnstock API calls                     │    │
│  │  • processors - Data aggregation                    │    │
│  │  • quant - Quantitative metrics                     │    │
│  │  • fundamentals - Financial ratios                  │    │
│  └─────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Optimization Models (6 algorithms)                  │    │
│  │  • Markowitz, Max Sharpe, Min Volatility           │    │
│  │  • HRP, Min CVaR, Min CDaR                          │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                            ↕ SQLAlchemy ORM
┌─────────────────────────────────────────────────────────────┐
│                DATABASE (PostgreSQL)                         │
│  • users, user_sessions, user_settings                      │
│  • portfolios, portfolio_stocks                             │
│  • watchlists, watchlist_stocks                             │
│  • optimization_runs, backtest_results                      │
│  • stock_prices_cache, fundamentals_cache                   │
└─────────────────────────────────────────────────────────────┘
                            ↕ External APIs
┌─────────────────────────────────────────────────────────────┐
│             EXTERNAL SERVICES                                │
│  • vnstock API (Vietnamese stock market data)               │
│  • Google Gemini AI (chatbot)                               │
│  • News feeds (RSS + Web scraping)                          │
└─────────────────────────────────────────────────────────────┘
```

### File Structure

```
backend-api/
├── app/
│   ├── api/                          # API Routes Layer
│   │   ├── __init__.py
│   │   ├── auth.py                   # Authentication endpoints
│   │   ├── market.py                 # Market data endpoints
│   │   ├── portfolios.py             # Portfolio CRUD endpoints
│   │   ├── optimize.py               # Optimization endpoints
│   │   └── chat.py                   # Chatbot endpoints
│   │
│   ├── models/                       # Database Models (SQLAlchemy)
│   │   ├── __init__.py
│   │   ├── user.py                   # User, UserSession, UserSettings
│   │   ├── portfolio.py              # Portfolio, PortfolioStock, Watchlist, WatchlistStock
│   │   └── optimization.py           # OptimizationRun, BacktestResult, Cache models
│   │
│   ├── schemas/                      # API Schemas (Pydantic)
│   │   ├── __init__.py
│   │   ├── user.py                   # User registration, login, token
│   │   ├── portfolio.py              # Portfolio CRUD schemas
│   │   ├── optimization.py           # Optimization request/response
│   │   ├── market.py                 # Market data schemas
│   │   └── chat.py                   # Chat message schemas
│   │
│   ├── services/                     # Business Logic Layer
│   │   ├── __init__.py
│   │   ├── market_service.py         # Market data service
│   │   ├── optimization_service.py   # Optimization orchestrator
│   │   ├── news_service.py           # News aggregation
│   │   └── market_overview_service.py# Dashboard data service
│   │
│   ├── data_process/                 # Data Processing Layer
│   │   ├── data_collect.py           # Data collection utilities
│   │   ├── data_loader.py            # Data loading utilities
│   │   ├── fetchers.py               # vnstock API integration
│   │   ├── processors.py             # Data aggregation & processing
│   │   ├── quant.py                  # Quantitative metrics
│   │   └── fundamentals.py           # Fundamental analysis
│   │
│   ├── portfolio_models/             # Optimization Algorithms
│   │   ├── __init__.py
│   │   └── portfolio_models.py       # 6 optimization models
│   │
│   ├── chatbot/                      # AI Chatbot Integration
│   │   ├── chatbot_service.py        # Gemini AI service
│   │   ├── chatbot_ui.py             # Chatbot UI utilities
│   │   └── market_data_adapter.py    # Market context provider
│   │
│   ├── utils/                        # Utility Functions
│   │   ├── __init__.py
│   │   └── auth.py                   # JWT & password utilities
│   │
│   ├── config.py                     # Application configuration
│   ├── database.py                   # SQLAlchemy setup & session
│   ├── dependencies.py               # FastAPI dependencies
│   ├── supabase_client.py           # Supabase client setup
│   └── main.py                       # FastAPI application entry
│
├── alembic/                          # Database Migrations
│   ├── versions/                     # Migration scripts
│   ├── env.py                        # Alembic environment
│   └── script.py.mako                # Migration template
│
├── tests/                            # Tests
├── requirements.txt                  # Dependencies
├── alembic.ini                       # Alembic configuration
├── run.py                            # Server startup script
├── test_manual.py                    # Manual API testing
├── .env.example                      # Environment template
└── .gitignore
```

---

## Database Models

### 1. User Management Models

#### User Model
```python
class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")
    settings = relationship("UserSettings", back_populates="user", uselist=False, cascade="all, delete-orphan")
    portfolios = relationship("Portfolio", back_populates="user", cascade="all, delete-orphan")
    watchlists = relationship("Watchlist", back_populates="user", cascade="all, delete-orphan")
```

#### UserSession Model (JWT Token Management)
```python
class UserSession(Base):
    __tablename__ = "user_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    refresh_token = Column(String(500), unique=True, nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    user = relationship("User", back_populates="sessions")
```

#### UserSettings Model
```python
class UserSettings(Base):
    __tablename__ = "user_settings"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    default_investment = Column(Numeric(15, 2), default=10000000)
    preferred_market = Column(String(20), default="HOSE")
    risk_tolerance = Column(String(20), default="moderate")
    notifications_enabled = Column(Boolean, default=True)
    theme = Column(String(20), default="light")
    
    user = relationship("User", back_populates="settings")
```

### 2. Portfolio Models

#### Portfolio Model
```python
class Portfolio(Base):
    __tablename__ = "portfolios"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    total_investment = Column(Numeric(15, 2), default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    user = relationship("User", back_populates="portfolios")
    stocks = relationship("PortfolioStock", back_populates="portfolio", cascade="all, delete-orphan")
```

#### PortfolioStock Model
```python
class PortfolioStock(Base):
    __tablename__ = "portfolio_stocks"
    
    id = Column(Integer, primary_key=True, index=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=False)
    symbol = Column(String(10), nullable=False)
    shares = Column(Integer, nullable=False)
    average_price = Column(Numeric(12, 2))
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    
    portfolio = relationship("Portfolio", back_populates="stocks")
```

### 3. Watchlist Models

```python
class Watchlist(Base):
    __tablename__ = "watchlists"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    user = relationship("User", back_populates="watchlists")
    stocks = relationship("WatchlistStock", back_populates="watchlist", cascade="all, delete-orphan")

class WatchlistStock(Base):
    __tablename__ = "watchlist_stocks"
    
    id = Column(Integer, primary_key=True, index=True)
    watchlist_id = Column(Integer, ForeignKey("watchlists.id"), nullable=False)
    symbol = Column(String(10), nullable=False)
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    
    watchlist = relationship("Watchlist", back_populates="stocks")
```

### 4. Optimization Models

```python
class OptimizationRun(Base):
    __tablename__ = "optimization_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=True)
    
    # Input parameters
    symbols = Column(JSON, nullable=False)  # List of stock symbols
    method = Column(String(50), nullable=False)  # max_sharpe, min_volatility, etc.
    total_investment = Column(Numeric(15, 2), nullable=False)
    start_date = Column(String(20))
    end_date = Column(String(20))
    risk_free_rate = Column(Numeric(5, 4), default=0.03)
    
    # Results
    weights = Column(JSON, nullable=False)  # {symbol: weight}
    allocation = Column(JSON, nullable=False)  # {symbol: amount}
    
    # Performance metrics
    expected_return = Column(Numeric(10, 6))
    volatility = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 6))
    
    # Status
    status = Column(String(20), default="completed")
    error_message = Column(Text)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class BacktestResult(Base):
    __tablename__ = "backtest_results"
    
    id = Column(Integer, primary_key=True, index=True)
    optimization_run_id = Column(Integer, ForeignKey("optimization_runs.id"), nullable=False)
    
    # Parameters
    start_date = Column(String(20), nullable=False)
    end_date = Column(String(20), nullable=False)
    initial_investment = Column(Numeric(15, 2), nullable=False)
    
    # Results
    final_value = Column(Numeric(15, 2))
    total_return = Column(Numeric(10, 6))
    annual_return = Column(Numeric(10, 6))
    volatility = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 6))
    max_drawdown = Column(Numeric(10, 6))
    
    # Time series data
    portfolio_value_history = Column(JSON)  # {date: value}
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
```

### 5. Cache Models

```python
class StockPriceCache(Base):
    __tablename__ = "stock_prices_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    cache_key = Column(String(255), unique=True, nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    start_date = Column(String(20))
    end_date = Column(String(20))
    price_data = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)

class FundamentalsCache(Base):
    __tablename__ = "fundamentals_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(10), unique=True, nullable=False, index=True)
    fundamentals_data = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
```

---

## API Layer

### Router Structure

```python
# app/main.py
from app.api import auth, market, portfolios, optimize, chat

app.include_router(auth.router, prefix=f"{settings.api_v1_prefix}/auth", tags=["auth"])
app.include_router(market.router, prefix=f"{settings.api_v1_prefix}/market", tags=["market"])
app.include_router(portfolios.router, prefix=f"{settings.api_v1_prefix}/portfolios", tags=["portfolios"])
app.include_router(optimize.router, prefix=f"{settings.api_v1_prefix}/optimize", tags=["optimize"])
app.include_router(chat.router, prefix=f"{settings.api_v1_prefix}/chat", tags=["chat"])
```

### Authentication Flow

```python
# app/api/auth.py
@router.post("/register", response_model=Token)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    # Check existing user
    existing = db.query(User).filter(User.email == user_data.email).first()
    if existing:
        raise HTTPException(400, "Email already registered")
    
    # Create user
    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        email=user_data.email,
        username=user_data.username,
        hashed_password=hashed_password,
        full_name=user_data.full_name
    )
    db.add(new_user)
    db.commit()
    
    # Generate tokens
    access_token = create_access_token(data={"user_id": new_user.id})
    refresh_token = create_refresh_token(data={"user_id": new_user.id})
    
    # Store refresh token
    session = UserSession(
        user_id=new_user.id,
        refresh_token=refresh_token,
        expires_at=datetime.utcnow() + timedelta(days=7)
    )
    db.add(session)
    db.commit()
    
    return {"access_token": access_token, "refresh_token": refresh_token}
```

### Protected Routes

```python
# app/dependencies.py
async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> User:
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=["HS256"])
        user_id: int = payload.get("user_id")
        if user_id is None:
            raise HTTPException(401, "Invalid token")
    except JWTError:
        raise HTTPException(401, "Invalid token")
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(404, "User not found")
    
    return user
```

---

## Services Layer

### Market Service

```python
# app/services/market_service.py
class MarketDataService:
    def __init__(self, db: Session):
        self.db = db
        self.fetcher = StockDataFetcher()
    
    def fetch_prices(self, query: PriceQuery) -> pd.DataFrame:
        """Fetch stock prices with caching"""
        cache_key = f"prices_{query.symbol}_{query.start_date}_{query.end_date}"
        
        # Check cache
        cached = self.db.query(StockPriceCache).filter(
            StockPriceCache.cache_key == cache_key,
            StockPriceCache.expires_at > datetime.utcnow()
        ).first()
        
        if cached:
            return pd.DataFrame(cached.price_data)
        
        # Fetch from vnstock
        data = self.fetcher.get_stock_data(
            query.symbol, 
            query.start_date, 
            query.end_date
        )
        
        # Store in cache
        cache_entry = StockPriceCache(
            cache_key=cache_key,
            symbol=query.symbol,
            start_date=query.start_date,
            end_date=query.end_date,
            price_data=data.to_dict(),
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        self.db.add(cache_entry)
        self.db.commit()
        
        return data
```

### Optimization Service

```python
# app/services/optimization_service.py
class OptimizationService:
    def __init__(self, db: Session):
        self.db = db
        self.market_service = MarketDataService(db)
    
    def run_optimization(
        self, 
        request: OptimizationRequest,
        user_id: int
    ) -> OptimizationRun:
        # Fetch historical data
        prices = self.market_service.fetch_prices_multiple(
            request.symbols,
            request.start_date,
            request.end_date
        )
        
        # Run optimization algorithm
        from app.portfolio_models.portfolio_models import PortfolioOptimizer
        optimizer = PortfolioOptimizer(prices, request.risk_free_rate)
        
        if request.method == "max_sharpe":
            weights = optimizer.maximize_sharpe_ratio()
        elif request.method == "min_volatility":
            weights = optimizer.minimize_volatility()
        # ... other methods
        
        # Calculate metrics
        metrics = optimizer.calculate_metrics(weights)
        
        # Calculate allocation
        allocation = {
            symbol: float(weight * request.total_investment)
            for symbol, weight in weights.items()
        }
        
        # Store results
        optimization = OptimizationRun(
            user_id=user_id,
            symbols=request.symbols,
            method=request.method,
            total_investment=request.total_investment,
            weights=weights,
            allocation=allocation,
            expected_return=metrics["return"],
            volatility=metrics["volatility"],
            sharpe_ratio=metrics["sharpe"],
            status="completed"
        )
        self.db.add(optimization)
        self.db.commit()
        
        return optimization
```

---

## Data Processing

### Data Fetchers (vnstock Integration)

```python
# app/data_process/fetchers.py
class StockDataFetcher:
    def get_stock_data(
        self, 
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Fetch stock OHLCV data from vnstock"""
        from vnstock import stock_historical_data
        
        df = stock_historical_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            resolution="1D",
            type="stock"
        )
        
        return df
    
    def get_fundamentals(self, symbol: str) -> dict:
        """Fetch fundamental data"""
        from vnstock import financial_ratio
        
        ratios = financial_ratio(
            symbol=symbol,
            mode="quarterly",
            is_all=False
        )
        
        return ratios.to_dict()
```

---

## Migration từ Streamlit

### Mapping chuyển đổi

| Streamlit (CŨ) | FastAPI (MỚI) |
|----------------|---------------|
| `st.session_state.selected_stocks` | Database `Portfolio` + `PortfolioStock` |
| `st.session_state.manual_optimization_results` | Database `OptimizationRun` |
| Direct service calls in UI | API endpoints with services |
| Single user | Multi-user với authentication |
| Session storage | PostgreSQL persistence |
| Streamlit UI | REST API (JSON) |

### Bước migration chi tiết

#### 1. Session State → Database
**CŨ:**
```python
# utils/session_manager.py
st.session_state.selected_stocks = ["VCB", "FPT"]
st.session_state.manual_optimization_results = {...}
```

**MỚI:**
```python
# Lưu vào database
portfolio = Portfolio(user_id=current_user.id, name="My Portfolio")
db.add(portfolio)

for symbol in ["VCB", "FPT"]:
    stock = PortfolioStock(portfolio_id=portfolio.id, symbol=symbol, shares=100)
    db.add(stock)

db.commit()
```

#### 2. UI Functions → API Endpoints
**CŨ:**
```python
def main_manual_selection(start_date, end_date):
    st.title("Tối ưu hóa danh mục")
    selected_stocks = st.session_state.selected_stocks
    data = _fetch_prices(selected_stocks, ...)
    run_models(data)
```

**MỚI:**
```python
@router.post("/optimize/run")
async def optimize_portfolio(
    request: OptimizationRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    service = OptimizationService(db)
    result = service.run_optimization(request, user.id)
    return result
```

---

## Database Migrations

### Alembic Setup

```bash
# Initialize (đã làm)
alembic init alembic

# Create migration
alembic revision --autogenerate -m "Add user and portfolio tables"

# Apply migration
alembic upgrade head

# Rollback
alembic downgrade -1

# View history
alembic history
```

### Migration Best Practices

1. **Always review autogenerated migrations**
2. **Test in development first**
3. **Keep migrations small and focused**
4. **Never edit applied migrations**
5. **Include both upgrade and downgrade**

---

## Storage Strategy

### Caching Tiers

```
┌─────────────────────────────────────┐
│  1. Redis (Optional)                │
│     - Real-time data (5-15 mins)   │
│     - Session data                  │
└─────────────────────────────────────┘
             ↓
┌─────────────────────────────────────┐
│  2. PostgreSQL Cache                │
│     - Historical data (1-24 hours) │
│     - Fundamentals                  │
└─────────────────────────────────────┘
             ↓
┌─────────────────────────────────────┐
│  3. External APIs (vnstock)        │
│     - Fresh data fetch              │
└─────────────────────────────────────┘
```

### Cache Expiration

| Data Type | TTL | Update Frequency |
|-----------|-----|------------------|
| Real-time prices | 5 mins | During market hours |
| Historical prices | 1 hour | After market close |
| Fundamentals | 24 hours | Daily |
| Market indices | 5 mins | During market hours |
| Sector data | 1 hour | After market close |

---

## Best Practices

### 1. Code Organization
- Keep routes thin, logic in services
- Use dependency injection
- Separate concerns (routes/services/models)
- Write reusable utilities

### 2. Error Handling
```python
try:
    result = service.process()
except ValueError as e:
    raise HTTPException(400, f"Invalid input: {str(e)}")
except Exception as e:
    logger.error(f"Error: {e}")
    raise HTTPException(500, "Internal server error")
```

### 3. Database Performance
- Use indexes on frequently queried columns
- Implement pagination for large datasets
- Use connection pooling
- Clean up expired cache regularly

### 4. Security
- Never commit `.env` or secrets
- Use environment variables
- Hash passwords with bcrypt
- Validate all inputs with Pydantic
- Use parameterized queries (SQLAlchemy)

### 5. Testing
```python
# tests/test_market_api.py
def test_get_stock_price(client, auth_headers):
    response = client.get(
        "/api/v1/market/stock/VNM/price",
        headers=auth_headers,
        params={"start_date": "2024-01-01", "end_date": "2024-12-31"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "symbol" in data
    assert data["symbol"] == "VNM"
```

---

## Development Workflow

### 1. Tạo feature mới
```bash
# Create branch
git checkout -b feature/new-endpoint

# Make changes
# ... edit code ...

# Create migration if needed
alembic revision --autogenerate -m "Add new table"

# Test locally
pytest tests/

# Commit and push
git add .
git commit -m "Add new endpoint"
git push origin feature/new-endpoint
```

### 2. Code Review
- Kiểm tra tests đã pass
- Review security implications
- Check performance impact
- Verify documentation updates

### 3. Deployment
```bash
# Backup database
pg_dump portfolio_db > backup.sql

# Pull latest code
git pull origin main

# Run migrations
alembic upgrade head

# Restart server
systemctl restart portfolio-api
```

---

## Troubleshooting

### Common Issues

#### Database connection error
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check .env DATABASE_URL
echo $DATABASE_URL
```

#### Import errors
```bash
# Run from backend-api directory
cd backend-api
python -m pytest

# Check PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

#### Migration conflicts
```bash
# Check current revision
alembic current

# Merge heads if needed
alembic merge heads -m "Merge migrations"
```

---

## Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [SQLAlchemy ORM](https://docs.sqlalchemy.org/en/20/orm/)
- [Alembic Migrations](https://alembic.sqlalchemy.org/)
- [vnstock API](https://github.com/thinh-vu/vnstock)
- [PyPortfolioOpt](https://pyportfolioopt.readthedocs.io/)
