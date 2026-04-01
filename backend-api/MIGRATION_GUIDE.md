# Hướng dẫn chuyển đổi từ Streamlit sang FastAPI

## Tổng quan chuyển đổi

### Kiến trúc CŨ (Streamlit)
```
main.py (Streamlit UI + Logic)
    ↓
backend/services/ (Business Logic)
    ↓
data_process/ (Data Layer)
    ↓
External APIs (vnstock, Gemini)
```

### Kiến trúc MỚI (FastAPI)
```
Frontend (Next.js - chưa có)
    ↓ HTTP/WebSocket
backend-api/ (FastAPI REST API)
    ↓
SQLAlchemy ORM
    ↓
PostgreSQL Database
```

## Mapping chuyển đổi

### 1. Session State → Database + JWT

**CŨ (Streamlit)**:
```python
# utils/session_manager.py
st.session_state.selected_stocks = ["VCB", "FPT"]
st.session_state.manual_optimization_results = {...}
```

**MỚI (FastAPI)**:
```python
# Lưu vào database
portfolio = Portfolio(user_id=current_user.id, ...)
db.add(portfolio)
db.commit()

# Authenticate với JWT
token = create_access_token(data={"user_id": user.id})
```

### 2. UI Functions → API Endpoints

**CŨ (Streamlit)**:
```python
def main_manual_selection(start_date, end_date):
    st.title("Tối ưu hóa danh mục")
    selected_stocks = st.session_state.selected_stocks
    data = _fetch_prices(selected_stocks, ...)
    run_models(data)
```

**MỚI (FastAPI)**:
```python
@router.post("/api/v1/optimize")
async def optimize_portfolio(
    request: OptimizationRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    result = optimization_service.run_model(...)
    # Lưu vào database
    return result
```

### 3. Direct Service Calls → API Layer

**CŨ (Streamlit)**:
```python
# Gọi trực tiếp trong main.py
from backend.services.market_service import get_market_data_service

market_service = get_market_data_service()
data = market_service.fetch_prices(query)
```

**MỚI (FastAPI)**:
```python
# API endpoint
@router.get("/api/v1/market/stocks/{symbol}")
async def get_stock_data(symbol: str):
    service = get_market_data_service()
    data = service.fetch_prices(...)
    return {"symbol": symbol, "data": data}
```

## Các bước migration chi tiết

### BƯỚC 1: Authentication Routes

**File mới**: `backend-api/app/api/auth.py`

```python
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.schemas.user import UserCreate, UserLogin, Token
from app.utils.auth import verify_password, get_password_hash, create_access_token

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/register", response_model=Token)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    # Check if user exists
    existing_user = db.query(User).filter(User.email == user_data.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create new user
    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        email=user_data.email,
        hashed_password=hashed_password,
        full_name=user_data.full_name
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    # Generate tokens
    access_token = create_access_token(data={"user_id": new_user.id, "email": new_user.email})
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/login", response_model=Token)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == credentials.email).first()
    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    access_token = create_access_token(data={"user_id": user.id, "email": user.email})
    return {"access_token": access_token, "token_type": "bearer"}
```

### BƯỚC 2: Market Data Routes

**File mới**: `backend-api/app/api/market.py`

```python
from fastapi import APIRouter, Depends
from app.services.market_service import get_market_data_service
from app.dependencies import get_current_user

router = APIRouter(prefix="/market", tags=["market"])

@router.get("/stocks/{symbol}")
async def get_stock_info(
    symbol: str,
    current_user = Depends(get_current_user)
):
    service = get_market_data_service()
    # Reuse existing service logic
    data = service.fetch_prices(PriceQuery(symbols=[symbol], ...))
    return {"symbol": symbol, "data": data}
```

### BƯỚC 3: Update main.py

**Sửa**: `backend-api/app/main.py`

```python
# Import routers
from app.api import auth, market, portfolios, optimize

# Include routers
app.include_router(auth.router, prefix=f"{settings.api_v1_prefix}")
app.include_router(market.router, prefix=f"{settings.api_v1_prefix}")
app.include_router(portfolios.router, prefix=f"{settings.api_v1_prefix}")
app.include_router(optimize.router, prefix=f"{settings.api_v1_prefix}")
```

## Cần điều chỉnh trong Services cũ

### 1. Import paths

**CŨ**:
```python
from utils.config import ANALYSIS_START_DATE
from backend.services.market_service import get_market_data_service
```

**MỚI**:
```python
from app.config import settings
from app.services.market_service import get_market_data_service
```

### 2. Config constants

**CŨ** (`utils/config.py`):
```python
ANALYSIS_START_DATE = "2022-01-01"
DEFAULT_MARKET = "HOSE"
```

**MỚI** (`app/config.py`):
```python
class Settings(BaseSettings):
    default_market: str = "HOSE"
    # Access via settings.default_market
```

### 3. Session state → Database

**CŨ**:
```python
st.session_state.manual_optimization_results = result
```

**MỚI**:
```python
optimization = OptimizationRun(
    user_id=user.id,
    model_name="Markowitz",
    weights=result["weights"],
    ...
)
db.add(optimization)
db.commit()
```

## Dependencies cần thêm

### Vào backend-api directory
```bash
cd backend-api
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### Setup PostgreSQL
```bash
# Download từ: https://www.postgresql.org/download/windows/
# Hoặc dùng Docker
docker run --name portfolio-postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres:15
```

### Tạo database
```bash
createdb portfolio_db
# Hoặc trong psql:
# CREATE DATABASE portfolio_db;
```

## Testing workflow

### 1. Chạy server
```bash
cd backend-api
python run.py
```

### 2. Test với curl/Postman

**Register**:
```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "password123",
    "full_name": "Test User"
  }'
```

**Login**:
```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "password123"
  }'
```

**Protected endpoint**:
```bash
curl -X GET http://localhost:8000/api/v1/market/stocks/VCB \
  -H "Authorization: Bearer <access_token>"
```

## Frontend integration (sau này)

### Next.js API calls
```typescript
// lib/api.ts
const API_URL = 'http://localhost:8000/api/v1'

export async function optimizePortfolio(data: OptimizationRequest) {
  const token = localStorage.getItem('access_token')
  
  const response = await fetch(`${API_URL}/optimize`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify(data)
  })
  
  return response.json()
}
```

## Checklist Migration

- [x] Tạo backend-api structure
- [x] Setup models (User, Portfolio, Optimization)
- [x] Setup schemas (Pydantic)
- [x] Setup authentication (JWT)
- [x] Copy services cũ
- [ ] Tạo API routes (auth, market, portfolios, optimize)
- [ ] Setup Alembic migrations
- [ ] Test authentication flow
- [ ] Test optimization endpoints
- [ ] Setup Redis caching
- [ ] Setup WebSocket
- [ ] Write tests
- [ ] Deploy backend
- [ ] Tạo frontend Next.js
- [ ] Connect frontend-backend

## Tips

1. **Incremental migration**: Làm từng feature một, test kỹ trước khi làm tiếp
2. **Keep old code**: Giữ `main.py` cũ để tham khảo logic
3. **Use Swagger**: FastAPI tự động generate docs tại `/docs`
4. **Database first**: Setup database và test models trước khi làm routes
5. **Test early**: Viết tests ngay từ đầu, dễ debug hơn

## Ready for production

Khi đã test xong local:
1. Setup CI/CD (GitHub Actions)
2. Deploy backend lên Railway/Render
3. Deploy frontend lên Vercel
4. Setup production database (Supabase/Railway Postgres)
5. Configure CORS cho production domain
6. Add monitoring (Sentry, LogRocket)
