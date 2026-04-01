# Portfolio Optimization API - Backend

Backend API cho hệ thống tối ưu hóa danh mục đầu tư chứng khoán Việt Nam.

## Tính năng

- **Authentication**: JWT-based authentication với access/refresh tokens
- **Portfolio Management**: Quản lý danh mục đầu tư và watchlists
- **Optimization Engine**: 6 mô hình tối ưu hóa (Markowitz, Sharpe, HRP, CVaR, CDaR)
- **Market Data**: Real-time data từ vnstock API
- **Backtesting**: Kiểm tra hiệu quả danh mục
- **AI Chatbot**: Tích hợp Google Gemini AI

## Yêu cầu

- Python 3.11+
- PostgreSQL 15+
- Redis 7+ (optional, cho caching)

## Cài đặt

### 1. Clone và setup môi trường

\`\`\`bash
cd backend-api
python -m venv venv

# Windows
.\\venv\\Scripts\\activate

# Linux/Mac
source venv/bin/activate
\`\`\`

### 2. Cài đặt dependencies

\`\`\`bash
pip install -r requirements.txt
\`\`\`

### 3. Cấu hình môi trường

Tạo file `.env` từ template:

\`\`\`bash
cp .env.example .env
\`\`\`

Chỉnh sửa `.env` với thông tin của bạn:

\`\`\`env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/portfolio_db

# JWT Secret (generate với: openssl rand -hex 32)
SECRET_KEY=your-secret-key-here

# Gemini API Key
GEMINI_API_KEY=your-gemini-api-key
\`\`\`

### 4. Setup Database

\`\`\`bash
# Tạo database PostgreSQL
createdb portfolio_db

# Chạy migrations (sau khi setup Alembic)
alembic upgrade head
\`\`\`

## Chạy Server

### Development mode (với auto-reload)

\`\`\`bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
\`\`\`

Hoặc:

\`\`\`bash
python -m app.main
\`\`\`

### Production mode

\`\`\`bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
\`\`\`

API sẽ chạy tại: `http://localhost:8000`

API documentation (Swagger): `http://localhost:8000/docs`

## API Endpoints

### Authentication

\`\`\`
POST   /api/v1/auth/register       - Đăng ký tài khoản
POST   /api/v1/auth/login          - Đăng nhập
POST   /api/v1/auth/logout         - Đăng xuất
GET    /api/v1/auth/me             - Thông tin user hiện tại
POST   /api/v1/auth/refresh        - Refresh access token
\`\`\`

### Market Data

\`\`\`
GET    /api/v1/market/indices      - Lấy dữ liệu chỉ số (VN-Index, HNX, ...)
GET    /api/v1/market/stocks/{symbol}  - Chi tiết 1 mã cổ phiếu
GET    /api/v1/market/stocks/{symbol}/history  - Lịch sử giá
GET    /api/v1/market/sectors      - Tổng quan ngành
GET    /api/v1/market/news         - Tin tức thị trường
\`\`\`

### Portfolios

\`\`\`
GET    /api/v1/portfolios          - Danh sách portfolios
POST   /api/v1/portfolios          - Tạo portfolio mới
GET    /api/v1/portfolios/{id}     - Chi tiết portfolio
PUT    /api/v1/portfolios/{id}     - Cập nhật portfolio
DELETE /api/v1/portfolios/{id}     - Xóa portfolio
\`\`\`

### Optimization

\`\`\`
POST   /api/v1/optimize            - Chạy tối ưu hóa
GET    /api/v1/optimize/history    - Lịch sử optimization
GET    /api/v1/optimize/{id}       - Kết quả chi tiết
POST   /api/v1/optimize/{id}/backtest  - Chạy backtest
\`\`\`

## Cấu trúc dự án

\`\`\`
backend-api/
├── app/
│   ├── api/              # API routes
│   ├── models/           # SQLAlchemy models
│   ├── schemas/          # Pydantic schemas
│   ├── services/         # Business logic
│   ├── utils/            # Utilities
│   ├── config.py         # Configuration
│   ├── database.py       # Database setup
│   ├── dependencies.py   # FastAPI dependencies
│   └── main.py           # FastAPI app
├── tests/                # Tests
├── alembic/              # Database migrations
├── requirements.txt
└── .env
\`\`\`

## Testing

\`\`\`bash
# Chạy tests
pytest

# Với coverage report
pytest --cov=app --cov-report=html
\`\`\`

## Database Migrations

### Tạo migration mới

\`\`\`bash
alembic revision --autogenerate -m "description"
\`\`\`

### Chạy migrations

\`\`\`bash
# Upgrade
alembic upgrade head

# Downgrade
alembic downgrade -1
\`\`\`

## Security

- Passwords được hash bằng bcrypt
- JWT tokens cho authentication
- CORS protection
- SQL injection prevention (SQLAlchemy ORM)
- Input validation (Pydantic)

## TODO

- [ ] Implement API routes (auth, market, portfolios, optimize)
- [ ] Setup Alembic migrations
- [ ] Copy services từ backend cũ
- [ ] Implement caching layer (Redis)
- [ ] WebSocket cho real-time data
- [ ] Background tasks (Celery)
- [ ] Write tests
- [ ] Setup CI/CD
- [ ] Docker containerization

## Contributing

1. Tạo branch mới từ `main`
2. Implement changes
3. Write tests
4. Submit pull request

## License

MIT License
