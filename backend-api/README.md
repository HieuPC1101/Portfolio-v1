# Backend API - Portfolio Project

Backend viết bằng **FastAPI** cho ứng dụng quản lý danh mục đầu tư chứng khoán Việt Nam.

## Backend làm được gì?

- Đăng ký/đăng nhập bằng JWT (`access_token`, `refresh_token`)
- Lấy dữ liệu thị trường, chỉ số, tin tức, thông tin cổ phiếu
- Quản lý portfolio, watchlist và cổ phiếu trong danh mục
- Tối ưu danh mục (Markowitz, Max Sharpe, Min Volatility, HRP, CVaR, CDaR)
- Backtest kết quả tối ưu
- Chatbot tư vấn đầu tư (Gemini)
- Hệ thống thông báo và rule thông báo cho người dùng

## Công nghệ chính

- FastAPI + SQLAlchemy
- PostgreSQL (có tùy chọn Supabase)
- JWT Authentication
- Redis/Celery (cho tác vụ nền)
- Thư viện dữ liệu tài chính: `vnstock`, `PyPortfolioOpt`, `pandas`, `numpy`

## Cấu trúc backend

`backend-api/`

- `app/main.py`: khởi tạo app và router
- `app/api/`: các endpoint (`auth`, `market`, `portfolios`, `optimize`, `chat`, `notifications`)
- `app/models/`: model database
- `app/schemas/`: schema request/response
- `app/services/`: nghiệp vụ chính
- `tests/`: test API và logic

## Chạy nhanh local

1) Vào thư mục backend:

```bash
cd backend-api
```

2) Cài thư viện:

```bash
pip install -r requirements.txt
```

3) Tạo file `.env` từ `.env.example` và điền các biến quan trọng:

- `DATABASE_URL`
- `SECRET_KEY`
- `GEMINI_API_KEY` (nếu dùng chatbot)
- `VNSTOCK_API_KEY` (khuyến nghị để tăng giới hạn request)

4) Update Database và chạy server:

```bash
alembic upgrade head
```

```bash
python run.py / uvicorn app.main:app --reload
```

## Endpoint quan trọng

- Health check: `GET /health`
- Swagger docs: `GET /docs` (khi `DEBUG=True`)
- API info: `GET /api/v1/info`

Nhóm API chính:

- `/api/v1/auth`
- `/api/v1/market`
- `/api/v1/portfolios`
- `/api/v1/optimize`
- `/api/v1/chat`
- `/api/v1/notifications`

## Chạy test

```bash
pytest
```
