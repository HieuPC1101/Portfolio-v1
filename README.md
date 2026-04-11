# Portfolio Project

Ứng dụng quản lý và tối ưu danh mục đầu tư chứng khoán Việt Nam, gồm:

- **Frontend**: React + Vite + TypeScript
- **Backend**: FastAPI + SQLAlchemy + PostgreSQL
![pic](https://github.com/user-attachments/assets/4e385172-cd50-4228-9bdf-f16f6645aa90)

## Tính năng chính

- Xác thực người dùng (JWT: access token + refresh token)
- Theo dõi dữ liệu thị trường, chỉ số, thông tin cổ phiếu, tin tức
- Quản lý portfolio, watchlist và cổ phiếu trong danh mục
- Tối ưu danh mục (Markowitz, Max Sharpe, Min Volatility, HRP, CVaR, CDaR)
- Backtest chiến lược đầu tư
- Chatbot hỗ trợ phân tích đầu tư
- Hệ thống thông báo và rule thông báo

## Cấu trúc dự án

```text
Portfolio-Project/
|-- frontend/       # Ứng dụng web (React + Vite)
`-- backend-api/    # REST API (FastAPI)
```

Chi tiết từng phần:

- Frontend: `frontend/README.md`
- Backend: `backend-api/README.MD`

## Yêu cầu môi trường

- Node.js 18+
- Python 3.10+
- PostgreSQL
- (Tùy chọn) Redis cho tác vụ nền

## Chạy nhanh local

## 1) Chạy backend

```bash
cd backend-api
pip install -r requirements.txt
```

Tạo file `.env` từ `.env.example` và cấu hình tối thiểu:

- `DATABASE_URL`
- `SECRET_KEY`
- `GEMINI_API_KEY` (nếu dùng chatbot)
- `VNSTOCK_API_KEY` (khuyến nghị)

Chạy migration và khởi động API:

```bash
alembic upgrade head
python run.py
```

Backend mặc định chạy tại: `http://localhost:8000`

## 2) Chạy frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend mặc định chạy tại: `http://localhost:8080`

Frontend proxy `/api/*` sang backend. Nếu backend chạy host/port khác, tạo `frontend/.env.local`:

```bash
VITE_API_PROXY_TARGET=http://127.0.0.1:8001
```

## Tài liệu API

- Swagger UI: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`
- API info: `http://localhost:8000/api/v1/info`

## Chạy test

Backend:

```bash
cd backend-api
pytest
```

Frontend:

```bash
cd frontend
npm run test
```

## Ghi chú

- Không lưu token/API key nhạy cảm vào source code.
- Dự án đang tách rõ frontend/backend để dễ scale và triển khai độc lập.
