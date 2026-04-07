# Hướng dẫn chạy Backend (Nhanh)

## 1. Cài đặt

```bash
cd backend-api

python -m venv venv
.\.venv\Scripts\activate        # Windows
# source venv/bin/activate     # Linux/Mac

pip install -r requirements.txt
```

## 2. Cấu hình `.env`

```bash
copy .env.example .env   # Windows
# cp .env.example .env   # Linux/Mac
```

Chỉnh sửa `.env`, điền các giá trị bắt buộc:

| Biến | Mô tả |
|------|-------|
| `DATABASE_URL` | Connection string PostgreSQL hoặc Supabase |
| `SECRET_KEY` | Chuỗi bí mật JWT (dùng `openssl rand -hex 32`) |
| `USE_SUPABASE` | `True` nếu dùng Supabase, `False` nếu PostgreSQL local |
| `SUPABASE_URL` | URL project Supabase (nếu `USE_SUPABASE=True`) |
| `SUPABASE_KEY` | Anon public key Supabase (nếu `USE_SUPABASE=True`) |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key Supabase (nếu `USE_SUPABASE=True`) |
| `GEMINI_API_KEY` | Google Gemini API key (để `dummy_key` nếu không dùng chatbot) |

## 3. Chạy migration

```bash
alembic upgrade head
```

## 4. Khởi động server

```bash
python run.py / uvicorn app.main:app --reload

```

Server chạy tại: `http://localhost:8000`

## 5. Kiểm tra

```
http://localhost:8000/health   → Health check
http://localhost:8000/docs     → Swagger UI
```

---

> Chi tiết đầy đủ xem `SETUP.md`
