# Hướng dẫn Setup Backend

Hướng dẫn từng bước để setup và chạy Portfolio Optimization API.

## Mục lục

- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt cơ bản](#cài-đặt-cơ-bản)
- [Setup Database - Option 1: PostgreSQL Local](#option-1-postgresql-local)
- [Setup Database - Option 2: Supabase](#option-2-supabase-cloud)
- [Cấu hình môi trường](#cấu-hình-môi-trường)
- [Chạy Migrations](#chạy-migrations)
- [Khởi động Server](#khởi-động-server)
- [Kiểm tra cài đặt](#kiểm-tra-cài-đặt)
- [Troubleshooting](#troubleshooting)

---

## Yêu cầu hệ thống

### Phần mềm cần thiết

**Bắt buộc:**
- Python 3.11 hoặc cao hơn
- pip (Python package manager)
- Git

**Tùy chọn (chọn 1 trong 2):**
- PostgreSQL 15+ (local database)
- Hoặc tài khoản Supabase (cloud database - miễn phí)

### Kiểm tra version

```bash
# Kiểm tra Python
python --version
# Output: Python 3.11.x hoặc cao hơn

# Kiểm tra pip
pip --version
# Output: pip 23.x.x

# Kiểm tra Git
git --version
# Output: git version 2.x.x
```

---

## Cài đặt cơ bản

### Bước 1: Clone repository (nếu chưa có)

```bash
# Clone project
git clone <repository-url>

# Di chuyển vào thư mục backend
cd Portfolio-Project/backend-api
```

### Bước 2: Tạo môi trường ảo

**Windows:**
```bash
# Tạo virtual environment
python -m venv venv

# Kích hoạt
.\venv\Scripts\activate

# Nếu gặp lỗi execution policy
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Linux/Mac:**
```bash
# Tạo virtual environment
python3 -m venv venv

# Kích hoạt
source venv/bin/activate
```

Sau khi kích hoạt, bạn sẽ thấy `(venv)` ở đầu dòng lệnh.

### Bước 3: Cài đặt dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Cài đặt tất cả packages
pip install -r requirements.txt

# Đợi 2-5 phút tùy tốc độ mạng
```

**Verify installation:**
```bash
# Kiểm tra FastAPI đã cài
pip show fastapi
# Output: Name: fastapi, Version: 0.110.x

# Kiểm tra SQLAlchemy
pip show sqlalchemy
# Output: Name: SQLAlchemy, Version: 2.0.x
```

---

## Option 1: PostgreSQL Local

### Bước 1: Cài đặt PostgreSQL

**Windows:**
1. Download từ: https://www.postgresql.org/download/windows/
2. Chạy installer
3. Chọn port: 5432 (mặc định)
4. Đặt password cho user `postgres` (ghi nhớ password này)
5. Hoàn tất cài đặt

**Linux (Ubuntu/Debian):**
```bash
# Update package list
sudo apt update

# Cài đặt PostgreSQL
sudo apt install postgresql postgresql-contrib

# Start service
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Mac (với Homebrew):**
```bash
# Cài đặt
brew install postgresql@15

# Start service
brew services start postgresql@15
```

### Bước 2: Tạo database

**Windows:**
```bash
# Mở psql (thay <password> bằng password bạn đã đặt)
psql -U postgres

# Trong psql prompt
CREATE DATABASE portfolio_db;

# Tạo user riêng (optional nhưng khuyến nghị)
CREATE USER portfolio_user WITH PASSWORD 'your_password_here';
GRANT ALL PRIVILEGES ON DATABASE portfolio_db TO portfolio_user;

# Thoát psql
\q
```

**Linux/Mac:**
```bash
# Switch sang postgres user
sudo -u postgres psql

# Tạo database
CREATE DATABASE portfolio_db;

# Tạo user
CREATE USER portfolio_user WITH PASSWORD 'your_password_here';
GRANT ALL PRIVILEGES ON DATABASE portfolio_db TO portfolio_user;

# Thoát
\q
```

### Bước 3: Test kết nối

```bash
# Test với psql
psql -U portfolio_user -d portfolio_db -h localhost

# Nếu kết nối thành công, bạn sẽ thấy prompt:
# portfolio_db=>

# Thoát
\q
```

### Bước 4: Lấy connection string

Database URL của bạn sẽ có dạng:
```
postgresql://portfolio_user:your_password_here@localhost:5432/portfolio_db
```

Lưu string này để dùng ở bước cấu hình môi trường.

---

## Option 2: Supabase (Cloud)

### Bước 1: Tạo tài khoản và project

1. Truy cập https://supabase.com
2. Click "Start your project"
3. Đăng nhập bằng GitHub (hoặc email)
4. Click "New Project"

### Bước 2: Cấu hình project

**Thông tin cần điền:**
- Name: `portfolio-optimization` (hoặc tên bạn muốn)
- Database Password: Tạo password mạnh (ghi nhớ)
- Region: Chọn `Singapore` (gần Việt Nam nhất)
- Pricing Plan: `Free` (đủ cho development)

Click "Create new project" và đợi 1-2 phút.

### Bước 3: Lấy credentials

Sau khi project được tạo:

1. Vào **Settings** (icon bánh răng ở sidebar)
2. Chọn **Database** trong menu
3. Scroll xuống phần **Connection string**
4. Copy URI (chọn mode "URI")

Connection string sẽ có dạng:
```
postgresql://postgres.xxxxx:password@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres
```

5. Vào **API** trong Settings menu
6. Copy:
   - Project URL: `https://xxxxx.supabase.co`
   - anon public key: `eyJhbGc...`
   - service_role key: `eyJhbGc...` (bấm "Reveal" để xem)

### Bước 4: Tạo database schema (Optional)

Supabase đã có database sẵn, nhưng bạn có thể tạo schema trước nếu muốn:

1. Vào **SQL Editor** trong sidebar
2. Tạo các bảng cơ bản (hoặc bỏ qua, sẽ tạo bằng Alembic sau)

```sql
-- Tạo schema cơ bản
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tables sẽ được tạo bởi Alembic migrations
```

---

## Cấu hình môi trường

### Bước 1: Tạo file .env

```bash
# Copy file template
cp .env.example .env

# Hoặc trên Windows
copy .env.example .env
```

### Bước 2: Chỉnh sửa .env

Mở file `.env` bằng text editor và điền thông tin:

**Nếu dùng PostgreSQL Local:**
```env
# Database
DATABASE_URL=postgresql://portfolio_user:your_password_here@localhost:5432/portfolio_db

# Supabase (để trống)
USE_SUPABASE=False
SUPABASE_URL=
SUPABASE_KEY=
SUPABASE_SERVICE_ROLE_KEY=

# JWT Secret
SECRET_KEY=your-secret-key-here-generate-with-openssl-rand-hex-32
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Gemini API (tùy chọn)
GEMINI_API_KEY=your-gemini-api-key-here

# Environment
DEBUG=True
ENVIRONMENT=development
API_V1_PREFIX=/api/v1
```

**Nếu dùng Supabase:**
```env
# Database
DATABASE_URL=postgresql://postgres.xxxxx:password@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres

# Supabase
USE_SUPABASE=True
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=your-anon-public-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# JWT Secret
SECRET_KEY=your-secret-key-here
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Gemini API
GEMINI_API_KEY=your-gemini-api-key-here

# Environment
DEBUG=True
ENVIRONMENT=development
API_V1_PREFIX=/api/v1
```

### Bước 3: Generate SECRET_KEY

**Windows (PowerShell):**
```powershell
# Generate random 32-byte hex string
-join ((48..57) + (97..102) | Get-Random -Count 64 | % {[char]$_})
```

**Linux/Mac:**
```bash
# Generate với openssl
openssl rand -hex 32
```

Copy output và paste vào `SECRET_KEY` trong file `.env`.

### Bước 4: Lấy Gemini API Key (tùy chọn)

Nếu muốn dùng AI chatbot:

1. Truy cập https://makersuite.google.com/app/apikey
2. Đăng nhập Google account
3. Click "Create API Key"
4. Copy key và paste vào `GEMINI_API_KEY`

Nếu không dùng chatbot, để trống hoặc đặt `GEMINI_API_KEY=dummy_key`.

---

## Chạy Migrations

### Bước 1: Kiểm tra Alembic config

```bash
# Kiểm tra file alembic.ini tồn tại
ls alembic.ini

# Kiểm tra thư mục alembic
ls alembic/
# Output: env.py, script.py.mako, versions/
```

### Bước 2: Tạo migration đầu tiên

```bash
# Generate migration từ models
alembic revision --autogenerate -m "Initial schema"

# Output:
# Generating migrations/versions/xxxx_initial_schema.py ... done
```

### Bước 3: Review migration file

```bash
# Xem file migration vừa tạo
ls alembic/versions/

# Mở file để kiểm tra (optional)
# Đảm bảo có các bảng: users, portfolios, optimization_runs, etc.
```

### Bước 4: Apply migration

```bash
# Chạy migration
alembic upgrade head

# Output sẽ hiện các bảng được tạo:
# INFO  [alembic.runtime.migration] Running upgrade -> xxxx, Initial schema
# INFO  [alembic.runtime.migration] Created table users
# INFO  [alembic.runtime.migration] Created table portfolios
# ...
```

### Bước 5: Verify database

**PostgreSQL Local:**
```bash
# Kết nối vào database
psql -U portfolio_user -d portfolio_db -h localhost

# Liệt kê các bảng
\dt

# Output sẽ hiện:
# public | users
# public | user_sessions
# public | portfolios
# public | optimization_runs
# ...

# Thoát
\q
```

**Supabase:**
1. Vào Supabase Dashboard
2. Click **Table Editor** ở sidebar
3. Kiểm tra các bảng đã được tạo

---

## Khởi động Server

### Bước 1: Chạy server

**Option A - Script nhanh:**
```bash
python run.py
```

**Option B - Uvicorn trực tiếp:**
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Output thành công:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process using StatReload
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### Bước 2: Kiểm tra server đang chạy

Mở browser và truy cập:

**API Root:**
```
http://localhost:8000
```

Sẽ thấy response:
```json
{
  "app": "Portfolio Optimization API",
  "version": "1.0.0",
  "status": "running"
}
```

**Health Check:**
```
http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy"
}
```

**Swagger UI:**
```
http://localhost:8000/docs
```

Sẽ thấy interactive API documentation.

---

## Kiểm tra cài đặt

### Test 1: Health check với cURL

```bash
curl http://localhost:8000/health
```

Expected output:
```json
{"status":"healthy"}
```

### Test 2: API Info

```bash
curl http://localhost:8000/api/v1/info
```

Expected output:
```json
{
  "app": "Portfolio Optimization API",
  "version": "1.0.0",
  "environment": "development"
}
```

### Test 3: Register user

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

Expected output:
```json
{
  "access_token": "eyJhbGc...",
  "refresh_token": "eyJhbGc...",
  "token_type": "bearer"
}
```

### Test 4: Login

```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "password123"
  }'
```

### Test 5: Chạy test script

```bash
python test_manual.py
```

Script sẽ test 13 endpoints và hiện kết quả.

---

## Troubleshooting

### Lỗi 1: ModuleNotFoundError

**Triệu chứng:**
```
ModuleNotFoundError: No module named 'fastapi'
```

**Giải pháp:**
```bash
# Kiểm tra virtual environment đã activate chưa
which python  # Linux/Mac
where python  # Windows

# Phải trỏ đến venv/bin/python hoặc venv\Scripts\python.exe

# Nếu chưa activate
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

# Cài lại dependencies
pip install -r requirements.txt
```

### Lỗi 2: Database connection failed

**Triệu chứng:**
```
sqlalchemy.exc.OperationalError: could not connect to server
```

**Giải pháp PostgreSQL Local:**
```bash
# Kiểm tra PostgreSQL có chạy không
# Windows
services.msc  # Tìm PostgreSQL service

# Linux
sudo systemctl status postgresql

# Mac
brew services list

# Khởi động PostgreSQL nếu chưa chạy
# Linux
sudo systemctl start postgresql

# Mac
brew services start postgresql@15

# Kiểm tra DATABASE_URL trong .env đúng chưa
# Đúng format: postgresql://user:password@localhost:5432/database
```

**Giải pháp Supabase:**
```bash
# Kiểm tra connection string
# Đảm bảo password không có ký tự đặc biệt chưa encode
# Hoặc dùng connection pooler URL từ Supabase

# Test kết nối
psql "postgresql://postgres.xxxxx:password@..."
```

### Lỗi 3: Alembic migration failed

**Triệu chứng:**
```
alembic.util.exc.CommandError: Target database is not up to date
```

**Giải pháp:**
```bash
# Kiểm tra current revision
alembic current

# Nếu không có revision nào
alembic stamp head

# Hoặc rollback và chạy lại
alembic downgrade base
alembic upgrade head
```

### Lỗi 4: Port 8000 already in use

**Triệu chứng:**
```
ERROR: [Errno 48] Address already in use
```

**Giải pháp:**

**Windows:**
```bash
# Tìm process đang dùng port 8000
netstat -ano | findstr :8000

# Kill process (thay <PID> bằng số ở cột cuối)
taskkill /PID <PID> /F

# Hoặc dùng port khác
uvicorn app.main:app --reload --port 8001
```

**Linux/Mac:**
```bash
# Tìm process
lsof -i :8000

# Kill process
kill -9 <PID>

# Hoặc dùng port khác
uvicorn app.main:app --reload --port 8001
```

### Lỗi 5: Import vnstock failed

**Triệu chứng:**
```
ModuleNotFoundError: No module named 'vnstock'
```

**Giải pháp:**
```bash
# Cài vnstock riêng
pip install vnstock==2.0.0

# Hoặc upgrade
pip install --upgrade vnstock

# Nếu vẫn lỗi, thử version khác
pip install vnstock==1.0.30
```

### Lỗi 6: SECRET_KEY not set

**Triệu chứng:**
```
ValueError: SECRET_KEY environment variable not set
```

**Giải pháp:**
```bash
# Kiểm tra file .env tồn tại
ls .env

# Nếu không có, tạo từ template
cp .env.example .env

# Mở .env và set SECRET_KEY
# Generate key mới
openssl rand -hex 32

# Hoặc trên Windows PowerShell
-join ((48..57) + (97..102) | Get-Random -Count 64 | % {[char]$_})
```

### Lỗi 7: GEMINI_API_KEY invalid

**Triệu chứng:**
```
google.generativeai.types.generation_types.StopCandidateException
```

**Giải pháp:**
```bash
# Nếu không dùng chatbot, comment out GEMINI_API_KEY
# Hoặc set dummy value
GEMINI_API_KEY=dummy_key

# Nếu muốn dùng, lấy key mới từ
# https://makersuite.google.com/app/apikey
```

---

## Next Steps

Sau khi setup thành công:

**1. Explore API với Swagger:**
```
http://localhost:8000/docs
```

**2. Đọc API documentation:**
```bash
cat API.md
```

**3. Đọc Development guide:**
```bash
cat DEVELOPMENT.md
```

**4. Chạy production mode:**
```bash
# Stop dev server (Ctrl+C)

# Chạy production
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

**5. Deploy lên cloud:**
- Railway: https://railway.app
- Render: https://render.com
- Vercel: https://vercel.com

Xem hướng dẫn chi tiết trong `API.md` phần Deployment.

---

## Tóm tắt các lệnh quan trọng

```bash
# 1. Setup
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Chỉnh sửa .env

# 3. Database
createdb portfolio_db  # PostgreSQL local
alembic upgrade head

# 4. Run server
python run.py

# 5. Test
curl http://localhost:8000/health
python test_manual.py
```

---

## Liên hệ hỗ trợ

Nếu gặp vấn đề không nằm trong troubleshooting:

1. Kiểm tra logs trong terminal
2. Đọc error message kỹ
3. Tìm kiếm error trên Google/Stack Overflow
4. Mở issue trên GitHub repository
