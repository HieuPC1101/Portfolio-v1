# Frontend - Portfolio Project

Frontend được xây bằng **React + Vite + TypeScript**.

## Yêu cầu

- Node.js 18+
- Backend đang chạy tại `http://localhost:8000`

## Chạy nhanh local

```bash
npm install
npm run dev
```

- Frontend chạy ở: `http://localhost:8080`
- Mặc định proxy `/api/*` sang: `http://127.0.0.1:8000`

## Đổi địa chỉ backend (nếu cần)

Tạo file `.env.local` trong thư mục `frontend`:

```bash
VITE_API_PROXY_TARGET=http://127.0.0.1:8001
```

## Lỗi thường gặp: `ECONNREFUSED`

Nếu thấy lỗi proxy `ECONNREFUSED`, frontend chưa kết nối được backend.

1. Chạy backend tại thư mục `backend-api`:

```bash
python run.py
```

2. Kiểm tra backend:

```bash
curl http://localhost:8000/health
```

3. Nếu backend không chạy ở `:8000`, đặt `VITE_API_PROXY_TARGET` như trên.

## Authentication

- Token được backend cấp sau khi đăng nhập.
- Token lưu ở `localStorage`.
- Không đặt token vào file env frontend.

## Lệnh hữu ích

```bash
npm run test
npm run build
npm run preview
```
