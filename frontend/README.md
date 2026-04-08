# Frontend Setup

## Prerequisites

- Node.js 18+
- Backend API running at `http://localhost:8000`

## Install

```bash
npm install
```

## Run in development

```bash
npm run dev
```

- Frontend runs at `http://localhost:8080`
- Dev server proxies `/api/*` requests to `http://localhost:8000`

## Authentication flow

- Do not put access tokens in frontend env files.
- Access token and refresh token are created by backend login and stored in browser `localStorage`.

## Test and build

```bash
npm run test
npm run build
```
