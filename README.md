# HireNext Backend (Railway)

Express API backend for HireNext, designed to run independently from the frontend.

## Environment variables

Create `.env` from `.env.example`.

Required core variables:

- `PORT`
- Database config: either `DATABASE_URL` or `DB_HOST` + `DB_USER` + `DB_PASSWORD` + `DB_NAME`
- `FRONTEND_URL` for CORS (Vercel frontend URL)

Optional:

- `FRONTEND_URLS` for multiple allowed origins (comma-separated)
- `ALLOW_VERCEL_PREVIEWS=true` to allow `*.vercel.app` preview domains
- `AUTH_SECRET`, `ADMIN_API_KEY`
- `GEMINI_API_KEY`, `GEMINI_TIMEOUT_MS`

## Run locally

```bash
npm install
npm start
```

Health check:

- `GET /health`
