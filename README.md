# HireNext Backend (Railway)

Express API backend for HireNext, designed to run independently from the frontend.

## Environment variables

Create `.env` from `.env.example`.

Required core variables:

- `PORT`
- Database config: either `DATABASE_URL` or `DB_HOST` + `DB_USER` + `DB_PASSWORD` + `DB_NAME`
- `FRONTEND_URL` for CORS (for example `https://hirenextindia.com`)

Optional:

- `FRONTEND_URLS` for multiple allowed origins (comma-separated), for example `https://www.hirenextindia.com`
- `ALLOW_VERCEL_PREVIEWS=true` to allow `*.vercel.app` preview domains
- `ALLOW_PRIVATE_NETWORK_ORIGINS=true` to allow local/LAN frontend URLs such as `http://192.168.1.10:5173` during development. Defaults to `true` outside production.
- `AUTH_SECRET`, `ADMIN_API_KEY`
- `GEMINI_API_KEY`, `GEMINI_TIMEOUT_MS`

## Run locally

```bash
npm install
npm start
```

Health check:

- `GET /health`
