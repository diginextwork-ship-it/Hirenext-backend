process.on('uncaughtException', (err) => {
  console.error('FATAL UNCAUGHT EXCEPTION:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('FATAL UNHANDLED REJECTION:', reason);
});

console.log("=== server.js starting ===");
console.log("Node version:", process.version);
console.log("PORT env:", process.env.PORT);
console.log("NODE_ENV:", process.env.NODE_ENV);
console.log("DB_HOST env:", process.env.DB_HOST ? "SET" : "NOT SET");
console.log("DB_USER env:", process.env.DB_USER ? "SET" : "NOT SET");
console.log("DB_NAME env:", process.env.DB_NAME ? "SET" : "NOT SET");
console.log("DB_PASSWORD env:", process.env.DB_PASSWORD ? "SET" : "NOT SET");

const express = require('express');

// Use PORT from env, default to 3000 (Hostinger standard for Node.js apps)
const PORT = process.env.PORT || 3000;

let startupError = null;

try {
  const dotenv = require("dotenv");
  dotenv.config();

  console.log("After dotenv - DB_HOST:", process.env.DB_HOST ? "SET" : "NOT SET");

  const app = require("./src/app");
  const pool = require("./src/config/db");
  const { processBillingTransitions } = require("./src/routes/jobRoutes");

  // Add a debug endpoint before other routes
  app.get('/debug-startup', (_req, res) => {
    res.json({
      ok: true,
      nodeVersion: process.version,
      port: PORT,
      env: process.env.NODE_ENV,
      dbHost: process.env.DB_HOST ? "SET" : "NOT SET",
      dbUser: process.env.DB_USER ? "SET" : "NOT SET",
      dbName: process.env.DB_NAME ? "SET" : "NOT SET",
      dbPassword: process.env.DB_PASSWORD ? "SET" : "NOT SET",
      geminiKey: process.env.GEMINI_API_KEY ? "SET" : "NOT SET",
      frontendUrl: process.env.FRONTEND_URL || "NOT SET",
      frontendUrls: process.env.FRONTEND_URLS || "NOT SET",
      startupError: startupError,
      uptime: process.uptime(),
    });
  });

  // Start server IMMEDIATELY
  const server = app.listen(PORT, () => {
    console.log(`=== Server running on port ${PORT} ===`);
  });
  server.timeout = 30000;
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;

  // Initialize database in background (non-blocking)
  pool.initDatabase().then(() => {
    console.log("Database initialized successfully");
    const interval = Number(process.env.BILLING_CHECK_INTERVAL_MS) || 3600000;
    processBillingTransitions();
    setInterval(processBillingTransitions, interval);
  }).catch(err => {
    console.error("Database initialization failed:", err.message);
    startupError = err.message;
  });

} catch (error) {
  console.error("CRITICAL STARTUP ERROR:", error);
  startupError = error.message;

  // Fallback server that returns the error
  const fallback = express();
  fallback.use(express.json());
  fallback.use((_req, res) => {
    res.status(500).json({
      ok: false,
      error: error.message,
      stack: error.stack,
      nodeVersion: process.version,
      port: PORT,
      dbHost: process.env.DB_HOST ? "SET" : "NOT SET",
      dbUser: process.env.DB_USER ? "SET" : "NOT SET",
      dbName: process.env.DB_NAME ? "SET" : "NOT SET",
    });
  });
  fallback.listen(PORT, () => {
    console.log(`Fallback server running on port ${PORT} due to crash`);
  });
}
