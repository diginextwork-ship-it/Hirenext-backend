const express = require('express');

process.on('uncaughtException', (err) => {
  console.error('FATAL UNCAUGHT EXCEPTION:', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('FATAL UNHANDLED REJECTION:', reason);
});

console.log("Starting server.js...");

try {
  const dotenv = require("dotenv");
  dotenv.config();
  
  const app = require("./src/app");
  const pool = require("./src/config/db");
  const { processBillingTransitions } = require("./src/routes/jobRoutes");
  
  const PORT = process.env.PORT || 5000;
  
  // Start server IMMEDIATELY so Hostinger health checks pass
  const server = app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
  server.requestTimeout = 0;
  
  // Initialize database in background
  pool.initDatabase().then(() => {
    console.log("Database initialized successfully");
    const interval = Number(process.env.BILLING_CHECK_INTERVAL_MS) || 3600000;
    processBillingTransitions();
    setInterval(processBillingTransitions, interval);
  }).catch(err => {
    console.error("Database initialization failed:", err.message);
  });
  
} catch (error) {
  console.error("CRITICAL STARTUP ERROR:", error);
  // Fallback server so Hostinger gets a port bind and logs the error
  const fallback = express();
  fallback.use((req, res) => res.status(500).json({ error: error.message, stack: error.stack }));
  fallback.listen(process.env.PORT || 5000, () => {
    console.log("Fallback server running due to crash");
  });
}

