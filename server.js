const dotenv = require("dotenv");
dotenv.config();

const app = require("./src/app");
const pool = require("./src/config/db");
const { processBillingTransitions } = require("./src/routes/jobRoutes");

const PORT = process.env.PORT || 5000;
const BILLING_CHECK_INTERVAL_MS =
  Number(process.env.BILLING_CHECK_INTERVAL_MS) || 3600000; // default 1 hour

const startServer = async () => {
  // Start server IMMEDIATELY - don't wait for database
  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || "development"}`);
    const configuredOrigins = [
      process.env.FRONTEND_URL,
      ...String(process.env.FRONTEND_URLS || "")
        .split(",")
        .map((origin) => origin.trim())
        .filter(Boolean),
    ].filter(Boolean);
    console.log(
      `Allowed origins: ${configuredOrigins.join(", ") || "localhost only"}`,
    );
  });

  // Initialize database in background (non-blocking)
  try {
    await pool.initDatabase();
    console.log("Database initialized successfully");

    // Start auto-billing background job after DB is ready
    processBillingTransitions(); // Run once immediately
    setInterval(processBillingTransitions, BILLING_CHECK_INTERVAL_MS);
    console.log(
      `Auto-billing check scheduled every ${Math.round(BILLING_CHECK_INTERVAL_MS / 60000)} minute(s)`,
    );
  } catch (error) {
    console.error("Database initialization failed:", error.message);
    console.error("Server is running but database may not be ready");
    // Don't exit - let server continue running
  }

  // Graceful shutdown (important for Railway restarts)
  process.on("SIGTERM", () => {
    console.log("SIGTERM received, closing server...");
    server.close(() => {
      console.log("Server closed");
      process.exit(0);
    });
  });
};

startServer();
