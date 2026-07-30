const dotenv = require("dotenv");
dotenv.config();

console.log("Starting server.js...");

const app = require("./src/app");
const pool = require("./src/config/db");
const { processBillingTransitions } = require("./src/routes/jobRoutes");

const PORT = process.env.PORT || 5000;
const BILLING_CHECK_INTERVAL_MS =
  Number(process.env.BILLING_CHECK_INTERVAL_MS) || 3600000; // default 1 hour

const normalizeOrigin = (value) => {
  const origin = String(value || "").trim();
  if (!origin) return "";

  try {
    return new URL(origin).origin.toLowerCase();
  } catch (_error) {
    return origin.replace(/\/+$/, "").toLowerCase();
  }
};

const startServer = async () => {
  console.log("Initializing database connection...");
  // Initialize database first to make sure it doesn't crash silently
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
    // Don't exit - let server continue running to serve error responses or other routes
  }

  // Start server
  const server = app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || "development"}`);
  });
  server.requestTimeout = 0;

  process.on("SIGTERM", () => {
    console.log("SIGTERM received, closing server...");
    server.close(() => {
      console.log("Server closed");
      process.exit(0);
    });
  });
};

startServer().catch(err => console.error("Fatal startup error:", err));

