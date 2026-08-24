process.on("uncaughtException", (err) => {
  console.error("FATAL UNCAUGHT EXCEPTION:", err);
});
process.on("unhandledRejection", (reason) => {
  console.error("FATAL UNHANDLED REJECTION:", reason);
});

const systemPort = process.env.PORT;
const dotenv = require("dotenv");
dotenv.config();

console.log("Starting server.js...");

const app = require("./src/app");
const pool = require("./src/config/db");
const { processBillingTransitions } = require("./src/routes/jobRoutes");

const PORT = systemPort || (process.env.NODE_ENV === "production" ? (process.env.PORT || 3000) : (process.env.PORT || 5001));
const BILLING_CHECK_INTERVAL_MS =
  Number(process.env.BILLING_CHECK_INTERVAL_MS) || 3600000; // default 1 hour

const startServer = async () => {
  const server = app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || "development"}`);
  });
  server.requestTimeout = 0;

  console.log("Initializing database connection...");
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
    // Don't exit - let server continue running to serve error responses
  }

  process.on("SIGTERM", () => {
    console.log("SIGTERM received, closing server...");
    server.close(() => {
      console.log("Server closed");
      process.exit(0);
    });
  });
};

startServer().catch(err => console.error("Fatal startup error:", err));
