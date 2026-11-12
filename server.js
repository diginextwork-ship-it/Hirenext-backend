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

const PORT = systemPort || (process.env.NODE_ENV === "production" ? (process.env.PORT || 3000) : (process.env.PORT || 5001));

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
  } catch (error) {
    console.error("Database initialization failed:", error.message);
    // Don't exit - let server continue running to serve error responses
  }

  try {
    if (typeof pool.revertAutoBilledCandidates === "function") {
      await pool.revertAutoBilledCandidates();
    }
  } catch (revertError) {
    console.warn("Revert auto-billed candidates check:", revertError.message);
  }

  // Auto-billing is disabled: candidates can only be moved to billed status manually by admin.
  console.log(
    "Auto-billing is disabled. Candidates can only be moved to billed by admin.",
  );

  process.on("SIGTERM", () => {
    console.log("SIGTERM received, closing server...");
    server.close(() => {
      console.log("Server closed");
      process.exit(0);
    });
  });
};

startServer().catch(err => console.error("Fatal startup error:", err));
