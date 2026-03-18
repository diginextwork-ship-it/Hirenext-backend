const express = require("express");
const pool = require("../config/db");
const { getGeminiStatus } = require("../resumeparser/resumeparser");

const router = express.Router();

router.get("/test-aiven-reach", async (_req, res) => {
  const net = require("net");
  const socket = new net.Socket();

  socket.setTimeout(10000);

  socket.connect(10036, "mysql-2148b472-diginext-78a2.d.aivencloud.com", () => {
    socket.destroy();
    res.json({
      reachable: true,
      message: "Can connect to Aiven host:port",
    });
  });

  socket.on("timeout", () => {
    socket.destroy();
    res.json({
      reachable: false,
      error: "Connection timeout - Aiven might be blocking Railway IPs",
    });
  });

  socket.on("error", (err) => {
    res.json({ reachable: false, error: err.message });
  });
});

router.get("/health", async (_req, res) => {
  // Always return 200 - don't depend on database for health check
  const health = {
    ok: true,
    server: "running",
    timestamp: new Date().toISOString(),
  };

  // Try database connection but don't fail if it's down
  try {
    await pool.query("SELECT 1");
    health.database = "connected";
  } catch (error) {
    health.database = "disconnected";
    health.databaseError = error.message;
  }

  // Add Gemini status
  try {
    health.gemini = getGeminiStatus();
  } catch (error) {
    health.gemini = "unavailable";
  }
  res.status(200).json(health);
});

module.exports = router;
