const express = require("express");
const cors = require("cors");
const healthRoutes = require("./routes/healthRoutes");
const recruiterRoutes = require("./routes/recruiterRoutes");
const jobRoutes = require("./routes/jobRoutes");
const adminRoutes = require("./routes/adminRoutes");
const statusRoutes = require("./routes/statusRoutes");
const jdParserRoutes = require("./jdparser/jdParser");
const { createRateLimiter } = require("./middleware/rateLimiter");

const app = express();

// CORS Configuration - CRITICAL FOR PRODUCTION
const allowedOrigins = [
  "http://localhost:3000",
  "http://localhost:5173",
  "http://localhost:5174", // Vite sometimes uses this
  process.env.FRONTEND_URL, // Single frontend URL
  ...String(process.env.FRONTEND_URLS || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean), // Comma-separated URLs for multiple deployments
].filter(Boolean); // Remove undefined values

const allowVercelPreviews =
  String(process.env.ALLOW_VERCEL_PREVIEWS || "false").toLowerCase() === "true";

app.use(
  cors({
    origin: function (origin, callback) {
      // Allow requests with no origin (mobile apps, curl, Postman)
      if (!origin) return callback(null, true);

      let isAllowed = allowedOrigins.includes(origin);
      if (!isAllowed && allowVercelPreviews) {
        try {
          const hostname = new URL(origin).hostname.toLowerCase();
          isAllowed = hostname.endsWith(".vercel.app");
        } catch (_error) {
          isAllowed = false;
        }
      }

      if (isAllowed) {
        callback(null, true);
      } else {
        console.warn(`⚠ Blocked CORS request from: ${origin}`);
        callback(new Error("Not allowed by CORS"));
      }
    },
    credentials: true, // Allow cookies/auth headers
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

app.use(express.json({ limit: "25mb" }));

const authRateLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  maxRequests: 10,
  keyPrefix: "auth",
  message: "Too many login attempts. Please wait a minute and try again.",
});

const submissionRateLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  maxRequests: 10,
  keyPrefix: "resume-submit",
  message: "Too many resume submissions. Please wait a minute and retry.",
});

const parseRateLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  maxRequests: 20,
  keyPrefix: "resume-parse",
  message: "Too many parse requests. Please wait a minute and retry.",
});

app.use("/api/recruiters/login", authRateLimiter);
app.use("/api/admin/login", authRateLimiter);
app.use("/api/resumes/submit", submissionRateLimiter);
app.use("/api/applications", submissionRateLimiter);
app.use("/api/applications/parse-resume", parseRateLimiter);

const jdParseRateLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  maxRequests: 10,
  keyPrefix: "jd-parse",
  message: "Too many JD parse requests. Please wait a minute and retry.",
});
app.use("/api/jd/upload", jdParseRateLimiter);
app.use("/api/jd/parse-text", jdParseRateLimiter);

app.get("/", (_req, res) => {
  res.status(200).json({
    ok: true,
    message: "Backend is running.",
    health: "/health",
  });
});

// Routes
app.use(healthRoutes);
app.use(recruiterRoutes);
app.use(jobRoutes);
app.use(adminRoutes);
app.use(statusRoutes);
app.use("/api/jd", jdParserRoutes);

// 404 handler for API routes
app.use("/api", (_req, res) => {
  res.status(404).json({ message: "Route not found." });
});

// Global error handler
app.use((err, _req, res, _next) => {
  console.error("Error:", err);

  // Don't expose internal errors in production
  if (process.env.NODE_ENV === "production") {
    res.status(err.status || 500).json({
      message: err.status === 404 ? err.message : "Internal server error.",
    });
  } else {
    res.status(err.status || 500).json({
      message: err.message || "Internal server error.",
      stack: err.stack, // Only in development
    });
  }
});

module.exports = app;
