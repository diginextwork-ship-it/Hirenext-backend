const WINDOW_MS_DEFAULT = 60 * 1000;
const MAX_REQUESTS_DEFAULT = 60;

const normalizeIp = (rawIp) => {
  if (!rawIp) return "unknown";
  if (rawIp.startsWith("::ffff:")) return rawIp.slice(7);
  return rawIp;
};

const resolveClientIp = (req) => {
  const forwarded = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  if (forwarded) return normalizeIp(forwarded);
  return normalizeIp(req.ip || req.socket?.remoteAddress || "");
};

const createRateLimiter = ({
  windowMs = WINDOW_MS_DEFAULT,
  maxRequests = MAX_REQUESTS_DEFAULT,
  keyPrefix = "global",
  message = "Too many requests. Please try again shortly.",
} = {}) => {
  const requestsByKey = new Map();

  return (req, res, next) => {
    const now = Date.now();
    const key = `${keyPrefix}:${resolveClientIp(req)}`;
    const bucket = requestsByKey.get(key);

    if (!bucket || now - bucket.windowStart >= windowMs) {
      requestsByKey.set(key, { count: 1, windowStart: now });
      return next();
    }

    if (bucket.count >= maxRequests) {
      const retryAfterSeconds = Math.max(1, Math.ceil((windowMs - (now - bucket.windowStart)) / 1000));
      res.setHeader("Retry-After", String(retryAfterSeconds));
      return res.status(429).json({ message });
    }

    bucket.count += 1;
    return next();
  };
};

module.exports = { createRateLimiter };
