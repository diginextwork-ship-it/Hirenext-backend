const crypto = require("crypto");

const AUTH_SECRET = String(process.env.AUTH_SECRET || "hirenext-auth-secret-change-me");
const TOKEN_TTL_SECONDS = Number.parseInt(process.env.AUTH_TOKEN_TTL_SECONDS || "43200", 10);

const toBase64Url = (value) =>
  Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");

const fromBase64Url = (value) => {
  const normalized = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  return Buffer.from(padded, "base64").toString("utf8");
};

const signPayload = (payloadSegment) =>
  crypto.createHmac("sha256", AUTH_SECRET).update(payloadSegment).digest("base64url");

const createAuthToken = (payload) => {
  const now = Math.floor(Date.now() / 1000);
  const tokenPayload = {
    ...payload,
    iat: now,
    exp: now + (Number.isFinite(TOKEN_TTL_SECONDS) ? TOKEN_TTL_SECONDS : 43200),
  };

  const payloadSegment = toBase64Url(JSON.stringify(tokenPayload));
  const signature = signPayload(payloadSegment);
  return `${payloadSegment}.${signature}`;
};

const verifyAuthToken = (token) => {
  const [payloadSegment, signature] = String(token || "").split(".");
  if (!payloadSegment || !signature) {
    throw new Error("Invalid token format.");
  }

  const expectedSignature = signPayload(payloadSegment);
  if (signature !== expectedSignature) {
    throw new Error("Invalid token signature.");
  }

  const parsedPayload = JSON.parse(fromBase64Url(payloadSegment));
  if (!parsedPayload || typeof parsedPayload !== "object") {
    throw new Error("Invalid token payload.");
  }

  const now = Math.floor(Date.now() / 1000);
  if (!parsedPayload.exp || now >= Number(parsedPayload.exp)) {
    throw new Error("Token expired.");
  }

  return parsedPayload;
};

const getTokenFromRequest = (req) => {
  const authHeader = String(req.headers.authorization || "");
  if (authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim();
  }
  return String(req.query?.token || "").trim();
};

const normalizeRoleAlias = (role) => {
  const normalized = String(role || "").trim().toLowerCase();
  if (normalized === "job adder" || normalized === "job_adder" || normalized === "team_leader") {
    return "team leader";
  }
  return normalized;
};

const requireAuth = (req, res, next) => {
  try {
    const token = getTokenFromRequest(req);
    if (!token) {
      return res.status(401).json({ message: "Authentication required." });
    }

    const payload = verifyAuthToken(token);
    req.auth = payload;
    return next();
  } catch {
    return res.status(401).json({ message: "Invalid or expired authentication token." });
  }
};

const requireRoles = (...allowedRoles) => (req, res, next) => {
  const role = normalizeRoleAlias(req.auth?.role);
  const allowed = allowedRoles.map((item) => normalizeRoleAlias(item));
  if (!allowed.includes(role)) {
    return res.status(403).json({ message: "You do not have access to this resource." });
  }
  return next();
};

const requireRecruiterOwner = (req, res, next) => {
  const ridInPath = String(req.params.rid || "").trim();
  const ridInToken = String(req.auth?.rid || "").trim();
  if (!ridInPath || !ridInToken || ridInPath !== ridInToken) {
    return res.status(403).json({ message: "You can only access your own recruiter resources." });
  }
  return next();
};

module.exports = {
  createAuthToken,
  normalizeRoleAlias,
  requireAuth,
  requireRoles,
  requireRecruiterOwner,
};
