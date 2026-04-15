const jwt = require("jsonwebtoken");

const AUTH_SECRET = String(
  process.env.JWT_SECRET || process.env.AUTH_SECRET || "hirenext-auth-secret-change-me",
);
const TOKEN_TTL_SECONDS = Number.parseInt(process.env.AUTH_TOKEN_TTL_SECONDS || "43200", 10);

const createAuthToken = (payload) => {
  const expiresIn = Number.isFinite(TOKEN_TTL_SECONDS) ? TOKEN_TTL_SECONDS : 43200;
  return jwt.sign({ ...payload }, AUTH_SECRET, {
    algorithm: "HS256",
    expiresIn,
  });
};

const verifyAuthToken = (token) => {
  const payload = jwt.verify(String(token || ""), AUTH_SECRET, {
    algorithms: ["HS256"],
  });

  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid token payload.");
  }

  return payload;
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
