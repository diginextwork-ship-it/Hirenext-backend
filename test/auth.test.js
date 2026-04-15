const { test } = require("node:test");
const assert = require("node:assert/strict");

process.env.JWT_SECRET = "test-jwt-secret";
process.env.AUTH_TOKEN_TTL_SECONDS = "3600";

const {
  createAuthToken,
  requireAuth,
  requireRoles,
  requireRecruiterOwner,
} = require("../src/middleware/auth");

const createMockResponse = () => {
  const response = {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };

  return response;
};

test("requireAuth accepts a valid bearer JWT and populates req.auth", () => {
  const token = createAuthToken({
    role: "team leader",
    rid: "RID-100",
    email: "lead@example.com",
  });
  const req = {
    headers: { authorization: `Bearer ${token}` },
    query: {},
  };
  const res = createMockResponse();
  let nextCalled = false;

  requireAuth(req, res, () => {
    nextCalled = true;
  });

  assert.equal(nextCalled, true);
  assert.equal(req.auth.role, "team leader");
  assert.equal(req.auth.rid, "RID-100");
  assert.equal(req.auth.email, "lead@example.com");
  assert.equal(typeof req.auth.iat, "number");
  assert.equal(typeof req.auth.exp, "number");
});

test("requireAuth rejects an invalid JWT", () => {
  const req = {
    headers: { authorization: "Bearer invalid.token.value" },
    query: {},
  };
  const res = createMockResponse();
  let nextCalled = false;

  requireAuth(req, res, () => {
    nextCalled = true;
  });

  assert.equal(nextCalled, false);
  assert.equal(res.statusCode, 401);
  assert.deepEqual(res.body, { message: "Invalid or expired authentication token." });
});

test("role and recruiter ownership guards still work with req.auth claims", () => {
  const req = {
    auth: { role: "job_adder", rid: "RID-100" },
    params: { rid: "RID-100" },
  };
  const res = createMockResponse();
  let nextCount = 0;

  requireRoles("team leader")(req, res, () => {
    nextCount += 1;
  });
  requireRecruiterOwner(req, res, () => {
    nextCount += 1;
  });

  assert.equal(nextCount, 2);
});
