const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");

process.env.DB_HOST = process.env.DB_HOST || "localhost";
process.env.DB_USER = process.env.DB_USER || "root";
process.env.DB_PASSWORD = process.env.DB_PASSWORD || "evoljonny";
process.env.DB_NAME = process.env.DB_NAME || "hirenext";
process.env.AUTH_SECRET =
  process.env.AUTH_SECRET || "hirenext-auth-secret-change-me";

const app = require("../src/app");
const pool = require("../src/config/db");
const { createAuthToken } = require("../src/middleware/auth");

const adminToken = createAuthToken({ role: "admin", name: "Admin" });
const teamLeaderToken = createAuthToken({
  role: "team leader",
  rid: "hnr-1",
  email: "teamleader@gmail.com",
  name: "team lead",
});
const recruiterToken = createAuthToken({
  role: "recruiter",
  rid: "hnr-2",
  email: "recruiter@gmail.com",
  name: "recruiter role",
});

let server;
let baseUrl;

const requestJson = async (path, options = {}) => {
  const response = await fetch(`${baseUrl}${path}`, options);
  const text = await response.text();
  return {
    status: response.status,
    body: text ? JSON.parse(text) : null,
  };
};

let tempResumeCounter = 0;
const buildTempResumeId = (suffix) => {
  tempResumeCounter += 1;
  const compactSuffix = String(suffix || "t")
    .replace(/[^a-z0-9]/gi, "")
    .slice(0, 6)
    .toLowerCase();
  return `res_${compactSuffix}${tempResumeCounter}`;
};

const createTempResume = async (resId) => {
  await pool.query(
    `INSERT INTO resumes_data
      (res_id, resume, rid, job_jid, resume_filename, resume_type, submitted_by_role, ats_raw_json, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      resId,
      Buffer.from("resume-verification-test"),
      "hnr-2",
      "JID-5",
      `${resId}.pdf`,
      "pdf",
      "recruiter",
      JSON.stringify({
        parsedData: {
          name: "Regression Candidate",
          email: `${resId}@example.com`,
        },
      }),
      "test",
    ],
  );
};

const cleanupTempResume = async (resId) => {
  await pool.query("DELETE FROM extra_info WHERE res_id = ? OR resume_id = ?", [
    resId,
    resId,
  ]);
  await pool.query("DELETE FROM candidate WHERE res_id = ?", [resId]);
  await pool.query("DELETE FROM job_resume_selection WHERE res_id = ?", [resId]);
  await pool.query("DELETE FROM resumes_data WHERE res_id = ?", [resId]);
};

const setCandidateRevenue = async (resId, revenue) => {
  await pool.query(
    `INSERT INTO candidate (cid, res_id, job_jid, recruiter_rid, name, revenue)
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
      revenue = VALUES(revenue),
      name = VALUES(name),
      job_jid = VALUES(job_jid),
      recruiter_rid = VALUES(recruiter_rid)`,
    [`c_${resId.replace(/^res_/, "")}`, resId, "JID-5", "hnr-2", "Revenue Candidate", revenue],
  );
};

const getLatestMoneySumId = async () => {
  const [rows] = await pool.query(
    `SELECT COALESCE(MAX(id), 0) AS maxId
     FROM money_sum`,
  );
  return Number(rows[0]?.maxId) || 0;
};

const cleanupMoneySumAfter = async (maxId) => {
  await pool.query("DELETE FROM money_sum WHERE id > ?", [maxId]);
};

const snapshotStatusRow = async (rid) => {
  const [rows] = await pool.query(
    `SELECT recruiter_rid, submitted, verified, walk_in, \`select\`, reject, joined, dropout, billed, \`left\`, pending_join
     FROM status
     WHERE recruiter_rid = ?
     LIMIT 1`,
    [rid],
  );
  return rows[0] || null;
};

const restoreStatusRow = async (rid, snapshot) => {
  if (!snapshot) {
    await pool.query("DELETE FROM status WHERE recruiter_rid = ?", [rid]);
    return;
  }

  await pool.query(
    `INSERT INTO status
      (recruiter_rid, submitted, verified, walk_in, \`select\`, reject, joined, dropout, billed, \`left\`, pending_join, last_updated)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
     ON DUPLICATE KEY UPDATE
      submitted = VALUES(submitted),
      verified = VALUES(verified),
      walk_in = VALUES(walk_in),
      \`select\` = VALUES(\`select\`),
      reject = VALUES(reject),
      joined = VALUES(joined),
      dropout = VALUES(dropout),
      billed = VALUES(billed),
      \`left\` = VALUES(\`left\`),
      pending_join = VALUES(pending_join),
      last_updated = CURRENT_TIMESTAMP`,
    [
      rid,
      snapshot.submitted,
      snapshot.verified,
      snapshot.walk_in,
      snapshot.select,
      snapshot.reject,
      snapshot.joined,
      snapshot.dropout,
      snapshot.billed,
      snapshot.left,
      snapshot.pending_join,
    ],
  );
};

before(async () => {
  await pool.query("SELECT 1");
  server = app.listen(0);
  await new Promise((resolve) => {
    server.once("listening", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

test("admin verify route accepts canonical verified and persists verify reason", async () => {
  const resId = buildTempResumeId("admin");
  await createTempResume(resId);

  try {
    const response = await requestJson(`/api/admin/resumes/${resId}/advance-status`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        status: "verified",
        verifiedReason: "checked in regression test",
      }),
    });

    assert.equal(response.status, 200);
    assert.equal(response.body?.data?.status, "verified");
    assert.equal(
      response.body?.data?.verifiedReason,
      "checked in regression test",
    );

    const [selectionRows] = await pool.query(
      `SELECT selection_status AS status, selection_note AS note
       FROM job_resume_selection
       WHERE res_id = ?
       LIMIT 1`,
      [resId],
    );
    assert.equal(selectionRows[0]?.status, "verified");
    assert.equal(selectionRows[0]?.note, "checked in regression test");

    const [extraInfoRows] = await pool.query(
      `SELECT verified_reason AS verifiedReason
       FROM extra_info
       WHERE res_id = ? OR resume_id = ?
       LIMIT 1`,
      [resId, resId],
    );
    assert.equal(
      extraInfoRows[0]?.verifiedReason,
      "checked in regression test",
    );
  } finally {
    await cleanupTempResume(resId);
  }
});

test("legacy verify aliases are normalized to canonical verified on admin and team leader routes", async () => {
  const adminResId = buildTempResumeId("alias_admin");
  const jobResId = buildTempResumeId("alias_job");
  const recruiterStatusSnapshot = await snapshotStatusRow("hnr-2");

  await createTempResume(adminResId);
  await createTempResume(jobResId);

  try {
    const adminResponse = await requestJson(
      `/api/admin/resumes/${adminResId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          status: "verfied",
          reason: "legacy typo still maps",
        }),
      },
    );

    assert.equal(adminResponse.status, 200);
    assert.equal(adminResponse.body?.data?.status, "verified");

    const jobResponse = await requestJson("/api/jobs/JID-5/resume-statuses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${teamLeaderToken}`,
      },
      body: JSON.stringify({
        resId: jobResId,
        status: "verify",
        verified_reason: "legacy alias still maps",
      }),
    });

    assert.equal(jobResponse.status, 200);
    assert.equal(jobResponse.body?.data?.status, "verified");
    assert.equal(
      jobResponse.body?.data?.verifiedReason,
      "legacy alias still maps",
    );
  } finally {
    await cleanupTempResume(adminResId);
    await cleanupTempResume(jobResId);
    await restoreStatusRow("hnr-2", recruiterStatusSnapshot);
  }
});

test("team leader billed update creates admin intake entry from candidate revenue", async () => {
  const resId = buildTempResumeId("billed");
  const previousMoneySumId = await getLatestMoneySumId();
  const recruiterStatusSnapshot = await snapshotStatusRow("hnr-2");

  await createTempResume(resId);
  await setCandidateRevenue(resId, 4321);

  try {
    const response = await requestJson("/api/jobs/JID-5/resume-statuses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${teamLeaderToken}`,
      },
      body: JSON.stringify({
        resId,
        status: "billed",
        billedReason: "candidate billed in regression test",
      }),
    });

    assert.equal(response.status, 200);
    assert.equal(response.body?.data?.status, "billed");

    const [moneyRows] = await pool.query(
      `SELECT company_rev AS companyRev, reason, entry_type AS entryType
       FROM money_sum
       WHERE id > ?
       ORDER BY id DESC
       LIMIT 1`,
      [previousMoneySumId],
    );

    assert.equal(Number(moneyRows[0]?.companyRev), 4321);
    assert.equal(moneyRows[0]?.entryType, "intake");
    assert.equal(moneyRows[0]?.reason, "candidate's bill");
  } finally {
    await cleanupMoneySumAfter(previousMoneySumId);
    await cleanupTempResume(resId);
    await restoreStatusRow("hnr-2", recruiterStatusSnapshot);
  }
});

test("verify routes enforce auth and role contract", async () => {
  const resId = buildTempResumeId("access");
  await createTempResume(resId);

  try {
    const noAuthAdmin = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "verified" }),
      },
    );
    assert.equal(noAuthAdmin.status, 401);
    assert.equal(noAuthAdmin.body?.message, "Authentication required.");

    const wrongRoleAdmin = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${teamLeaderToken}`,
        },
        body: JSON.stringify({ status: "verified" }),
      },
    );
    assert.equal(wrongRoleAdmin.status, 403);

    const recruiterOnJobRoute = await requestJson("/api/jobs/JID-5/resume-statuses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${recruiterToken}`,
      },
      body: JSON.stringify({ resId, status: "verified" }),
    });
    assert.equal(recruiterOnJobRoute.status, 403);

    const recruiterVerifyAdvance = await requestJson(
      `/api/recruiters/hnr-2/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${recruiterToken}`,
        },
        body: JSON.stringify({ status: "verified" }),
      },
    );
    assert.equal(recruiterVerifyAdvance.status, 403);
    assert.match(
      recruiterVerifyAdvance.body?.message || "",
      /cannot mark resumes as verified/i,
    );
  } finally {
    await cleanupTempResume(resId);
  }
});

after(async () => {
  await new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
  await pool.end();
});
