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

let tempEntityCounter = 0;
const buildTempId = (prefix) => {
  tempEntityCounter += 1;
  return `${prefix}_${Date.now()}_${tempEntityCounter}`.slice(0, 20);
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

const createTempRecruiter = async ({ rid, name, email, role, points = 0 }) => {
  await pool.query(
    `INSERT INTO recruiter (rid, name, email, password, role, points)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [rid, name, email, "pass123", role, points],
  );
};

const createTempJob = async ({
  jid,
  recruiterRid,
  companyName,
  roleName,
  createdAt,
}) => {
  await pool.query(
    `INSERT INTO jobs
      (jid, recruiter_rid, city, state, pincode, company_name, role_name, positions_open, access_mode, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      jid,
      recruiterRid,
      "Bengaluru",
      "Karnataka",
      "560001",
      companyName,
      roleName,
      3,
      "open",
      createdAt,
    ],
  );
};

const createPerformanceResume = async ({
  resId,
  recruiterRid,
  jobJid,
  uploadedAt,
  candidateName,
  candidatePhone,
  walkInDate = null,
  joiningDate = null,
  currentStatus = null,
  currentStatusAt = null,
  extraInfo = {},
}) => {
  await pool.query(
    `INSERT INTO resumes_data
      (res_id, resume, rid, job_jid, resume_filename, resume_type, submitted_by_role, uploaded_at, ats_raw_json, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      resId,
      Buffer.from("admin-performance-test"),
      recruiterRid,
      jobJid,
      `${resId}.pdf`,
      "pdf",
      "recruiter",
      uploadedAt,
      JSON.stringify({
        parsedData: {
          name: candidateName,
          phone: candidatePhone,
          email: `${resId}@example.com`,
        },
      }),
      "test",
    ],
  );

  await pool.query(
    `INSERT INTO candidate (cid, res_id, job_jid, recruiter_rid, name, phone, joining_date, walk_in)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [`c_${resId.replace(/^res_/, "").slice(0, 16)}`, resId, jobJid, recruiterRid, candidateName, candidatePhone, joiningDate, walkInDate],
  );

  if (currentStatus) {
    await pool.query(
      `INSERT INTO job_resume_selection
        (job_jid, res_id, selected_by_admin, selection_status, selection_note, selected_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [jobJid, resId, "test-suite", currentStatus, "test status", currentStatusAt || uploadedAt],
    );
  }

  const extraColumns = ["res_id", "resume_id", "job_jid", "recruiter_rid"];
  const extraValues = [resId, resId, jobJid, recruiterRid];
  for (const [column, value] of Object.entries(extraInfo)) {
    extraColumns.push(column);
    extraValues.push(value);
  }
  await pool.query(
    `INSERT INTO extra_info (${extraColumns.join(", ")})
     VALUES (${extraColumns.map(() => "?").join(", ")})`,
    extraValues,
  );
};

const cleanupPerformanceFixture = async ({ recruiterRid, teamLeaderRid, jobJid, resIds }) => {
  await pool.query("DELETE FROM extra_info WHERE res_id IN (?) OR resume_id IN (?)", [resIds, resIds]);
  await pool.query("DELETE FROM candidate WHERE res_id IN (?)", [resIds]);
  await pool.query("DELETE FROM job_resume_selection WHERE res_id IN (?)", [resIds]);
  await pool.query("DELETE FROM resumes_data WHERE res_id IN (?)", [resIds]);
  await pool.query("DELETE FROM jobs WHERE jid = ?", [jobJid]);
  await pool.query("DELETE FROM recruiter WHERE rid IN (?, ?)", [recruiterRid, teamLeaderRid]);
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
  await pool.initDatabase();
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

test("admin pending_joining requires joining_date without revenue", async () => {
  const resId = buildTempResumeId("pendjoin");

  await createTempResume(resId);

  try {
    const verifyResponse = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ status: "verified" }),
      },
    );
    assert.equal(verifyResponse.status, 200);

    const walkInResponse = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ status: "walk_in" }),
      },
    );
    assert.equal(walkInResponse.status, 200);

    const selectedResponse = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ status: "selected" }),
      },
    );
    assert.equal(selectedResponse.status, 200);

    const pendingJoiningResponse = await requestJson(
      `/api/admin/resumes/${resId}/advance-status`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          status: "pending_joining",
          joining_date: "2026-04-05",
        }),
      },
    );

    assert.equal(pendingJoiningResponse.status, 200);
    assert.equal(pendingJoiningResponse.body?.data?.status, "pending_joining");
    assert.equal(
      pendingJoiningResponse.body?.data?.joining_date,
      null,
    );

    const [candidateRows] = await pool.query(
      `SELECT DATE_FORMAT(joining_date, '%Y-%m-%d') AS joiningDate, revenue
       FROM candidate
       WHERE res_id = ?
       LIMIT 1`,
      [resId],
    );
    assert.equal(candidateRows[0]?.joiningDate, "2026-04-05");
    assert.equal(candidateRows[0]?.revenue, null);
  } finally {
    await cleanupTempResume(resId);
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

test("admin performance endpoint applies inclusive date filters across summary, recruiters, team leaders, and drilldown", async () => {
  const todayDate = "2037-03-30";
  const yesterdayDate = "2037-03-29";
  const monthStart = "2037-03-01";
  const monthEnd = "2037-03-31";
  const recruiterRid = buildTempId("rperf");
  const teamLeaderRid = buildTempId("tlperf");
  const jobJid = buildTempId("jobperf");
  const todayResId = buildTempResumeId("perfday");
  const yesterdayResId = buildTempResumeId("perfprev");

  await createTempRecruiter({
    rid: recruiterRid,
    name: "Perf Recruiter",
    email: `${recruiterRid}@example.com`,
    role: "recruiter",
    points: 12,
  });
  await createTempRecruiter({
    rid: teamLeaderRid,
    name: "Perf Team Lead",
    email: `${teamLeaderRid}@example.com`,
    role: "team leader",
    points: 7,
  });
  await createTempJob({
    jid: jobJid,
    recruiterRid: teamLeaderRid,
    companyName: "Perf Co",
    roleName: "QA Engineer",
    createdAt: `${todayDate} 08:30:00`,
  });

  await createPerformanceResume({
    resId: todayResId,
    recruiterRid,
    jobJid,
    uploadedAt: `${todayDate} 09:00:00`,
    candidateName: "March Thirty",
    candidatePhone: "9999991111",
    walkInDate: todayDate,
    joiningDate: todayDate,
    currentStatus: "billed",
    currentStatusAt: `${todayDate} 16:00:00`,
    extraInfo: {
      submitted_at: `${todayDate} 09:00:00`,
      verified_at: `${todayDate} 10:00:00`,
      walk_in_at: `${todayDate} 11:00:00`,
      selected_at: `${todayDate} 12:00:00`,
      pending_joining_at: `${todayDate} 13:00:00`,
      joined_at: `${todayDate} 14:00:00`,
      billed_at: `${todayDate} 15:00:00`,
    },
  });

  await createPerformanceResume({
    resId: yesterdayResId,
    recruiterRid,
    jobJid,
    uploadedAt: `${yesterdayDate} 09:00:00`,
    candidateName: "March Twenty Nine",
    candidatePhone: "9999992222",
    currentStatus: "left",
    currentStatusAt: `${yesterdayDate} 18:00:00`,
    extraInfo: {
      submitted_at: `${yesterdayDate} 09:00:00`,
      rejected_at: `${yesterdayDate} 10:00:00`,
      dropout_at: `${yesterdayDate} 11:00:00`,
      left_at: `${yesterdayDate} 12:00:00`,
    },
  });

  try {
    const todayResponse = await requestJson(
      `/api/admin/performance?startDate=${todayDate}&endDate=${todayDate}`,
      {
        headers: {
          Authorization: `Bearer ${adminToken}`,
        },
      },
    );
    assert.equal(todayResponse.status, 200);
    assert.equal(todayResponse.body?.summary?.totalSubmitted, 1);
    assert.equal(todayResponse.body?.summary?.totalVerified, 1);
    assert.equal(todayResponse.body?.summary?.totalWalkIn, 1);
    assert.equal(todayResponse.body?.summary?.totalSelected, 1);
    assert.equal(todayResponse.body?.summary?.totalPendingJoining, 1);
    assert.equal(todayResponse.body?.summary?.totalJoined, 1);
    assert.equal(todayResponse.body?.summary?.totalBilled, 1);
    assert.equal(todayResponse.body?.summary?.totalRejected, 0);
    assert.equal(todayResponse.body?.summary?.totalDropout, 0);
    assert.equal(todayResponse.body?.summary?.totalLeft, 0);
    assert.equal(todayResponse.body?.statusDrilldown?.submitted?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.verified?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.walk_in?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.selected?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.pending_joining?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.joined?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.billed?.length, 1);
    assert.equal(todayResponse.body?.statusDrilldown?.rejected?.length, 0);

    const todayRecruiter = todayResponse.body?.recruiters?.find(
      (item) => item.rid === recruiterRid,
    );
    assert.equal(todayRecruiter?.submitted, 1);
    assert.equal(todayRecruiter?.verified, 1);
    assert.equal(todayRecruiter?.walk_in, 1);
    assert.equal(todayRecruiter?.selected, 1);
    assert.equal(todayRecruiter?.pending_joining, 1);
    assert.equal(todayRecruiter?.joined, 1);
    assert.equal(todayRecruiter?.billed, 1);
    assert.equal(todayRecruiter?.rejected, 0);
    assert.equal(todayRecruiter?.dropout, 0);
    assert.equal(todayRecruiter?.left, 0);

    const todayTeamLeader = todayResponse.body?.teamLeaders?.find(
      (item) => item.rid === teamLeaderRid,
    );
    assert.equal(todayTeamLeader?.jobsCreated, 1);

    const yesterdayResponse = await requestJson(
      `/api/admin/performance?startDate=${yesterdayDate}&endDate=${yesterdayDate}`,
      {
        headers: {
          Authorization: `Bearer ${adminToken}`,
        },
      },
    );
    assert.equal(yesterdayResponse.status, 200);
    assert.equal(yesterdayResponse.body?.summary?.totalSubmitted, 1);
    assert.equal(yesterdayResponse.body?.summary?.totalVerified, 0);
    assert.equal(yesterdayResponse.body?.summary?.totalRejected, 1);
    assert.equal(yesterdayResponse.body?.summary?.totalDropout, 1);
    assert.equal(yesterdayResponse.body?.summary?.totalLeft, 1);
    assert.equal(yesterdayResponse.body?.summary?.totalBilled, 0);
    assert.equal(yesterdayResponse.body?.statusDrilldown?.submitted?.length, 1);
    assert.equal(yesterdayResponse.body?.statusDrilldown?.rejected?.length, 1);
    assert.equal(yesterdayResponse.body?.statusDrilldown?.dropout?.length, 1);
    assert.equal(yesterdayResponse.body?.statusDrilldown?.left?.length, 1);

    const yesterdayTeamLeader = yesterdayResponse.body?.teamLeaders?.find(
      (item) => item.rid === teamLeaderRid,
    );
    assert.equal(yesterdayTeamLeader?.jobsCreated, 0);

    const monthResponse = await requestJson(
      `/api/admin/performance?startDate=${monthStart}&endDate=${monthEnd}`,
      {
        headers: {
          Authorization: `Bearer ${adminToken}`,
        },
      },
    );
    assert.equal(monthResponse.status, 200);
    assert.equal(monthResponse.body?.summary?.totalSubmitted, 2);
    assert.equal(monthResponse.body?.summary?.totalVerified, 1);
    assert.equal(monthResponse.body?.summary?.totalRejected, 1);
    assert.equal(monthResponse.body?.summary?.totalDropout, 1);
    assert.equal(monthResponse.body?.summary?.totalLeft, 1);
    assert.equal(monthResponse.body?.summary?.totalBilled, 1);
  } finally {
    await cleanupPerformanceFixture({
      recruiterRid,
      teamLeaderRid,
      jobJid,
      resIds: [todayResId, yesterdayResId],
    });
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
