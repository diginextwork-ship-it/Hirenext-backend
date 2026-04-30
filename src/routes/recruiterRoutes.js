const express = require("express");
const multer = require("multer");
const pool = require("../config/db");
const {
  extractResumeAts,
  parseResumeWithAts,
  extractApplicantName,
  isImageResumeType,
} = require("../resumeparser/service");
const {
  createAuthToken,
  normalizeRoleAlias,
  requireAuth,
  requireRoles,
  requireRecruiterOwner,
} = require("../middleware/auth");
const { validateResumeFile } = require("../middleware/uploadValidation");
const {
  tableExists,
  columnExists,
  findResumeDuplicateDecision,
  buildResumeBinarySelect,
  fetchExtraInfoByResumeIds,
  storeResumeBinary,
  upsertExtraInfoFields,
  upsertCandidateFields,
  addCandidateBillIntakeEntry,
} = require("../utils/dbHelpers");
const {
  toNumberOrNull,
  toMoneyNumber,
  toMoneyOrNull,
  normalizeJobJid,
  normalizeAccessMode: _normalizeAccessMode,
  normalizePhoneForStorage,
  safeJsonOrNull,
  parseJsonField,
  escapeLike,
  extractCandidateSnapshot,
  buildJobAtsContext,
} = require("../utils/formatters");
const {
  CANONICAL_VERIFY_STATUS,
  RECRUITER_STATUS_TRANSITIONS,
  getPreviousWorkflowStatus,
  normalizeResumeStatusInput,
  normalizeWorkflowStatus,
} = require("../utils/resumeStatusFlow");
const {
  STATUS_REASON_FIELD_MAP,
  buildResumeCompatibilityFields,
  resolveStatusReasonInput,
} = require("../utils/resumeCompatibility");
const {
  getCurrentDateOnlyInBusinessTimeZone,
  isValidDateOnly,
} = require("../utils/dateTime");
const {
  TASK_ASSIGNMENT_STATUS,
  ensureTaskTables,
  getTaskAssignmentActionState,
  normalizeTaskAssignmentStatus,
  resolveEffectiveTaskAssignmentStatus,
} = require("../utils/taskAssignments");

const router = express.Router();
const buildCandidateId = (sequenceValue) => `c_${sequenceValue}`;
const uploadResume = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
});
const RESUME_UPLOAD_FIELD_NAMES = [
  "resume_file",
  "resume",
  "resumeFile",
  "file",
];

// normalizeAccessMode from shared utils returns "" for invalid; this wrapper defaults to "open"
const normalizeAccessMode = (value) => _normalizeAccessMode(value) || "open";
const resumeSourceMap = new Map([
  ["naukri", "Naukri"],
  ["indeed", "Indeed"],
  ["shine", "Shine"],
  ["instagram", "Instagram ads"],
  ["instagram ads", "Instagram ads"],
  ["instagramads", "Instagram ads"],
  ["reference", "Reference"],
  ["apna job", "Apna Job"],
  ["apnajob", "Apna Job"],
  ["job hai", "Job Hai"],
  ["jobhai", "Job Hai"],
  ["work india", "Work India"],
  ["workindia", "Work India"],
  ["linkedin", "LinkedIn"],
  ["internal database", "Internal Database"],
  ["internaldatabase", "Internal Database"],
]);
const resumeSourceOptions = [
  "Naukri",
  "Indeed",
  "Shine",
  "Instagram ads",
  "Reference",
  "Apna Job",
  "Job Hai",
  "Work India",
  "LinkedIn",
  "Internal Database",
];

const normalizeResumeSource = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "";
  return resumeSourceMap.get(normalized) || "";
};

const buildResumeProcessingState = (overrides = {}) => ({
  status: "completed",
  resumeParsed: true,
  atsCalculated: true,
  submitAllowed: true,
  ...overrides,
});

const toNonNegativeInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return parsed;
};

const resolveRevenueAmount = (...candidates) => {
  for (const candidate of candidates) {
    const parsed = Number(candidate);
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.round(parsed * 100) / 100;
    }
  }
  return null;
};

const mergeCandidateSnapshotIntoParsedData = (parsedData, candidateSnapshot = {}) => {
  const safeParsedData =
    parsedData && typeof parsedData === "object" && !Array.isArray(parsedData)
      ? parsedData
      : null;

  if (!safeParsedData) {
    return parsedData || null;
  }

  const mergedParsedData = { ...safeParsedData };
  const primaryEducation =
    Array.isArray(safeParsedData.education) && safeParsedData.education.length > 0
      ? safeParsedData.education[0]
      : {};
  const mergedEducation = {
    ...(primaryEducation && typeof primaryEducation === "object"
      ? primaryEducation
      : {}),
  };

  if (candidateSnapshot.name) {
    mergedParsedData.full_name = candidateSnapshot.name;
    mergedParsedData.fullName = candidateSnapshot.name;
    mergedParsedData.name = candidateSnapshot.name;
    mergedParsedData.candidate_name = candidateSnapshot.name;
    mergedParsedData.candidateName = candidateSnapshot.name;
    mergedParsedData.applicant_name = candidateSnapshot.name;
    mergedParsedData.applicantName = candidateSnapshot.name;
  }

  if (candidateSnapshot.phone) {
    mergedParsedData.phone = candidateSnapshot.phone;
    mergedParsedData.phone_number = candidateSnapshot.phone;
    mergedParsedData.phoneNumber = candidateSnapshot.phone;
    mergedParsedData.mobile = candidateSnapshot.phone;
    mergedParsedData.mobile_number = candidateSnapshot.phone;
    mergedParsedData.mobileNumber = candidateSnapshot.phone;
  }

  if (candidateSnapshot.email) {
    mergedParsedData.email = candidateSnapshot.email;
    mergedParsedData.mail = candidateSnapshot.email;
  }

  if (candidateSnapshot.age !== null && candidateSnapshot.age !== undefined) {
    mergedParsedData.age = String(candidateSnapshot.age);
  }

  if (candidateSnapshot.levelOfEdu) {
    mergedEducation.latest_education_level = candidateSnapshot.levelOfEdu;
    mergedEducation.latestEducationLevel = candidateSnapshot.levelOfEdu;
    mergedEducation.education_level = candidateSnapshot.levelOfEdu;
    mergedEducation.degree = candidateSnapshot.levelOfEdu;
    mergedEducation.qualification = candidateSnapshot.levelOfEdu;
  }

  if (candidateSnapshot.boardUni) {
    mergedEducation.board_university = candidateSnapshot.boardUni;
    mergedEducation.boardUniversity = candidateSnapshot.boardUni;
    mergedEducation.university = candidateSnapshot.boardUni;
    mergedEducation.university_name = candidateSnapshot.boardUni;
    mergedEducation.board = candidateSnapshot.boardUni;
  }

  if (candidateSnapshot.institutionName) {
    mergedEducation.institution_name = candidateSnapshot.institutionName;
    mergedEducation.institutionName = candidateSnapshot.institutionName;
    mergedEducation.college_name = candidateSnapshot.institutionName;
    mergedEducation.college = candidateSnapshot.institutionName;
    mergedEducation.school_name = candidateSnapshot.institutionName;
    mergedEducation.school = candidateSnapshot.institutionName;
  }

  if (Object.keys(mergedEducation).length > 0) {
    mergedParsedData.education = [mergedEducation];
  }

  return mergedParsedData;
};

const getRecruiterIdColumn = async (tableName) => {
  if (await columnExists(tableName, "recruiter_rid")) return "recruiter_rid";
  if (await columnExists(tableName, "rid")) return "rid";
  return null;
};

const normalizeRecruiterRole = (value, addjobValue) => {
  const normalized = normalizeRoleAlias(value);
  if (
    normalized === "job creator" ||
    normalized === "team leader" ||
    normalized === "team_leader"
  ) {
    return "team leader";
  }
  if (normalized === "recruiter") {
    return "recruiter";
  }
  return Boolean(addjobValue) ? "team leader" : "recruiter";
};

const authorizeRecruiterResourceView = (req, res, rid) => {
  const role = normalizeRoleAlias(req.auth?.role);
  const authRid = String(req.auth?.rid || "").trim();

  if (role === "recruiter" && authRid !== rid) {
    res
      .status(403)
      .json({ message: "You can only access your own recruiter resources." });
    return false;
  }

  if (role === "team leader" || role === "team_leader") {
    return true;
  }

  if (role !== "recruiter") {
    res
      .status(403)
      .json({ message: "You do not have access to this resource." });
    return false;
  }

  return true;
};

const isTeamLeaderRole = (role) => {
  const normalized = normalizeRoleAlias(role);
  return normalized === "team leader" || normalized === "team_leader";
};

const isTeamLeaderCreatedRole = (role) => {
  const normalized = normalizeRoleAlias(role);
  return (
    normalized === "team leader" ||
    normalized === "team_leader" ||
    normalized === "job creator"
  );
};

const checkJobAccess = async (recruiterId, jobId, role = "recruiter") => {
  const safeJobId = normalizeJobJid(jobId);
  if (!safeJobId) {
    return { canAccess: false, reason: "job_jid is required." };
  }

  const hasAccessModeColumn = await columnExists("jobs", "access_mode");
  const hasRecruiterRoleColumn = await columnExists("recruiter", "role");
  const [jobRows] = await pool.query(
    `SELECT
      j.jid,
      j.company_name,
      ${hasAccessModeColumn ? "j.access_mode" : "'open'"} AS access_mode,
      ${
        hasRecruiterRoleColumn
          ? "creator.role AS creatorRole"
          : "NULL AS creatorRole"
      }
    FROM jobs j
    LEFT JOIN recruiter creator
      ON creator.rid = j.recruiter_rid
    WHERE jid = ?
    LIMIT 1`,
    [safeJobId],
  );

  if (jobRows.length === 0) {
    return { canAccess: false, reason: "Job not found", jobDetails: null };
  }

  const job = jobRows[0];
  const accessMode = normalizeAccessMode(job.access_mode);
  const normalizedRole = normalizeRoleAlias(role);
  const creatorRole = normalizeRoleAlias(job.creatorRole);
  const jobDetails = {
    jid: String(job.jid || "").trim(),
    company_name: job.company_name || "",
    access_mode: accessMode,
  };

  if (
    isTeamLeaderRole(normalizedRole) &&
    (!hasRecruiterRoleColumn || isTeamLeaderCreatedRole(creatorRole))
  ) {
    return {
      canAccess: true,
      reason: "Team leaders can access all jobs created by team leaders",
      jobDetails,
    };
  }

  if (accessMode === "open") {
    return {
      canAccess: true,
      reason: "Job is open to all recruiters",
      jobDetails,
    };
  }

  const [accessRows] = await pool.query(
    `SELECT id
     FROM job_recruiter_access
     WHERE job_jid = ? AND recruiter_rid = ? AND is_active = TRUE
     LIMIT 1`,
    [safeJobId, recruiterId],
  );

  if (accessRows.length > 0) {
    return {
      canAccess: true,
      reason: "You have been granted access to this job",
      jobDetails,
    };
  }

  return {
    canAccess: false,
    reason: "This job is restricted and you don't have access",
    jobDetails,
  };
};

const pickUploadedResumeFile = (req) => {
  if (req.file?.buffer?.length) {
    return req.file;
  }

  const groupedFiles =
    req.files && typeof req.files === "object" && !Array.isArray(req.files)
      ? req.files
      : {};

  for (const fieldName of RESUME_UPLOAD_FIELD_NAMES) {
    const candidates = Array.isArray(groupedFiles[fieldName])
      ? groupedFiles[fieldName]
      : [];
    const match = candidates.find((file) => file?.buffer?.length);
    if (match) return match;
  }

  return null;
};

const runResumeUpload = (req, res) =>
  new Promise((resolve, reject) => {
    uploadResume.fields(
      RESUME_UPLOAD_FIELD_NAMES.map((name) => ({ name, maxCount: 1 })),
    )(req, res, (error) => {
      if (error) return reject(error);
      req.file = pickUploadedResumeFile(req);
      return resolve();
    });
  });

const getRecruiterSummary = async (rid) => {
  const hasSuccessColumn = await columnExists("recruiter", "success");
  const hasPointsColumn = await columnExists("recruiter", "points");
  let success = 0;
  let points = 0;
  let thisMonth = 0;

  if (hasSuccessColumn) {
    const [rows] = await pool.query(
      "SELECT COALESCE(success, 0) AS success FROM recruiter WHERE rid = ? LIMIT 1",
      [rid],
    );
    success = Number(rows?.[0]?.success) || 0;
  }

  if (hasPointsColumn) {
    const [rows] = await pool.query(
      "SELECT COALESCE(points, 0) AS points FROM recruiter WHERE rid = ? LIMIT 1",
      [rid],
    );
    points = Number(rows?.[0]?.points) || 0;
  }

  const hasClicksTable = await tableExists("recruiter_candidate_clicks");
  if (hasClicksTable) {
    const recruiterIdColumn = await getRecruiterIdColumn(
      "recruiter_candidate_clicks",
    );
    if (!recruiterIdColumn) {
      return { success, points, thisMonth };
    }

    const [monthRows] = await pool.query(
      `SELECT COUNT(*) AS clicks
       FROM recruiter_candidate_clicks
       WHERE ${recruiterIdColumn} = ?
         AND YEAR(created_at) = YEAR(CURDATE())
         AND MONTH(created_at) = MONTH(CURDATE())`,
      [rid],
    );

    thisMonth = Number(monthRows?.[0]?.clicks) || 0;
  }

  return { success, points, thisMonth };
};

const buildRecruiterTaskPayload = (rows, recruiterRid) => {
  const taskMap = new Map();

  for (const row of rows) {
    const taskId = Number(row.taskId) || null;
    if (!taskId) continue;

    const effectiveStatus = resolveEffectiveTaskAssignmentStatus(
      row.assignmentStatus,
      row.assignmentDate,
    );
    const actionState = getTaskAssignmentActionState(row.assignmentDate);

    if (!taskMap.has(taskId)) {
      taskMap.set(taskId, {
        id: taskId,
        heading: row.heading || "",
        description: row.description || "",
        createdAt: row.taskCreatedAt || null,
        updatedAt: row.taskUpdatedAt || null,
        assignmentId: Number(row.assignmentId) || null,
        assignedAt: row.assignedAt || null,
        assignmentDate: row.assignmentDate || null,
        rescheduledFromDate: row.rescheduledFromDate || null,
        rescheduledAt: row.rescheduledAt || null,
        rescheduledByRid: row.rescheduledByRid || null,
        rescheduledByName: row.rescheduledByName || null,
        recruiterRid,
        status: effectiveStatus,
        rawStatus: normalizeTaskAssignmentStatus(row.assignmentStatus),
        actedAt: row.actedAt || null,
        isActionableToday: actionState.isActionableToday,
        isScheduledForFuture: actionState.isScheduledForFuture,
        recruiters: [],
      });
    }

    taskMap.get(taskId).recruiters.push({
      recruiterRid: row.memberRid || null,
      recruiterName: row.memberName || null,
      recruiterEmail: row.memberEmail || null,
      isSelf: row.memberRid === recruiterRid,
    });
  }

  return Array.from(taskMap.values()).sort((a, b) => {
    const left = new Date(b.assignedAt || b.createdAt || 0).getTime();
    const right = new Date(a.assignedAt || a.createdAt || 0).getTime();
    return left - right;
  });
};

const ensureRecruiterSalaryHistoryTable = async (connection = pool) => {
  await connection.query(
    `CREATE TABLE IF NOT EXISTS recruiter_salary_history (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      recruiter_rid VARCHAR(20) NOT NULL,
      monthly_salary DECIMAL(12,2) NOT NULL,
      daily_salary DECIMAL(12,2) NOT NULL,
      effective_from DATE NOT NULL,
      created_by VARCHAR(50) NOT NULL DEFAULT 'admin',
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_recruiter_salary_history_staff_effective (recruiter_rid, effective_from),
      CONSTRAINT fk_recruiter_salary_history_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
  );
};

router.post(
  "/api/recruiters",
  requireAuth,
  requireRoles("admin"),
  async (req, res) => {
    const { name, email, password, role, monthlySalary } = req.body || {};

    if (!name || !email || !password || !String(role || "").trim()) {
      return res.status(400).json({
        message: "name, email, password, and role are required.",
      });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const normalizedMonthlySalary = String(monthlySalary ?? "").trim();
    const monthlySalaryAmount = toMoneyOrNull(normalizedMonthlySalary);
    const dailySalaryAmount =
      monthlySalaryAmount === null
        ? null
        : Math.round((monthlySalaryAmount / 30) * 100) / 100;
    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailPattern.test(normalizedEmail)) {
      return res.status(400).json({
        message: "Email must be a valid email address.",
      });
    }
    if (normalizedMonthlySalary.length > 120) {
      return res.status(400).json({
        message: "monthlySalary must not exceed 120 characters.",
      });
    }
    if (normalizedMonthlySalary && monthlySalaryAmount === null) {
      return res.status(400).json({
        message: "monthlySalary must be a valid non-negative number.",
      });
    }

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      await ensureRecruiterSalaryHistoryTable(connection);

      const [existing] = await connection.query(
        `SELECT rid
       FROM recruiter
       WHERE LOWER(email) = ?
       LIMIT 1`,
        [normalizedEmail],
      );
      if (existing.length > 0) {
        await connection.rollback();
        return res.status(409).json({
          message: "Recruiter email already exists.",
        });
      }

      const [rows] = await connection.query(
        "SELECT rid FROM recruiter WHERE rid LIKE 'hnr-%' ORDER BY CAST(SUBSTRING(rid, 5) AS UNSIGNED) DESC LIMIT 1 FOR UPDATE",
      );

      const lastRid = rows.length > 0 ? rows[0].rid : null;
      const nextNumber = lastRid
        ? Number.parseInt(lastRid.replace("hnr-", ""), 10) + 1
        : 1;
      const rid = `hnr-${nextNumber}`;

      const normalizedRole = String(role).trim().toLowerCase();
      const allowedRoles = new Set(["team leader", "team_leader", "recruiter"]);
      if (!allowedRoles.has(normalizedRole)) {
        await connection.rollback();
        return res.status(400).json({
          message:
            "role must be one of 'team leader', 'team_leader', or 'recruiter'.",
        });
      }

      const canAddJob =
        normalizedRole === "team leader" || normalizedRole === "team_leader";
      const hasRoleColumn = await columnExists("recruiter", "role");
      const hasAddJobColumn = await columnExists("recruiter", "addjob");
      const hasPointsColumn = await columnExists("recruiter", "points");
      const hasSalaryColumn = await columnExists("recruiter", "salary");
      const hasMonthlySalaryColumn = await columnExists(
        "recruiter",
        "monthly_salary",
      );
      const hasDailySalaryColumn = await columnExists(
        "recruiter",
        "daily_salary",
      );
      const insertColumns = ["rid", "name", "email", "password"];
      const insertValues = [rid, name.trim(), normalizedEmail, password];

      if (!hasRoleColumn) {
        await connection.rollback();
        return res.status(500).json({
          message: "Recruiter role column is required to create a user.",
        });
      }
      insertColumns.push("role");
      insertValues.push(normalizedRole);
      if (hasAddJobColumn) {
        insertColumns.push("addjob");
        insertValues.push(canAddJob);
      }
      if (hasPointsColumn) {
        insertColumns.push("points");
        insertValues.push(0);
      }
      if (hasSalaryColumn) {
        insertColumns.push("salary");
        insertValues.push(normalizedMonthlySalary || null);
      }
      if (hasMonthlySalaryColumn) {
        insertColumns.push("monthly_salary");
        insertValues.push(monthlySalaryAmount);
      }
      if (hasDailySalaryColumn) {
        insertColumns.push("daily_salary");
        insertValues.push(dailySalaryAmount);
      }

      const placeholders = insertColumns.map(() => "?").join(", ");
      await connection.query(
        `INSERT INTO recruiter (${insertColumns.join(", ")}) VALUES (${placeholders})`,
        insertValues,
      );

      if (monthlySalaryAmount !== null) {
        await connection.query(
          `INSERT INTO recruiter_salary_history
            (recruiter_rid, monthly_salary, daily_salary, effective_from, created_by)
           VALUES (?, ?, ?, ?, ?)`,
          [
            rid,
            monthlySalaryAmount,
            dailySalaryAmount,
            getCurrentDateOnlyInBusinessTimeZone(),
            "admin-create",
          ],
        );
      }

      await connection.commit();
      return res.status(201).json({
        message: "Recruiter created successfully.",
        recruiter: {
          rid,
          name: name.trim(),
          email: normalizedEmail,
          role: normalizedRole,
          addjob: canAddJob,
          salary: normalizedMonthlySalary || null,
          monthlySalary: monthlySalaryAmount,
          dailySalary: dailySalaryAmount,
        },
      });
    } catch (error) {
      await connection.rollback();
      if (error && error.code === "ER_DUP_ENTRY") {
        return res.status(409).json({ message: "Duplicate recruiter entry." });
      }

      return res.status(500).json({
        message: "Failed to create recruiter.",
        error: error.message,
      });
    } finally {
      connection.release();
    }
  },
);

router.post("/api/recruiters/login", async (req, res) => {
  const { email, password } = req.body || {};

  if (!email || !password) {
    return res
      .status(400)
      .json({ message: "email and password are required." });
  }

  try {
    const hasRoleColumn = await columnExists("recruiter", "role");
    const hasAddJobColumn = await columnExists("recruiter", "addjob");
    const hasPasswordChangedColumn = await columnExists(
      "recruiter",
      "password_changed",
    );
    const hasAccountStatusColumn = await columnExists(
      "recruiter",
      "account_status",
    );
    const selectRole = hasRoleColumn ? "role" : "NULL AS role";
    const selectAddJob = hasAddJobColumn ? "addjob" : "0 AS addjob";
    const selectPasswordChanged = hasPasswordChangedColumn
      ? "password_changed"
      : "1 AS password_changed";
    const selectAccountStatus = hasAccountStatusColumn
      ? "COALESCE(NULLIF(TRIM(account_status), ''), 'active') AS account_status"
      : "'active' AS account_status";

    const [rows] = await pool.query(
      `SELECT rid, name, email, ${selectRole}, ${selectAddJob}, ${selectPasswordChanged}, ${selectAccountStatus}
       FROM recruiter
       WHERE email = ? AND password = ?
       LIMIT 1`,
      [email.trim().toLowerCase(), password],
    );

    if (rows.length === 0) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const recruiter = rows[0];
    if (String(recruiter.account_status || "active").trim().toLowerCase() === "inactive") {
      return res.status(403).json({
        message: "This account is deactivated. Please contact the admin.",
      });
    }
    const recruiterRole = normalizeRecruiterRole(
      recruiter.role,
      recruiter.addjob,
    );

    const token = createAuthToken({
      role: recruiterRole,
      rid: recruiter.rid,
      email: recruiter.email,
      name: recruiter.name,
    });

    return res.status(200).json({
      message: "Login successful.",
      token,
      recruiter: {
        rid: recruiter.rid,
        name: recruiter.name,
        email: recruiter.email,
        role: recruiterRole,
        addjob: Boolean(recruiter.addjob),
        passwordChanged: Boolean(recruiter.password_changed),
      },
    });
  } catch (error) {
    return res.status(500).json({
      message: "Login failed.",
      error: error.message,
    });
  }
});

router.get(
  "/api/recruiters/list",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  async (req, res) => {
    const rawSearch = String(req.query?.search || "").trim();
    const safeLike = `%${rawSearch.replace(/[%_]/g, "\\$&")}%`;

    try {
      const hasPointsColumn = await columnExists("recruiter", "points");
      const hasRoleColumn = await columnExists("recruiter", "role");
      const pointsSelect = hasPointsColumn ? "COALESCE(points, 0)" : "0";
      const roleFilter = hasRoleColumn
        ? "AND LOWER(TRIM(role)) = 'recruiter'"
        : "";

      const query = rawSearch
        ? `SELECT rid, name, email, ${pointsSelect} AS points
           FROM recruiter
           WHERE 1 = 1
             ${roleFilter}
             AND (name LIKE ? ESCAPE '\\' OR email LIKE ? ESCAPE '\\' OR rid LIKE ? ESCAPE '\\')
           ORDER BY name ASC, rid ASC`
        : `SELECT rid, name, email, ${pointsSelect} AS points
           FROM recruiter
           WHERE 1 = 1
             ${roleFilter}
           ORDER BY name ASC, rid ASC`;

      const params = rawSearch ? [safeLike, safeLike, safeLike] : [];
      const [rows] = await pool.query(query, params);

      return res.status(200).json({
        recruiters: rows.map((row) => ({
          rid: row.rid,
          name: row.name,
          email: row.email,
          points: Number(row.points) || 0,
        })),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiters list.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/accessible-jobs",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.params.rid || "").trim();
    if (!rid) {
      return res.status(400).json({ message: "rid is required." });
    }

    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    const locationFilter = String(req.query?.location || "").trim();
    const companyFilter = String(req.query?.company || "").trim();
    const searchFilter = String(req.query?.search || "").trim();
    const limit = Math.min(toNonNegativeInt(req.query?.limit, 10), 100);
    const offset = toNonNegativeInt(req.query?.offset, 0);

    try {
      const hasAccessModeColumn = await columnExists("jobs", "access_mode");
      const hasRecruiterRoleColumn = await columnExists("recruiter", "role");
      const authRole = normalizeRoleAlias(req.auth?.role);
      const teamLeaderScope = isTeamLeaderRole(authRole);

      const whereClauses = [];
      if (teamLeaderScope) {
        whereClauses.push(
          hasRecruiterRoleColumn
            ? "LOWER(TRIM(COALESCE(creator.role, ''))) IN ('team leader', 'team_leader', 'job creator')"
            : "1 = 1",
        );
      } else {
        whereClauses.push(
          hasAccessModeColumn
            ? "(j.access_mode = 'open' OR (j.access_mode = 'restricted' AND jra.id IS NOT NULL))"
            : "1 = 1",
        );
      }
      const whereParams = [rid];

      if (locationFilter) {
        const safeLocation = `%${escapeLike(locationFilter)}%`;
        whereClauses.push("j.city LIKE ? ESCAPE '\\\\'");
        whereParams.push(safeLocation);
      }

      if (companyFilter) {
        const safeCompany = `%${escapeLike(companyFilter)}%`;
        whereClauses.push("j.company_name LIKE ? ESCAPE '\\\\'");
        whereParams.push(safeCompany);
      }

      if (searchFilter) {
        const safeSearch = `%${escapeLike(searchFilter)}%`;
        whereClauses.push(
          "(j.company_name LIKE ? ESCAPE '\\\\' OR j.role_name LIKE ? ESCAPE '\\\\')",
        );
        whereParams.push(safeSearch, safeSearch);
      }

      const whereSql = whereClauses.join(" AND ");

      const [jobs] = await pool.query(
        `SELECT DISTINCT
          j.jid,
          j.company_name,
          j.role_name,
          j.city,
          j.state,
          j.pincode,
          j.salary,
          j.positions_open,
          ${hasAccessModeColumn ? "j.access_mode" : "'open'"} AS access_mode,
          j.skills,
          j.job_description,
          j.experience,
          j.qualification,
          j.benefits,
          j.created_at
        FROM jobs j
        LEFT JOIN recruiter creator
          ON creator.rid = j.recruiter_rid
        LEFT JOIN job_recruiter_access jra
          ON j.jid = jra.job_jid
         AND jra.recruiter_rid = ?
         AND jra.is_active = TRUE
        WHERE ${whereSql}
        ORDER BY j.created_at DESC
        LIMIT ? OFFSET ?`,
        [...whereParams, limit, offset],
      );

      const [countRows] = await pool.query(
        `SELECT COUNT(DISTINCT j.jid) AS total
         FROM jobs j
         LEFT JOIN recruiter creator
           ON creator.rid = j.recruiter_rid
         LEFT JOIN job_recruiter_access jra
           ON j.jid = jra.job_jid
          AND jra.recruiter_rid = ?
          AND jra.is_active = TRUE
         WHERE ${whereSql}`,
        whereParams,
      );

      const total = Number(countRows?.[0]?.total) || 0;
      return res.status(200).json({
        jobs: jobs.map((job) => ({
          jid: String(job.jid || "").trim(),
          company_name: job.company_name || "",
          role_name: job.role_name || "",
          city: job.city || "",
          state: job.state || "",
          pincode: job.pincode || "",
          salary: job.salary || null,
          positions_open: Number(job.positions_open) || 0,
          access_mode: normalizeAccessMode(job.access_mode),
          skills: job.skills || "",
          job_description: job.job_description || "",
          experience: job.experience || "",
          qualification: job.qualification || "",
          benefits: job.benefits || "",
          created_at: job.created_at,
        })),
        total,
        hasMore: offset + jobs.length < total,
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch accessible jobs.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/can-access/:jid",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.params.rid || "").trim();
    const safeJobId = normalizeJobJid(req.params.jid);
    if (!rid) {
      return res.status(400).json({ message: "rid is required." });
    }
    if (!safeJobId) {
      return res.status(400).json({ message: "jid is required." });
    }

    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      const access = await checkJobAccess(rid, safeJobId, req.auth?.role);
      return res.status(200).json({
        canAccess: Boolean(access.canAccess),
        reason: access.reason,
        jobDetails: access.jobDetails || null,
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to validate job access.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/resumes/submit",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    try {
      await runResumeUpload(req, res);
    } catch (error) {
      if (error?.code === "LIMIT_FILE_SIZE") {
        return res.status(400).json({
          success: false,
          error: "Resume file size must be 5MB or less.",
        });
      }
      if (error?.code === "LIMIT_FIELD_VALUE") {
        return res.status(400).json({
          success: false,
          error:
            "Resume upload payload is too large. Submit the file directly without an oversized text payload.",
        });
      }
      return res.status(400).json({
        success: false,
        error: "Invalid resume upload payload.",
      });
    }

    const recruiterRid = String(req.body?.recruiter_rid || "").trim();
    const authRid = String(req.auth?.rid || "").trim();
    const submitterRole = normalizeRoleAlias(req.auth?.role);
    const safeJobId = normalizeJobJid(req.body?.job_jid ?? req.body?.jid);
    const resumeSource = normalizeResumeSource(req.body?.source);
    const rawOfficeLocationOption = String(
      req.body?.office_location_option ?? req.body?.officeLocationOption ?? "",
    )
      .trim()
      .toLowerCase();
    const requestedOfficeLocationCity = String(
      req.body?.office_location_city ?? req.body?.officeLocationCity ?? "",
    ).trim();
    const officeLocationOption =
      rawOfficeLocationOption === "manual" ||
      rawOfficeLocationOption === "enter_manually"
        ? "manual"
        : "jd";
    const officeLocationCity =
      officeLocationOption === "manual" ? requestedOfficeLocationCity : null;

    if (!authRid || !recruiterRid || authRid !== recruiterRid) {
      return res.status(403).json({
        success: false,
        error: "recruiter_rid must match logged-in recruiter.",
      });
    }

    if (!safeJobId) {
      return res.status(400).json({
        success: false,
        error: "job_jid is required.",
      });
    }

    if (!resumeSource) {
      return res.status(400).json({
        success: false,
        error: `source is required. Allowed values: ${resumeSourceOptions.join(", ")}.`,
      });
    }

    if (officeLocationOption === "manual" && !officeLocationCity) {
      return res.status(400).json({
        success: false,
        error: "office_location_city is required when entering the office location manually.",
      });
    }

    try {
      const access = await checkJobAccess(recruiterRid, safeJobId, submitterRole);
      if (!access.canAccess) {
        return res.status(403).json({
          success: false,
          error: "You don't have permission to submit resumes for this job",
          canAccess: false,
        });
      }

      const resumeFile = req.file;
      if (!resumeFile || !resumeFile.buffer || resumeFile.buffer.length === 0) {
        return res.status(400).json({
          success: false,
          error: "A resume file is required.",
        });
      }

      const originalName = String(resumeFile.originalname || "").trim();
      const validation = validateResumeFile({
        filename: originalName,
        mimetype: resumeFile.mimetype,
        buffer: resumeFile.buffer,
        maxBytes: 5 * 1024 * 1024,
      });

      if (!validation.ok) {
        return res.status(400).json({
          success: false,
          error: validation.message,
        });
      }

      const [jobRows] = await pool.query(
        `SELECT
        jid,
        role_name,
        company_name,
        job_description,
        skills,
        qualification,
        benefits,
        experience,
        city,
        state,
        pincode
      FROM jobs
      WHERE jid = ?
      LIMIT 1`,
        [safeJobId],
      );
      if (jobRows.length === 0) {
        return res.status(404).json({
          success: false,
          error: "Job not found.",
        });
      }

      const clientParsedData = parseJsonField(req.body?.parsedData);
      const clientAtsScore = toNumberOrNull(
        req.body?.atsScore ?? req.body?.ats_score,
      );
      const clientAtsMatch = toNumberOrNull(
        req.body?.atsMatchPercentage ?? req.body?.ats_match_percentage,
      );
      const clientAtsRawJson = parseJsonField(
        req.body?.atsRawJson ?? req.body?.ats_raw_json,
      );
      const shouldSkipParsing =
        clientParsedData ||
        clientAtsRawJson ||
        clientAtsScore !== null ||
        clientAtsMatch !== null;
      const isImageResume = isImageResumeType(validation.extension);

      const parsed = shouldSkipParsing
        ? {
            ok: true,
            parsedData: clientParsedData || null,
            atsScore: clientAtsScore ?? null,
            atsMatchPercentage: clientAtsMatch ?? null,
            atsRawJson: clientAtsRawJson || null,
          }
        : await parseResumeWithAts({
            resumeBuffer: resumeFile.buffer,
            resumeFilename: originalName,
            jobDescription: buildJobAtsContext(jobRows[0]),
          });

      if (!parsed.ok) {
        return res.status(503).json({
          success: false,
          error: parsed.message,
        });
      }

      const candidateSnapshot = extractCandidateSnapshot({
        source: req.body,
        parsedData: parsed.parsedData,
        fallback: {
          name: extractApplicantName(parsed.parsedData) || "",
          jobJid: safeJobId,
          recruiterRid,
        },
      });
      const candidateName = candidateSnapshot.name;
      const phone = candidateSnapshot.phone;
      const email = candidateSnapshot.email;
      const latestEducationLevel = candidateSnapshot.levelOfEdu;
      const boardUniversity = candidateSnapshot.boardUni;
      const normalizedBoardUniversity =
        String(boardUniversity || "").trim() || null;
      const institutionName = candidateSnapshot.institutionName;
      const location = String(candidateSnapshot.location || "").trim() || null;
      const age = candidateSnapshot.age;
      const submittedReason = String(
        req.body?.submitted_reason ??
          req.body?.submittedReason ??
          req.body?.notes ??
          "",
      ).trim();
      const storedParsedData = mergeCandidateSnapshotIntoParsedData(
        parsed.parsedData,
        candidateSnapshot,
      );

      if (
        !candidateName ||
        !phone ||
        !email ||
        !latestEducationLevel ||
        !institutionName ||
        age === null
      ) {
        return res.status(400).json({
          success: false,
          error:
            "Resume parsing could not fill all required fields. Please provide candidate_name, phone, email, latest_education_level, institution_name, and age.",
        });
      }

      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        return res.status(400).json({
          success: false,
          error: "email must be valid.",
        });
      }

      if (!/^\d{10}$/.test(phone)) {
        return res.status(400).json({
          success: false,
          error: "phone must be exactly 10 digits.",
        });
      }

      if (!Number.isFinite(age) || age < 18 || age > 100) {
        return res.status(400).json({
          success: false,
          error: "age must be between 18 and 100.",
        });
      }

      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        const hasSourceColumn = await columnExists("resumes_data", "source");
        const duplicateCheck = await findResumeDuplicateDecision(connection, {
          candidateName,
          phone,
          email,
        });
        if (!duplicateCheck.allowSubmission) {
          await connection.rollback();
          return res.status(409).json({
            success: false,
            error: "Duplicate entry of resume already exists.",
            existingResume: duplicateCheck.blockingMatch,
          });
        }

        const [sequenceResult] = await connection.query(
          "INSERT INTO resume_id_sequence VALUES ()",
        );
        const sequenceValue = Number(sequenceResult.insertId);
        const resId = `res_${sequenceValue}`;
        const cid = buildCandidateId(sequenceValue);

        await connection.query(
          `INSERT INTO applications
          (
            job_jid,
            res_id,
            resume_filename,
            resume_parsed_data,
            ats_score,
            ats_match_percentage,
            ats_raw_json
          )
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [
            safeJobId,
            resId,
            originalName,
            safeJsonOrNull(storedParsedData),
            parsed.atsScore,
            parsed.atsMatchPercentage,
            safeJsonOrNull({
              ats_score: parsed.atsScore,
              ats_match_percentage: parsed.atsMatchPercentage,
              ats_details: parsed.atsRawJson,
              parsed_data: storedParsedData,
            }),
          ],
        );

        const resumeInsertColumns = [
          "res_id",
          "rid",
          "job_jid",
          "resume_filename",
          "resume_type",
        ];
        const resumeInsertValuesSql = ["?", "?", "?", "?", "?"];
        const resumeInsertValues = [
          resId,
          recruiterRid,
          safeJobId,
          originalName,
          validation.extension,
        ];

        if (await columnExists("resumes_data", "resume")) {
          resumeInsertColumns.splice(3, 0, "resume");
          resumeInsertValuesSql.splice(3, 0, "?");
          resumeInsertValues.splice(3, 0, resumeFile.buffer);
        }

        if (hasSourceColumn) {
          resumeInsertColumns.push("source");
          resumeInsertValuesSql.push("?");
          resumeInsertValues.push(resumeSource);
        }

        resumeInsertColumns.push(
          "submitted_by_role",
          "ats_score",
          "ats_match_percentage",
          "ats_raw_json",
        );
        resumeInsertValuesSql.push("'recruiter'", "?", "?", "?");
        resumeInsertValues.push(
          parsed.atsScore,
          parsed.atsMatchPercentage,
          safeJsonOrNull({
            ats_score: parsed.atsScore,
            ats_match_percentage: parsed.atsMatchPercentage,
            ats_details: parsed.atsRawJson,
            parsed_data: storedParsedData,
          }),
        );

        await connection.query(
          `INSERT INTO resumes_data (${resumeInsertColumns.join(", ")}) VALUES (${resumeInsertValuesSql.join(", ")})`,
          resumeInsertValues,
        );
        await storeResumeBinary(connection, resId, resumeFile.buffer);

        await upsertCandidateFields(connection, {
          cid,
          resId,
          jobJid: safeJobId,
          recruiterRid,
          name: candidateName,
          phone,
          email,
          levelOfEdu: latestEducationLevel,
          boardUni: normalizedBoardUniversity,
          institutionName,
          location,
          age,
        });

        await connection.query(
          `INSERT INTO status (recruiter_rid, submitted, last_updated)
         VALUES (?, 1, CURRENT_TIMESTAMP)
         ON DUPLICATE KEY UPDATE
           submitted = COALESCE(submitted, 0) + 1,
           last_updated = CURRENT_TIMESTAMP`,
          [recruiterRid],
        );

        await upsertExtraInfoFields(connection, {
          resId,
          recruiterRid,
          jobJid: safeJobId,
          candidateName,
          email,
          phone,
          officeLocationCity,
          submittedReason: String(submittedReason || "").trim() || null,
          submittedAt: "__CURRENT_TIMESTAMP__",
        });

        const [statusRows] = await connection.query(
          `SELECT COALESCE(submitted, 0) AS submittedCount
         FROM status
         WHERE recruiter_rid = ?
         LIMIT 1`,
          [recruiterRid],
        );

        await connection.commit();
        return res.status(201).json({
          success: true,
          message: "Resume submitted successfully",
          resumeId: resId,
          atsScore: parsed.atsScore,
          atsMatchPercentage: parsed.atsMatchPercentage,
          submittedCount: Number(statusRows?.[0]?.submittedCount) || 0,
          companyName: jobRows[0]?.company_name || null,
          officeLocationCity,
        });
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }
    } catch (error) {
      return res.status(500).json({
        success: false,
        error: "Failed to submit resume.",
        details: error.message,
      });
    }
  },
);

router.post(
  "/api/recruiters/:rid/resumes",
  requireAuth,
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;
    const {
      job_jid,
      resumeBase64,
      resumeFilename,
      resumeMimeType,
      source,
      candidate_location,
      candidateLocation,
      location,
    } = req.body || {};

    if (!job_jid || !resumeBase64 || !resumeFilename) {
      return res.status(400).json({
        message: "job_jid, resumeBase64, and resumeFilename are required.",
      });
    }

    const safeJobId = normalizeJobJid(job_jid);
    const resumeSource = normalizeResumeSource(source);
    if (!safeJobId) {
      return res.status(400).json({ message: "job_jid is required." });
    }

    if (!resumeSource) {
      return res.status(400).json({
        message: `source is required. Allowed values: ${resumeSourceOptions.join(", ")}.`,
      });
    }

    const normalizedFilename = String(resumeFilename).trim();

    try {
      const hasRoleColumn = await columnExists("recruiter", "role");
      const hasAddJobColumn = await columnExists("recruiter", "addjob");
      const recruiterSelectFields = [];
      if (hasRoleColumn) recruiterSelectFields.push("role");
      if (hasAddJobColumn) recruiterSelectFields.push("addjob");
      if (recruiterSelectFields.length === 0)
        recruiterSelectFields.push("NULL AS role");

      const [recruiterRows] = await pool.query(
        `SELECT ${recruiterSelectFields.join(", ")}
       FROM recruiter
       WHERE rid = ?
       LIMIT 1`,
        [rid],
      );

      if (recruiterRows.length === 0) {
        return res.status(404).json({ message: "Recruiter not found." });
      }

      const recruiterRole = normalizeRecruiterRole(
        recruiterRows[0].role,
        recruiterRows[0].addjob,
      );
      if (recruiterRole !== "recruiter") {
        return res.status(403).json({
          message: "Only recruiter role can add resumes.",
        });
      }

      const [jobRows] = await pool.query(
        `SELECT
        jid,
        role_name,
        company_name,
        job_description,
        skills,
        qualification,
        benefits,
        experience,
        city,
        state,
        pincode
      FROM jobs
      WHERE jid = ?
      LIMIT 1`,
        [safeJobId],
      );
      if (jobRows.length === 0) {
        return res.status(404).json({ message: "Job not found." });
      }

      const base64Payload = String(resumeBase64).includes(",")
        ? String(resumeBase64).split(",").pop()
        : String(resumeBase64);
      const resumeBuffer = Buffer.from(base64Payload, "base64");
      const validation = validateResumeFile({
        filename: normalizedFilename,
        mimetype: resumeMimeType,
        buffer: resumeBuffer,
        maxBytes: 5 * 1024 * 1024,
      });
      if (!validation.ok) {
        return res.status(400).json({ message: validation.message });
      }

      const hasJobJidColumn = await columnExists("resumes_data", "job_jid");
      const hasApplicantNameColumn = await columnExists(
        "resumes_data",
        "applicant_name",
      );
      const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
      const hasAtsMatchColumn = await columnExists(
        "resumes_data",
        "ats_match_percentage",
      );
      const hasAtsRawColumn = await columnExists(
        "resumes_data",
        "ats_raw_json",
      );
      const hasSubmittedByRoleColumn = await columnExists(
        "resumes_data",
        "submitted_by_role",
      );
      const hasSourceColumn = await columnExists("resumes_data", "source");
      const normalizedMimeType = String(resumeMimeType || "")
        .trim()
        .toLowerCase();

      const shouldExtractResumeData =
        hasApplicantNameColumn ||
        hasAtsScoreColumn ||
        hasAtsMatchColumn ||
        hasAtsRawColumn;
      const resumeAts = shouldExtractResumeData
        ? await extractResumeAts({
            resumeBuffer,
            resumeFilename: normalizedFilename,
            jobDescription: buildJobAtsContext(jobRows[0]),
          })
        : {
            atsScore: null,
            atsMatchPercentage: null,
            atsRawJson: null,
            applicantName: null,
            atsStatus: "not_stored",
          };

      const requestParsedData = parseJsonField(
        req.body?.parsedData ?? req.body?.parsed_data,
      );
      const requestAtsRawJson = parseJsonField(
        req.body?.atsRawJson ?? req.body?.ats_raw_json,
      );
      const candidateSnapshot = extractCandidateSnapshot({
        source: req.body,
        parsedData: requestParsedData,
        fallback: {
          name: resumeAts.applicantName || "",
          jobJid: safeJobId,
          recruiterRid: rid,
          location:
            String(
              candidate_location ?? candidateLocation ?? location ?? "",
            ).trim() || null,
        },
      });
      const storedAtsRawJson =
        requestAtsRawJson !== null
          ? {
              ...(requestAtsRawJson &&
              typeof requestAtsRawJson === "object" &&
              !Array.isArray(requestAtsRawJson)
                ? requestAtsRawJson
                : {}),
              parsed_data: requestParsedData ?? null,
            }
          : resumeAts.atsRawJson ?? null;

      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        const duplicateCheck = await findResumeDuplicateDecision(connection, {
          candidateName:
            candidateSnapshot.name || resumeAts.applicantName || null,
          phone: candidateSnapshot.phone || null,
          email: candidateSnapshot.email || null,
        });
        if (!duplicateCheck.allowSubmission) {
          await connection.rollback();
          return res.status(409).json({
            message: "Duplicate entry of resume already exists.",
            existingResume: duplicateCheck.blockingMatch,
          });
        }

        const [sequenceResult] = await connection.query(
          "INSERT INTO resume_id_sequence VALUES ()",
        );
        const sequenceValue = Number(sequenceResult.insertId);
        const resId = `res_${sequenceValue}`;
        const cid = buildCandidateId(sequenceValue);

        const insertColumns = ["res_id", "rid"];
        const insertValues = [resId, rid];

        if (hasJobJidColumn) {
          insertColumns.push("job_jid");
          insertValues.push(safeJobId);
        }

        if (await columnExists("resumes_data", "resume")) {
          insertColumns.push("resume");
          insertValues.push(resumeBuffer);
        }
        insertColumns.push("resume_filename", "resume_type");
        insertValues.push(normalizedFilename, validation.extension);

        if (hasSubmittedByRoleColumn) {
          insertColumns.push("submitted_by_role");
          insertValues.push(
            isTeamLeaderRole(submitterRole) ? "team leader" : "recruiter",
          );
        }

        if (hasSourceColumn) {
          insertColumns.push("source");
          insertValues.push(resumeSource);
        }

        if (hasApplicantNameColumn) {
          insertColumns.push("applicant_name");
          insertValues.push(
            candidateSnapshot.name || resumeAts.applicantName || null,
          );
        }

        if (hasAtsScoreColumn) {
          insertColumns.push("ats_score");
          insertValues.push(resumeAts.atsScore);
        }

        if (hasAtsMatchColumn) {
          insertColumns.push("ats_match_percentage");
          insertValues.push(resumeAts.atsMatchPercentage);
        }

        if (hasAtsRawColumn) {
          insertColumns.push("ats_raw_json");
          insertValues.push(
            storedAtsRawJson === null ? null : JSON.stringify(storedAtsRawJson),
          );
        }

        const placeholders = insertColumns.map(() => "?").join(", ");
        await connection.query(
          `INSERT INTO resumes_data (${insertColumns.join(", ")}) VALUES (${placeholders})`,
          insertValues,
        );
        await storeResumeBinary(connection, resId, resumeBuffer);

        await upsertCandidateFields(connection, {
          cid,
          resId,
          jobJid: safeJobId,
          recruiterRid: rid,
          name:
            candidateSnapshot.name || resumeAts.applicantName || "Unknown Candidate",
          phone: candidateSnapshot.phone || undefined,
          email: candidateSnapshot.email || undefined,
          levelOfEdu: candidateSnapshot.levelOfEdu || undefined,
          boardUni: candidateSnapshot.boardUni || undefined,
          institutionName: candidateSnapshot.institutionName || undefined,
          location: candidateSnapshot.location || undefined,
          age: candidateSnapshot.age,
        });

        await upsertExtraInfoFields(connection, {
          resId,
          jobJid: safeJobId,
          recruiterRid: rid,
          candidateName:
            candidateSnapshot.name || resumeAts.applicantName || "Unknown Candidate",
          email: candidateSnapshot.email || undefined,
          phone: candidateSnapshot.phone || undefined,
          submittedAt: "__CURRENT_TIMESTAMP__",
        });

        await connection.commit();
        const manualEntryRequired =
          isImageResume || resumeAts.atsStatus === "manual_entry_required";
        return res.status(201).json({
          message: manualEntryRequired
            ? "Image resume added successfully. Fill candidate details manually."
            : "Resume added successfully.",
          parsedData: manualEntryRequired ? null : requestParsedData ?? null,
          atsScore: manualEntryRequired ? null : resumeAts.atsScore,
          atsMatchPercentage: manualEntryRequired
            ? null
            : resumeAts.atsMatchPercentage,
          atsRawJson: manualEntryRequired ? null : storedAtsRawJson,
          parserStatus: manualEntryRequired
            ? "manual_entry_required"
            : resumeAts.atsStatus,
          resume: {
            resId,
            rid,
            jobJid: safeJobId,
            resumeFilename: normalizedFilename,
            resumeType: validation.extension,
            resumeMimeType: normalizedMimeType || null,
            atsScore: resumeAts.atsScore,
            atsMatchPercentage: resumeAts.atsMatchPercentage,
            atsStatus: resumeAts.atsStatus,
          },
          processing: buildResumeProcessingState({
            status: manualEntryRequired ? "manual_entry_required" : "completed",
            resumeParsed: !manualEntryRequired,
            atsCalculated: !manualEntryRequired && resumeAts.atsStatus === "scored",
            submitAllowed: true,
          }),
        });
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }
    } catch (error) {
      return res.status(500).json({
        message: "Failed to add resume.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/resumes",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const { rid } = req.params;
    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
      const hasAtsMatchColumn = await columnExists(
        "resumes_data",
        "ats_match_percentage",
      );
      const atsScoreSelection = hasAtsScoreColumn
        ? "ats_score AS atsScore,"
        : "NULL AS atsScore,";
      const atsMatchSelection = hasAtsMatchColumn
        ? "ats_match_percentage AS atsMatchPercentage,"
        : "NULL AS atsMatchPercentage,";

      const [rows] = await pool.query(
        `SELECT
        rd.res_id AS resId,
        rd.job_jid AS jobJid,
        rd.ats_raw_json AS atsRawJson,
        c.name AS candidateName,
        c.phone AS candidatePhone,
        DATE_FORMAT(c.walk_in, '%Y-%m-%d') AS walkInDate,
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType,
        ${atsScoreSelection}
        ${atsMatchSelection}
        rd.uploaded_at AS uploadedAt,
        COALESCE(jrs.selection_status, 'pending') AS workflowStatus,
        jrs.selection_note AS workflowNote,
        jrs.selected_at AS workflowUpdatedAt,
        DATE_FORMAT(c.joining_date, '%Y-%m-%d') AS joiningDate,
        j.company_name AS companyName,
        j.role_name AS roleName,
        j.city AS city
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
      LEFT JOIN jobs j ON j.jid = rd.job_jid
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid
       AND jrs.res_id = rd.res_id
      WHERE rd.rid = ?
      ORDER BY rd.uploaded_at DESC`,
        [rid],
      );

      const extraInfoByResumeId = await fetchExtraInfoByResumeIds(
        rows.map((row) => row.resId),
      );

      return res.status(200).json({
        resumes: rows.map((row) => {
          const extraInfo =
            extraInfoByResumeId.get(String(row.resId || "").trim()) || {};
          const parsedResumePayload = parseJsonField(row.atsRawJson);
          const candidateSnapshot = extractCandidateSnapshot({
            source: {
              candidate_name: row.candidateName,
              candidate_phone: row.candidatePhone,
              job_jid: row.jobJid,
            },
            parsedData:
              parsedResumePayload?.parsed_data ||
              parsedResumePayload?.parsedData ||
              parsedResumePayload,
            fallback: {
              jobJid: row.jobJid,
            },
          });
          const compatibilityFields = buildResumeCompatibilityFields({
            ...extraInfo,
            ...row,
            othersReason:
              extraInfo?.othersReason || row.workflowNote || null,
            candidateName: candidateSnapshot.name || row.candidateName || null,
            candidatePhone: candidateSnapshot.phone || row.candidatePhone || null,
            reason: row.workflowNote || null,
            note: row.workflowNote || null,
          });
          return {
            ...row,
            ...extraInfo,
            ...compatibilityFields,
            name: compatibilityFields.candidateName,
            phone: compatibilityFields.candidatePhone,
            atsScore: row.atsScore === null ? null : Number(row.atsScore),
            atsMatchPercentage:
              row.atsMatchPercentage === null
                ? null
                : Number(row.atsMatchPercentage),
            workflowUpdatedAt: row.workflowUpdatedAt || null,
            job: {
              jobJid: compatibilityFields.jobJid,
              companyName: compatibilityFields.companyName,
              roleName: compatibilityFields.roleName,
              city: compatibilityFields.city,
            },
          };
        }),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch resumes.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/resumes/:resId/file",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const { rid, resId } = req.params;
    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      const { resumeSelectSql, resumeJoinSql } = await buildResumeBinarySelect(
        pool,
        { resumesAlias: "rd", blobAlias: "rb" },
      );
      const [rows] = await pool.query(
        `SELECT
        ${resumeSelectSql},
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType
      FROM resumes_data rd
      ${resumeJoinSql}
      WHERE rd.res_id = ? AND rd.rid = ?
      LIMIT 1`,
        [resId, rid],
      );

      if (rows.length === 0) {
        return res.status(404).json({ message: "Resume not found." });
      }

      const row = rows[0];
      const mimeTypeByResumeType = {
        pdf: "application/pdf",
        doc: "application/msword",
        docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        jpg: "image/jpeg",
        jpeg: "image/jpeg",
        png: "image/png",
        webp: "image/webp",
      };
      const mimeType =
        mimeTypeByResumeType[String(row.resumeType || "").toLowerCase()] ||
        "application/octet-stream";

      res.setHeader("Content-Type", mimeType);
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${String(row.resumeFilename || "resume").replace(/"/g, "")}"`,
      );
      return res.send(row.resume);
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch resume file.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/reimbursements",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.auth?.rid || "").trim();
    const role = String(req.auth?.role || "")
      .trim()
      .toLowerCase();
    const amount = Number(req.body?.amount);
    const description = String(req.body?.description || "").trim();

    if (!rid)
      return res.status(401).json({ message: "Authentication required." });
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ message: "Positive amount is required." });
    }

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      console.log(
        `[Reimbursement] Processing submission: amount=${amount}, rid=${rid}`,
      );

      // Get last profit to calculate new profit
      const [profitRows] = await connection.query(
        "SELECT COALESCE(profit, 0) AS lastProfit FROM money_sum ORDER BY created_at DESC, id DESC LIMIT 1",
      );
      const lastProfit = toMoneyNumber(profitRows?.[0]?.lastProfit) || 0;
      const expense = toMoneyNumber(amount);
      const nextProfit = Math.round((lastProfit - expense) * 100) / 100;
      const reason = `Reimbursement request from RID ${rid}: ${description || "No description"}`;

      console.log(
        `[Reimbursement] Calculated: lastProfit=${lastProfit}, expense=${expense}, nextProfit=${nextProfit}`,
      );

      // Insert into money_sum
      const [moneySumResult] = await connection.query(
        `INSERT INTO money_sum (company_rev, expense, profit, reason, entry_type)
         VALUES (0, ?, ?, ?, 'expense')`,
        [expense, nextProfit, reason],
      );

      console.log(
        `[Reimbursement] Created money_sum entry: id=${moneySumResult.insertId}`,
      );

      // Insert into reimbursements with money_sum_id
      const [result] = await connection.query(
        `INSERT INTO reimbursements (rid, role, amount, description, status, money_sum_id)
         VALUES (?, ?, ?, ?, 'pending', ?)`,
        [rid, role, amount, description || null, moneySumResult.insertId],
      );

      console.log(
        `[Reimbursement] Created reimbursement entry: id=${result.insertId}`,
      );

      await connection.commit();
      console.log(`[Reimbursement] Transaction committed successfully`);
      return res.status(201).json({
        id: result.insertId,
        rid,
        role,
        amount,
        description,
        status: "pending",
        money_sum_id: moneySumResult.insertId,
      });
    } catch (error) {
      console.error(`[Reimbursement] Error during submission:`, error);
      await connection.rollback();
      return res.status(500).json({
        message: "Failed to submit reimbursement.",
        error: error.message,
      });
    } finally {
      connection.release();
    }
  },
);

router.get(
  "/api/reimbursements/my",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.auth?.rid || "").trim();
    if (!rid)
      return res.status(401).json({ message: "Authentication required." });

    try {
      const [rows] = await pool.query(
        `SELECT id, rid, role, amount, description, status, admin_note AS adminNote, created_at AS createdAt, updated_at AS updatedAt
         FROM reimbursements
         WHERE rid = ?
         ORDER BY created_at DESC`,
        [rid],
      );
      return res.status(200).json({ reimbursements: rows });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch reimbursements.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/dashboard",
  requireAuth,
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;

    try {
      const [rows] = await pool.query(
        "SELECT rid FROM recruiter WHERE rid = ? LIMIT 1",
        [rid],
      );
      if (rows.length === 0) {
        return res.status(404).json({ message: "Recruiter not found." });
      }

      const summary = await getRecruiterSummary(rid);
      return res.status(200).json({
        summary: {
          success: summary.success,
          points: summary.points,
          thisMonth: summary.thisMonth,
        },
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiter dashboard.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/tasks",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.params?.rid || "").trim();
    if (!rid) {
      return res.status(400).json({ message: "rid is required." });
    }

    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      await ensureTaskTables();

      const [rows] = await pool.query(
        `SELECT
          t.id AS taskId,
          t.heading,
          t.description,
          t.created_at AS taskCreatedAt,
          t.updated_at AS taskUpdatedAt,
          ta_self.id AS assignmentId,
          ta_self.status AS assignmentStatus,
          ta_self.assignment_date AS assignmentDate,
          ta_self.rescheduled_from_date AS rescheduledFromDate,
          ta_self.rescheduled_at AS rescheduledAt,
          ta_self.rescheduled_by_rid AS rescheduledByRid,
          rb.name AS rescheduledByName,
          ta_self.acted_at AS actedAt,
          ta_self.created_at AS assignedAt,
          ta_all.recruiter_rid AS memberRid,
          r.name AS memberName,
          r.email AS memberEmail
        FROM task_assignments ta_self
        INNER JOIN tasks t ON t.id = ta_self.task_id
        LEFT JOIN task_assignments ta_all ON ta_all.task_id = t.id
        LEFT JOIN recruiter r ON r.rid = ta_all.recruiter_rid
        LEFT JOIN recruiter rb ON rb.rid = ta_self.rescheduled_by_rid
        WHERE ta_self.recruiter_rid = ?
        ORDER BY ta_self.created_at DESC, ta_all.created_at ASC, ta_all.id ASC`,
        [rid],
      );

      return res.status(200).json({
        tasks: buildRecruiterTaskPayload(rows, rid),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiter tasks.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/recruiters/:rid/tasks/:assignmentId/status",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.params?.rid || "").trim();
    const assignmentId = Number(req.params?.assignmentId);
    if (!rid) {
      return res.status(400).json({ message: "rid is required." });
    }
    if (!Number.isInteger(assignmentId) || assignmentId <= 0) {
      return res.status(400).json({ message: "Valid assignment ID is required." });
    }

    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    const requestedStatus = String(req.body?.status || "")
      .trim()
      .toLowerCase();
    if (!["completed", "rejected"].includes(requestedStatus)) {
      return res.status(400).json({
        message: "status must be either 'completed' or 'rejected'.",
      });
    }

    try {
      await ensureTaskTables();

      const [rows] = await pool.query(
        `SELECT
          ta.id,
          ta.status,
          ta.assignment_date AS assignmentDate,
          ta.recruiter_rid AS recruiterRid
        FROM task_assignments ta
        WHERE ta.id = ? AND ta.recruiter_rid = ?
        LIMIT 1`,
        [assignmentId, rid],
      );

      if (rows.length === 0) {
        return res.status(404).json({ message: "Task assignment not found." });
      }

      const assignment = rows[0];
      const actionState = getTaskAssignmentActionState(assignment.assignmentDate);
      const effectiveStatus = resolveEffectiveTaskAssignmentStatus(
        assignment.status,
        assignment.assignmentDate,
      );

      if (!actionState.isActionableToday && actionState.isScheduledForFuture) {
        return res.status(409).json({
          message: `This task can only be updated on ${assignment.assignmentDate}.`,
        });
      }

      if (effectiveStatus === TASK_ASSIGNMENT_STATUS.TIMED_OUT) {
        return res.status(409).json({
          message: "This task has already timed out for the day.",
        });
      }

      if (effectiveStatus !== TASK_ASSIGNMENT_STATUS.PENDING) {
        return res.status(409).json({
          message: `This task is already marked as ${effectiveStatus}.`,
        });
      }

      await pool.query(
        `UPDATE task_assignments
         SET status = ?, acted_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [requestedStatus, assignmentId],
      );

      return res.status(200).json({
        message: "Task status updated successfully.",
        assignment: {
          id: assignmentId,
          recruiterRid: rid,
          status: requestedStatus,
        },
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to update task status.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/recruiters/:rid/tasks/:assignmentId/reschedule",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const rid = String(req.params?.rid || "").trim();
    const assignmentId = Number(req.params?.assignmentId);
    const requestedDate = String(req.body?.assignmentDate || "").trim();
    const actorRid = String(req.auth?.rid || "").trim();
    const today = getCurrentDateOnlyInBusinessTimeZone();

    if (!rid) {
      return res.status(400).json({ message: "rid is required." });
    }
    if (!Number.isInteger(assignmentId) || assignmentId <= 0) {
      return res.status(400).json({ message: "Valid assignment ID is required." });
    }
    if (!requestedDate) {
      return res.status(400).json({ message: "assignmentDate is required." });
    }
    if (!isValidDateOnly(requestedDate)) {
      return res.status(400).json({
        message: "assignmentDate must be in YYYY-MM-DD format.",
      });
    }

    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      await ensureTaskTables();

      const [rows] = await pool.query(
        `SELECT
          ta.id,
          ta.status,
          ta.assignment_date AS assignmentDate,
          ta.recruiter_rid AS recruiterRid
        FROM task_assignments ta
        WHERE ta.id = ? AND ta.recruiter_rid = ?
        LIMIT 1`,
        [assignmentId, rid],
      );

      if (rows.length === 0) {
        return res.status(404).json({ message: "Task assignment not found." });
      }

      const assignment = rows[0];
      const effectiveStatus = resolveEffectiveTaskAssignmentStatus(
        assignment.status,
        assignment.assignmentDate,
      );

      if (effectiveStatus === TASK_ASSIGNMENT_STATUS.TIMED_OUT) {
        return res.status(409).json({
          message: "This task has already timed out and cannot be rescheduled.",
        });
      }

      if (effectiveStatus !== TASK_ASSIGNMENT_STATUS.PENDING) {
        return res.status(409).json({
          message: `This task is already marked as ${effectiveStatus}.`,
        });
      }

      if (requestedDate <= today) {
        return res.status(400).json({
          message: `Reschedule date must be after ${today}.`,
        });
      }

      if (requestedDate === String(assignment.assignmentDate || "").trim()) {
        return res.status(400).json({
          message: "Choose a different future date to reschedule this task.",
        });
      }

      await pool.query(
        `UPDATE task_assignments
         SET assignment_date = ?,
             rescheduled_from_date = assignment_date,
             rescheduled_at = CURRENT_TIMESTAMP,
             rescheduled_by_rid = ?,
             acted_at = NULL
         WHERE id = ?`,
        [requestedDate, actorRid || rid, assignmentId],
      );

      return res.status(200).json({
        message: `Task rescheduled to ${requestedDate}.`,
        assignment: {
          id: assignmentId,
          recruiterRid: rid,
          assignmentDate: requestedDate,
          rescheduledFromDate: assignment.assignmentDate || null,
          rescheduledAt: new Date().toISOString(),
          rescheduledByRid: actorRid || rid,
          status: TASK_ASSIGNMENT_STATUS.PENDING,
          rawStatus: TASK_ASSIGNMENT_STATUS.PENDING,
          isActionableToday: false,
          isScheduledForFuture: true,
        },
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to reschedule task.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/recruiters/:rid/candidate-click",
  requireAuth,
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;
    const { candidateName } = req.body || {};

    try {
      const [rows] = await pool.query(
        "SELECT rid FROM recruiter WHERE rid = ? LIMIT 1",
        [rid],
      );
      if (rows.length === 0) {
        return res.status(404).json({ message: "Recruiter not found." });
      }

      if (await columnExists("recruiter", "success")) {
        await pool.query(
          "UPDATE recruiter SET success = COALESCE(success, 0) + 1 WHERE rid = ?",
          [rid],
        );
      }

      if (await tableExists("recruiter_candidate_clicks")) {
        const recruiterIdColumn = await getRecruiterIdColumn(
          "recruiter_candidate_clicks",
        );
        if (recruiterIdColumn) {
          await pool.query(
            `INSERT INTO recruiter_candidate_clicks (${recruiterIdColumn}, candidate_name) VALUES (?, ?)`,
            [rid, candidateName?.trim() || null],
          );
        }
      }

      const summary = await getRecruiterSummary(rid);
      return res.status(200).json({
        message: "Candidate completion updated.",
        summary: {
          success: summary.success,
          points: summary.points,
          thisMonth: summary.thisMonth,
        },
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to update completion count.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/recruiters/:rid/applications",
  requireAuth,
  requireRoles("recruiter", "job creator", "team leader", "team_leader"),
  async (req, res) => {
    const { rid } = req.params;
    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    try {
      const hasApplicationsTable = await tableExists("applications");
      const hasJobsTable = await tableExists("jobs");
      const jobsRecruiterIdColumn = hasJobsTable
        ? await getRecruiterIdColumn("jobs")
        : null;
      const hasRecruiterRoleColumn = hasJobsTable
        ? await columnExists("recruiter", "role")
        : false;
      const teamLeaderScope = isTeamLeaderRole(req.auth?.role);

      if (!hasApplicationsTable || !hasJobsTable || !jobsRecruiterIdColumn) {
        return res.status(200).json({ applications: [] });
      }

      const whereClause = teamLeaderScope
        ? hasRecruiterRoleColumn
          ? "LOWER(TRIM(COALESCE(creator.role, ''))) IN ('team leader', 'team_leader', 'job creator')"
          : "1 = 1"
        : `j.${jobsRecruiterIdColumn} = ?`;
      const queryParams = teamLeaderScope ? [] : [rid];

      const [rows] = await pool.query(
        `SELECT
        a.id,
        a.res_id AS resId,
        a.job_jid AS jobJid,
        c.name AS candidateName,
        c.phone AS candidatePhone,
        c.email,
        a.ats_score AS atsScore,
        a.ats_match_percentage AS atsMatchPercentage,
        a.resume_filename AS resumeFilename,
        a.created_at AS createdAt,
        COALESCE(jrs.selection_status, 'pending') AS workflowStatus,
        jrs.selection_note AS workflowNote,
        jrs.selected_at AS workflowUpdatedAt,
        j.role_name AS roleName,
        j.company_name AS companyName,
        j.city AS city,
        ei.office_location_city AS officeLocationCity,
        ei.submitted_reason AS submittedReason,
        ei.verified_reason AS verifiedReason,
        ei.others_reason AS othersReason
      FROM applications a
      LEFT JOIN candidate c ON c.res_id = a.res_id
      INNER JOIN jobs j ON j.jid = a.job_jid
      LEFT JOIN recruiter creator ON creator.rid = j.${jobsRecruiterIdColumn}
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = a.job_jid AND jrs.res_id = a.res_id
      LEFT JOIN extra_info ei
        ON ei.res_id = a.res_id OR ei.resume_id = a.res_id
      WHERE ${whereClause}
      ORDER BY a.created_at DESC`,
        queryParams,
      );

      return res.status(200).json({
        applications: rows.map((row) => {
          const compatibilityFields = buildResumeCompatibilityFields({
            ...row,
            reason: row.workflowNote || null,
            note: row.workflowNote || null,
          });

          return {
            ...row,
            ...compatibilityFields,
            id: row.id,
            name: compatibilityFields.candidateName || row.candidateName || null,
            candidateName:
              compatibilityFields.candidateName || row.candidateName || null,
            candidatePhone:
              compatibilityFields.candidatePhone || row.candidatePhone || null,
            email: row.email,
            jobJid:
              row.jobJid === null || row.jobJid === undefined
                ? null
                : String(row.jobJid),
            atsScore: row.atsScore === null ? null : Number(row.atsScore),
            atsMatchPercentage:
              row.atsMatchPercentage === null
                ? null
                : Number(row.atsMatchPercentage),
            resumeFilename: row.resumeFilename || null,
            createdAt: row.createdAt,
            workflowUpdatedAt: row.workflowUpdatedAt || null,
            job: {
              roleName: compatibilityFields.roleName || row.roleName,
              companyName: compatibilityFields.companyName || row.companyName,
              officeLocationCity: row.officeLocationCity || null,
              city: compatibilityFields.city || row.city || null,
            },
          };
        }),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiter applications.",
        error: error.message,
      });
    }
  },
);

const allowedRecruiterTransitions = RECRUITER_STATUS_TRANSITIONS;

const resolveRecruiterRollbackTarget = (resume = {}) => {
  const currentStatus = normalizeWorkflowStatus(resume.currentStatus, "submitted");

  if (currentStatus === "rejected") {
    if (resume.currentJoiningDate || resume.selectedAt) return "selected";
    if (resume.shortlistedAt) return "shortlisted";
    if (resume.currentWalkInDate || resume.walkInAt) return "walk_in";
    if (resume.othersAt || resume.othersReason) return "others";
    if (resume.verifiedAt || resume.verifiedReason) return "verified";
    return "submitted";
  }

  if (currentStatus === "dropout") {
    return resume.currentJoiningDate || resume.selectedAt
      ? "selected"
      : "shortlisted";
  }

  if (currentStatus === "others") {
    return resume.verifiedAt || resume.verifiedReason ? "verified" : "submitted";
  }

  if (currentStatus === "walk_in" && (resume.othersReason || resume.othersAt)) {
    return "others";
  }

  if (currentStatus === "left" || currentStatus === "billed") {
    return "joined";
  }

  return getPreviousWorkflowStatus(currentStatus) || null;
};

const statusReasonColumnMap = {
  others: "others_reason",
  walk_in: "walk_in_reason",
  further: "further_reason",
  selected: "select_reason",
  rejected: "reject_reason",
  shortlisted: "shortlisted_reason",
  joined: "joined_reason",
  dropout: "dropout_reason",
  billed: "billed_reason",
  left: "left_reason",
};

router.post(
  "/api/recruiters/:rid/resumes/:resId/advance-status",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const { rid, resId } = req.params;
    if (!authorizeRecruiterResourceView(req, res, rid)) return;
    const targetStatus = normalizeResumeStatusInput(req.body?.status);
    const statusReason = resolveStatusReasonInput(req.body, targetStatus);
    const reason = statusReason || null;
    const joiningDate = req.body?.joining_date
      ? String(req.body.joining_date).trim()
      : null;
    const joinedReasonSource =
      req.body?.joinedReason ??
      req.body?.joined_reason ??
      req.body?.joiningNote ??
      req.body?.joining_note;
    const joinedReason =
      joinedReasonSource === undefined || joinedReasonSource === null
        ? null
        : String(joinedReasonSource).trim();

    if (!targetStatus) {
      return res.status(400).json({ message: "status is required." });
    }

    if (targetStatus === CANONICAL_VERIFY_STATUS) {
      return res.status(403).json({
        message:
          "Recruiters cannot mark resumes as verified. Use a team leader or admin verify route.",
      });
    }

    if (
      (targetStatus === "others" ||
        targetStatus === "billed" ||
        targetStatus === "left") &&
      !reason
    ) {
      return res.status(400).json({ message: "reason is required." });
    }

    if (targetStatus === "joined" && joiningDate) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate)) {
        return res.status(400).json({
          message: "joining_date must be in YYYY-MM-DD format.",
        });
      }
    }

    try {
      const [resumeRows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.job_jid AS jobJid,
          rd.rid AS recruiterRid,
          rd.ats_raw_json AS atsRawJson,
          c.name AS candidateName,
          c.email AS email,
          c.revenue AS candidateRevenue,
          j.revenue AS companyRevenue,
          COALESCE(jrs.selection_status, 'pending') AS currentStatus
        FROM resumes_data rd
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        LEFT JOIN jobs j ON j.jid = rd.job_jid
        LEFT JOIN job_resume_selection jrs
          ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
        WHERE rd.res_id = ? AND rd.rid = ?
        LIMIT 1`,
        [resId, rid],
      );

      if (resumeRows.length === 0) {
        return res.status(404).json({ message: "Resume not found." });
      }

      const resume = resumeRows[0];
      const parsedResumePayload = parseJsonField(resume.atsRawJson);
      const statusCandidateSnapshot = extractCandidateSnapshot({
        source: {
          candidate_name: resume.candidateName,
          email: resume.email,
          job_jid: resume.jobJid,
          recruiter_rid: resume.recruiterRid,
        },
        parsedData:
          parsedResumePayload?.parsed_data ||
          parsedResumePayload?.parsedData ||
          parsedResumePayload,
        fallback: {
          jobJid: resume.jobJid,
          recruiterRid: resume.recruiterRid,
        },
      });
      const currentStatus = normalizeWorkflowStatus(resume.currentStatus);
      const billedRevenueAmount =
        targetStatus === "billed" && currentStatus !== "billed"
          ? resolveRevenueAmount(
              resume.candidateRevenue,
              resume.companyRevenue,
            )
          : null;
      const selectionNoteValue =
        targetStatus === "joined" ? joinedReason || reason : reason;
      const allowed = allowedRecruiterTransitions[currentStatus];

      if (!allowed || !allowed.includes(targetStatus)) {
        return res.status(400).json({
          message: `Cannot transition from '${currentStatus}' to '${targetStatus}'.`,
        });
      }

      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        await connection.query(
          `INSERT INTO job_resume_selection
            (job_jid, res_id, selected_by_admin, selection_status, selection_note)
           VALUES (?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             selected_by_admin = VALUES(selected_by_admin),
             selection_status = VALUES(selection_status),
             selection_note = VALUES(selection_note),
             selected_at = CURRENT_TIMESTAMP`,
          [
            resume.jobJid,
            resId,
            rid,
            targetStatus,
            selectionNoteValue,
          ],
        );

        if (statusCandidateSnapshot.name) {
          await upsertCandidateFields(connection, {
            resId,
            cid: undefined,
            jobJid: resume.jobJid || undefined,
            recruiterRid: resume.recruiterRid || undefined,
            name: statusCandidateSnapshot.name,
            phone: statusCandidateSnapshot.phone || undefined,
            email: statusCandidateSnapshot.email || undefined,
            levelOfEdu: statusCandidateSnapshot.levelOfEdu || undefined,
            boardUni: statusCandidateSnapshot.boardUni || undefined,
            institutionName:
              statusCandidateSnapshot.institutionName || undefined,
            age: statusCandidateSnapshot.age,
            revenue:
              targetStatus === "billed" ? billedRevenueAmount : undefined,
          });
        }

        if (targetStatus === "walk_in") {
          await upsertCandidateFields(connection, {
            resId,
            cid: undefined,
            walkIn: getCurrentDateOnlyInBusinessTimeZone(),
          });
        }

        if (targetStatus === "joined" && joiningDate) {
          await upsertCandidateFields(connection, {
            resId,
            cid: undefined,
            joiningDate,
          });
        }

        const reasonField = STATUS_REASON_FIELD_MAP[targetStatus];
        const statusReasonValue =
          targetStatus === "joined" ? joinedReason : reason;
        if (reasonField) {
          const statusTimestampFieldMap = {
            verified: "verifiedAt",
            others: "othersAt",
            walk_in: "walkInAt",
            further: "furtherAt",
            selected: "selectedAt",
            shortlisted: "shortlistedAt",
            rejected: "rejectedAt",
            joined: "joinedAt",
            dropout: "dropoutAt",
            billed: "billedAt",
            left: "leftAt",
          };
          await upsertExtraInfoFields(connection, {
            resId,
            jobJid: resume.jobJid || undefined,
            recruiterRid: rid || undefined,
            [reasonField]: statusReasonValue,
            [statusTimestampFieldMap[targetStatus]]: "__CURRENT_TIMESTAMP__",
          });
        }

        const statusDeltaMap = {
          verified: "verified",
          others: "others",
          walk_in: "walk_in",
          selected: "`select`",
          rejected: "reject",
          joined: "joined",
          dropout: "dropout",
          billed: "billed",
          left: "`left`",
        };
        const currentCol = statusDeltaMap[currentStatus];
        const targetCol = statusDeltaMap[targetStatus];

        if (currentCol || targetCol) {
          const updateParts = [];
          if (currentCol)
            updateParts.push(
              `${currentCol} = GREATEST(0, COALESCE(${currentCol}, 0) - 1)`,
            );
          if (targetCol)
            updateParts.push(`${targetCol} = COALESCE(${targetCol}, 0) + 1`);
          updateParts.push("last_updated = CURRENT_TIMESTAMP");

          await connection.query(
            `INSERT INTO status (recruiter_rid, submitted, last_updated)
             VALUES (?, 0, CURRENT_TIMESTAMP)
             ON DUPLICATE KEY UPDATE ${updateParts.join(", ")}`,
            [rid],
          );
        }

        if (targetStatus === "billed" && currentStatus !== "billed") {
          if (
            !Number.isFinite(billedRevenueAmount) ||
            billedRevenueAmount <= 0
          ) {
            await connection.rollback();
            return res.status(422).json({
              message: "Revenue amount is required before moving candidate to billed.",
            });
          }

          await addCandidateBillIntakeEntry(connection, resId, {
            amount: billedRevenueAmount,
          });
        }

        await connection.commit();
        const responseFields = buildResumeCompatibilityFields({
          resId,
          candidateName: statusCandidateSnapshot.name || resume.candidateName || null,
          candidatePhone:
            statusCandidateSnapshot.phone || resume.candidatePhone || null,
          workflowStatus: targetStatus,
          reason: selectionNoteValue,
          note: selectionNoteValue,
          othersReason: targetStatus === "others" ? reason : null,
          joinedReason: targetStatus === "joined" ? joinedReason : null,
          joiningNote: targetStatus === "joined" ? joinedReason : null,
          joiningDate: targetStatus === "joined" ? joiningDate : null,
          jobJid: resume.jobJid || null,
        });
        return res.status(200).json({
          message: "Resume status advanced successfully.",
          data: {
            ...responseFields,
            resId,
            previousStatus: currentStatus,
            status: targetStatus,
            reason,
            othersReason: targetStatus === "others" ? reason : undefined,
            joining_date: responseFields.joiningDate || undefined,
            joinedReason: responseFields.joinedReason || undefined,
            joiningNote: responseFields.joiningNote || undefined,
            revenue:
              targetStatus === "billed" ? billedRevenueAmount : undefined,
            company_rev:
              targetStatus === "billed" ? billedRevenueAmount : undefined,
          },
        });
      } catch (innerError) {
        await connection.rollback();
        throw innerError;
      } finally {
        connection.release();
      }
    } catch (error) {
      return res.status(500).json({
        message: "Failed to advance resume status.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/recruiters/:rid/resumes/:resId/rollback-status",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    const { rid, resId } = req.params;
    if (!authorizeRecruiterResourceView(req, res, rid)) return;

    const normalizedResId = String(resId || "").trim();
    if (!normalizedResId) {
      return res.status(400).json({ message: "resId is required." });
    }

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      const [resumeRows] = await connection.query(
        `SELECT
          rd.res_id AS resId,
          rd.rid AS recruiterRid,
          rd.job_jid AS jobJid,
          COALESCE(jrs.selection_status, 'submitted') AS currentStatus,
          c.joining_date AS currentJoiningDate,
          c.walk_in AS currentWalkInDate,
          ei.verified_reason AS verifiedReason,
          ei.others_reason AS othersReason,
          ei.verified_at AS verifiedAt,
          ei.others_at AS othersAt,
          ei.walk_in_at AS walkInAt,
          ei.selected_at AS selectedAt,
          ei.shortlisted_at AS shortlistedAt
        FROM resumes_data rd
        LEFT JOIN job_resume_selection jrs
          ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        LEFT JOIN extra_info ei
          ON ei.res_id = rd.res_id OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)
        WHERE rd.res_id = ? AND rd.rid = ?
        LIMIT 1
        FOR UPDATE`,
        [normalizedResId, rid],
      );

      if (resumeRows.length === 0) {
        await connection.rollback();
        return res.status(404).json({ message: "Resume not found." });
      }

      const resume = resumeRows[0];
      const currentStatus = normalizeWorkflowStatus(resume.currentStatus, "submitted");
      const rollbackTarget = resolveRecruiterRollbackTarget(resume);

      if (!rollbackTarget) {
        await connection.rollback();
        return res.status(400).json({
          message: `Rollback is not supported for '${currentStatus}'.`,
        });
      }

      if (!resume.jobJid) {
        await connection.rollback();
        return res.status(400).json({
          message: "Cannot rollback a resume without a linked job ID.",
        });
      }

      if (rollbackTarget === "submitted") {
        await connection.query(
          `DELETE FROM job_resume_selection
           WHERE job_jid = ? AND res_id = ?`,
          [resume.jobJid, normalizedResId],
        );
      } else {
        await connection.query(
          `INSERT INTO job_resume_selection
            (job_jid, res_id, selected_by_admin, selection_status, selection_note)
           VALUES (?, ?, ?, ?, NULL)
           ON DUPLICATE KEY UPDATE
            selected_by_admin = VALUES(selected_by_admin),
            selection_status = VALUES(selection_status),
            selection_note = VALUES(selection_note),
            selected_at = CURRENT_TIMESTAMP`,
          [resume.jobJid, normalizedResId, rid || "recruiter-rollback", rollbackTarget],
        );
      }

      if (currentStatus === "verified" || currentStatus === "others") {
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          verifiedReason: currentStatus === "verified" ? null : undefined,
          verifiedAt: currentStatus === "verified" ? null : undefined,
          othersReason: currentStatus === "others" ? null : undefined,
          othersAt: currentStatus === "others" ? null : undefined,
        });
      }

      if (currentStatus === "walk_in") {
        await upsertCandidateFields(connection, {
          resId: normalizedResId,
          walkIn: null,
        });
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          walkInReason: null,
          walkInAt: null,
        });
      }

      if (currentStatus === "selected") {
        await upsertCandidateFields(connection, {
          resId: normalizedResId,
          joiningDate: null,
          revenue: null,
        });
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          selectReason: null,
          selectedAt: null,
        });
      }

      if (currentStatus === "rejected") {
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          rejectReason: null,
          rejectedAt: null,
        });
      }

      if (currentStatus === "shortlisted") {
        await upsertCandidateFields(connection, {
          resId: normalizedResId,
          joiningDate: null,
          revenue: null,
        });
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          shortlistedAt: null,
        });
      }

      if (currentStatus === "joined") {
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          joinedReason: null,
          joinedAt: null,
        });
      }

      if (currentStatus === "dropout") {
        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid: resume.jobJid || undefined,
          recruiterRid: rid || undefined,
          dropoutReason: null,
          dropoutAt: null,
        });
      }

      await connection.commit();
      return res.status(200).json({
        message: "Resume status rolled back successfully.",
        data: {
          resId: normalizedResId,
          previousStatus: currentStatus,
          status: rollbackTarget,
        },
      });
    } catch (error) {
      await connection.rollback();
      return res.status(500).json({
        message: "Failed to rollback resume status.",
        error: error.message,
      });
    } finally {
      connection.release();
    }
  },
);

// Password change endpoint for first-time login
router.post(
  "/api/recruiters/:rid/change-password",
  requireAuth,
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;
    const { oldPassword, newPassword } = req.body || {};

    if (!newPassword || String(newPassword || "").trim().length < 6) {
      return res.status(400).json({
        message: "New password is required and must be at least 6 characters.",
      });
    }

    try {
      // Verify the recruiter exists
      const [recruiterRows] = await pool.query(
        "SELECT rid, email FROM recruiter WHERE rid = ? LIMIT 1",
        [rid],
      );

      if (recruiterRows.length === 0) {
        return res.status(404).json({ message: "Recruiter not found." });
      }

      // If oldPassword is provided, verify it matches
      if (oldPassword) {
        const [authRows] = await pool.query(
          "SELECT rid FROM recruiter WHERE rid = ? AND password = ? LIMIT 1",
          [rid, oldPassword],
        );

        if (authRows.length === 0) {
          return res
            .status(401)
            .json({ message: "Old password is incorrect." });
        }
      }

      // Update the password and mark as changed
      const hasPasswordChangedColumn = await columnExists(
        "recruiter",
        "password_changed",
      );
      const updateQuery = hasPasswordChangedColumn
        ? "UPDATE recruiter SET password = ?, password_changed = TRUE WHERE rid = ?"
        : "UPDATE recruiter SET password = ? WHERE rid = ?";

      const updateParams = hasPasswordChangedColumn
        ? [newPassword.trim(), rid]
        : [newPassword.trim(), rid];

      await pool.query(updateQuery, updateParams);

      return res.status(200).json({
        message: "Password changed successfully.",
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to change password.",
        error: error.message,
      });
    }
  },
);

module.exports = router;
