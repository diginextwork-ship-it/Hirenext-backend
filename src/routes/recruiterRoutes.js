const express = require("express");
const crypto = require("crypto");
const multer = require("multer");
const pool = require("../config/db");
const {
  extractResumeAts,
  parseResumeWithAts,
  extractApplicantName,
} = require("../resumeparser/service");
const {
  createAuthToken,
  normalizeRoleAlias,
  requireAuth,
  requireRoles,
  requireRecruiterOwner,
} = require("../middleware/auth");
const { validateResumeFile } = require("../middleware/uploadValidation");

const router = express.Router();
const uploadResume = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
});

const tableExists = async (tableName) => {
  try {
    const [rows] = await pool.query(
      `SELECT 1
       FROM information_schema.tables
       WHERE table_schema = DATABASE() AND table_name = ?
       LIMIT 1`,
      [tableName],
    );
    if (rows.length > 0) return true;
  } catch {}

  try {
    await pool.query(`SELECT 1 FROM \`${tableName}\` LIMIT 1`);
    return true;
  } catch {
    return false;
  }
};

const columnExists = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  return rows.length > 0;
};

const getTableColumns = async (tableName, connection = pool) => {
  const [rows] = await connection.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ?`,
    [tableName],
  );
  return new Set(
    rows.map((row) =>
      String(
        row.column_name ?? row.COLUMN_NAME ?? Object.values(row)[0] ?? "",
      ).trim(),
    ),
  );
};

const fetchExtraInfoByResumeIds = async (resumeIds, connection = pool) => {
  const normalizedResumeIds = Array.from(
    new Set(
      (Array.isArray(resumeIds) ? resumeIds : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );
  if (normalizedResumeIds.length === 0) return new Map();
  if (!(await tableExists("extra_info"))) return new Map();

  const columns = await getTableColumns("extra_info", connection);
  const resumeIdColumn = columns.has("res_id")
    ? "res_id"
    : columns.has("resume_id")
      ? "resume_id"
      : "";
  const hasSubmittedReason = columns.has("submitted_reason");
  const hasVerifiedReason = columns.has("verified_reason");
  const hasWalkInReason = columns.has("walk_in_reason");
  const hasSelectReason = columns.has("select_reason");
  const hasJoinedReason = columns.has("joined_reason");
  const hasDropoutReason = columns.has("dropout_reason");
  const hasRejectReason = columns.has("reject_reason");

  const hasAnyReason =
    hasSubmittedReason ||
    hasVerifiedReason ||
    hasWalkInReason ||
    hasSelectReason ||
    hasJoinedReason ||
    hasDropoutReason ||
    hasRejectReason;

  if (!resumeIdColumn || !hasAnyReason) return new Map();

  const selectColumns = [`${resumeIdColumn} AS resumeId`];
  if (hasSubmittedReason)
    selectColumns.push("submitted_reason AS submittedReason");
  if (hasVerifiedReason)
    selectColumns.push("verified_reason AS verifiedReason");
  if (hasWalkInReason) selectColumns.push("walk_in_reason AS walkInReason");
  if (hasSelectReason) selectColumns.push("select_reason AS selectReason");
  if (hasJoinedReason) selectColumns.push("joined_reason AS joinedReason");
  if (hasDropoutReason) selectColumns.push("dropout_reason AS dropoutReason");
  if (hasRejectReason) selectColumns.push("reject_reason AS rejectReason");

  const [rows] = await connection.query(
    `SELECT ${selectColumns.join(", ")}
     FROM extra_info
     WHERE ${resumeIdColumn} IN (?)`,
    [normalizedResumeIds],
  );

  return new Map(
    rows.map((row) => [
      String(row.resumeId || "").trim(),
      {
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        walkInReason: row.walkInReason || null,
        selectReason: row.selectReason || null,
        joinedReason: row.joinedReason || null,
        dropoutReason: row.dropoutReason || null,
        rejectReason: row.rejectReason || null,
      },
    ]),
  );
};

const sha256Hex = (buffer) =>
  crypto.createHash("sha256").update(buffer).digest("hex");

const shouldDebugExtraInfo = () =>
  String(process.env.DEBUG_EXTRA_INFO || "").trim() === "1";

const upsertSubmittedReason = async (
  connection,
  { resId, recruiterRid, jobJid, candidateName, email, phone, submittedReason },
) => {
  const reason = String(submittedReason || "").trim() || null;
  if (!(await tableExists("extra_info"))) return;

  const columns = await getTableColumns("extra_info", connection);
  if (!columns.has("res_id") && !columns.has("resume_id")) {
    if (shouldDebugExtraInfo()) {
      console.warn(
        "[extra_info] Skipping submitted_reason upsert because extra_info has no res_id/resume_id column.",
      );
    }
    return;
  }

  const insertColumns = [];
  const insertValues = [];
  const placeholders = [];
  const updateAssignments = [];

  if (columns.has("submitted_reason")) {
    updateAssignments.push("submitted_reason = VALUES(submitted_reason)");
  }

  const addColumnValue = (columnName, value) => {
    if (!columns.has(columnName) || value === undefined) return;
    insertColumns.push(columnName);
    insertValues.push(value);
    placeholders.push("?");
  };

  addColumnValue("res_id", resId);
  addColumnValue("resume_id", resId);
  addColumnValue("job_jid", jobJid);
  addColumnValue("recruiter_rid", recruiterRid);
  addColumnValue("rid", recruiterRid);
  addColumnValue("candidate_name", candidateName);
  addColumnValue("applicant_name", candidateName);
  addColumnValue("candidate_email", email);
  addColumnValue("applicant_email", email);
  addColumnValue("email", email);
  addColumnValue("phone", phone);
  addColumnValue("submitted_reason", reason);

  if (insertColumns.length === 0) return;
  if (columns.has("updated_at")) {
    updateAssignments.push("updated_at = CURRENT_TIMESTAMP");
  }
  if (updateAssignments.length === 0) return;

  const [result] = await connection.query(
    `INSERT INTO extra_info (${insertColumns.map((column) => `\`${column}\``).join(", ")})
     VALUES (${placeholders.join(", ")})
     ON DUPLICATE KEY UPDATE ${updateAssignments.join(", ")}`,
    insertValues,
  );

  if (shouldDebugExtraInfo()) {
    console.log("[extra_info] Upserted submitted_reason", {
      resId,
      affectedRows: result?.affectedRows,
      changedRows: result?.changedRows,
      warningStatus: result?.warningStatus,
    });
  }
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

const normalizeAccessMode = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "restricted") return "restricted";
  return "open";
};

const normalizeJobJid = (value) => {
  const normalized = String(value || "").trim();
  return normalized || null;
};

const toNonNegativeInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return parsed;
};

const toNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toMoneyOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return Math.round(parsed * 100) / 100;
};

const toMoneyNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed * 100) / 100;
};

const safeJsonOrNull = (value) => {
  if (value === undefined || value === null) return null;
  return JSON.stringify(value);
};

const parseJsonField = (value) => {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
};

const normalizePhoneForStorage = (phone) => {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length === 10) return digits;
  if (digits.length > 10) return digits.slice(-10);
  return digits;
};

const buildAutofillFromParsedData = (parsedData) => {
  const safeData =
    parsedData && typeof parsedData === "object" && !Array.isArray(parsedData)
      ? parsedData
      : {};
  const educationCandidates = Array.isArray(safeData.education)
    ? safeData.education.filter((item) => item && typeof item === "object")
    : safeData.education && typeof safeData.education === "object"
      ? [safeData.education]
      : [];

  const pickString = (...values) => {
    for (const value of values) {
      const candidate =
        value === undefined || value === null ? "" : String(value).trim();
      if (candidate) return candidate;
    }
    return "";
  };

  const pickFromEducation = (...keys) => {
    for (const education of educationCandidates) {
      for (const key of keys) {
        const candidate = pickString(education[key]);
        if (candidate) return candidate;
      }
    }
    return "";
  };

  const toAgeFromDob = (dobValue) => {
    const dobText = pickString(dobValue);
    if (!dobText) return "";
    const dob = new Date(dobText);
    if (Number.isNaN(dob.getTime())) return "";
    const now = new Date();
    let ageYears = now.getFullYear() - dob.getFullYear();
    const monthDiff = now.getMonth() - dob.getMonth();
    const dayDiff = now.getDate() - dob.getDate();
    if (monthDiff < 0 || (monthDiff === 0 && dayDiff < 0)) {
      ageYears -= 1;
    }
    if (ageYears < 16 || ageYears > 100) return "";
    return String(ageYears);
  };

  const ageValue = pickString(safeData.age, safeData.current_age);
  const derivedAge =
    ageValue || toAgeFromDob(safeData.dob || safeData.date_of_birth);

  return {
    name: pickString(safeData.full_name, safeData.fullName, safeData.name),
    phone: normalizePhoneForStorage(
      pickString(safeData.phone, safeData.phone_number),
    ),
    email: pickString(safeData.email, safeData.mail).toLowerCase(),
    latestEducationLevel: pickFromEducation(
      "latest_education_level",
      "latestEducationLevel",
      "education_level",
      "degree",
      "qualification",
    ),
    boardUniversity: pickFromEducation(
      "board_university",
      "boardUniversity",
      "university",
      "university_name",
      "board",
    ),
    institutionName: pickFromEducation(
      "institution_name",
      "institutionName",
      "college_name",
      "college",
      "school_name",
      "school",
    ),
    age: derivedAge,
  };
};

const buildJobAtsContext = (jobRow) => {
  if (!jobRow || typeof jobRow !== "object") return "";
  const parts = [
    `Role: ${String(jobRow.role_name || "").trim()}`,
    `Company: ${String(jobRow.company_name || "").trim()}`,
    `Job Description: ${String(jobRow.job_description || "").trim()}`,
    `Required Skills: ${String(jobRow.skills || "").trim()}`,
    `Qualification: ${String(jobRow.qualification || "").trim()}`,
    `Benefits: ${String(jobRow.benefits || "").trim()}`,
    `Experience: ${String(jobRow.experience || "").trim()}`,
    `Location: ${[jobRow.city, jobRow.state, jobRow.pincode]
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .join(", ")}`,
  ];

  return parts.filter((line) => !line.endsWith(":")).join("\n");
};

const escapeLike = (value) => String(value || "").replace(/[\\%_]/g, "\\$&");

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

const checkJobAccess = async (recruiterId, jobId) => {
  const safeJobId = normalizeJobJid(jobId);
  if (!safeJobId) {
    return { canAccess: false, reason: "job_jid is required." };
  }

  const hasAccessModeColumn = await columnExists("jobs", "access_mode");
  const [jobRows] = await pool.query(
    `SELECT
      jid,
      company_name,
      ${hasAccessModeColumn ? "access_mode" : "'open'"} AS access_mode
    FROM jobs
    WHERE jid = ?
    LIMIT 1`,
    [safeJobId],
  );

  if (jobRows.length === 0) {
    return { canAccess: false, reason: "Job not found", jobDetails: null };
  }

  const job = jobRows[0];
  const accessMode = normalizeAccessMode(job.access_mode);
  const jobDetails = {
    jid: String(job.jid || "").trim(),
    company_name: job.company_name || "",
    access_mode: accessMode,
  };

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

const runResumeUpload = (req, res) =>
  new Promise((resolve, reject) => {
    uploadResume.single("resume_file")(req, res, (error) => {
      if (error) return reject(error);
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
    const selectRole = hasRoleColumn ? "role" : "NULL AS role";
    const selectAddJob = hasAddJobColumn ? "addjob" : "0 AS addjob";
    const selectPasswordChanged = hasPasswordChangedColumn
      ? "password_changed"
      : "1 AS password_changed";

    const [rows] = await pool.query(
      `SELECT rid, name, email, ${selectRole}, ${selectAddJob}, ${selectPasswordChanged}
       FROM recruiter
       WHERE email = ? AND password = ?
       LIMIT 1`,
      [email.trim().toLowerCase(), password],
    );

    if (rows.length === 0) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const recruiter = rows[0];
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

      const whereClauses = [
        hasAccessModeColumn
          ? "(j.access_mode = 'open' OR (j.access_mode = 'restricted' AND jra.id IS NOT NULL))"
          : "1 = 1",
      ];
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
          j.salary,
          j.positions_open,
          ${hasAccessModeColumn ? "j.access_mode" : "'open'"} AS access_mode,
          j.skills,
          j.created_at
        FROM jobs j
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
          salary: job.salary || null,
          positions_open: Number(job.positions_open) || 0,
          access_mode: normalizeAccessMode(job.access_mode),
          skills: job.skills || "",
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
      const access = await checkJobAccess(rid, safeJobId);
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
  requireRoles("recruiter"),
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
      return res.status(400).json({
        success: false,
        error: "Invalid resume upload payload.",
      });
    }

    const recruiterRid = String(req.body?.recruiter_rid || "").trim();
    const authRid = String(req.auth?.rid || "").trim();
    const safeJobId = normalizeJobJid(req.body?.job_jid);

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

    try {
      const access = await checkJobAccess(recruiterRid, safeJobId);
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
          error: "resume_file is required.",
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

      const autofill = buildAutofillFromParsedData(parsed.parsedData);
      const candidateName = String(
        req.body?.candidate_name ||
          autofill.name ||
          extractApplicantName(parsed.parsedData) ||
          "",
      ).trim();
      const phone = normalizePhoneForStorage(
        req.body?.phone || autofill.phone || "",
      );
      const email = String(req.body?.email || autofill.email || "")
        .trim()
        .toLowerCase();
      const latestEducationLevel = String(
        req.body?.latest_education_level || autofill.latestEducationLevel || "",
      ).trim();
      const boardUniversity = String(
        req.body?.board_university || autofill.boardUniversity || "",
      ).trim();
      const institutionName = String(
        req.body?.institution_name || autofill.institutionName || "",
      ).trim();
      const age = toNumberOrNull(req.body?.age ?? autofill.age);
      const submittedReason = String(
        req.body?.submitted_reason ??
          req.body?.submittedReason ??
          req.body?.notes ??
          "",
      ).trim();

      if (
        !candidateName ||
        !phone ||
        !email ||
        !latestEducationLevel ||
        !boardUniversity ||
        !institutionName ||
        age === null
      ) {
        return res.status(400).json({
          success: false,
          error:
            "Resume parsing could not fill all required fields. Please provide candidate_name, phone, email, latest_education_level, board_university, institution_name, and age.",
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

        const hasFileHashColumn = await columnExists(
          "resumes_data",
          "file_hash",
        );
        const fileHash = hasFileHashColumn
          ? sha256Hex(resumeFile.buffer)
          : null;
        if (hasFileHashColumn && fileHash) {
          const [duplicateRows] = await connection.query(
            "SELECT res_id AS resId FROM resumes_data WHERE file_hash = ? LIMIT 1",
            [fileHash],
          );
          if (duplicateRows.length > 0) {
            await connection.rollback();
            return res.status(409).json({
              success: false,
              error:
                "A copy of the provided resume already exists in our database.",
            });
          }
        }

        const [sequenceResult] = await connection.query(
          "INSERT INTO resume_id_sequence VALUES ()",
        );
        const sequenceValue = Number(sequenceResult.insertId);
        const resId = `res_${sequenceValue}`;

        await connection.query(
          `INSERT INTO applications
          (
            job_jid,
            candidate_name,
            phone,
            email,
            latest_education_level,
            board_university,
            institution_name,
            age,
            resume_filename,
            resume_parsed_data,
            ats_score,
            ats_match_percentage,
            ats_raw_json
          )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            safeJobId,
            candidateName,
            phone,
            email,
            latestEducationLevel,
            boardUniversity,
            institutionName,
            age,
            originalName,
            safeJsonOrNull(parsed.parsedData),
            parsed.atsScore,
            parsed.atsMatchPercentage,
            safeJsonOrNull({
              ats_score: parsed.atsScore,
              ats_match_percentage: parsed.atsMatchPercentage,
              ats_details: parsed.atsRawJson,
              parsed_data: parsed.parsedData,
            }),
          ],
        );

        const resumeInsertColumns = [
          "res_id",
          "rid",
          "applicant_name",
          "job_jid",
          "resume",
          "resume_filename",
          "resume_type",
        ];
        const resumeInsertValuesSql = ["?", "?", "?", "?", "?", "?", "?"];
        const resumeInsertValues = [
          resId,
          recruiterRid,
          candidateName,
          safeJobId,
          resumeFile.buffer,
          originalName,
          validation.extension,
        ];

        if (hasFileHashColumn) {
          resumeInsertColumns.push("file_hash");
          resumeInsertValuesSql.push("?");
          resumeInsertValues.push(fileHash);
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
            parsed_data: parsed.parsedData,
          }),
        );

        await connection.query(
          `INSERT INTO resumes_data (${resumeInsertColumns.join(", ")}) VALUES (${resumeInsertValuesSql.join(", ")})`,
          resumeInsertValues,
        );

        await connection.query(
          `INSERT INTO status (recruiter_rid, submitted, last_updated)
         VALUES (?, 1, CURRENT_TIMESTAMP)
         ON DUPLICATE KEY UPDATE
           submitted = COALESCE(submitted, 0) + 1,
           last_updated = CURRENT_TIMESTAMP`,
          [recruiterRid],
        );

        await upsertSubmittedReason(connection, {
          resId,
          recruiterRid,
          jobJid: safeJobId,
          candidateName,
          email,
          phone,
          submittedReason,
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
    const { job_jid, resumeBase64, resumeFilename, resumeMimeType } =
      req.body || {};

    if (!job_jid || !resumeBase64 || !resumeFilename) {
      return res.status(400).json({
        message: "job_jid, resumeBase64, and resumeFilename are required.",
      });
    }

    const safeJobId = normalizeJobJid(job_jid);
    if (!safeJobId) {
      return res.status(400).json({ message: "job_jid is required." });
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
      const hasFileHashColumn = await columnExists("resumes_data", "file_hash");
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

      const fileHash = hasFileHashColumn ? sha256Hex(resumeBuffer) : null;

      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        if (hasFileHashColumn && fileHash) {
          const [duplicateRows] = await connection.query(
            "SELECT res_id AS resId FROM resumes_data WHERE file_hash = ? LIMIT 1",
            [fileHash],
          );
          if (duplicateRows.length > 0) {
            await connection.rollback();
            return res.status(409).json({
              message:
                "A copy of the provided resume already exists in our database.",
            });
          }
        }

        const [sequenceResult] = await connection.query(
          "INSERT INTO resume_id_sequence VALUES ()",
        );
        const sequenceValue = Number(sequenceResult.insertId);
        const resId = `res_${sequenceValue}`;

        const insertColumns = ["res_id", "rid"];
        const insertValues = [resId, rid];

        if (hasJobJidColumn) {
          insertColumns.push("job_jid");
          insertValues.push(safeJobId);
        }

        insertColumns.push("resume", "resume_filename", "resume_type");
        insertValues.push(
          resumeBuffer,
          normalizedFilename,
          validation.extension,
        );

        if (hasFileHashColumn) {
          insertColumns.push("file_hash");
          insertValues.push(fileHash);
        }

        if (hasSubmittedByRoleColumn) {
          insertColumns.push("submitted_by_role");
          insertValues.push("recruiter");
        }

        if (hasApplicantNameColumn) {
          insertColumns.push("applicant_name");
          insertValues.push(resumeAts.applicantName || null);
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
            resumeAts.atsRawJson === undefined || resumeAts.atsRawJson === null
              ? null
              : JSON.stringify(resumeAts.atsRawJson),
          );
        }

        const placeholders = insertColumns.map(() => "?").join(", ");
        await connection.query(
          `INSERT INTO resumes_data (${insertColumns.join(", ")}) VALUES (${placeholders})`,
          insertValues,
        );

        await connection.commit();
        return res.status(201).json({
          message: "Resume added successfully.",
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
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;

    try {
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
      const jobJidSelection = hasJobJidColumn
        ? "rd.job_jid AS jobJid,"
        : "NULL AS jobJid,";
      const candidateNameSelection = hasApplicantNameColumn
        ? "rd.applicant_name AS candidateName,"
        : "NULL AS candidateName,";
      const atsScoreSelection = hasAtsScoreColumn
        ? "ats_score AS atsScore,"
        : "NULL AS atsScore,";
      const atsMatchSelection = hasAtsMatchColumn
        ? "ats_match_percentage AS atsMatchPercentage,"
        : "NULL AS atsMatchPercentage,";

      const [rows] = await pool.query(
        `SELECT
        rd.res_id AS resId,
        ${jobJidSelection}
        ${candidateNameSelection}
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType,
        ${atsScoreSelection}
        ${atsMatchSelection}
        rd.uploaded_at AS uploadedAt,
        COALESCE(jrs.selection_status, 'pending') AS workflowStatus,
        jrs.selected_at AS workflowUpdatedAt
      FROM resumes_data rd
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
        resumes: rows.map((row) => ({
          ...row,
          candidateName: row.candidateName || null,
          atsScore: row.atsScore === null ? null : Number(row.atsScore),
          atsMatchPercentage:
            row.atsMatchPercentage === null
              ? null
              : Number(row.atsMatchPercentage),
          workflowStatus: row.workflowStatus || "pending",
          workflowUpdatedAt: row.workflowUpdatedAt || null,
          ...(extraInfoByResumeId.get(String(row.resId || "").trim()) || {}),
        })),
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
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid, resId } = req.params;

    try {
      const [rows] = await pool.query(
        `SELECT
        resume,
        resume_filename AS resumeFilename,
        resume_type AS resumeType
      FROM resumes_data
      WHERE res_id = ? AND rid = ?
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
  requireRecruiterOwner,
  async (req, res) => {
    const { rid } = req.params;

    try {
      const hasApplicationsTable = await tableExists("applications");
      const hasJobsTable = await tableExists("jobs");
      const jobsRecruiterIdColumn = hasJobsTable
        ? await getRecruiterIdColumn("jobs")
        : null;

      if (!hasApplicationsTable || !hasJobsTable || !jobsRecruiterIdColumn) {
        return res.status(200).json({ applications: [] });
      }

      const [rows] = await pool.query(
        `SELECT
        a.id,
        a.job_jid AS jobJid,
        a.candidate_name AS candidateName,
        a.email,
        a.ats_score AS atsScore,
        a.ats_match_percentage AS atsMatchPercentage,
        a.resume_filename AS resumeFilename,
        a.created_at AS createdAt,
        j.role_name AS roleName,
        j.company_name AS companyName
      FROM applications a
      INNER JOIN jobs j ON j.jid = a.job_jid
      WHERE j.${jobsRecruiterIdColumn} = ?
      ORDER BY a.created_at DESC`,
        [rid],
      );

      return res.status(200).json({
        applications: rows.map((row) => ({
          id: row.id,
          candidateName: row.candidateName,
          email: row.email,
          jobJid: row.jobJid === null ? null : Number(row.jobJid),
          atsScore: row.atsScore === null ? null : Number(row.atsScore),
          atsMatchPercentage:
            row.atsMatchPercentage === null
              ? null
              : Number(row.atsMatchPercentage),
          resumeFilename: row.resumeFilename || null,
          createdAt: row.createdAt,
          job: {
            roleName: row.roleName,
            companyName: row.companyName,
          },
        })),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiter applications.",
        error: error.message,
      });
    }
  },
);

const allowedRecruiterTransitions = {
  verified: ["walk_in", "rejected"],
  walk_in: ["selected", "rejected"],
  selected: ["joined", "dropout", "rejected"],
};

const statusReasonColumnMap = {
  walk_in: "walk_in_reason",
  selected: "select_reason",
  rejected: "reject_reason",
  joined: "joined_reason",
  dropout: "dropout_reason",
};

router.post(
  "/api/recruiters/:rid/resumes/:resId/advance-status",
  requireAuth,
  requireRoles("recruiter"),
  requireRecruiterOwner,
  async (req, res) => {
    const { rid, resId } = req.params;
    const targetStatus = String(req.body?.status || "")
      .trim()
      .toLowerCase();
    const reason = String(req.body?.reason || "").trim() || null;

    if (!targetStatus) {
      return res.status(400).json({ message: "status is required." });
    }

    try {
      const [resumeRows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.job_jid AS jobJid,
          rd.rid AS recruiterRid,
          rd.applicant_name AS candidateName,
          rd.applicant_email AS email,
          COALESCE(jrs.selection_status, 'pending') AS currentStatus
        FROM resumes_data rd
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
      const currentStatus = String(
        resume.currentStatus || "pending",
      ).toLowerCase();
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
          [resume.jobJid, resId, rid, targetStatus, reason],
        );

        const reasonColumn = statusReasonColumnMap[targetStatus];
        if (reasonColumn && reason && (await tableExists("extra_info"))) {
          const extraColumns = await getTableColumns("extra_info", connection);
          if (extraColumns.has(reasonColumn)) {
            const idCol = extraColumns.has("res_id")
              ? "res_id"
              : extraColumns.has("resume_id")
                ? "resume_id"
                : null;
            if (idCol) {
              const insertCols = [idCol];
              const insertVals = [resId];
              const placeholders = ["?"];
              const updates = [`${reasonColumn} = VALUES(${reasonColumn})`];

              if (extraColumns.has("job_jid")) {
                insertCols.push("job_jid");
                insertVals.push(resume.jobJid);
                placeholders.push("?");
              }
              if (extraColumns.has("recruiter_rid")) {
                insertCols.push("recruiter_rid");
                insertVals.push(rid);
                placeholders.push("?");
              }
              if (extraColumns.has("rid")) {
                insertCols.push("rid");
                insertVals.push(rid);
                placeholders.push("?");
              }
              insertCols.push(reasonColumn);
              insertVals.push(reason);
              placeholders.push("?");
              if (extraColumns.has("updated_at")) {
                updates.push("updated_at = CURRENT_TIMESTAMP");
              }

              await connection.query(
                `INSERT INTO extra_info (${insertCols.map((c) => `\`${c}\``).join(", ")})
                 VALUES (${placeholders.join(", ")})
                 ON DUPLICATE KEY UPDATE ${updates.join(", ")}`,
                insertVals,
              );
            }
          }
        }

        const statusDeltaMap = {
          verified: "verified",
          walk_in: "walk_in",
          selected: "`select`",
          rejected: "reject",
          joined: "joined",
          dropout: "dropout",
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

        await connection.commit();
        return res.status(200).json({
          message: "Resume status advanced successfully.",
          data: {
            resId,
            previousStatus: currentStatus,
            status: targetStatus,
            reason,
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
