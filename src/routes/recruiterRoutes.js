const express = require("express");
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
const {
  tableExists,
  columnExists,
  getTableColumns,
  fetchExtraInfoByResumeIds,
  upsertExtraInfoFields,
  upsertCandidateFields,
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
  sha256Hex,
  buildAutofillFromParsedData,
  buildJobAtsContext,
} = require("../utils/formatters");

const router = express.Router();
const buildCandidateId = (sequenceValue) => `c_${sequenceValue}`;
const uploadResume = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
});

// normalizeAccessMode from shared utils returns "" for invalid; this wrapper defaults to "open"
const normalizeAccessMode = (value) => _normalizeAccessMode(value) || "open";

const toNonNegativeInt = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) return fallback;
  return parsed;
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
          "job_jid",
          "resume",
          "resume_filename",
          "resume_type",
        ];
        const resumeInsertValuesSql = ["?", "?", "?", "?", "?", "?"];
        const resumeInsertValues = [
          resId,
          recruiterRid,
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

        await upsertCandidateFields(connection, {
          cid,
          resId,
          jobJid: safeJobId,
          recruiterRid,
          name: candidateName,
          phone,
          email,
          levelOfEdu: latestEducationLevel,
          boardUni: boardUniversity,
          institutionName,
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
          submittedReason: String(submittedReason || "").trim() || null,
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

        await upsertCandidateFields(connection, {
          cid,
          resId,
          jobJid: safeJobId,
          recruiterRid: rid,
          name: resumeAts.applicantName || "Unknown Candidate",
        });

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
        c.name AS candidateName,
        c.walk_in AS walkInDate,
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType,
        ${atsScoreSelection}
        ${atsMatchSelection}
        rd.uploaded_at AS uploadedAt,
        COALESCE(jrs.selection_status, 'pending') AS workflowStatus,
        jrs.selected_at AS workflowUpdatedAt,
        c.joining_date AS joiningDate,
        jrs.joining_note AS joiningNote,
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
        resumes: rows.map((row) => ({
          ...row,
          candidateName: row.candidateName || null,
          atsScore: row.atsScore === null ? null : Number(row.atsScore),
          atsMatchPercentage:
            row.atsMatchPercentage === null
              ? null
              : Number(row.atsMatchPercentage),
          walkInDate: row.walkInDate || null,
          workflowStatus: row.workflowStatus || "pending",
          workflowUpdatedAt: row.workflowUpdatedAt || null,
          joiningDate: row.joiningDate || null,
          joiningNote: row.joiningNote || null,
          job: {
            jobJid: row.jobJid ? String(row.jobJid).trim() : null,
            companyName: row.companyName || null,
            roleName: row.roleName || null,
            city: row.city || null,
          },
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
        a.res_id AS resId,
        a.job_jid AS jobJid,
        c.name AS candidateName,
        c.email,
        a.ats_score AS atsScore,
        a.ats_match_percentage AS atsMatchPercentage,
        a.resume_filename AS resumeFilename,
        a.created_at AS createdAt,
        j.role_name AS roleName,
        j.company_name AS companyName,
        j.city AS city
      FROM applications a
      LEFT JOIN candidate c ON c.res_id = a.res_id
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
            city: row.city || null,
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
  walk_in: ["further", "selected", "rejected"],
  further: ["selected", "rejected"],
  selected: ["joined", "dropout", "rejected"],
  joined: ["billed", "left"],
};

const statusReasonColumnMap = {
  walk_in: "walk_in_reason",
  further: "further_reason",
  selected: "select_reason",
  rejected: "reject_reason",
  joined: "joined_reason",
  dropout: "dropout_reason",
  billed: "billed_reason",
  left: "left_reason",
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
    const joiningDate = req.body?.joining_date
      ? String(req.body.joining_date).trim()
      : null;
    const joiningNote = req.body?.joining_note
      ? String(req.body.joining_note).trim()
      : null;

    if (!targetStatus) {
      return res.status(400).json({ message: "status is required." });
    }

    if ((targetStatus === "billed" || targetStatus === "left") && !reason) {
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
          c.name AS candidateName,
          c.email AS email,
          COALESCE(jrs.selection_status, 'pending') AS currentStatus
        FROM resumes_data rd
        LEFT JOIN candidate c ON c.res_id = rd.res_id
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
            (job_jid, res_id, selected_by_admin, selection_status, selection_note, joining_note)
           VALUES (?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             selected_by_admin = VALUES(selected_by_admin),
             selection_status = VALUES(selection_status),
             selection_note = VALUES(selection_note),
             joining_note = CASE WHEN VALUES(selection_status) = 'joined' THEN VALUES(joining_note) ELSE joining_note END,
             selected_at = CURRENT_TIMESTAMP`,
          [
            resume.jobJid,
            resId,
            rid,
            targetStatus,
            reason,
            targetStatus === "joined" ? joiningNote : null,
          ],
        );

        if (targetStatus === "walk_in") {
          await upsertCandidateFields(connection, {
            resId,
            cid: undefined,
            walkIn: new Date().toISOString().slice(0, 10),
          });
        }

        if (targetStatus === "joined" && joiningDate) {
          await upsertCandidateFields(connection, {
            resId,
            cid: undefined,
            joiningDate,
          });
        }

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

        await connection.commit();
        return res.status(200).json({
          message: "Resume status advanced successfully.",
          data: {
            resId,
            previousStatus: currentStatus,
            status: targetStatus,
            reason,
            joining_date: targetStatus === "joined" ? joiningDate : undefined,
            joining_note: targetStatus === "joined" ? joiningNote : undefined,
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
