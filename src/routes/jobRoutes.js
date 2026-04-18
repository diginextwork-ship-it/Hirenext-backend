const express = require("express");
const pool = require("../config/db");
const {
  SUPPORTED_RESUME_TYPES,
  getResumeExtension,
  decodeResumeBuffer,
  parseResumeWithAts,
  extractApplicantName,
  isImageResumeType,
} = require("../resumeparser/service");
const {
  normalizeRoleAlias,
  requireAuth,
  requireRoles,
} = require("../middleware/auth");
const { validateResumeFile } = require("../middleware/uploadValidation");
const {
  tableExists,
  columnExists,
  getTableColumns,
  getColumnMaxLength,
  fetchExtraInfoByResumeIds,
  upsertExtraInfoFields,
  upsertCandidateFields,
  addCandidateBillIntakeEntry,
} = require("../utils/dbHelpers");
const {
  toNumberOrNull,
  normalizeJobJid,
  toTrimmedString,
  normalizeAccessMode,
  normalizePhoneForStorage,
  safeJsonOrNull,
  parseJsonField,
  sha256Hex,
  dedupeStringList,
  buildAutofillFromParsedData,
  extractCandidateSnapshot,
  buildJobAtsContext,
} = require("../utils/formatters");
const {
  CANONICAL_RESUME_STATUSES,
  getPreviousWorkflowStatus,
  normalizeResumeStatusInput,
  normalizeWorkflowStatus,
} = require("../utils/resumeStatusFlow");
const {
  STATUS_REASON_FIELD_MAP,
  buildResumeCompatibilityFields,
  resolveStatusReasonInput,
} = require("../utils/resumeCompatibility");
const { getCurrentDateOnlyInBusinessTimeZone } = require("../utils/dateTime");

const router = express.Router();
const buildCandidateId = (sequenceValue) => `c_${sequenceValue}`;
const buildResumeProcessingState = (overrides = {}) => ({
  status: "completed",
  resumeParsed: true,
  atsCalculated: true,
  submitAllowed: true,
  ...overrides,
});

// ─── Top Performing Recruiters Leaderboard ─────────────────────────────
// Returns top 10 recruiters by total points (and billed points)
router.get(
  "/api/recruiters/top-performers",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader", "admin"),
  async (_req, res) => {
    try {
      // Get top 10 recruiters by points
      const [rows] = await pool.query(
        `SELECT
          r.rid,
          r.name,
          r.email,
          COALESCE(r.points, 0) AS totalPoints,
          COALESCE(bl.billed_points, 0) AS billedPoints,
          COALESCE(bl.billed_count, 0) AS billedCount
        FROM recruiter r
        LEFT JOIN (
          SELECT
            recruiter_rid,
            SUM(points) AS billed_points,
            COUNT(*) AS billed_count
          FROM recruiter_points_log
          GROUP BY recruiter_rid
        ) bl ON bl.recruiter_rid = r.rid
        WHERE LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'
        ORDER BY COALESCE(r.points, 0) DESC, r.name ASC
        LIMIT 10`,
      );

      const leaderboard = rows.map((row, idx) => ({
        rank: idx + 1,
        rid: row.rid,
        name: row.name,
        email: row.email,
        totalPoints: Number(row.totalPoints) || 0,
        billedPoints: Number(row.billedPoints) || 0,
        billedCount: Number(row.billedCount) || 0,
      }));

      return res.status(200).json({
        leaderboard,
        count: leaderboard.length,
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch top recruiters leaderboard.",
        details: error.message,
      });
    }
  },
);

const toPositiveIntOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) return null;
  return parsed;
};

const toNonNegativeIntOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) return null;
  return parsed;
};

const toNonNegativeNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return parsed;
};

const toAtsNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Number(parsed.toFixed(2));
};

const ensureJobIdSequenceTable = async (connection) => {
  await connection.query(
    `CREATE TABLE IF NOT EXISTS job_id_sequence (
      seq_id BIGINT AUTO_INCREMENT PRIMARY KEY
    )`,
  );
};

const syncJobIdSequenceWithJobs = async (connection) => {
  const [maxRows] = await connection.query(
    `SELECT COALESCE(MAX(CAST(SUBSTRING(jid, 5) AS UNSIGNED)), 0) AS maxJidNumber
     FROM jobs
     WHERE jid REGEXP '^JID-[0-9]+$'`,
  );
  const maxJidNumber = Number(maxRows?.[0]?.maxJidNumber) || 0;

  const [autoIncrementRows] = await connection.query(
    `SELECT COALESCE(AUTO_INCREMENT, 1) AS autoIncrementValue
     FROM information_schema.tables
     WHERE table_schema = DATABASE()
       AND table_name = 'job_id_sequence'
     LIMIT 1`,
  );
  const autoIncrementValue =
    Number(autoIncrementRows?.[0]?.autoIncrementValue) || 1;

  if (autoIncrementValue <= maxJidNumber) {
    await connection.query(
      `ALTER TABLE job_id_sequence AUTO_INCREMENT = ${maxJidNumber + 1}`,
    );
  }
};

const allocateNextJobJid = async (connection) => {
  await ensureJobIdSequenceTable(connection);
  await syncJobIdSequenceWithJobs(connection);
  const [sequenceResult] = await connection.query(
    "INSERT INTO job_id_sequence VALUES ()",
  );
  const sequenceValue = Number(sequenceResult.insertId);
  if (!Number.isInteger(sequenceValue) || sequenceValue <= 0) {
    throw new Error("Failed to allocate next job jid.");
  }
  return `JID-${sequenceValue}`;
};

const getActiveAccessCount = async (jobId) => {
  const [rows] = await pool.query(
    `SELECT COUNT(*) AS total
     FROM job_recruiter_access
     WHERE job_jid = ? AND is_active = TRUE`,
    [jobId],
  );
  return Number(rows?.[0]?.total) || 0;
};

const isTeamLeaderLikeRole = (role) => {
  const normalized = normalizeRoleAlias(role);
  return (
    normalized === "team leader" ||
    normalized === "team_leader" ||
    normalized === "job creator"
  );
};

const requireOwnedJob = async (req, res, next) => {
  const safeJobId = normalizeJobJid(req.params.jid);
  if (!safeJobId) {
    return res.status(400).json({ message: "jid is required." });
  }

  try {
    const hasAccessModeColumn = await columnExists("jobs", "access_mode");
    const hasRecruiterRoleColumn = await columnExists("recruiter", "role");
    const [rows] = await pool.query(
      `SELECT
        j.jid,
        j.recruiter_rid AS recruiterRid,
        ${hasAccessModeColumn ? "j.access_mode" : "'open'"} AS accessMode,
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

    if (rows.length === 0) {
      return res.status(404).json({ message: "Job not found." });
    }

    const authRid = toTrimmedString(req.auth?.rid);
    const authRole = normalizeRoleAlias(req.auth?.role);
    const recruiterRid = toTrimmedString(rows[0].recruiterRid);
    const creatorRole = normalizeRoleAlias(rows[0].creatorRole);
    const canManageAsTeamLeader =
      isTeamLeaderLikeRole(authRole) &&
      (!hasRecruiterRoleColumn || isTeamLeaderLikeRole(creatorRole));

    if (!authRid || (recruiterRid !== authRid && !canManageAsTeamLeader)) {
      return res
        .status(403)
        .json({ message: "You do not have permission to manage this job." });
    }

    req.ownedJob = {
      jid: safeJobId,
      recruiterRid,
      accessMode: normalizeAccessMode(rows[0].accessMode) || "open",
    };
    return next();
  } catch (error) {
    return res.status(500).json({
      message: "Failed to validate job ownership.",
      error: error.message,
    });
  }
};

const validateRecruiterIds = async (recruiterIds) => {
  const uniqueRecruiterIds = dedupeStringList(recruiterIds);
  if (uniqueRecruiterIds.length === 0) {
    return { validRecruiterIds: [], invalidRecruiterIds: [] };
  }

  const hasRoleColumn = await columnExists("recruiter", "role");
  const [rows] = hasRoleColumn
    ? await pool.query(
        `SELECT rid
         FROM recruiter
         WHERE rid IN (?) AND LOWER(TRIM(role)) = 'recruiter'`,
        [uniqueRecruiterIds],
      )
    : await pool.query(
        `SELECT rid
         FROM recruiter
         WHERE rid IN (?)`,
        [uniqueRecruiterIds],
      );

  const validRecruiterSet = new Set(rows.map((row) => String(row.rid)));
  const invalidRecruiterIds = uniqueRecruiterIds.filter(
    (rid) => !validRecruiterSet.has(rid),
  );
  return {
    validRecruiterIds: uniqueRecruiterIds.filter((rid) =>
      validRecruiterSet.has(rid),
    ),
    invalidRecruiterIds,
  };
};

const allowedManualResumeStatuses = new Set(
  Array.from(CANONICAL_RESUME_STATUSES).filter(
    (status) => status !== "further",
  ),
);

const resolveManualRollbackTarget = (resume = {}) => {
  const currentStatus = normalizeWorkflowStatus(
    resume.currentStatus,
    "submitted",
  );
  const currentDerivedStatus =
    currentStatus === "shortlisted" && resume.currentJoiningDate
      ? "selected"
      : currentStatus;

  if (currentDerivedStatus === "rejected") {
    if (resume.currentJoiningDate || resume.selectedAt) return "selected";
    if (resume.shortlistedAt) return "shortlisted";
    if (resume.currentWalkInDate || resume.walkInAt) return "walk_in";
    if (resume.othersAt || resume.othersReason) return "others";
    if (resume.verifiedAt || resume.verifiedReason) return "verified";
    return "submitted";
  }

  if (currentDerivedStatus === "dropout") {
    return resume.currentJoiningDate || resume.selectedAt
      ? "selected"
      : "shortlisted";
  }

  return getPreviousWorkflowStatus(currentDerivedStatus) || null;
};

router.get("/api/jobs", async (_req, res) => {
  try {
    const hasCityColumn = await columnExists("jobs", "city");
    const hasStateColumn = await columnExists("jobs", "state");
    const hasPincodeColumn = await columnExists("jobs", "pincode");
    const hasPositionsOpenColumn = await columnExists("jobs", "positions_open");
    const hasRevenueColumn = await columnExists("jobs", "revenue");
    const hasPointsPerJoiningColumn = await columnExists(
      "jobs",
      "points_per_joining",
    );
    const hasSkillsColumn = await columnExists("jobs", "skills");
    const hasExperienceColumn = await columnExists("jobs", "experience");
    const hasSalaryColumn = await columnExists("jobs", "salary");
    const hasQualificationColumn = await columnExists("jobs", "qualification");
    const hasBenefitsColumn = await columnExists("jobs", "benefits");
    const hasCreatedAtColumn = await columnExists("jobs", "created_at");
    const hasAccessModeColumn = await columnExists("jobs", "access_mode");

    const [rows] = await pool.query(
      `SELECT
        jid,
        recruiter_rid,
        ${hasCityColumn ? "city" : "NULL AS city"},
        ${hasStateColumn ? "state" : "NULL AS state"},
        ${hasPincodeColumn ? "pincode" : "NULL AS pincode"},
        company_name,
        role_name,
        ${hasPositionsOpenColumn ? "positions_open" : "1 AS positions_open"},
        ${hasRevenueColumn ? "revenue" : "NULL AS revenue"},
        ${hasPointsPerJoiningColumn ? "points_per_joining" : "0 AS points_per_joining"},
        ${hasSkillsColumn ? "skills" : "NULL AS skills"},
        job_description,
        ${hasExperienceColumn ? "experience" : "NULL AS experience"},
        ${hasSalaryColumn ? "salary" : "NULL AS salary"},
        ${hasQualificationColumn ? "qualification" : "NULL AS qualification"},
        ${hasBenefitsColumn ? "benefits" : "NULL AS benefits"},
        ${hasCreatedAtColumn ? "created_at" : "NULL AS created_at"},
        ${hasAccessModeColumn ? "access_mode" : "'open' AS access_mode"}
      FROM jobs
      ORDER BY jid DESC`,
    );

    return res.status(200).json({ jobs: rows });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch jobs.",
      error: error.message,
    });
  }
});

router.post(
  "/api/jobs",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader", "recruiter"),
  async (req, res) => {
    const {
      recruiter_rid,
      city,
      state,
      pincode,
      company_name,
      role_name,
      positions_open,
      revenue,
      points_per_joining,
      skills,
      job_description,
      experience,
      salary,
      qualification,
      benefits,
      access_mode,
      recruiterIds,
      accessNotes,
    } = req.body || {};

    const normalizedCompanyName = toTrimmedString(
      company_name ?? req.body?.companyName,
    );
    const normalizedCity = toTrimmedString(city ?? req.body?.city);

    const safePositionsOpen = toPositiveIntOrNull(positions_open);
    const safeRevenue = toNonNegativeNumberOrNull(revenue);
    const safePointsPerJoining = toNonNegativeIntOrNull(points_per_joining);
    const normalizedAccessMode =
      normalizeAccessMode(access_mode || "open") || "open";
    const requestedRecruiterIds = dedupeStringList(recruiterIds);
    const normalizedAccessNotes = toTrimmedString(accessNotes) || null;

    const hasCityColumn = await columnExists("jobs", "city");
    const hasStateColumn = await columnExists("jobs", "state");
    const hasPincodeColumn = await columnExists("jobs", "pincode");
    const hasJobDescriptionColumn = await columnExists(
      "jobs",
      "job_description",
    );
    const hasSkillsColumn = await columnExists("jobs", "skills");
    const hasExperienceColumn = await columnExists("jobs", "experience");
    const hasSalaryColumn = await columnExists("jobs", "salary");
    const hasQualificationColumn = await columnExists("jobs", "qualification");
    const hasBenefitsColumn = await columnExists("jobs", "benefits");
    const hasPositionsOpenColumn = await columnExists("jobs", "positions_open");
    const hasRevenueColumn = await columnExists("jobs", "revenue");
    const hasPointsPerJoiningColumn = await columnExists(
      "jobs",
      "points_per_joining",
    );
    const hasAccessModeColumn = await columnExists("jobs", "access_mode");

    if (!recruiter_rid || !normalizedCompanyName || !role_name) {
      return res.status(400).json({
        message: "recruiter_rid, company_name, and role_name are required.",
      });
    }

    const authRid = String(req.auth?.rid || "").trim();
    if (!authRid || authRid !== String(recruiter_rid).trim()) {
      return res.status(403).json({
        message: "You can only create jobs for your own recruiter ID.",
      });
    }

    if (access_mode !== undefined && !normalizeAccessMode(access_mode)) {
      return res.status(400).json({
        message: "access_mode must be either 'open' or 'restricted'.",
      });
    }

    if (hasJobDescriptionColumn && !job_description) {
      return res.status(400).json({
        message: "job_description is required.",
      });
    }
    if (hasPositionsOpenColumn && safePositionsOpen === null) {
      return res.status(400).json({
        message: "positions_open must be a positive integer.",
      });
    }
    if (
      hasRevenueColumn &&
      revenue !== undefined &&
      revenue !== null &&
      revenue !== "" &&
      safeRevenue === null
    ) {
      return res.status(400).json({
        message: "revenue must be a non-negative number.",
      });
    }
    if (
      hasPointsPerJoiningColumn &&
      points_per_joining !== undefined &&
      points_per_joining !== null &&
      points_per_joining !== "" &&
      safePointsPerJoining === null
    ) {
      return res.status(400).json({
        message: "points_per_joining must be a non-negative integer.",
      });
    }

    if (
      hasQualificationColumn &&
      qualification !== undefined &&
      qualification !== null
    ) {
      const maxLength = await getColumnMaxLength("jobs", "qualification");
      const qualificationText = String(qualification).trim();
      if (maxLength && qualificationText.length > maxLength) {
        return res.status(400).json({
          message: `qualification is too long (max ${maxLength} characters).`,
        });
      }
    }

    try {
      const hasRoleColumn = await columnExists("recruiter", "role");
      const hasAddJobColumn = await columnExists("recruiter", "addjob");
      const fields = [];
      if (hasRoleColumn) fields.push("role");
      if (hasAddJobColumn) fields.push("addjob");

      if (fields.length === 0) {
        return res
          .status(403)
          .json({ message: "Recruiter is not authorized." });
      }

      const [recruiters] = await pool.query(
        `SELECT ${fields.join(", ")} FROM recruiter WHERE rid = ? LIMIT 1`,
        [recruiter_rid],
      );

      if (recruiters.length === 0) {
        return res
          .status(403)
          .json({ message: "Recruiter is not authorized." });
      }

      const recruiterRole = normalizeRoleAlias(recruiters[0].role);
      const canCreateJobs = hasRoleColumn
        ? recruiterRole === "job creator" ||
          recruiterRole === "team leader" ||
          Boolean(recruiters[0].addjob)
        : Boolean(recruiters[0].addjob);

      if (!canCreateJobs) {
        return res
          .status(403)
          .json({ message: "Only job creator/team leader can add jobs." });
      }

      const { validRecruiterIds, invalidRecruiterIds } =
        await validateRecruiterIds(requestedRecruiterIds);
      if (invalidRecruiterIds.length > 0) {
        return res.status(400).json({
          message: "Some recruiterIds are invalid or not recruiter role users.",
          invalidRecruiterIds,
        });
      }

      const insertColumns = [
        "jid",
        "recruiter_rid",
        "company_name",
        "role_name",
      ];
      const insertValues = [
        null,
        recruiter_rid.trim(),
        normalizedCompanyName.trim(),
        role_name.trim(),
      ];

      if (hasCityColumn) {
        insertColumns.push("city");
        insertValues.push(String(normalizedCity || "N/A").trim() || "N/A");
      }
      if (hasStateColumn) {
        insertColumns.push("state");
        insertValues.push(String(state || "N/A").trim() || "N/A");
      }
      if (hasPincodeColumn) {
        insertColumns.push("pincode");
        insertValues.push(String(pincode || "N/A").trim() || "N/A");
      }
      if (hasPositionsOpenColumn) {
        insertColumns.push("positions_open");
        insertValues.push(safePositionsOpen === null ? 1 : safePositionsOpen);
      }
      if (hasRevenueColumn) {
        insertColumns.push("revenue");
        insertValues.push(safeRevenue);
      }
      if (hasPointsPerJoiningColumn) {
        insertColumns.push("points_per_joining");
        insertValues.push(
          safePointsPerJoining === null ? 0 : safePointsPerJoining,
        );
      }
      if (hasSkillsColumn) {
        insertColumns.push("skills");
        insertValues.push(toTrimmedString(skills) || null);
      }
      if (hasJobDescriptionColumn) {
        insertColumns.push("job_description");
        insertValues.push(toTrimmedString(job_description) || null);
      }
      if (hasExperienceColumn) {
        insertColumns.push("experience");
        insertValues.push(toTrimmedString(experience) || null);
      }
      if (hasSalaryColumn) {
        insertColumns.push("salary");
        insertValues.push(toTrimmedString(salary) || null);
      }
      if (hasQualificationColumn) {
        insertColumns.push("qualification");
        insertValues.push(toTrimmedString(qualification) || null);
      }
      if (hasBenefitsColumn) {
        insertColumns.push("benefits");
        insertValues.push(toTrimmedString(benefits) || null);
      }
      if (hasAccessModeColumn) {
        insertColumns.push("access_mode");
        insertValues.push(normalizedAccessMode);
      }

      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();
        const generatedJobJid = await allocateNextJobJid(connection);
        insertValues[0] = generatedJobJid;

        const placeholders = insertColumns.map(() => "?").join(", ");
        await connection.query(
          `INSERT INTO jobs (${insertColumns.join(", ")}) VALUES (${placeholders})`,
          insertValues,
        );

        if (
          normalizedAccessMode === "restricted" &&
          validRecruiterIds.length > 0
        ) {
          for (const recruiterId of validRecruiterIds) {
            await connection.query(
              `INSERT INTO job_recruiter_access
              (job_jid, recruiter_rid, granted_by, notes, is_active)
             VALUES (?, ?, ?, ?, TRUE)
             ON DUPLICATE KEY UPDATE
               is_active = TRUE,
               granted_by = VALUES(granted_by),
               granted_at = CURRENT_TIMESTAMP,
               notes = VALUES(notes)`,
              [generatedJobJid, recruiterId, authRid, normalizedAccessNotes],
            );
          }
        }

        await connection.commit();

        const safeCity = String(normalizedCity || "").trim();
        const safeState = String(state || "").trim();
        const safePincode = String(pincode || "").trim();
        const warning =
          normalizedAccessMode === "restricted" &&
          validRecruiterIds.length === 0
            ? "Job is restricted but no recruiters are assigned yet."
            : null;

        return res.status(201).json({
          message: "Job created successfully.",
          warning,
          job: {
            jid: generatedJobJid,
            recruiter_rid: recruiter_rid.trim(),
            city: safeCity,
            state: safeState,
            pincode: safePincode,
            company_name: normalizedCompanyName.trim(),
            role_name: role_name.trim(),
            positions_open: safePositionsOpen,
            revenue: safeRevenue,
            points_per_joining: safePointsPerJoining,
            access_mode: hasAccessModeColumn ? normalizedAccessMode : "open",
            recruiterCount: validRecruiterIds.length,
          },
        });
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }
    } catch (error) {
      if (error && error.code === "ER_DATA_TOO_LONG") {
        return res.status(400).json({
          message:
            "One of the text fields is too long for the database column.",
          error: error.message,
        });
      }

      return res.status(500).json({
        message: "Failed to create job.",
        error: error.message,
      });
    }
  },
);

router.post("/api/applications/parse-resume", async (req, res) => {
  try {
    const { jid, resumeBase64, resumeFilename, resumeMimeType } =
      req.body || {};
    if (!jid || !resumeBase64 || !resumeFilename) {
      return res.status(400).json({
        message: "jid, resumeBase64, and resumeFilename are required.",
      });
    }

    const safeJobId = normalizeJobJid(jid);
    if (!safeJobId) {
      return res.status(400).json({ message: "jid is required." });
    }

    const extension = getResumeExtension(resumeFilename);
    if (!SUPPORTED_RESUME_TYPES.has(extension) && !isImageResumeType(extension)) {
      return res.status(400).json({
        message: "Only PDF, DOCX, JPG, JPEG, PNG, and WEBP resumes are supported.",
      });
    }

    const [jobs] = await pool.query(
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
    if (jobs.length === 0) {
      return res.status(404).json({ message: "Job not found." });
    }

    const resumeBuffer = decodeResumeBuffer(resumeBase64);
    const validation = validateResumeFile({
      filename: resumeFilename,
      mimetype: resumeMimeType,
      buffer: resumeBuffer,
      maxBytes: 5 * 1024 * 1024,
    });
    if (!validation.ok) {
      return res.status(400).json({ message: validation.message });
    }

    const parsed = await parseResumeWithAts({
      resumeBuffer,
      resumeFilename: String(resumeFilename).trim(),
      jobDescription: buildJobAtsContext(jobs[0]),
    });

    if (!parsed.ok) {
      return res.status(503).json({ message: parsed.message });
    }

    const isImageResume = isImageResumeType(validation.extension);

    return res.status(200).json({
      message: isImageResume
        ? "Image resume uploaded successfully. Fill candidate details manually."
        : "Resume parsed successfully.",
      parsedData: parsed.parsedData,
      autofill: buildAutofillFromParsedData(parsed.parsedData),
      atsScore: parsed.atsScore ?? null,
      atsMatchPercentage: parsed.atsMatchPercentage ?? null,
      atsRawJson: parsed.atsRawJson ?? null,
      parser_meta: parsed.parserMeta || null,
      processing: buildResumeProcessingState({
        status: isImageResume ? "manual_entry_required" : "completed",
        resumeParsed: !isImageResume,
        atsCalculated: !isImageResume,
      }),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to parse resume.",
      error: error.message,
    });
  }
});

router.get(
  "/api/jobs/my",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  async (req, res) => {
    try {
      const authRid = toTrimmedString(req.auth?.rid);
      const authRole = normalizeRoleAlias(req.auth?.role);
      if (!authRid) {
        return res.status(401).json({ message: "Authentication required." });
      }

      const hasAccessModeColumn = await columnExists("jobs", "access_mode");
      const hasRecruiterRoleColumn = await columnExists("recruiter", "role");
      const isTeamLeader = isTeamLeaderLikeRole(authRole);
      const whereClause = isTeamLeader
        ? hasRecruiterRoleColumn
          ? `LOWER(TRIM(COALESCE(creator.role, ''))) IN ('team leader', 'team_leader', 'job creator')`
          : "1 = 1"
        : "j.recruiter_rid = ?";
      const queryParams = isTeamLeader ? [] : [authRid];
      const [rows] = await pool.query(
        `SELECT
          j.jid,
          j.recruiter_rid,
          j.company_name,
          j.role_name,
          j.city,
          j.state,
          j.pincode,
          j.created_at,
          ${hasAccessModeColumn ? "j.access_mode" : "'open' AS access_mode"},
          COUNT(jra.id) AS recruiterCount
        FROM jobs j
        LEFT JOIN recruiter creator
          ON creator.rid = j.recruiter_rid
        LEFT JOIN job_recruiter_access jra
          ON j.jid = jra.job_jid
         AND jra.is_active = TRUE
        WHERE ${whereClause}
        GROUP BY
          j.jid, j.recruiter_rid, j.company_name, j.role_name, j.city, j.state, j.pincode, j.created_at
          ${hasAccessModeColumn ? ", j.access_mode" : ""}
        ORDER BY j.created_at DESC, j.jid DESC`,
        queryParams,
      );

      return res.status(200).json({
        jobs: rows.map((row) => ({
          ...row,
          recruiterCount: Number(row.recruiterCount) || 0,
          access_mode: normalizeAccessMode(row.access_mode) || "open",
        })),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch your jobs.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/jobs/:jid/resume-statuses",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    try {
      const [jobRows] = await pool.query(
        `SELECT
          company_name AS companyName,
          role_name AS roleName,
          city AS city
        FROM jobs
        WHERE jid = ?
        LIMIT 1`,
        [req.ownedJob.jid],
      );

      const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
      const hasAtsMatchColumn = await columnExists(
        "resumes_data",
        "ats_match_percentage",
      );
      const atsScoreSelection = hasAtsScoreColumn
        ? "rd.ats_score AS atsScore,"
        : "NULL AS atsScore,";
      const atsMatchSelection = hasAtsMatchColumn
        ? "rd.ats_match_percentage AS atsMatchPercentage,"
        : "NULL AS atsMatchPercentage,";
      const walkInSelection = "c.walk_in AS walkInDate,";

      const [rows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.rid AS rid,
          rd.job_jid AS jobJid,
          rd.ats_raw_json AS atsRawJson,
          c.name AS candidateName,
          c.phone AS candidatePhone,
          c.email AS candidateEmail,
          r.name AS recruiterName,
          r.email AS recruiterEmail,
          rd.resume_filename AS resumeFilename,
          rd.resume_type AS resumeType,
          ${atsScoreSelection}
          ${atsMatchSelection}
          ${walkInSelection}
          rd.uploaded_at AS uploadedAt,
          jrs.selection_status AS workflowStatus,
          jrs.selection_note AS workflowNote,
          jrs.selected_by_admin AS updatedBy,
          jrs.selected_at AS updatedAt
        FROM resumes_data rd
        INNER JOIN recruiter r ON r.rid = rd.rid
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        LEFT JOIN job_resume_selection jrs
          ON jrs.job_jid = rd.job_jid
         AND jrs.res_id = rd.res_id
        WHERE rd.job_jid = ?
          AND LOWER(TRIM(COALESCE(rd.submitted_by_role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')
        ORDER BY rd.uploaded_at DESC, rd.res_id ASC`,
        [req.ownedJob.jid],
      );

      const extraInfoByResumeId = await fetchExtraInfoByResumeIds(
        rows.map((row) => row.resId),
      );

      return res.status(200).json({
        jobId: req.ownedJob.jid,
        job: {
          jobJid: req.ownedJob.jid,
          companyName: jobRows[0]?.companyName || null,
          roleName: jobRows[0]?.roleName || null,
          city: jobRows[0]?.city || null,
        },
        resumes: rows.map((row) => {
          const extraInfo =
            extraInfoByResumeId.get(String(row.resId || "").trim()) || {};
          const parsedResumePayload = parseJsonField(row.atsRawJson);
          const candidateSnapshot = extractCandidateSnapshot({
            source: {
              candidate_name: row.candidateName,
              candidate_phone: row.candidatePhone,
              candidate_email: row.candidateEmail,
              job_jid: row.jobJid,
              recruiter_rid: row.rid,
            },
            parsedData:
              parsedResumePayload?.parsed_data ||
              parsedResumePayload?.parsedData ||
              parsedResumePayload,
            fallback: {
              jobJid: row.jobJid || req.ownedJob.jid,
              recruiterRid: row.rid,
            },
          });

          return {
            ...extraInfo,
            resId: row.resId,
            rid: row.rid,
            recruiterRid: row.rid,
            jobJid: row.jobJid || req.ownedJob.jid,
            name: candidateSnapshot.name || row.candidateName || null,
            candidateName: candidateSnapshot.name || row.candidateName || null,
            candidatePhone:
              candidateSnapshot.phone || row.candidatePhone || null,
            candidate_phone:
              candidateSnapshot.phone || row.candidatePhone || null,
            phone: candidateSnapshot.phone || row.candidatePhone || null,
            candidateEmail:
              candidateSnapshot.email || row.candidateEmail || null,
            candidate_email:
              candidateSnapshot.email || row.candidateEmail || null,
            email: candidateSnapshot.email || row.candidateEmail || null,
            recruiterName: row.recruiterName || "Unknown",
            recruiterEmail: row.recruiterEmail || null,
            resumeFilename: row.resumeFilename || null,
            resumeType: row.resumeType || null,
            atsScore: row.atsScore === null ? null : Number(row.atsScore),
            atsMatchPercentage:
              row.atsMatchPercentage === null
                ? null
                : Number(row.atsMatchPercentage),
            uploadedAt: row.uploadedAt || null,
            status: row.workflowStatus || "pending",
            note: row.workflowNote || null,
            updatedBy: row.updatedBy || null,
            updatedAt: row.updatedAt || null,
            walkInDate: row.walkInDate || null,
            job: {
              jobJid: req.ownedJob.jid,
              companyName: jobRows[0]?.companyName || null,
              roleName: jobRows[0]?.roleName || null,
              city: jobRows[0]?.city || null,
            },
          };
        }),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch recruiter resume statuses.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/jobs/:jid/resume-statuses",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    const normalizedResId = toTrimmedString(req.body?.resId);
    const normalizedStatus = normalizeResumeStatusInput(req.body?.status);
    const joiningDate = req.body?.joining_date
      ? String(req.body.joining_date).trim()
      : null;
    const rawNote = resolveStatusReasonInput(req.body, normalizedStatus);
    const normalizedNote =
      rawNote === undefined || rawNote === null
        ? null
        : toTrimmedString(rawNote);
    const actorRid = toTrimmedString(req.auth?.rid);

    if (
      !normalizedResId ||
      !allowedManualResumeStatuses.has(normalizedStatus)
    ) {
      return res.status(400).json({
        message: "resId and a valid status are required.",
      });
    }

    if (normalizedStatus === "selected") {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate || "")) {
        return res.status(400).json({
          message: "joining_date is required in YYYY-MM-DD format.",
        });
      }
    } else if (normalizedStatus === "shortlisted" && joiningDate) {
      return res.status(400).json({
        message: "joining_date should only be provided for selected.",
      });
    } else if (joiningDate && !/^\d{4}-\d{2}-\d{2}$/.test(joiningDate)) {
      return res.status(400).json({
        message: "joining_date must be in YYYY-MM-DD format.",
      });
    }

    try {
      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        const [resumeRows] = await connection.query(
          `SELECT
            rd.res_id AS resId,
            rd.job_jid AS jobJid,
            rd.rid AS recruiterRid,
            rd.ats_raw_json AS atsRawJson,
            c.name AS candidateName,
            c.email AS email,
            c.joining_date AS currentJoiningDate,
            c.revenue AS candidateRevenue,
            j.revenue AS companyRevenue
           FROM resumes_data rd
           LEFT JOIN candidate c ON c.res_id = rd.res_id
           LEFT JOIN jobs j ON j.jid = rd.job_jid
           WHERE rd.res_id = ?
           LIMIT 1`,
          [normalizedResId],
        );

        if (resumeRows.length === 0) {
          await connection.rollback();
          return res.status(404).json({ message: "Resume not found." });
        }

        if (toTrimmedString(resumeRows[0].jobJid) !== req.ownedJob.jid) {
          await connection.rollback();
          return res.status(400).json({
            message: "The provided resume does not belong to this job.",
          });
        }

        const recruiterRid = toTrimmedString(resumeRows[0].recruiterRid);
        const parsedResumePayload = parseJsonField(resumeRows[0].atsRawJson);
        const statusCandidateSnapshot = extractCandidateSnapshot({
          source: {
            candidate_name: resumeRows[0].candidateName,
            email: resumeRows[0].email,
            job_jid: resumeRows[0].jobJid,
            recruiter_rid: recruiterRid,
          },
          parsedData:
            parsedResumePayload?.parsed_data ||
            parsedResumePayload?.parsedData ||
            parsedResumePayload,
          fallback: {
            jobJid: resumeRows[0].jobJid,
            recruiterRid,
          },
        });
        const reasonField = STATUS_REASON_FIELD_MAP[normalizedStatus];
        const statusReasonValue = reasonField
          ? normalizedNote || null
          : undefined;
        const [existingSelectionRows] = await connection.query(
          `SELECT selection_status AS selectionStatus
           FROM job_resume_selection
           WHERE job_jid = ? AND res_id = ?
           LIMIT 1`,
          [req.ownedJob.jid, normalizedResId],
        );
        const previousStatus = normalizeWorkflowStatus(
          existingSelectionRows[0]?.selectionStatus,
        );
        const currentDerivedStatus =
          previousStatus === "shortlisted" && resumeRows[0]?.currentJoiningDate
            ? "selected"
            : previousStatus;
        const persistedStatus = normalizedStatus;
        const effectiveJoiningDate = /^\d{4}-\d{2}-\d{2}$/.test(joiningDate || "")
          ? joiningDate
          : resumeRows[0]?.currentJoiningDate || null;

        // "left" can only be set from "joined"
        if (normalizedStatus === "left") {
          if (currentDerivedStatus !== "joined") {
            await connection.rollback();
            return res.status(400).json({
              message:
                "Cannot move to 'left' status. Only candidates in 'joined' status can be moved to 'left'.",
            });
          }
        }

        if (normalizedStatus === "pending") {
          await connection.query(
            `DELETE FROM job_resume_selection
             WHERE job_jid = ? AND res_id = ?`,
            [req.ownedJob.jid, normalizedResId],
          );
        } else {
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
              req.ownedJob.jid,
              normalizedResId,
              actorRid || "team-leader",
              persistedStatus,
              normalizedNote || null,
            ],
          );
        }

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
        if (reasonField && statusReasonValue !== undefined) {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid,
            candidateName:
              toTrimmedString(resumeRows[0].candidateName) || undefined,
            email: toTrimmedString(resumeRows[0].email) || undefined,
            [reasonField]: statusReasonValue,
            [statusTimestampFieldMap[normalizedStatus]]: "__CURRENT_TIMESTAMP__",
          });
        } else if (statusTimestampFieldMap[normalizedStatus]) {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid,
            candidateName:
              toTrimmedString(resumeRows[0].candidateName) || undefined,
            email: toTrimmedString(resumeRows[0].email) || undefined,
            [statusTimestampFieldMap[normalizedStatus]]: "__CURRENT_TIMESTAMP__",
          });
        }

        if (statusCandidateSnapshot.name) {
          let candidateJoiningDateValue = resumeRows[0]?.currentJoiningDate || null;
          if (normalizedStatus === "shortlisted") {
            candidateJoiningDateValue = null;
          } else if (normalizedStatus === "selected") {
            candidateJoiningDateValue = joiningDate;
          } else if (normalizedStatus === "joined" && effectiveJoiningDate) {
            candidateJoiningDateValue = effectiveJoiningDate;
          }
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            cid: undefined,
            jobJid: req.ownedJob.jid,
            recruiterRid,
            name: statusCandidateSnapshot.name,
            phone: statusCandidateSnapshot.phone || undefined,
            email: statusCandidateSnapshot.email || undefined,
            levelOfEdu: statusCandidateSnapshot.levelOfEdu || undefined,
            boardUni: statusCandidateSnapshot.boardUni || undefined,
            institutionName:
              statusCandidateSnapshot.institutionName || undefined,
            age: statusCandidateSnapshot.age,
            joiningDate: candidateJoiningDateValue,
            revenue:
              normalizedStatus === "billed"
                ? [resumeRows[0]?.candidateRevenue, resumeRows[0]?.companyRevenue]
                    .map((value) => Number(value))
                    .find((value) => Number.isFinite(value) && value > 0) ?? null
                : undefined,
          });
        }

        if (normalizedStatus === "walk_in") {
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            cid: undefined,
            walkIn: getCurrentDateOnlyInBusinessTimeZone(),
          });
        }

        if (recruiterRid && previousStatus !== normalizedStatus) {
          const verifiedDelta =
            (normalizedStatus === "verified" ? 1 : 0) -
            (previousStatus === "verified" ? 1 : 0);
          const othersDelta =
            (normalizedStatus === "others" ? 1 : 0) -
            (previousStatus === "others" ? 1 : 0);
          const selectDelta =
            (normalizedStatus === "selected" ? 1 : 0) -
            (previousStatus === "selected" ? 1 : 0);
          const rejectDelta =
            (normalizedStatus === "rejected" ? 1 : 0) -
            (previousStatus === "rejected" ? 1 : 0);
          const billedDelta =
            (normalizedStatus === "billed" ? 1 : 0) -
            (previousStatus === "billed" ? 1 : 0);
          const leftDelta =
            (normalizedStatus === "left" ? 1 : 0) -
            (previousStatus === "left" ? 1 : 0);

          if (
            verifiedDelta !== 0 ||
            othersDelta !== 0 ||
            selectDelta !== 0 ||
            rejectDelta !== 0 ||
            billedDelta !== 0 ||
            leftDelta !== 0
          ) {
            await connection.query(
              `INSERT INTO status (recruiter_rid, submitted, verified, others, \`select\`, reject, billed, \`left\`, last_updated)
               VALUES (?, 0, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON DUPLICATE KEY UPDATE
                 verified = GREATEST(0, COALESCE(verified, 0) + ?),
                 others = GREATEST(0, COALESCE(others, 0) + ?),
                 \`select\` = GREATEST(0, COALESCE(\`select\`, 0) + ?),
                 reject = GREATEST(0, COALESCE(reject, 0) + ?),
                 billed = GREATEST(0, COALESCE(billed, 0) + ?),
                 \`left\` = GREATEST(0, COALESCE(\`left\`, 0) + ?),
                 last_updated = CURRENT_TIMESTAMP`,
              [
                recruiterRid,
                Math.max(0, verifiedDelta),
                Math.max(0, othersDelta),
                Math.max(0, selectDelta),
                Math.max(0, rejectDelta),
                Math.max(0, billedDelta),
                Math.max(0, leftDelta),
                verifiedDelta,
                othersDelta,
                selectDelta,
                rejectDelta,
                billedDelta,
                leftDelta,
              ],
            );
          }
        }

        const billedRevenueAmount =
          normalizedStatus === "billed"
            ? [resumeRows[0]?.candidateRevenue, resumeRows[0]?.companyRevenue]
                .map((value) => Number(value))
                .find((value) => Number.isFinite(value) && value > 0) ?? null
            : null;

        if (normalizedStatus === "billed" && previousStatus !== "billed") {
          if (!Number.isFinite(billedRevenueAmount) || billedRevenueAmount <= 0) {
            await connection.rollback();
            return res.status(422).json({
              message: "Revenue amount is required before moving candidate to billed.",
            });
          }

          await addCandidateBillIntakeEntry(connection, normalizedResId, {
            amount: billedRevenueAmount,
          });
        }

        const responseFields = buildResumeCompatibilityFields({
          resId: normalizedResId,
          candidateName:
            statusCandidateSnapshot.name ||
            toTrimmedString(resumeRows[0].candidateName) ||
            null,
          workflowStatus: normalizedStatus,
          reason: normalizedNote || null,
          note: normalizedNote || null,
          joiningDate:
            normalizedStatus === "selected" ? joiningDate : effectiveJoiningDate,
          jobJid: req.ownedJob.jid,
        });
        await connection.commit();
        return res.status(200).json({
          message: "Resume status updated successfully.",
          data: {
            ...responseFields,
            jobId: req.ownedJob.jid,
            resId: normalizedResId,
            status: normalizedStatus,
            note: normalizedNote || null,
            othersReason:
              reasonField === "othersReason" ? (statusReasonValue ?? null) : null,
            joining_date:
              normalizedStatus === "selected" ? joiningDate : null,
            verifiedReason:
              reasonField === "verifiedReason"
                ? (statusReasonValue ?? null)
                : null,
            updatedBy: actorRid || "team-leader",
            revenue:
              normalizedStatus === "billed" ? billedRevenueAmount : null,
            company_rev:
              normalizedStatus === "billed" ? billedRevenueAmount : null,
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
        message: "Failed to update resume status.",
        error: error.message,
      });
    }
  },
);

// Recruiter endpoint: move own candidate from billed -> left with required note
router.post(
  "/api/jobs/:jid/resume-statuses/left",
  requireAuth,
  requireRoles("recruiter", "job creator", "team leader", "team_leader"),
  async (req, res) => {
    const normalizedResId = toTrimmedString(req.body?.resId);
    const rawNote =
      req.body?.note ?? req.body?.leftReason ?? req.body?.left_reason;
    const normalizedNote =
      rawNote === undefined || rawNote === null
        ? null
        : toTrimmedString(rawNote);
    const actorRid = toTrimmedString(req.auth?.rid);
    const jobJid = normalizeJobJid(req.params.jid);

    if (!normalizedResId) {
      return res.status(400).json({ message: "resId is required." });
    }
    if (!jobJid) {
      return res.status(400).json({ message: "Valid job jid is required." });
    }
    try {
      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        const [resumeRows] = await connection.query(
          `SELECT
            rd.res_id AS resId,
            rd.job_jid AS jobJid,
            rd.rid AS recruiterRid,
            rd.ats_raw_json AS atsRawJson,
            c.name AS candidateName,
            c.email AS email
           FROM resumes_data rd
           LEFT JOIN candidate c ON c.res_id = rd.res_id
           WHERE rd.res_id = ? AND rd.job_jid = ?
           LIMIT 1`,
          [normalizedResId, jobJid],
        );

        if (resumeRows.length === 0) {
          await connection.rollback();
          return res
            .status(404)
            .json({ message: "Resume not found for this job." });
        }

        const recruiterRid = toTrimmedString(resumeRows[0].recruiterRid);
        const parsedResumePayload = parseJsonField(resumeRows[0].atsRawJson);
        const leftStatusCandidateSnapshot = extractCandidateSnapshot({
          source: {
            candidate_name: resumeRows[0].candidateName,
            email: resumeRows[0].email,
            job_jid: resumeRows[0].jobJid,
            recruiter_rid: recruiterRid,
          },
          parsedData:
            parsedResumePayload?.parsed_data ||
            parsedResumePayload?.parsedData ||
            parsedResumePayload,
          fallback: {
            jobJid: resumeRows[0].jobJid,
            recruiterRid,
          },
        });
        const authRole = normalizeRoleAlias(req.auth?.role);

        // Recruiters can only move their own candidates
        if (authRole === "recruiter" && recruiterRid !== actorRid) {
          await connection.rollback();
          return res.status(403).json({
            message: "You can only update status for your own candidates.",
          });
        }

        const [selectionRows] = await connection.query(
          `SELECT selection_status AS selectionStatus
           FROM job_resume_selection
           WHERE job_jid = ? AND res_id = ?
           LIMIT 1`,
          [jobJid, normalizedResId],
        );

        const currentStatus =
          toTrimmedString(selectionRows[0]?.selectionStatus).toLowerCase() ||
          "pending";

        if (currentStatus !== "billed") {
          await connection.rollback();
          return res.status(400).json({
            message:
              "Cannot move to 'left' status. Only candidates in 'billed' status can be moved to 'left'.",
          });
        }

        await connection.query(
          `UPDATE job_resume_selection
           SET selection_status = 'left',
               selection_note = ?,
               selected_by_admin = ?,
               selected_at = CURRENT_TIMESTAMP
           WHERE job_jid = ? AND res_id = ?`,
          [normalizedNote, actorRid || "recruiter", jobJid, normalizedResId],
        );

        await upsertExtraInfoFields(connection, {
          resId: normalizedResId,
          jobJid,
          recruiterRid,
          candidateName:
            toTrimmedString(resumeRows[0].candidateName) || undefined,
          email: toTrimmedString(resumeRows[0].email) || undefined,
          leftReason: normalizedNote,
          leftAt: "__CURRENT_TIMESTAMP__",
        });

        if (leftStatusCandidateSnapshot.name) {
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            cid: undefined,
            jobJid,
            recruiterRid,
            name: leftStatusCandidateSnapshot.name,
            phone: leftStatusCandidateSnapshot.phone || undefined,
            email: leftStatusCandidateSnapshot.email || undefined,
            levelOfEdu: leftStatusCandidateSnapshot.levelOfEdu || undefined,
            boardUni: leftStatusCandidateSnapshot.boardUni || undefined,
            institutionName:
              leftStatusCandidateSnapshot.institutionName || undefined,
            age: leftStatusCandidateSnapshot.age,
          });
        }

        if (recruiterRid) {
          await connection.query(
            `INSERT INTO status (recruiter_rid, submitted, billed, \`left\`, last_updated)
             VALUES (?, 0, 0, 1, CURRENT_TIMESTAMP)
             ON DUPLICATE KEY UPDATE
               billed = GREATEST(0, COALESCE(billed, 0) - 1),
               \`left\` = GREATEST(0, COALESCE(\`left\`, 0) + 1),
               last_updated = CURRENT_TIMESTAMP`,
            [recruiterRid],
          );
        }

        await connection.commit();
        return res.status(200).json({
          message: "Candidate moved from billed to left successfully.",
          data: {
            jobId: jobJid,
            resId: normalizedResId,
            status: "left",
            leftReason: normalizedNote,
            updatedBy: actorRid,
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
        message: "Failed to update candidate status to left.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/jobs/:jid/resume-statuses/:resId/rollback",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    const normalizedResId = toTrimmedString(req.params?.resId);
    if (!normalizedResId) {
      return res.status(400).json({ message: "resId is required." });
    }

    try {
      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        const [resumeRows] = await connection.query(
          `SELECT
            rd.res_id AS resId,
            rd.rid AS recruiterRid,
            COALESCE(rd.job_jid, jrs.job_jid) AS jobJid,
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
            ON jrs.res_id = rd.res_id
            AND jrs.job_jid = COALESCE(rd.job_jid, jrs.job_jid)
          LEFT JOIN candidate c ON c.res_id = rd.res_id
          LEFT JOIN extra_info ei
            ON ei.res_id = rd.res_id OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)
          WHERE rd.res_id = ?
          LIMIT 1
          FOR UPDATE`,
          [normalizedResId],
        );

        if (resumeRows.length === 0) {
          await connection.rollback();
          return res.status(404).json({ message: "Resume not found." });
        }

        const resume = resumeRows[0];
        if (toTrimmedString(resume.jobJid) !== req.ownedJob.jid) {
          await connection.rollback();
          return res.status(400).json({
            message: "The provided resume does not belong to this job.",
          });
        }

        const currentStatus = normalizeWorkflowStatus(
          resume.currentStatus,
          "submitted",
        );
        const currentDerivedStatus =
          currentStatus === "shortlisted" && resume.currentJoiningDate
            ? "selected"
            : currentStatus;
        const rollbackTarget = resolveManualRollbackTarget(resume);

        if (!rollbackTarget) {
          await connection.rollback();
          return res.status(400).json({
            message: `Rollback is not supported for '${currentDerivedStatus}'.`,
          });
        }

        if (rollbackTarget === "submitted") {
          await connection.query(
            `DELETE FROM job_resume_selection
             WHERE job_jid = ? AND res_id = ?`,
            [req.ownedJob.jid, normalizedResId],
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
            [
              req.ownedJob.jid,
              normalizedResId,
              toTrimmedString(req.auth?.rid) || "team-leader-rollback",
              rollbackTarget,
            ],
          );
        }

        if (currentDerivedStatus === "verified" || currentDerivedStatus === "others") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            verifiedReason: currentDerivedStatus === "verified" ? null : undefined,
            othersReason: currentDerivedStatus === "others" ? null : undefined,
          });
        }

        if (currentDerivedStatus === "walk_in") {
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            walkIn: null,
          });
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            walkInReason: null,
          });
        }

        if (currentDerivedStatus === "selected") {
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            joiningDate: null,
            revenue: null,
          });
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            selectReason: null,
          });
        }

        if (currentDerivedStatus === "rejected") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            rejectReason: null,
          });
        }

        if (currentDerivedStatus === "shortlisted") {
          await upsertCandidateFields(connection, {
            resId: normalizedResId,
            joiningDate: null,
            revenue: null,
          });
        }

        if (currentDerivedStatus === "joined") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            joinedReason: null,
          });
        }

        if (currentDerivedStatus === "dropout") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            dropoutReason: null,
          });
        }

        if (currentDerivedStatus === "left") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            leftReason: null,
          });
        }

        if (currentDerivedStatus === "billed") {
          await upsertExtraInfoFields(connection, {
            resId: normalizedResId,
            jobJid: req.ownedJob.jid,
            recruiterRid: resume.recruiterRid || undefined,
            billedReason: null,
          });
        }

        await connection.commit();
        return res.status(200).json({
          message: "Resume rolled back successfully.",
          data: {
            resId: normalizedResId,
            jobId: req.ownedJob.jid,
            previousStatus: currentDerivedStatus,
            status: rollbackTarget,
            updatedBy: toTrimmedString(req.auth?.rid) || "team-leader-rollback",
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
        message: "Failed to rollback resume status.",
        error: error.message,
      });
    }
  },
);

router.get(
  "/api/jobs/:jid/access",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    try {
      const [rows] = await pool.query(
        `SELECT
          r.rid,
          r.name,
          r.email,
          jra.granted_at AS grantedAt,
          jra.granted_by AS grantedBy,
          jra.is_active AS isActive,
          jra.notes
        FROM job_recruiter_access jra
        INNER JOIN recruiter r ON r.rid = jra.recruiter_rid
        WHERE jra.job_jid = ?
          AND jra.is_active = TRUE
        ORDER BY r.name ASC, r.rid ASC`,
        [req.ownedJob.jid],
      );

      return res.status(200).json({
        jobId: req.ownedJob.jid,
        accessMode: req.ownedJob.accessMode,
        recruiters: rows.map((row) => ({
          rid: row.rid,
          name: row.name,
          email: row.email,
          grantedAt: row.grantedAt,
          grantedBy: row.grantedBy,
          isActive: Boolean(row.isActive),
          notes: row.notes || null,
        })),
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to fetch job access list.",
        error: error.message,
      });
    }
  },
);

router.post(
  "/api/jobs/:jid/access",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    const { recruiterIds, notes } = req.body || {};
    const normalizedNotes = toTrimmedString(notes) || null;
    const uniqueRecruiterIds = dedupeStringList(recruiterIds);

    if (uniqueRecruiterIds.length === 0) {
      return res.status(400).json({
        message: "recruiterIds must contain at least one recruiter ID.",
      });
    }

    try {
      const { validRecruiterIds, invalidRecruiterIds } =
        await validateRecruiterIds(uniqueRecruiterIds);
      if (invalidRecruiterIds.length > 0) {
        return res.status(400).json({
          message: "Some recruiterIds are invalid or not recruiter role users.",
          invalidRecruiterIds,
        });
      }

      const authRid = toTrimmedString(req.auth?.rid);
      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();
        for (const recruiterId of validRecruiterIds) {
          await connection.query(
            `INSERT INTO job_recruiter_access
              (job_jid, recruiter_rid, granted_by, notes, is_active)
             VALUES (?, ?, ?, ?, TRUE)
             ON DUPLICATE KEY UPDATE
               is_active = TRUE,
               granted_by = VALUES(granted_by),
               granted_at = CURRENT_TIMESTAMP,
               notes = VALUES(notes)`,
            [req.ownedJob.jid, recruiterId, authRid, normalizedNotes],
          );
        }
        await connection.commit();
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
      }

      return res
        .status(200)
        .json({ success: true, assigned: validRecruiterIds.length });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to assign recruiters for this job.",
        error: error.message,
      });
    }
  },
);

router.delete(
  "/api/jobs/:jid/access/:rid",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    const recruiterRid = toTrimmedString(req.params.rid);
    if (!recruiterRid) {
      return res.status(400).json({ message: "rid is required." });
    }

    try {
      const [existingRecruiters] = await pool.query(
        "SELECT rid FROM recruiter WHERE rid = ? LIMIT 1",
        [recruiterRid],
      );
      if (existingRecruiters.length === 0) {
        return res.status(404).json({ message: "Recruiter not found." });
      }

      await pool.query(
        `UPDATE job_recruiter_access
         SET is_active = FALSE
         WHERE job_jid = ? AND recruiter_rid = ?`,
        [req.ownedJob.jid, recruiterRid],
      );

      return res.status(200).json({
        success: true,
        message: `Access revoked for ${recruiterRid}`,
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to revoke recruiter access.",
        error: error.message,
      });
    }
  },
);

router.put(
  "/api/jobs/:jid/access-mode",
  requireAuth,
  requireRoles("job creator", "team leader", "team_leader"),
  requireOwnedJob,
  async (req, res) => {
    const normalizedAccessMode = normalizeAccessMode(req.body?.accessMode);
    if (!normalizedAccessMode) {
      return res.status(400).json({
        message: "accessMode must be either 'open' or 'restricted'.",
      });
    }

    try {
      const hasAccessModeColumn = await columnExists("jobs", "access_mode");
      if (!hasAccessModeColumn) {
        return res.status(500).json({
          message: "jobs.access_mode column is not initialized.",
        });
      }

      await pool.query(
        `UPDATE jobs
         SET access_mode = ?
         WHERE jid = ? AND recruiter_rid = ?`,
        [normalizedAccessMode, req.ownedJob.jid, req.ownedJob.recruiterRid],
      );

      const warning =
        normalizedAccessMode === "restricted" &&
        (await getActiveAccessCount(req.ownedJob.jid)) === 0
          ? "No recruiters are currently assigned to this restricted job."
          : null;

      return res.status(200).json({
        success: true,
        accessMode: normalizedAccessMode,
        warning,
      });
    } catch (error) {
      return res.status(500).json({
        message: "Failed to update job access mode.",
        error: error.message,
      });
    }
  },
);

router.post("/api/applications", async (req, res) => {
  try {
    const mergedBody = req.body || {};
    const { jid, resumeBase64, resumeFilename, resumeMimeType } = mergedBody;
    if (!jid || !resumeBase64 || !resumeFilename) {
      return res.status(400).json({
        message: "jid, resumeBase64, and resumeFilename are required.",
      });
    }

    const safeJobId = normalizeJobJid(jid);
    if (!safeJobId) {
      return res.status(400).json({ message: "jid is required." });
    }

    const extension = getResumeExtension(resumeFilename);
    if (!SUPPORTED_RESUME_TYPES.has(extension) && !isImageResumeType(extension)) {
      return res.status(400).json({
        message: "Only PDF, DOCX, JPG, JPEG, PNG, and WEBP resumes are supported.",
      });
    }

    const [jobs] = await pool.query(
      `SELECT
        jid,
        recruiter_rid,
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
    if (jobs.length === 0) {
      return res.status(404).json({ message: "Job not found." });
    }
    const selectedJob = jobs[0];

    const resumeBuffer = decodeResumeBuffer(resumeBase64);
    const validation = validateResumeFile({
      filename: resumeFilename,
      mimetype: resumeMimeType,
      buffer: resumeBuffer,
      maxBytes: 5 * 1024 * 1024,
    });
    if (!validation.ok) {
      return res.status(400).json({ message: validation.message });
    }

    const clientParsedData =
      mergedBody?.parsedData &&
      typeof mergedBody.parsedData === "object" &&
      !Array.isArray(mergedBody.parsedData)
        ? mergedBody.parsedData
        : null;
    const clientAtsScore = toAtsNumberOrNull(mergedBody?.atsScore);
    const clientAtsMatchPercentage = toAtsNumberOrNull(
      mergedBody?.atsMatchPercentage,
    );
    const clientAtsRawJson =
      mergedBody?.atsRawJson &&
      typeof mergedBody.atsRawJson === "object" &&
      !Array.isArray(mergedBody.atsRawJson)
        ? mergedBody.atsRawJson
        : null;

    const parsed = clientParsedData
      ? {
          ok: true,
          message: "",
          parsedData: clientParsedData,
          applicantName: extractApplicantName(clientParsedData),
          atsScore: clientAtsScore,
          atsMatchPercentage: clientAtsMatchPercentage,
          atsRawJson: clientAtsRawJson || {
            ats_score: clientAtsScore,
            ats_match_percentage: clientAtsMatchPercentage,
            parsed_data: clientParsedData,
          },
          parserMeta: {
            parsedDataSource: "client",
            atsSource: "client",
          },
        }
      : await parseResumeWithAts({
          resumeBuffer,
          resumeFilename: String(resumeFilename).trim(),
          jobDescription: buildJobAtsContext(selectedJob),
        });
    if (!parsed.ok) {
      return res.status(503).json({ message: parsed.message });
    }
    const autofill = buildAutofillFromParsedData(parsed.parsedData);

    const {
      name,
      phone,
      email,
      hasPriorExperience,
      experienceIndustry,
      experienceIndustryOther,
      currentSalary,
      expectedSalary,
      noticePeriod,
      yearsOfExperience,
      latestEducationLevel,
      boardUniversity,
      institutionName,
      age,
    } = mergedBody;

    const finalName = String(name || autofill.name || "").trim();
    const finalPhone = normalizePhoneForStorage(phone || autofill.phone || "");
    const finalEmail = String(email || autofill.email || "")
      .trim()
      .toLowerCase();
    const finalLatestEducationLevel = String(
      latestEducationLevel || autofill.latestEducationLevel || "",
    ).trim();
    const finalBoardUniversity = String(
      boardUniversity || autofill.boardUniversity || "",
    ).trim();
    const finalInstitutionName = String(
      institutionName || autofill.institutionName || "",
    ).trim();
    const finalAge = toNumberOrNull(age ?? autofill.age);
    const normalizedHasPriorExperience = String(hasPriorExperience || "")
      .trim()
      .toLowerCase();
    const finalHasPriorExperience = normalizedHasPriorExperience === "yes";
    const finalExperienceIndustry = String(experienceIndustry || "")
      .trim()
      .toLowerCase();
    const finalExperienceIndustryOther = String(
      experienceIndustryOther || "",
    ).trim();
    const finalCurrentSalary = toNumberOrNull(currentSalary);
    const finalExpectedSalary = toNumberOrNull(expectedSalary);
    const finalNoticePeriod = String(noticePeriod || "").trim();
    const finalYearsOfExperience = toNumberOrNull(yearsOfExperience);
    const parsedApplicantName =
      extractApplicantName(parsed.parsedData) || finalName || null;
    const allowedIndustries = new Set([
      "it",
      "marketing",
      "sales",
      "finance",
      "others",
    ]);

    if (
      !finalName ||
      !finalPhone ||
      !finalEmail ||
      !finalLatestEducationLevel ||
      !finalBoardUniversity ||
      !finalInstitutionName ||
      finalAge === null
    ) {
      return res.status(400).json({
        message:
          "jid, name, phone, email, latestEducationLevel, boardUniversity, institutionName, and age are required.",
      });
    }

    if (!["yes", "no"].includes(normalizedHasPriorExperience)) {
      return res.status(400).json({
        message: "hasPriorExperience must be either 'yes' or 'no'.",
      });
    }

    if (finalHasPriorExperience) {
      if (
        !allowedIndustries.has(finalExperienceIndustry) ||
        finalCurrentSalary === null ||
        finalExpectedSalary === null ||
        !finalNoticePeriod ||
        finalYearsOfExperience === null
      ) {
        return res.status(400).json({
          message:
            "experienceIndustry, currentSalary, expectedSalary, noticePeriod, and yearsOfExperience are required when prior experience is yes.",
        });
      }

      if (
        finalExperienceIndustry === "others" &&
        !finalExperienceIndustryOther
      ) {
        return res.status(400).json({
          message: "Please specify the industry when selecting others.",
        });
      }
    }

    if (
      (finalCurrentSalary !== null && finalCurrentSalary < 0) ||
      (finalExpectedSalary !== null && finalExpectedSalary < 0) ||
      (finalYearsOfExperience !== null && finalYearsOfExperience < 0)
    ) {
      return res.status(400).json({
        message: "Experience salary and years values cannot be negative.",
      });
    }

    if (!/^\d{10}$/.test(finalPhone)) {
      return res.status(400).json({
        message: "Phone number must be exactly 10 digits.",
      });
    }

    const hasJobJidColumn = await columnExists("resumes_data", "job_jid");
    const hasSubmittedByRoleColumn = await columnExists(
      "resumes_data",
      "submitted_by_role",
    );
    const hasApplicantNameColumn = await columnExists(
      "resumes_data",
      "applicant_name",
    );
    const hasApplicantEmailColumn = await columnExists(
      "resumes_data",
      "applicant_email",
    );
    const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
    const hasAtsMatchColumn = await columnExists(
      "resumes_data",
      "ats_match_percentage",
    );
    const hasAtsRawColumn = await columnExists("resumes_data", "ats_raw_json");
    const normalizedFilename = String(resumeFilename).trim();
    const normalizedMimeType = String(resumeMimeType || "")
      .trim()
      .toLowerCase();

    const atsPayload = {
      ats_score: parsed.atsScore,
      ats_match_percentage: parsed.atsMatchPercentage,
      ats_details: parsed.atsRawJson,
      parsed_data: parsed.parsedData,
    };

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      const [sequenceResult] = await connection.query(
        "INSERT INTO resume_id_sequence VALUES ()",
      );
      const sequenceValue = Number(sequenceResult.insertId);
      const resId = `res_${sequenceValue}`;
      const cid = buildCandidateId(sequenceValue);

      const [applicationResult] = await connection.query(
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
          normalizedFilename,
          safeJsonOrNull(parsed.parsedData),
          parsed.atsScore,
          parsed.atsMatchPercentage,
          safeJsonOrNull(atsPayload),
        ],
      );

      const hasFileHashColumn = await columnExists("resumes_data", "file_hash");
      const fileHash = hasFileHashColumn ? sha256Hex(resumeBuffer) : null;
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

      const insertColumns = ["res_id", "rid"];
      const insertValues = [resId, selectedJob.recruiter_rid];

      if (hasJobJidColumn) {
        insertColumns.push("job_jid");
        insertValues.push(safeJobId);
      }

      insertColumns.push("resume", "resume_filename", "resume_type");
      insertValues.push(resumeBuffer, normalizedFilename, extension);

      if (hasFileHashColumn) {
        insertColumns.push("file_hash");
        insertValues.push(fileHash);
      }

      if (hasSubmittedByRoleColumn) {
        insertColumns.push("submitted_by_role");
        insertValues.push("candidate");
      }

      if (hasApplicantNameColumn) {
        insertColumns.push("applicant_name");
        insertValues.push(parsedApplicantName);
      }

      if (hasApplicantEmailColumn) {
        insertColumns.push("applicant_email");
        insertValues.push(finalEmail);
      }

      if (hasAtsScoreColumn) {
        insertColumns.push("ats_score");
        insertValues.push(parsed.atsScore);
      }

      if (hasAtsMatchColumn) {
        insertColumns.push("ats_match_percentage");
        insertValues.push(parsed.atsMatchPercentage);
      }

      if (hasAtsRawColumn) {
        insertColumns.push("ats_raw_json");
        insertValues.push(safeJsonOrNull(atsPayload));
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
        recruiterRid: selectedJob.recruiter_rid,
        name: finalName,
        phone: finalPhone,
        email: finalEmail,
        levelOfEdu: finalLatestEducationLevel,
        boardUni: finalBoardUniversity,
        institutionName: finalInstitutionName,
        age: finalAge,
        industry: finalHasPriorExperience ? finalExperienceIndustry : null,
        expectedSal: finalHasPriorExperience ? finalExpectedSalary : null,
        prevSal: finalHasPriorExperience ? finalCurrentSalary : null,
        noticePeriod: finalHasPriorExperience ? finalNoticePeriod : null,
        experience: finalHasPriorExperience,
        yearsOfExp: finalHasPriorExperience ? finalYearsOfExperience : null,
      });

      await connection.commit();
      return res.status(201).json({
        message: "Application submitted successfully.",
        application: {
          id: applicationResult.insertId,
          job_jid: safeJobId,
          candidate_name: finalName,
          resume_id: resId,
          resume_filename: normalizedFilename,
          resume_type: extension,
          resume_mime_type: normalizedMimeType || null,
        },
        parser_meta: parsed.parserMeta || null,
      });
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    return res.status(500).json({
      message: "Failed to submit application.",
      error: error.message,
    });
  }
});

// --- Auto-billing: move "joined" candidates to "billed" after BILLING_PERIOD_DAYS ---
const BILLING_PERIOD_DAYS = Math.max(
  0,
  Number(process.env.BILLING_PERIOD_DAYS) || 90,
);

const processBillingTransitions = async () => {
  try {
    const hasTable = await tableExists("job_resume_selection");
    if (!hasTable) return { transitioned: 0 };

    const hasBilledStatus = await columnExists("status", "billed");
    if (!hasBilledStatus) return { transitioned: 0 };

    // Find all "joined" candidates whose selected_at is older than the billing period
    const [joinedRows] = await pool.query(
      `SELECT jrs.id, jrs.job_jid, jrs.res_id, rd.rid AS recruiterRid,
              COALESCE(j.points_per_joining, 0) AS pointsPerJoining
       FROM job_resume_selection jrs
       INNER JOIN resumes_data rd ON rd.res_id = jrs.res_id AND rd.job_jid = jrs.job_jid
       LEFT JOIN jobs j ON j.jid = jrs.job_jid
       WHERE jrs.selection_status = 'joined'
         AND jrs.selected_at <= DATE_SUB(NOW(), INTERVAL ? DAY)`,
      [BILLING_PERIOD_DAYS],
    );

    if (joinedRows.length === 0) return { transitioned: 0 };

    let transitioned = 0;
    for (const row of joinedRows) {
      const connection = await pool.getConnection();
      try {
        await connection.beginTransaction();

        await connection.query(
          `UPDATE job_resume_selection
           SET selection_status = 'billed',
               selection_note = 'Auto-billed after billing period',
               selected_at = CURRENT_TIMESTAMP
           WHERE id = ? AND selection_status = 'joined'`,
          [row.id],
        );

        await upsertExtraInfoFields(connection, {
          resId: row.res_id,
          jobJid: row.job_jid,
          recruiterRid: row.recruiterRid || undefined,
          billedAt: "__CURRENT_TIMESTAMP__",
        });

        if (row.recruiterRid) {
          await connection.query(
            `INSERT INTO status (recruiter_rid, submitted, billed, joined, last_updated)
             VALUES (?, 0, 1, 0, CURRENT_TIMESTAMP)
             ON DUPLICATE KEY UPDATE
               billed = GREATEST(0, COALESCE(billed, 0) + 1),
               joined = GREATEST(0, COALESCE(joined, 0) - 1),
               last_updated = CURRENT_TIMESTAMP`,
            [row.recruiterRid],
          );

          // Credit points_per_joining to the recruiter on billed status
          const pts = Number(row.pointsPerJoining) || 0;
          if (pts > 0) {
            await connection.query(
              "UPDATE recruiter SET points = COALESCE(points, 0) + ? WHERE rid = ?",
              [pts, row.recruiterRid],
            );
            await connection.query(
              `INSERT INTO recruiter_points_log (recruiter_rid, job_jid, res_id, points, reason)
               VALUES (?, ?, ?, ?, 'billed')`,
              [row.recruiterRid, row.job_jid, row.res_id, pts],
            );
          }
        }

        await addCandidateBillIntakeEntry(connection, row.res_id);

        await connection.commit();
        transitioned += 1;
      } catch (innerError) {
        await connection.rollback();
        console.error(
          `[auto-billing] Failed to transition res_id=${row.res_id}:`,
          innerError.message,
        );
      } finally {
        connection.release();
      }
    }

    if (transitioned > 0) {
      console.log(
        `[auto-billing] Transitioned ${transitioned} candidate(s) from joined to billed`,
      );
    }

    return { transitioned };
  } catch (error) {
    console.error("[auto-billing] Error:", error.message);
    return { transitioned: 0, error: error.message };
  }
};

module.exports = router;
module.exports.processBillingTransitions = processBillingTransitions;
