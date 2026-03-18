const express = require("express");
const multer = require("multer");
const pool = require("../config/db");
const {
  createAuthToken,
  requireAuth,
  requireRoles,
} = require("../middleware/auth");

const router = express.Router();
const ADMIN_API_KEY = String(process.env.ADMIN_API_KEY || "admin123");
const ALLOWED_REVENUE_UPLOAD_MIME_TYPES = new Set([
  "image/jpeg",
  "image/jpg",
  "image/png",
  "image/webp",
  "application/pdf",
]);
const MAX_REVENUE_UPLOAD_BYTES = 8 * 1024 * 1024;

const revenueUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_REVENUE_UPLOAD_BYTES },
  fileFilter: (_req, file, callback) => {
    const mimeType = String(file?.mimetype || "")
      .trim()
      .toLowerCase();
    if (!ALLOWED_REVENUE_UPLOAD_MIME_TYPES.has(mimeType)) {
      return callback(
        new Error("Only JPG, PNG, WEBP images or PDF files are allowed."),
      );
    }
    return callback(null, true);
  },
});

const parseRevenueUpload = (req, res, next) => {
  revenueUpload.single("photo")(req, res, (error) => {
    if (!error) return next();
    if (error?.code === "LIMIT_FILE_SIZE") {
      return res
        .status(400)
        .json({ message: "Attachment must be 8MB or smaller." });
    }
    return res.status(400).json({
      message: error?.message || "Invalid attachment upload.",
    });
  });
};

const normalizeJobJid = (value) => {
  const normalized = String(value || "").trim();
  return normalized || null;
};

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

const getColumnMetadata = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT
      COLUMN_TYPE AS columnType,
      DATA_TYPE AS dataType,
      IS_NULLABLE AS isNullable
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  return rows[0] || null;
};

const constraintExists = async (tableName, constraintName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.table_constraints
     WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName],
  );
  return rows.length > 0;
};

const ensureMoneySumTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS money_sum (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      company_rev DECIMAL(14,2) NOT NULL DEFAULT 0,
      expense DECIMAL(14,2) NOT NULL DEFAULT 0,
      profit DECIMAL(14,2) NOT NULL DEFAULT 0,
      reason TEXT NULL,
      photo LONGTEXT NULL,
      entry_type VARCHAR(20) NOT NULL DEFAULT 'expense',
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )`,
  );

  if (!(await columnExists("money_sum", "company_rev"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN company_rev DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }
  if (!(await columnExists("money_sum", "expense"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN expense DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }
  if (!(await columnExists("money_sum", "profit"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN profit DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }
  if (!(await columnExists("money_sum", "reason"))) {
    await pool.query("ALTER TABLE money_sum ADD COLUMN reason TEXT NULL");
  }
  const reasonMetadata = await getColumnMetadata("money_sum", "reason");
  const reasonType = String(reasonMetadata?.dataType || "").toLowerCase();
  if (
    reasonType &&
    reasonType !== "text" &&
    reasonType !== "mediumtext" &&
    reasonType !== "longtext"
  ) {
    await pool.query("ALTER TABLE money_sum MODIFY COLUMN reason TEXT NULL");
  }
  if (!(await columnExists("money_sum", "photo"))) {
    await pool.query("ALTER TABLE money_sum ADD COLUMN photo LONGTEXT NULL");
  }
  const photoMetadata = await getColumnMetadata("money_sum", "photo");
  const photoType = String(photoMetadata?.dataType || "").toLowerCase();
  if (photoType && photoType !== "longtext") {
    await pool.query("ALTER TABLE money_sum MODIFY COLUMN photo LONGTEXT NULL");
  }
  if (!(await columnExists("money_sum", "entry_type"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN entry_type VARCHAR(20) NOT NULL DEFAULT 'expense'",
    );
  }
  if (!(await columnExists("money_sum", "created_at"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }
  if (!(await columnExists("money_sum", "updated_at"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("money_sum", "id"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST",
    );
  }

  await pool.query(
    "ALTER TABLE money_sum MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
  );
};

const ensureRecruiterAttendanceTable = async () => {
  await ensureMoneySumTable();

  await pool.query(
    `CREATE TABLE IF NOT EXISTS recruiter_attendance (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      recruiter_rid VARCHAR(20) NOT NULL,
      attendance_date DATE NOT NULL,
      status ENUM('present', 'absent', 'half_day') NOT NULL DEFAULT 'absent',
      salary_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
      money_sum_id BIGINT NULL,
      marked_by VARCHAR(50) NOT NULL DEFAULT 'admin',
      marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_recruiter_attendance_day (recruiter_rid, attendance_date),
      INDEX idx_recruiter_attendance_date_status (attendance_date, status),
      INDEX idx_recruiter_attendance_money_sum_id (money_sum_id),
      CONSTRAINT fk_recruiter_attendance_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON UPDATE CASCADE ON DELETE CASCADE,
      CONSTRAINT fk_recruiter_attendance_money_sum
        FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
        ON UPDATE CASCADE ON DELETE SET NULL
    )`,
  );

  if (!(await columnExists("recruiter_attendance", "salary_amount"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN salary_amount DECIMAL(12,2) NOT NULL DEFAULT 0",
    );
  }
  if (!(await columnExists("recruiter_attendance", "money_sum_id"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN money_sum_id BIGINT NULL",
    );
  }
  await pool.query(
    "ALTER TABLE recruiter_attendance MODIFY COLUMN money_sum_id BIGINT NULL",
  );
  if (!(await columnExists("recruiter_attendance", "marked_by"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN marked_by VARCHAR(50) NOT NULL DEFAULT 'admin'",
    );
  }
  if (!(await columnExists("recruiter_attendance", "marked_at"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }
  if (!(await columnExists("recruiter_attendance", "updated_at"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (
    !(await constraintExists(
      "recruiter_attendance",
      "fk_recruiter_attendance_money_sum",
    ))
  ) {
    await pool.query(
      `ALTER TABLE recruiter_attendance
       ADD CONSTRAINT fk_recruiter_attendance_money_sum
       FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
       ON UPDATE CASCADE ON DELETE SET NULL`,
    );
  }
};

const isAdminAuthorized = (req) => {
  return (
    String(req.auth?.role || "")
      .trim()
      .toLowerCase() === "admin"
  );
};

const ensureAdminAuthorized = (req, res) => {
  if (isAdminAuthorized(req)) return true;
  res.status(403).json({ message: "Admin authorization required." });
  return false;
};

const toPositiveMoney = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.round(parsed * 100) / 100;
};

const toMoneyNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed * 100) / 100;
};

const normalizeAttendanceStatus = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "present") return "present";
  if (normalized === "absent") return "absent";
  if (
    normalized === "half_day" ||
    normalized === "half-day" ||
    normalized === "half day"
  ) {
    return "half_day";
  }
  return "";
};

const normalizeAttendanceDate = (value) => {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return new Date().toISOString().slice(0, 10);
  }
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : "";
};

const normalizeStaffRole = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "team_leader") return "team leader";
  if (normalized === "job creator") return "team leader";
  if (normalized === "team leader") return "team leader";
  return "recruiter";
};

const calculateAttendanceExpense = (status, dailySalary) => {
  const safeDailySalary = toMoneyNumber(dailySalary);
  if (status === "present") return safeDailySalary;
  if (status === "half_day")
    return Math.round((safeDailySalary / 2) * 100) / 100;
  return 0;
};

const buildAttendanceReason = ({
  recruiterRid,
  recruiterName,
  attendanceDate,
  status,
}) => {
  const label = String(recruiterName || "").trim();
  const suffix = status === "half_day" ? "half day" : status;
  return label
    ? `attendance salary - ${attendanceDate} - ${recruiterRid} (${label}) - ${suffix}`
    : `attendance salary - ${attendanceDate} - ${recruiterRid} - ${suffix}`;
};

const normalizeRevenueEntryType = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "intake" ||
    normalized === "income" ||
    normalized === "in"
  ) {
    return "intake";
  }
  if (
    normalized === "expense" ||
    normalized === "outgoing" ||
    normalized === "out"
  ) {
    return "expense";
  }
  return "";
};

const normalizeRevenueReasonCategory = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "electricity bill") return "electricity bill";
  if (normalized === "salary") return "salary";
  if (normalized === "rent") return "rent";
  if (normalized === "extras") return "extras";
  if (normalized === "others") return "others";
  return "";
};

const revenueReasonFromPayload = ({
  reasonCategory,
  otherReason,
  recruiterRid,
  recruiterName,
}) => {
  const safeCategory = normalizeRevenueReasonCategory(reasonCategory);
  if (!safeCategory) {
    return { error: "A valid reason must be selected." };
  }

  if (safeCategory === "others") {
    const details = String(otherReason || "").trim();
    if (!details) {
      return { error: "Please specify the reason when selecting Others." };
    }
    return { reason: details };
  }

  if (safeCategory === "salary") {
    const rid = String(recruiterRid || "").trim();
    if (!rid) {
      return { error: "Recruiter RID is required for salary entries." };
    }
    const label = String(recruiterName || "").trim();
    return {
      reason: label ? `salary - ${rid} (${label})` : `salary - ${rid}`,
    };
  }

  return { reason: safeCategory };
};

const toRevenueAttachmentDataUrl = (file) => {
  if (!file?.buffer || !file?.mimetype) return "";
  const mimeType = String(file.mimetype || "")
    .trim()
    .toLowerCase();
  if (!ALLOWED_REVENUE_UPLOAD_MIME_TYPES.has(mimeType)) return "";
  const base64 = file.buffer.toString("base64");
  return `data:${mimeType};base64,${base64}`;
};

const normalizePhotoValue = (value) => {
  if (!value) return "";
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  return String(value);
};

const recomputeMoneyProfit = async (connection) => {
  const [rows] = await connection.query(
    `SELECT id, company_rev AS companyRev, expense
     FROM money_sum
     ORDER BY created_at ASC, id ASC`,
  );

  let runningProfit = 0;
  for (const row of rows) {
    runningProfit =
      Math.round(
        (runningProfit +
          toMoneyNumber(row.companyRev) -
          toMoneyNumber(row.expense)) *
          100,
      ) / 100;
    await connection.query("UPDATE money_sum SET profit = ? WHERE id = ?", [
      runningProfit,
      row.id,
    ]);
  }
};

router.post("/api/admin/login", (req, res) => {
  const providedKey = String(req.body?.adminKey || "").trim();
  if (!providedKey || providedKey !== ADMIN_API_KEY) {
    return res.status(401).json({ message: "Invalid admin credentials." });
  }

  const token = createAuthToken({ role: "admin", name: "Admin" });
  return res.status(200).json({
    message: "Admin login successful.",
    token,
    admin: { role: "admin", name: "Admin" },
  });
});

router.use("/api/admin", requireAuth, requireRoles("admin"));

router.get("/api/admin/dashboard", async (_req, res) => {
  try {
    let totalResumeCount = 0;
    let candidateResumeCount = 0;
    let recruiterResumeUploads = [];
    let topResumesByJob = [];

    if (await tableExists("resumes_data")) {
      const hasJobJidColumn = await columnExists("resumes_data", "job_jid");
      const hasSubmittedByRoleColumn = await columnExists(
        "resumes_data",
        "submitted_by_role",
      );
      const hasApplicantNameColumn = await columnExists(
        "resumes_data",
        "applicant_name",
      );
      const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
      const hasAtsMatchColumn = await columnExists(
        "resumes_data",
        "ats_match_percentage",
      );
      const hasAcceptedColumn = await columnExists(
        "resumes_data",
        "is_accepted",
      );
      const hasAcceptedAtColumn = await columnExists(
        "resumes_data",
        "accepted_at",
      );
      const hasAcceptedByAdminColumn = await columnExists(
        "resumes_data",
        "accepted_by_admin",
      );
      const jobJidSelect = hasJobJidColumn
        ? "rd.job_jid AS jobJid,"
        : "NULL AS jobJid,";
      const acceptedSelect = hasAcceptedColumn
        ? "rd.is_accepted AS isAccepted,"
        : "0 AS isAccepted,";
      const acceptedAtSelect = hasAcceptedAtColumn
        ? "rd.accepted_at AS acceptedAt,"
        : "NULL AS acceptedAt,";
      const acceptedByAdminSelect = hasAcceptedByAdminColumn
        ? "rd.accepted_by_admin AS acceptedByAdmin,"
        : "NULL AS acceptedByAdmin,";

      const [countRows] = await pool.query(
        "SELECT COUNT(*) AS totalResumeCount FROM resumes_data",
      );
      totalResumeCount = Number(countRows?.[0]?.totalResumeCount) || 0;
      if (hasSubmittedByRoleColumn) {
        const [candidateCountRows] = await pool.query(
          `SELECT COUNT(*) AS candidateResumeCount
           FROM resumes_data
           WHERE COALESCE(submitted_by_role, 'recruiter') = 'candidate'`,
        );
        candidateResumeCount =
          Number(candidateCountRows?.[0]?.candidateResumeCount) || 0;
      }

      const [rows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.rid AS rid,
          r.name AS recruiterName,
          r.email AS recruiterEmail,
          ${jobJidSelect}
          j.points_per_joining AS pointsPerJoining,
          j.revenue AS revenue,
          rd.resume_filename AS resumeFilename,
          rd.resume_type AS resumeType,
          ${acceptedSelect}
          ${acceptedAtSelect}
          ${acceptedByAdminSelect}
          rd.uploaded_at AS uploadedAt
        FROM resumes_data rd
        INNER JOIN recruiter r ON r.rid = rd.rid
        LEFT JOIN jobs j ON j.jid = rd.job_jid
        ORDER BY rd.uploaded_at DESC`,
      );

      recruiterResumeUploads = rows;

      if (hasJobJidColumn && (await tableExists("jobs"))) {
        const applicantNameSelect = hasApplicantNameColumn
          ? "ranked.applicant_name AS applicantName,"
          : "NULL AS applicantName,";
        const atsScoreSelect = hasAtsScoreColumn
          ? "ranked.ats_score AS atsScore,"
          : "NULL AS atsScore,";
        const atsMatchSelect = hasAtsMatchColumn
          ? "ranked.ats_match_percentage AS atsMatchPercentage,"
          : "NULL AS atsMatchPercentage,";

        const rankingFilter = hasAtsMatchColumn
          ? `rd2.ats_match_percentage > rd.ats_match_percentage
                OR (
                  rd2.ats_match_percentage = rd.ats_match_percentage
                  AND rd2.uploaded_at > rd.uploaded_at
                )
                OR (
                  rd2.ats_match_percentage = rd.ats_match_percentage
                  AND rd2.uploaded_at = rd.uploaded_at
                  AND rd2.res_id < rd.res_id
                )`
          : `rd2.uploaded_at > rd.uploaded_at
                OR (
                  rd2.uploaded_at = rd.uploaded_at
                  AND rd2.res_id < rd.res_id
                )`;

        const scoreNotNullFilter = hasAtsMatchColumn
          ? "AND rd.ats_match_percentage IS NOT NULL"
          : "";
        const scoreNotNullFilterInner = hasAtsMatchColumn
          ? "AND rd2.ats_match_percentage IS NOT NULL"
          : "";

        const [topRows] = await pool.query(
          `SELECT
            j.jid AS jobJid,
            j.role_name AS roleName,
            j.company_name AS companyName,
            ranked.res_id AS resId,
            ranked.rid AS rid,
            ${applicantNameSelect}
            ranked.resume_filename AS resumeFilename,
            ${atsScoreSelect}
            ${atsMatchSelect}
            ranked.uploaded_at AS uploadedAt
          FROM jobs j
          LEFT JOIN (
            SELECT rd.*
            FROM resumes_data rd
            WHERE rd.job_jid IS NOT NULL
              ${scoreNotNullFilter}
              AND (
                SELECT COUNT(*)
                FROM resumes_data rd2
                WHERE rd2.job_jid = rd.job_jid
                  ${scoreNotNullFilterInner}
                  AND (${rankingFilter})
              ) < 2
          ) ranked ON ranked.job_jid = j.jid
          ORDER BY j.jid DESC, ranked.job_jid IS NULL, ranked.uploaded_at DESC`,
        );

        const groupedByJob = new Map();
        for (const row of topRows) {
          const key = Number(row.jobJid);
          if (!groupedByJob.has(key)) {
            groupedByJob.set(key, {
              jobJid: key,
              roleName: row.roleName || null,
              companyName: row.companyName || null,
              topResumes: [],
            });
          }

          if (row.resId) {
            groupedByJob.get(key).topResumes.push({
              resId: row.resId,
              rid: row.rid,
              applicantName: row.applicantName || null,
              resumeFilename: row.resumeFilename || null,
              atsScore:
                row.atsScore === null || row.atsScore === undefined
                  ? null
                  : Number(row.atsScore),
              atsMatchPercentage:
                row.atsMatchPercentage === null ||
                row.atsMatchPercentage === undefined
                  ? null
                  : Number(row.atsMatchPercentage),
              uploadedAt: row.uploadedAt || null,
            });
          }
        }

        topResumesByJob = Array.from(groupedByJob.values()).map((job) => {
          const sorted = [...job.topResumes].sort((a, b) => {
            const matchA =
              a.atsMatchPercentage === null ? -1 : Number(a.atsMatchPercentage);
            const matchB =
              b.atsMatchPercentage === null ? -1 : Number(b.atsMatchPercentage);
            if (matchB !== matchA) return matchB - matchA;

            const timeA = a.uploadedAt ? new Date(a.uploadedAt).getTime() : 0;
            const timeB = b.uploadedAt ? new Date(b.uploadedAt).getTime() : 0;
            if (timeB !== timeA) return timeB - timeA;

            return String(a.resId || "").localeCompare(String(b.resId || ""));
          });

          return { ...job, topResumes: sorted.slice(0, 2) };
        });
      }
    }

    return res.status(200).json({
      totalResumeCount,
      candidateResumeCount,
      recruiterResumeUploads,
      topResumesByJob,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch admin dashboard.",
      error: error.message,
    });
  }
});

router.get("/api/admin/candidate-resumes", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    if (!(await tableExists("resumes_data"))) {
      return res.status(200).json({
        totalCount: 0,
        resumes: [],
      });
    }

    const hasSubmittedByRoleColumn = await columnExists(
      "resumes_data",
      "submitted_by_role",
    );
    if (!hasSubmittedByRoleColumn) {
      return res.status(200).json({
        totalCount: 0,
        resumes: [],
      });
    }

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
    const hasJobDescriptionColumn = await columnExists(
      "jobs",
      "job_description",
    );
    const hasApplicationsTable = await tableExists("applications");
    const hasSelectionTable = await tableExists("job_resume_selection");
    const hasExtraInfoTable = await tableExists("extra_info");
    const hasSubmittedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "submitted_reason"));
    const hasVerifiedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "verified_reason"));
    const hasPriorExperienceColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "has_prior_experience"));
    const hasExperienceIndustryColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "experience_industry"));
    const hasExperienceIndustryOtherColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "experience_industry_other"));
    const hasCurrentSalaryColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "current_salary"));
    const hasExpectedSalaryColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "expected_salary"));
    const hasNoticePeriodColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "notice_period"));
    const hasYearsOfExperienceColumn =
      hasApplicationsTable &&
      (await columnExists("applications", "years_of_experience"));

    const applicantNameSelect = hasApplicantNameColumn
      ? "rd.applicant_name AS applicantName,"
      : "NULL AS applicantName,";
    const applicantEmailLookupSql = hasApplicationsTable
      ? `(
          SELECT a.email
          FROM applications a
          WHERE a.job_jid = rd.job_jid
            AND (
              a.resume_filename = rd.resume_filename
              ${hasApplicantNameColumn ? "OR a.candidate_name = rd.applicant_name" : ""}
            )
          ORDER BY a.created_at DESC, a.id DESC
          LIMIT 1
        )`
      : "NULL";
    const applicationMatchSql = hasApplicationsTable
      ? `a.job_jid = rd.job_jid
          AND (
            ${hasApplicantEmailColumn ? "a.email = rd.applicant_email OR" : ""}
            a.resume_filename = rd.resume_filename
            ${hasApplicantNameColumn ? "OR a.candidate_name = rd.applicant_name" : ""}
          )`
      : "1 = 0";
    const priorExperienceSelect = hasPriorExperienceColumn
      ? `(
            SELECT a.has_prior_experience
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS hasPriorExperience,`
      : "NULL AS hasPriorExperience,";
    const experienceIndustrySelect = hasExperienceIndustryColumn
      ? `(
            SELECT a.experience_industry
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS experienceIndustry,`
      : "NULL AS experienceIndustry,";
    const experienceIndustryOtherSelect = hasExperienceIndustryOtherColumn
      ? `(
            SELECT a.experience_industry_other
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS experienceIndustryOther,`
      : "NULL AS experienceIndustryOther,";
    const currentSalarySelect = hasCurrentSalaryColumn
      ? `(
            SELECT a.current_salary
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS currentSalary,`
      : "NULL AS currentSalary,";
    const expectedSalarySelect = hasExpectedSalaryColumn
      ? `(
            SELECT a.expected_salary
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS expectedSalary,`
      : "NULL AS expectedSalary,";
    const noticePeriodSelect = hasNoticePeriodColumn
      ? `(
            SELECT a.notice_period
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS noticePeriod,`
      : "NULL AS noticePeriod,";
    const yearsOfExperienceSelect = hasYearsOfExperienceColumn
      ? `(
            SELECT a.years_of_experience
            FROM applications a
            WHERE ${applicationMatchSql}
            ORDER BY a.created_at DESC, a.id DESC
            LIMIT 1
          ) AS yearsOfExperience,`
      : "NULL AS yearsOfExperience,";
    const applicantEmailSelect = hasApplicantEmailColumn
      ? `COALESCE(rd.applicant_email, ${applicantEmailLookupSql}) AS applicantEmail,`
      : `${applicantEmailLookupSql} AS applicantEmail,`;
    const atsScoreSelect = hasAtsScoreColumn
      ? "rd.ats_score AS atsScore,"
      : "NULL AS atsScore,";
    const atsMatchSelect = hasAtsMatchColumn
      ? "rd.ats_match_percentage AS atsMatchPercentage,"
      : "NULL AS atsMatchPercentage,";
    const jobDescriptionSelect = hasJobDescriptionColumn
      ? "j.job_description AS jobDescription,"
      : "NULL AS jobDescription,";
    const selectionSelect = hasSelectionTable
      ? `jrs.selection_status AS selectionStatus,
        jrs.selection_note AS selectionNote,
        jrs.selected_by_admin AS selectedByAdmin,
        jrs.selected_at AS selectedAt`
      : `NULL AS selectionStatus,
        NULL AS selectionNote,
        NULL AS selectedByAdmin,
        NULL AS selectedAt`;
    const selectionJoin = hasSelectionTable
      ? `LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid
       AND jrs.res_id = rd.res_id`
      : "";
    const submittedReasonSelect = hasSubmittedReasonColumn
      ? "ei.submitted_reason AS submittedReason,"
      : "NULL AS submittedReason,";
    const verifiedReasonSelect = hasVerifiedReasonColumn
      ? "ei.verified_reason AS verifiedReason,"
      : "NULL AS verifiedReason,";
    const extraInfoJoin = hasExtraInfoTable
      ? `LEFT JOIN extra_info ei
        ON ei.res_id = rd.res_id
       OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)`
      : "";

    const [rows] = await pool.query(
      `SELECT
        rd.res_id AS resId,
        rd.job_jid AS jobJid,
        ${applicantNameSelect}
        ${applicantEmailSelect}
        ${priorExperienceSelect}
        ${experienceIndustrySelect}
        ${experienceIndustryOtherSelect}
        ${currentSalarySelect}
        ${expectedSalarySelect}
        ${noticePeriodSelect}
        ${yearsOfExperienceSelect}
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType,
        ${atsScoreSelect}
        ${atsMatchSelect}
        rd.uploaded_at AS uploadedAt,
        j.role_name AS roleName,
        j.company_name AS companyName,
        ${jobDescriptionSelect}
        j.skills AS skills,
        ${submittedReasonSelect}
        ${verifiedReasonSelect}
        ${selectionSelect}
      FROM resumes_data rd
      LEFT JOIN jobs j ON j.jid = rd.job_jid
      ${extraInfoJoin}
      ${selectionJoin}
      WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'candidate'
      ORDER BY rd.uploaded_at DESC, rd.res_id ASC`,
    );

    return res.status(200).json({
      totalCount: rows.length,
      resumes: rows.map((row) => ({
        resId: row.resId,
        jobJid: row.jobJid ? String(row.jobJid).trim() : null,
        applicantName: row.applicantName || null,
        applicantEmail: row.applicantEmail || null,
        hasPriorExperience:
          row.hasPriorExperience === null ||
          row.hasPriorExperience === undefined
            ? null
            : Boolean(row.hasPriorExperience),
        experience: {
          industry: row.experienceIndustry || null,
          industryOther: row.experienceIndustryOther || null,
          currentSalary:
            row.currentSalary === null || row.currentSalary === undefined
              ? null
              : Number(row.currentSalary),
          expectedSalary:
            row.expectedSalary === null || row.expectedSalary === undefined
              ? null
              : Number(row.expectedSalary),
          noticePeriod: row.noticePeriod || null,
          yearsOfExperience:
            row.yearsOfExperience === null ||
            row.yearsOfExperience === undefined
              ? null
              : Number(row.yearsOfExperience),
        },
        resumeFilename: row.resumeFilename || null,
        resumeType: row.resumeType || null,
        atsScore:
          row.atsScore === null || row.atsScore === undefined
            ? null
            : Number(row.atsScore),
        atsMatchPercentage:
          row.atsMatchPercentage === null ||
          row.atsMatchPercentage === undefined
            ? null
            : Number(row.atsMatchPercentage),
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        uploadedAt: row.uploadedAt || null,
        job: {
          roleName: row.roleName || null,
          companyName: row.companyName || null,
          jobDescription: row.jobDescription || null,
          skills: row.skills || null,
        },
        selection: row.selectionStatus
          ? {
              status: row.selectionStatus,
              note: row.selectionNote || null,
              selectedByAdmin: row.selectedByAdmin || null,
              selectedAt: row.selectedAt || null,
            }
          : null,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch candidate submitted resumes.",
      error: error.message,
    });
  }
});

router.post("/api/admin/resumes/:resId/accept", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const normalizedResId = String(req.params.resId || "").trim();
  const selectedByAdmin =
    String(req.body?.selected_by_admin || "admin-panel").trim() ||
    "admin-panel";
  if (!normalizedResId) {
    return res.status(400).json({ message: "resId is required." });
  }

  const hasPointsColumn = await columnExists("recruiter", "points");
  const hasAcceptedColumn = await columnExists("resumes_data", "is_accepted");
  const hasAcceptedAtColumn = await columnExists("resumes_data", "accepted_at");
  const hasAcceptedByAdminColumn = await columnExists(
    "resumes_data",
    "accepted_by_admin",
  );

  if (!hasAcceptedColumn) {
    return res
      .status(500)
      .json({ message: "Acceptance columns are not initialized." });
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [resumeRows] = await connection.query(
      `SELECT
        rd.res_id AS resId,
        rd.rid,
        rd.job_jid AS jobJid,
        rd.is_accepted AS isAccepted
      FROM resumes_data rd
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
    if (Boolean(resume.isAccepted)) {
      await connection.rollback();
      return res.status(200).json({
        message: "Resume is already accepted.",
        accepted: true,
        pointsAdded: 0,
        recruiterRid: resume.rid,
      });
    }

    let pointsPerJoining = 0;
    if (resume.jobJid) {
      const [jobRows] = await connection.query(
        "SELECT COALESCE(points_per_joining, 0) AS pointsPerJoining FROM jobs WHERE jid = ? LIMIT 1",
        [resume.jobJid],
      );
      pointsPerJoining = Number(jobRows?.[0]?.pointsPerJoining) || 0;
    }

    const updateAcceptedSegments = [];
    if (hasAcceptedColumn) updateAcceptedSegments.push("is_accepted = TRUE");
    if (hasAcceptedAtColumn)
      updateAcceptedSegments.push("accepted_at = CURRENT_TIMESTAMP");
    if (hasAcceptedByAdminColumn)
      updateAcceptedSegments.push("accepted_by_admin = ?");
    const updateParams = hasAcceptedByAdminColumn
      ? [selectedByAdmin, normalizedResId]
      : [normalizedResId];

    await connection.query(
      `UPDATE resumes_data SET ${updateAcceptedSegments.join(", ")} WHERE res_id = ?`,
      updateParams,
    );

    if (hasPointsColumn && pointsPerJoining > 0) {
      await connection.query(
        "UPDATE recruiter SET points = COALESCE(points, 0) + ? WHERE rid = ?",
        [pointsPerJoining, resume.rid],
      );
    }

    await connection.commit();
    return res.status(200).json({
      message: "Resume accepted and recruiter points updated.",
      accepted: true,
      pointsAdded: pointsPerJoining,
      recruiterRid: resume.rid,
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: "Failed to accept resume.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
});

router.get("/api/admin/job-alerts", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    const [rows] = await pool.query(
      `SELECT
        j.jid AS jobJid,
        j.company_name AS companyName,
        j.role_name AS roleName,
        j.positions_open AS positionsOpen,
        j.created_at AS createdAt,
        COUNT(rd.res_id) AS totalSubmittedResumes,
        SUM(CASE WHEN jrs.selection_status = 'selected' THEN 1 ELSE 0 END) AS selectedCount
      FROM jobs j
      LEFT JOIN resumes_data rd ON rd.job_jid = j.jid
      LEFT JOIN job_resume_selection jrs ON jrs.job_jid = j.jid AND jrs.res_id = rd.res_id
      GROUP BY j.jid, j.company_name, j.role_name, j.positions_open, j.created_at
      ORDER BY j.created_at DESC, j.jid DESC`,
    );

    return res.status(200).json({
      jobs: rows.map((row) => ({
        jobJid: String(row.jobJid || "").trim(),
        companyName: row.companyName,
        roleName: row.roleName,
        positionsOpen: Number(row.positionsOpen) || 1,
        createdAt: row.createdAt,
        totalSubmittedResumes: Number(row.totalSubmittedResumes) || 0,
        selectedCount: Number(row.selectedCount) || 0,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch job alerts.",
      error: error.message,
    });
  }
});

router.get("/api/admin/jobs/:jid/resumes", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const safeJobId = normalizeJobJid(req.params.jid);
  if (!safeJobId) {
    return res.status(400).json({ message: "jid is required." });
  }

  try {
    const [jobs] = await pool.query(
      `SELECT
        jid AS jobJid,
        company_name AS companyName,
        role_name AS roleName,
        positions_open AS positionsOpen
      FROM jobs
      WHERE jid = ?
      LIMIT 1`,
      [safeJobId],
    );

    if (jobs.length === 0) {
      return res.status(404).json({ message: "Job not found." });
    }

    const hasAtsScoreColumn = await columnExists("resumes_data", "ats_score");
    const hasAtsMatchColumn = await columnExists(
      "resumes_data",
      "ats_match_percentage",
    );
    const hasExtraInfoTable = await tableExists("extra_info");
    const hasSubmittedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "submitted_reason"));
    const hasVerifiedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "verified_reason"));

    const atsScoreSelect = hasAtsScoreColumn
      ? "rd.ats_score AS atsScore,"
      : "NULL AS atsScore,";
    const atsMatchSelect = hasAtsMatchColumn
      ? "rd.ats_match_percentage AS atsMatchPercentage,"
      : "NULL AS atsMatchPercentage,";
    const submittedReasonSelect = hasSubmittedReasonColumn
      ? "ei.submitted_reason AS submittedReason,"
      : "NULL AS submittedReason,";
    const verifiedReasonSelect = hasVerifiedReasonColumn
      ? "ei.verified_reason AS verifiedReason,"
      : "NULL AS verifiedReason,";
    const extraInfoJoin = hasExtraInfoTable
      ? `LEFT JOIN extra_info ei
        ON ei.res_id = rd.res_id
       OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)`
      : "";

    const [rows] = await pool.query(
      `SELECT
        rd.res_id AS resId,
        rd.rid AS rid,
        r.name AS recruiterName,
        r.email AS recruiterEmail,
        rd.resume_filename AS resumeFilename,
        rd.resume_type AS resumeType,
        ${atsScoreSelect}
        ${atsMatchSelect}
        rd.uploaded_at AS uploadedAt,
        ${submittedReasonSelect}
        ${verifiedReasonSelect}
        jrs.selection_status AS selectionStatus,
        jrs.selection_note AS selectionNote,
        jrs.selected_by_admin AS selectedByAdmin,
        jrs.selected_at AS selectedAt
      FROM resumes_data rd
      INNER JOIN recruiter r ON r.rid = rd.rid
      ${extraInfoJoin}
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid
       AND jrs.res_id = rd.res_id
      WHERE rd.job_jid = ?
      ORDER BY rd.uploaded_at DESC, rd.res_id ASC`,
      [safeJobId],
    );

    return res.status(200).json({
      job: {
        jobJid: String(jobs[0].jobJid || "").trim(),
        companyName: jobs[0].companyName,
        roleName: jobs[0].roleName,
        positionsOpen: Number(jobs[0].positionsOpen) || 1,
      },
      resumes: rows.map((row) => ({
        resId: row.resId,
        rid: row.rid,
        recruiterName: row.recruiterName,
        recruiterEmail: row.recruiterEmail,
        resumeFilename: row.resumeFilename,
        resumeType: row.resumeType,
        atsScore: row.atsScore === null ? null : Number(row.atsScore),
        atsMatchPercentage:
          row.atsMatchPercentage === null
            ? null
            : Number(row.atsMatchPercentage),
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        uploadedAt: row.uploadedAt,
        selection: row.selectionStatus
          ? {
              status: row.selectionStatus,
              note: row.selectionNote || null,
              selectedByAdmin: row.selectedByAdmin || null,
              selectedAt: row.selectedAt || null,
            }
          : null,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch job resumes.",
      error: error.message,
    });
  }
});

router.post("/api/admin/jobs/:jid/resume-selections", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const safeJobId = normalizeJobJid(req.params.jid);
  if (!safeJobId) {
    return res.status(400).json({ message: "jid is required." });
  }

  const { resId, selection_status, selection_note, selected_by_admin } =
    req.body || {};
  const normalizedResId = String(resId || "").trim();
  const normalizedStatus = String(selection_status || "")
    .trim()
    .toLowerCase();
  const normalizedSelectedByAdmin = String(selected_by_admin || "").trim();
  const normalizedSelectionNote =
    selection_note === undefined || selection_note === null
      ? null
      : String(selection_note).trim();
  const allowedStatuses = new Set(["selected", "rejected", "on_hold"]);

  if (
    !normalizedResId ||
    !normalizedSelectedByAdmin ||
    !allowedStatuses.has(normalizedStatus)
  ) {
    return res.status(400).json({
      message: "resId, selection_status, and selected_by_admin are required.",
    });
  }

  try {
    const [resumeRows] = await pool.query(
      `SELECT rd.res_id AS resId, rd.job_jid AS jobJid
       FROM resumes_data rd
       WHERE rd.res_id = ?
       LIMIT 1`,
      [normalizedResId],
    );

    if (resumeRows.length === 0) {
      return res.status(404).json({ message: "Resume not found." });
    }

    if (String(resumeRows[0].jobJid || "").trim() !== safeJobId) {
      return res.status(400).json({
        message: "The provided resume is not associated with this job.",
      });
    }

    await pool.query(
      `INSERT INTO job_resume_selection
        (job_jid, res_id, selected_by_admin, selection_status, selection_note)
       VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         selected_by_admin = VALUES(selected_by_admin),
         selection_status = VALUES(selection_status),
         selection_note = VALUES(selection_note),
         selected_at = CURRENT_TIMESTAMP`,
      [
        safeJobId,
        normalizedResId,
        normalizedSelectedByAdmin,
        normalizedStatus,
        normalizedSelectionNote || null,
      ],
    );

    return res.status(200).json({
      message: "Resume selection updated successfully.",
      selection: {
        jobJid: safeJobId,
        resId: normalizedResId,
        selectionStatus: normalizedStatus,
        selectionNote: normalizedSelectionNote || null,
        selectedByAdmin: normalizedSelectedByAdmin,
      },
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to update resume selection.",
      error: error.message,
    });
  }
});

router.get("/api/admin/jobs/:jid/selection-summary", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const safeJobId = normalizeJobJid(req.params.jid);
  if (!safeJobId) {
    return res.status(400).json({ message: "jid is required." });
  }

  try {
    const [jobRows] = await pool.query(
      `SELECT
        jid AS jobJid,
        company_name AS companyName,
        role_name AS roleName,
        positions_open AS positionsOpen
      FROM jobs
      WHERE jid = ?
      LIMIT 1`,
      [safeJobId],
    );

    if (jobRows.length === 0) {
      return res.status(404).json({ message: "Job not found." });
    }

    const [selectedRows] = await pool.query(
      `SELECT
        jrs.res_id AS resId,
        jrs.selection_note AS selectionNote,
        jrs.selected_by_admin AS selectedByAdmin,
        jrs.selected_at AS selectedAt,
        rd.rid AS rid,
        rd.resume_filename AS resumeFilename
      FROM job_resume_selection jrs
      INNER JOIN resumes_data rd ON rd.res_id = jrs.res_id
      WHERE jrs.job_jid = ?
        AND jrs.selection_status = 'selected'
      ORDER BY jrs.selected_at DESC, jrs.id DESC`,
      [safeJobId],
    );

    const positionsOpen = Number(jobRows[0].positionsOpen) || 1;
    const selectedCount = selectedRows.length;

    return res.status(200).json({
      summary: {
        jobJid: String(jobRows[0].jobJid || "").trim(),
        companyName: jobRows[0].companyName,
        roleName: jobRows[0].roleName,
        positionsOpen,
        selectedCount,
        remainingSlots: positionsOpen - selectedCount,
      },
      selectedResumes: selectedRows.map((row) => ({
        resId: row.resId,
        rid: row.rid,
        resumeFilename: row.resumeFilename,
        selectionNote: row.selectionNote || null,
        selectedByAdmin: row.selectedByAdmin || null,
        selectedAt: row.selectedAt || null,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch selection summary.",
      error: error.message,
    });
  }
});

router.get("/api/admin/attendance", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const attendanceDate = normalizeAttendanceDate(req.query?.date);
  if (!attendanceDate) {
    return res
      .status(400)
      .json({ message: "date must be in YYYY-MM-DD format." });
  }

  try {
    await ensureRecruiterAttendanceTable();

    const [rows] = await pool.query(
      `SELECT
        r.rid,
        r.name,
        CASE
          WHEN LOWER(TRIM(COALESCE(r.role, 'recruiter'))) IN ('team leader', 'team_leader', 'job creator') THEN 'team leader'
          ELSE 'recruiter'
        END AS role,
        COALESCE(
          r.daily_salary,
          ROUND(r.monthly_salary / 30, 2),
          CASE
            WHEN TRIM(COALESCE(r.salary, '')) REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN ROUND(CAST(TRIM(r.salary) AS DECIMAL(12,2)) / 30, 2)
            ELSE 0
          END
        ) AS dailySalary,
        ra.id AS attendanceId,
        ra.status,
        ra.salary_amount AS salaryAmount,
        ra.money_sum_id AS moneySumId,
        ra.marked_by AS markedBy,
        ra.marked_at AS markedAt,
        ra.updated_at AS updatedAt
      FROM recruiter r
      LEFT JOIN recruiter_attendance ra
        ON ra.recruiter_rid = r.rid
       AND ra.attendance_date = ?
      WHERE LOWER(TRIM(COALESCE(r.role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')
      ORDER BY
        CASE
          WHEN LOWER(TRIM(COALESCE(r.role, 'recruiter'))) IN ('team leader', 'team_leader', 'job creator') THEN 0
          ELSE 1
        END,
        r.name ASC,
        r.rid ASC`,
      [attendanceDate],
    );

    const staff = rows.map((row) => {
      const status = normalizeAttendanceStatus(row.status) || "absent";
      const dailySalary = toMoneyNumber(row.dailySalary);
      const salaryAmount = toMoneyNumber(row.salaryAmount);
      return {
        attendanceId: row.attendanceId ? Number(row.attendanceId) : null,
        rid: row.rid,
        name: row.name,
        role: normalizeStaffRole(row.role),
        dailySalary,
        status,
        salaryAmount,
        moneySumId: row.moneySumId ? Number(row.moneySumId) : null,
        markedBy: row.markedBy || null,
        markedAt: row.markedAt || null,
        updatedAt: row.updatedAt || null,
      };
    });

    const summary = staff.reduce(
      (accumulator, member) => {
        accumulator.totalStaff += 1;
        accumulator.dailyExpense =
          Math.round(
            (accumulator.dailyExpense + toMoneyNumber(member.salaryAmount)) *
              100,
          ) / 100;
        if (member.status === "present") accumulator.presentCount += 1;
        else if (member.status === "half_day") accumulator.halfDayCount += 1;
        else accumulator.absentCount += 1;
        return accumulator;
      },
      {
        totalStaff: 0,
        presentCount: 0,
        absentCount: 0,
        halfDayCount: 0,
        dailyExpense: 0,
      },
    );

    return res.status(200).json({
      date: attendanceDate,
      staff,
      summary,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch attendance.",
      error: error.message,
    });
  }
});

router.put("/api/admin/attendance", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const recruiterRid = String(req.body?.recruiterRid || "").trim();
  const attendanceDate = normalizeAttendanceDate(req.body?.attendanceDate);
  const status = normalizeAttendanceStatus(req.body?.status);
  const markedBy = String(req.body?.markedBy || "admin").trim() || "admin";

  if (!recruiterRid || !attendanceDate || !status) {
    return res.status(400).json({
      message: "recruiterRid, attendanceDate, and status are required.",
    });
  }

  const connection = await pool.getConnection();
  try {
    await ensureRecruiterAttendanceTable();
    await connection.beginTransaction();

    const [recruiterRows] = await connection.query(
      `SELECT
        rid,
        name,
        CASE
          WHEN LOWER(TRIM(COALESCE(role, 'recruiter'))) IN ('team leader', 'team_leader', 'job creator') THEN 'team leader'
          ELSE 'recruiter'
        END AS role,
        COALESCE(
          daily_salary,
          ROUND(monthly_salary / 30, 2),
          CASE
            WHEN TRIM(COALESCE(salary, '')) REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN ROUND(CAST(TRIM(salary) AS DECIMAL(12,2)) / 30, 2)
            ELSE 0
          END
        ) AS dailySalary
      FROM recruiter
      WHERE rid = ?
        AND LOWER(TRIM(COALESCE(role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')
      LIMIT 1
      FOR UPDATE`,
      [recruiterRid],
    );

    if (recruiterRows.length === 0) {
      await connection.rollback();
      return res
        .status(404)
        .json({ message: "Recruiter or team leader not found." });
    }

    const recruiter = recruiterRows[0];
    const expenseAmount = calculateAttendanceExpense(
      status,
      recruiter.dailySalary,
    );

    const [attendanceRows] = await connection.query(
      `SELECT id, money_sum_id AS moneySumId
       FROM recruiter_attendance
       WHERE recruiter_rid = ? AND attendance_date = ?
       LIMIT 1
       FOR UPDATE`,
      [recruiterRid, attendanceDate],
    );

    const existingAttendance = attendanceRows[0] || null;
    let moneySumId = existingAttendance?.moneySumId
      ? Number(existingAttendance.moneySumId)
      : null;

    if (status === "absent") {
      if (moneySumId) {
        await connection.query("DELETE FROM money_sum WHERE id = ?", [
          moneySumId,
        ]);
        moneySumId = null;
      }
    } else {
      const reason = buildAttendanceReason({
        recruiterRid,
        recruiterName: recruiter.name,
        attendanceDate,
        status,
      });

      let hasExistingMoneyRow = false;
      if (moneySumId) {
        const [moneyRows] = await connection.query(
          "SELECT id FROM money_sum WHERE id = ? LIMIT 1",
          [moneySumId],
        );
        hasExistingMoneyRow = moneyRows.length > 0;
      }

      if (hasExistingMoneyRow) {
        await connection.query(
          `UPDATE money_sum
           SET company_rev = 0,
               expense = ?,
               reason = ?,
               photo = NULL,
               entry_type = 'expense'
           WHERE id = ?`,
          [expenseAmount, reason, moneySumId],
        );
      } else {
        const [insertMoneySum] = await connection.query(
          `INSERT INTO money_sum (company_rev, expense, profit, reason, photo, entry_type)
           VALUES (0, ?, 0, ?, NULL, 'expense')`,
          [expenseAmount, reason],
        );
        moneySumId = Number(insertMoneySum.insertId);
      }
    }

    if (existingAttendance) {
      await connection.query(
        `UPDATE recruiter_attendance
         SET status = ?,
             salary_amount = ?,
             money_sum_id = ?,
             marked_by = ?,
             marked_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [status, expenseAmount, moneySumId, markedBy, existingAttendance.id],
      );
    } else {
      await connection.query(
        `INSERT INTO recruiter_attendance
          (recruiter_rid, attendance_date, status, salary_amount, money_sum_id, marked_by)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
          recruiterRid,
          attendanceDate,
          status,
          expenseAmount,
          moneySumId,
          markedBy,
        ],
      );
    }

    await recomputeMoneyProfit(connection);
    await connection.commit();

    return res.status(200).json({
      message: "Attendance updated successfully.",
      attendance: {
        recruiterRid,
        attendanceDate,
        status,
        salaryAmount: expenseAmount,
        moneySumId,
        role: normalizeStaffRole(recruiter.role),
        dailySalary: toMoneyNumber(recruiter.dailySalary),
      },
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: "Failed to update attendance.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
});

router.get("/api/admin/revenue", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    await ensureMoneySumTable();

    const hasMoneySumTable = await tableExists("money_sum");
    if (!hasMoneySumTable) {
      return res.status(200).json({
        entries: [],
        summary: {
          totalIntake: 0,
          totalExpense: 0,
          netProfit: 0,
        },
      });
    }

    const [rows] = await pool.query(
      `SELECT
        id,
        company_rev AS companyRev,
        expense,
        profit,
        reason,
        photo,
        entry_type AS entryType,
        created_at AS createdAt
      FROM money_sum
      ORDER BY created_at DESC, id DESC`,
    );

    const [summaryRows] = await pool.query(
      `SELECT
        COALESCE(SUM(company_rev), 0) AS totalIntake,
        COALESCE(SUM(expense), 0) AS totalExpense
      FROM money_sum`,
    );
    const totalIntake = toMoneyNumber(summaryRows?.[0]?.totalIntake);
    const totalExpense = toMoneyNumber(summaryRows?.[0]?.totalExpense);
    const netProfit = Math.round((totalIntake - totalExpense) * 100) / 100;

    return res.status(200).json({
      entries: rows.map((row) => ({
        id: Number(row.id),
        companyRev: toMoneyNumber(row.companyRev),
        expense: toMoneyNumber(row.expense),
        profit: toMoneyNumber(row.profit),
        reason: row.reason || "",
        photo: normalizePhotoValue(row.photo),
        entryType:
          normalizeRevenueEntryType(row.entryType) ||
          (toMoneyNumber(row.companyRev) > 0 ? "intake" : "expense"),
        createdAt: row.createdAt,
      })),
      summary: {
        totalIntake,
        totalExpense,
        netProfit,
      },
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch revenue dashboard.",
      error: error.message,
    });
  }
});

router.get("/api/admin/reimbursements", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;
  try {
    const [rows] = await pool.query(
      `SELECT id, rid, role, amount, description, status, admin_note AS adminNote, created_at AS createdAt, updated_at AS updatedAt
       FROM reimbursements
       ORDER BY created_at DESC`,
    );
    return res.status(200).json({ reimbursements: rows });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch reimbursements.",
      error: error.message,
    });
  }
});

router.post("/api/admin/reimbursements/:id/decision", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;
  const id = Number(req.params.id);
  const decision = String(req.body?.decision || "")
    .trim()
    .toLowerCase();
  const adminNote = String(req.body?.adminNote || "").trim();
  if (!id)
    return res
      .status(400)
      .json({ message: "Valid reimbursement id is required." });
  if (!["accepted", "rejected"].includes(decision)) {
    return res
      .status(400)
      .json({ message: "decision must be 'accepted' or 'rejected'." });
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    console.log(
      `[ReimbursementDecision] Processing decision: id=${id}, decision=${decision}`,
    );

    const [rows] = await connection.query(
      "SELECT id, rid, amount, description, status, money_sum_id FROM reimbursements WHERE id = ? FOR UPDATE",
      [id],
    );
    if (rows.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: "Reimbursement not found." });
    }
    const current = rows[0];
    console.log(`[ReimbursementDecision] Current reimbursement:`, current);

    if (current.status !== "pending") {
      await connection.rollback();
      return res
        .status(400)
        .json({ message: "Reimbursement already decided." });
    }

    await connection.query(
      `UPDATE reimbursements
       SET status = ?, admin_note = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [decision, adminNote || null, id],
    );

    // Handle money_sum updates
    if (decision === "rejected" && current.money_sum_id) {
      console.log(
        `[ReimbursementDecision] Processing rejection with money_sum_id=${current.money_sum_id}`,
      );
      // If rejected, create a reversal entry to negate the original expense
      const [moneySumRow] = await connection.query(
        "SELECT profit FROM money_sum WHERE id = ?",
        [current.money_sum_id],
      );
      if (moneySumRow.length > 0) {
        const currentProfit = toMoneyNumber(moneySumRow[0].profit);
        const expense = toMoneyNumber(current.amount);
        // Reversal: add back the expense to profit
        const reversedProfit =
          Math.round((currentProfit + expense) * 100) / 100;
        const reason = `Reimbursement REJECTED for RID ${current.rid}: ${current.description || "No description"}`;

        console.log(
          `[ReimbursementDecision] Creating reversal: currentProfit=${currentProfit}, expense=${expense}, reversedProfit=${reversedProfit}`,
        );

        await connection.query(
          `INSERT INTO money_sum (company_rev, expense, profit, reason, entry_type)
           VALUES (0, ?, ?, ?, 'expense_reversal')`,
          [-expense, reversedProfit, reason],
        );
      }
    } else if (decision === "accepted" && current.money_sum_id) {
      console.log(
        `[ReimbursementDecision] Processing acceptance with money_sum_id=${current.money_sum_id}`,
      );
      // If accepted, update the reason to indicate it's confirmed
      const reason = `Reimbursement ACCEPTED for RID ${current.rid}: ${current.description || "No description"}`;
      await connection.query(`UPDATE money_sum SET reason = ? WHERE id = ?`, [
        reason,
        current.money_sum_id,
      ]);
    }

    await connection.commit();
    console.log(`[ReimbursementDecision] Decision committed successfully`);
    return res
      .status(200)
      .json({ message: "Decision recorded.", status: decision });
  } catch (error) {
    console.error(`[ReimbursementDecision] Error:`, error);
    await connection.rollback();
    return res
      .status(500)
      .json({ message: "Failed to record decision.", error: error.message });
  } finally {
    connection.release();
  }
});

router.get("/api/admin/recruiters/list", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    const hasRoleColumn = await columnExists("recruiter", "role");
    const roleFilter = hasRoleColumn
      ? "WHERE LOWER(TRIM(COALESCE(role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')"
      : "";
    const [rows] = await pool.query(
      `SELECT rid, name, COALESCE(role, 'recruiter') AS role
       FROM recruiter
       ${roleFilter}
       ORDER BY name ASC, rid ASC`,
    );

    return res.status(200).json({
      recruiters: rows.map((row) => ({
        rid: row.rid,
        name: row.name,
        role: normalizeStaffRole(row.role),
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch recruiters list.",
      error: error.message,
    });
  }
});

router.post(
  "/api/admin/revenue/entries",
  parseRevenueUpload,
  async (req, res) => {
    if (!ensureAdminAuthorized(req, res)) return;

    await ensureMoneySumTable();

    const entryType = normalizeRevenueEntryType(req.body?.entryType);
    const amount = toPositiveMoney(req.body?.amount);
    const reasonCategory = req.body?.reasonCategory;
    const otherReason = req.body?.otherReason;
    const recruiterRid = req.body?.recruiterRid;

    if (!entryType || amount === null) {
      return res.status(400).json({
        message:
          "entryType ('intake' or 'expense') and positive amount are required.",
      });
    }

    let recruiterName = "";
    if (
      normalizeRevenueReasonCategory(reasonCategory) === "salary" &&
      String(recruiterRid || "").trim()
    ) {
      try {
        const [recruiterRows] = await pool.query(
          "SELECT name FROM recruiter WHERE rid = ? LIMIT 1",
          [String(recruiterRid).trim()],
        );
        recruiterName = recruiterRows?.[0]?.name || "";
      } catch {}
    }

    const reasonResult = revenueReasonFromPayload({
      reasonCategory,
      otherReason,
      recruiterRid,
      recruiterName,
    });
    if (reasonResult.error) {
      return res.status(400).json({
        message: reasonResult.error,
      });
    }

    const companyRev = entryType === "intake" ? amount : 0;
    const expense = entryType === "expense" ? amount : 0;
    const safeReason = reasonResult.reason || "";
    const safePhoto = toRevenueAttachmentDataUrl(req.file) || null;

    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      const [profitRows] = await connection.query(
        "SELECT COALESCE(profit, 0) AS lastProfit FROM money_sum ORDER BY created_at DESC, id DESC LIMIT 1",
      );
      const lastProfit = toMoneyNumber(profitRows?.[0]?.lastProfit);
      const nextProfit =
        Math.round((lastProfit + companyRev - expense) * 100) / 100;

      const [insertResult] = await connection.query(
        `INSERT INTO money_sum
        (company_rev, expense, profit, reason, photo, entry_type)
       VALUES (?, ?, ?, ?, ?, ?)`,
        [companyRev, expense, nextProfit, safeReason, safePhoto, entryType],
      );

      const [entryRows] = await connection.query(
        `SELECT
        id,
        company_rev AS companyRev,
        expense,
        profit,
        reason,
        photo,
        entry_type AS entryType,
        created_at AS createdAt
      FROM money_sum
      WHERE id = ?
      LIMIT 1`,
        [insertResult.insertId],
      );

      await connection.commit();
      return res.status(201).json({
        message: "Revenue entry added successfully.",
        entry:
          entryRows.length > 0
            ? {
                id: Number(entryRows[0].id),
                companyRev: toMoneyNumber(entryRows[0].companyRev),
                expense: toMoneyNumber(entryRows[0].expense),
                profit: toMoneyNumber(entryRows[0].profit),
                reason: entryRows[0].reason || "",
                photo: normalizePhotoValue(entryRows[0].photo),
                entryType: normalizeRevenueEntryType(entryRows[0].entryType),
                createdAt: entryRows[0].createdAt,
              }
            : null,
      });
    } catch (error) {
      await connection.rollback();
      return res.status(500).json({
        message: "Failed to add revenue entry.",
        error: error.message,
      });
    } finally {
      connection.release();
    }
  },
);

router.delete("/api/admin/revenue/entries/:id", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  await ensureMoneySumTable();

  const entryId = Number(req.params.id);
  if (!Number.isInteger(entryId) || entryId <= 0) {
    return res
      .status(400)
      .json({ message: "Entry id must be a positive integer." });
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [existing] = await connection.query(
      "SELECT id FROM money_sum WHERE id = ? LIMIT 1",
      [entryId],
    );
    if (existing.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: "Revenue entry not found." });
    }

    await connection.query("DELETE FROM money_sum WHERE id = ?", [entryId]);
    await recomputeMoneyProfit(connection);
    await connection.commit();

    return res
      .status(200)
      .json({ message: "Revenue entry removed successfully." });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: "Failed to remove revenue entry.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
});

router.put("/api/admin/resumes/:resId/verified-reason", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const resId = String(req.params.resId || "").trim();
  const verifiedReason =
    req.body?.verified_reason === undefined ||
    req.body?.verified_reason === null
      ? null
      : String(req.body.verified_reason).trim();

  if (!resId) {
    return res.status(400).json({ message: "resId is required." });
  }

  try {
    const hasExtraInfoTable = await tableExists("extra_info");
    if (!hasExtraInfoTable) {
      return res.status(500).json({
        message: "extra_info table is required to update verified reason.",
      });
    }

    const hasVerifiedReasonColumn = await columnExists(
      "extra_info",
      "verified_reason",
    );
    if (!hasVerifiedReasonColumn) {
      return res.status(500).json({
        message: "verified_reason column is required in extra_info table.",
      });
    }

    // Check if the resume exists
    const [resumeExists] = await pool.query(
      "SELECT res_id FROM resumes_data WHERE res_id = ? LIMIT 1",
      [resId],
    );

    if (resumeExists.length === 0) {
      return res.status(404).json({ message: "Resume not found." });
    }

    const updates = ["verified_reason = ?"];
    const hasUpdatedAtColumn = await columnExists("extra_info", "updated_at");
    if (hasUpdatedAtColumn) {
      updates.push("updated_at = CURRENT_TIMESTAMP");
    }

    const [result] = await pool.query(
      `UPDATE extra_info SET ${updates.join(", ")} WHERE res_id = ?`,
      [verifiedReason, resId],
    );

    // If no rows were updated, insert a new record
    if (result.affectedRows === 0) {
      const insertColumns = ["res_id", "resume_id", "verified_reason"];
      const insertValues = [resId, resId, verifiedReason];
      const placeholders = ["?", "?", "?"];

      // Avoid passing CURRENT_TIMESTAMP as a string parameter.
      if (hasUpdatedAtColumn) {
        insertColumns.push("updated_at");
        placeholders.push("CURRENT_TIMESTAMP");
      }

      await pool.query(
        `INSERT INTO extra_info (${insertColumns.join(", ")}) VALUES (${placeholders.join(", ")})`,
        insertValues,
      );
    }

    return res.status(200).json({
      message: "Team leader note updated successfully.",
      resId,
      verifiedReason,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to update team leader note.",
      error: error.message,
    });
  }
});

module.exports = router;
