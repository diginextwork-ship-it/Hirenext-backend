const express = require("express");
const multer = require("multer");
const pool = require("../config/db");
const {
  createAuthToken,
  requireAuth,
  requireRoles,
} = require("../middleware/auth");
const {
  tableExists,
  columnExists,
  getColumnMetadata,
  constraintExists,
  upsertExtraInfoFields,
  upsertCandidateFields,
  addCandidateBillIntakeEntry,
} = require("../utils/dbHelpers");
const {
  toMoneyNumber,
  normalizeJobJid,
  parseJsonField,
  extractCandidateSnapshot,
} = require("../utils/formatters");
const {
  ADMIN_STATUS_TRANSITIONS,
  CANONICAL_VERIFY_STATUS,
  normalizeResumeStatusInput,
  normalizeWorkflowStatus,
} = require("../utils/resumeStatusFlow");
const { parseInclusiveDateRange } = require("../utils/dateTime");

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
const ALLOWED_BILLED_ATTACHMENT_MIME_TYPES = new Set(["application/pdf"]);

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

const billedStatusUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_REVENUE_UPLOAD_BYTES },
  fileFilter: (_req, file, callback) => {
    const mimeType = String(file?.mimetype || "")
      .trim()
      .toLowerCase();
    if (!ALLOWED_BILLED_ATTACHMENT_MIME_TYPES.has(mimeType)) {
      return callback(new Error("Only PDF attachments are allowed."));
    }
    return callback(null, true);
  },
});

const parseAdvanceStatusUpload = (req, res, next) => {
  const contentType = String(req.headers?.["content-type"] || "")
    .trim()
    .toLowerCase();
  if (!contentType.includes("multipart/form-data")) {
    return next();
  }

  billedStatusUpload.single("photo")(req, res, (error) => {
    if (!error) return next();
    if (error?.code === "LIMIT_FILE_SIZE") {
      return res
        .status(400)
        .json({ message: "Billed attachment must be 8MB or smaller." });
    }
    return res.status(400).json({
      message: error?.message || "Invalid billed attachment upload.",
    });
  });
};

const parseAdminAdvanceStatusRequest = (req, res, next) => {
  const contentType = String(req.headers["content-type"] || "").toLowerCase();
  if (!contentType.includes("multipart/form-data")) {
    return next();
  }
  return parseRevenueUpload(req, res, next);
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

router.post("/api/admin/billing/process", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    const { processBillingTransitions } = require("./jobRoutes");
    const result = await processBillingTransitions();
    return res.status(200).json({
      message: `Billing processing complete. ${result.transitioned} candidate(s) transitioned from joined to billed.`,
      transitioned: result.transitioned,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to process billing transitions.",
      error: error.message,
    });
  }
});

router.get("/api/admin/dashboard", async (_req, res) => {
  try {
    let totalResumeCount = 0;
    let candidateResumeCount = 0;
    let recruiterResumeUploads = [];
    if (await tableExists("resumes_data")) {
      const hasJobJidColumn = await columnExists("resumes_data", "job_jid");
      const hasSubmittedByRoleColumn = await columnExists(
        "resumes_data",
        "submitted_by_role",
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
          teamLeader.rid AS teamLeaderRid,
          teamLeader.name AS teamLeaderName,
          c.name AS candidateName,
          c.phone AS candidatePhone,
          j.company_name AS companyName,
          j.city AS city,
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
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        LEFT JOIN jobs j ON j.jid = rd.job_jid
        LEFT JOIN recruiter teamLeader ON teamLeader.rid = j.recruiter_rid
        ORDER BY rd.uploaded_at DESC`,
      );

      recruiterResumeUploads = rows.map((row) => ({
        resId: row.resId || null,
        rid: row.rid || null,
        recruiterName: row.recruiterName || null,
        recruiterEmail: row.recruiterEmail || null,
        jobJid:
          row.jobJid === null || row.jobJid === undefined
            ? null
            : String(row.jobJid).trim(),
        teamLeaderRid: row.teamLeaderRid || null,
        teamLeaderName: row.teamLeaderName || null,
        name: row.candidateName || null,
        candidateName: row.candidateName || null,
        candidatePhone: row.candidatePhone || null,
        phone: row.candidatePhone || null,
        companyName: row.companyName || null,
        city: row.city || null,
        pointsPerJoining:
          row.pointsPerJoining === null || row.pointsPerJoining === undefined
            ? null
            : Number(row.pointsPerJoining),
        revenue:
          row.revenue === null || row.revenue === undefined
            ? null
            : Number(row.revenue),
        resumeFilename: row.resumeFilename || null,
        resumeType: row.resumeType || null,
        isAccepted: Boolean(row.isAccepted),
        acceptedAt: row.acceptedAt || null,
        acceptedByAdmin: row.acceptedByAdmin || null,
        uploadedAt: row.uploadedAt || null,
      }));
    }

    return res.status(200).json({
      totalResumeCount,
      candidateResumeCount,
      recruiterResumeUploads,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch admin dashboard.",
      error: error.message,
    });
  }
});

const getCandidateResumesHandler = async (req, res) => {
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

    const hasCandidateNameColumn = await columnExists("candidate", "name");
    const hasCandidatePhoneColumn = await columnExists("candidate", "phone");
    const hasCandidateEmailColumn = await columnExists("candidate", "email");
    const hasCandidateExperienceColumn = await columnExists(
      "candidate",
      "experience",
    );
    const hasCandidateIndustryColumn = await columnExists(
      "candidate",
      "industry",
    );
    const hasCandidatePrevSalaryColumn = await columnExists(
      "candidate",
      "prev_sal",
    );
    const hasCandidateExpectedSalaryColumn = await columnExists(
      "candidate",
      "expected_sal",
    );
    const hasCandidateNoticePeriodColumn = await columnExists(
      "candidate",
      "notice_period",
    );
    const hasCandidateYearsOfExperienceColumn = await columnExists(
      "candidate",
      "years_of_exp",
    );
    const hasCandidateWalkInColumn = await columnExists("candidate", "walk_in");
    const hasCandidateJoiningDateColumn = await columnExists(
      "candidate",
      "joining_date",
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
    const hasSelectionTable = await tableExists("job_resume_selection");
    const hasSelectionJobJidColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "job_jid"));
    const hasSelectionResIdColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "res_id"));
    const hasSelectionStatusColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "selection_status"));
    const hasSelectionNoteColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "selection_note"));
    const hasSelectedByAdminColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "selected_by_admin"));
    const hasSelectedAtColumn =
      hasSelectionTable &&
      (await columnExists("job_resume_selection", "selected_at"));
    const hasSelectionJoin =
      hasSelectionJobJidColumn && hasSelectionResIdColumn;
    const hasExtraInfoTable = await tableExists("extra_info");
    const hasSubmittedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "submitted_reason"));
    const hasVerifiedReasonColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "verified_reason"));
    const hasJoinedReasonColumn =
      hasExtraInfoTable && (await columnExists("extra_info", "joined_reason"));
    const applicantNameSelect = hasCandidateNameColumn
      ? "c.name AS applicantName,"
      : "NULL AS applicantName,";
    const applicantPhoneSelect = hasCandidatePhoneColumn
      ? "c.phone AS applicantPhone,"
      : "NULL AS applicantPhone,";
    const priorExperienceSelect = hasCandidateExperienceColumn
      ? "c.experience AS hasPriorExperience,"
      : "NULL AS hasPriorExperience,";
    const experienceIndustrySelect = hasCandidateIndustryColumn
      ? "c.industry AS experienceIndustry,"
      : "NULL AS experienceIndustry,";
    const experienceIndustryOtherSelect = "NULL AS experienceIndustryOther,";
    const currentSalarySelect = hasCandidatePrevSalaryColumn
      ? "c.prev_sal AS currentSalary,"
      : "NULL AS currentSalary,";
    const expectedSalarySelect = hasCandidateExpectedSalaryColumn
      ? "c.expected_sal AS expectedSalary,"
      : "NULL AS expectedSalary,";
    const noticePeriodSelect = hasCandidateNoticePeriodColumn
      ? "c.notice_period AS noticePeriod,"
      : "NULL AS noticePeriod,";
    const yearsOfExperienceSelect = hasCandidateYearsOfExperienceColumn
      ? "c.years_of_exp AS yearsOfExperience,"
      : "NULL AS yearsOfExperience,";
    const applicantEmailSelect = hasCandidateEmailColumn
      ? "c.email AS applicantEmail,"
      : "NULL AS applicantEmail,";
    const atsScoreSelect = hasAtsScoreColumn
      ? "rd.ats_score AS atsScore,"
      : "NULL AS atsScore,";
    const atsMatchSelect = hasAtsMatchColumn
      ? "rd.ats_match_percentage AS atsMatchPercentage,"
      : "NULL AS atsMatchPercentage,";
    const walkInDateSelect = hasCandidateWalkInColumn
      ? "c.walk_in AS walkInDate,"
      : "NULL AS walkInDate,";
    const resumeJoiningDateSelect = hasCandidateJoiningDateColumn
      ? "c.joining_date AS resumeJoiningDate,"
      : "NULL AS resumeJoiningDate,";
    const jobDescriptionSelect = hasJobDescriptionColumn
      ? "j.job_description AS jobDescription,"
      : "NULL AS jobDescription,";
    const selectionSelect = hasSelectionJoin
      ? `${hasSelectionStatusColumn ? "jrs.selection_status AS selectionStatus," : "NULL AS selectionStatus,"}
        ${hasSelectionNoteColumn ? "jrs.selection_note AS selectionNote," : "NULL AS selectionNote,"}
        ${hasSelectedByAdminColumn ? "jrs.selected_by_admin AS selectedByAdmin," : "NULL AS selectedByAdmin,"}
        ${hasSelectedAtColumn ? "jrs.selected_at AS selectedAt," : "NULL AS selectedAt,"}
        ${walkInDateSelect}
        ${resumeJoiningDateSelect}
        ${hasCandidateJoiningDateColumn ? "c.joining_date AS joiningDate," : "NULL AS joiningDate,"}
        ${hasJoinedReasonColumn ? "ei.joined_reason AS joinedReason" : "NULL AS joinedReason"}`
      : `NULL AS selectionStatus,
        NULL AS selectionNote,
        NULL AS selectedByAdmin,
        NULL AS selectedAt,
        ${walkInDateSelect}
        ${resumeJoiningDateSelect}
        NULL AS joiningDate,
        NULL AS joinedReason`;
    const selectionJoin = hasSelectionJoin
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
        ${applicantPhoneSelect}
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
        j.city AS city,
        ${jobDescriptionSelect}
        j.skills AS skills,
        ${submittedReasonSelect}
        ${verifiedReasonSelect}
        ${selectionSelect}
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
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
        applicantPhone: row.applicantPhone || null,
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
          city: row.city || null,
          jobDescription: row.jobDescription || null,
          skills: row.skills || null,
        },
        selection: row.selectionStatus
          ? {
              status: row.selectionStatus,
              note: row.selectionNote || null,
              selectedByAdmin: row.selectedByAdmin || null,
              selectedAt: row.selectedAt || null,
              walkInDate: row.walkInDate || null,
              resumeJoiningDate: row.resumeJoiningDate || null,
              joiningDate: row.joiningDate || null,
              joinedReason: row.joinedReason || null,
              joiningNote: row.joinedReason || null,
            }
          : null,
      })),
    });
  } catch (error) {
    console.error("GET /api/admin/candidate-resumes error:", error);
    return res.status(500).json({
      message: "Failed to fetch candidate submitted resumes.",
      error: error.message,
    });
  }
};

router.get("/api/admin/candidate-resumes", getCandidateResumesHandler);
router.get(
  "/api/admin/candidate-submitted-resumes",
  getCandidateResumesHandler,
);

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
        city AS city,
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
    const hasJoinedReasonColumn =
      hasExtraInfoTable && (await columnExists("extra_info", "joined_reason"));

    const atsScoreSelect = hasAtsScoreColumn
      ? "rd.ats_score AS atsScore,"
      : "NULL AS atsScore,";
    const atsMatchSelect = hasAtsMatchColumn
      ? "rd.ats_match_percentage AS atsMatchPercentage,"
      : "NULL AS atsMatchPercentage,";
    const walkInDateSelect = "c.walk_in AS walkInDate,";
    const resumeJoiningDateSelect = "c.joining_date AS resumeJoiningDate,";
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
        rd.ats_raw_json AS atsRawJson,
        c.name AS candidateName,
        c.phone AS candidatePhone,
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
        jrs.selected_at AS selectedAt,
        ${walkInDateSelect}
        ${resumeJoiningDateSelect}
        c.joining_date AS joiningDate,
        ${hasJoinedReasonColumn ? "ei.joined_reason AS joinedReason" : "NULL AS joinedReason"}
      FROM resumes_data rd
      INNER JOIN recruiter r ON r.rid = rd.rid
      LEFT JOIN candidate c ON c.res_id = rd.res_id
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
        city: jobs[0].city || null,
        roleName: jobs[0].roleName,
        positionsOpen: Number(jobs[0].positionsOpen) || 1,
      },
      resumes: rows.map((row) => ({
        ...(() => {
          const parsedResumePayload = parseJsonField(row.atsRawJson);
          const candidateSnapshot = extractCandidateSnapshot({
            source: {
              candidate_name: row.candidateName,
              candidate_phone: row.candidatePhone,
              recruiter_rid: row.rid,
            },
            parsedData:
              parsedResumePayload?.parsed_data ||
              parsedResumePayload?.parsedData ||
              parsedResumePayload,
            fallback: {
              jobJid: safeJobId,
              recruiterRid: row.rid,
            },
          });

          return {
            resId: row.resId,
            rid: row.rid,
            name: candidateSnapshot.name || row.candidateName || null,
            candidateName:
              candidateSnapshot.name || row.candidateName || null,
            candidatePhone:
              candidateSnapshot.phone || row.candidatePhone || null,
            phone: candidateSnapshot.phone || row.candidatePhone || null,
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
                  walkInDate: row.walkInDate || null,
                  resumeJoiningDate: row.resumeJoiningDate || null,
                  joiningDate: row.joiningDate || null,
                  joinedReason: row.joinedReason || null,
                  joiningNote: row.joinedReason || null,
                }
              : null,
          };
        })(),
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

// ─── Advance Resume Workflow Status ─────────────────────────────────────────────

const VALID_STATUS_TRANSITIONS = ADMIN_STATUS_TRANSITIONS;
const ADMIN_ROLLBACK_TARGETS = {
  verified: "submitted",
  walk_in: "verified",
  selected: "walk_in",
  pending_joining: "selected",
  joined: "pending_joining",
};

const PERFORMANCE_EVENT_KEYS = [
  "submitted",
  "verified",
  "walk_in",
  "selected",
  "rejected",
  "pending_joining",
  "joined",
  "dropout",
  "billed",
  "left",
];

const PERFORMANCE_EVENT_META = {
  submitted: { recruiterField: "submitted", summaryField: "totalSubmitted" },
  verified: { recruiterField: "verified", summaryField: "totalVerified" },
  walk_in: { recruiterField: "walk_in", summaryField: "totalWalkIn" },
  selected: { recruiterField: "selected", summaryField: "totalSelected" },
  rejected: { recruiterField: "rejected", summaryField: "totalRejected" },
  pending_joining: {
    recruiterField: "pending_joining",
    summaryField: "totalPendingJoining",
  },
  joined: { recruiterField: "joined", summaryField: "totalJoined" },
  dropout: { recruiterField: "dropout", summaryField: "totalDropout" },
  billed: { recruiterField: "billed", summaryField: "totalBilled" },
  left: { recruiterField: "left", summaryField: "totalLeft" },
};

const normalizePerformanceTimestamp = (value) =>
  value == null ? null : String(value).trim() || null;

const isTimestampWithinInclusiveRange = (value, range) => {
  if (!value) return false;
  if (!range?.hasDateRange) return true;
  return value >= range.startDateTime && value <= range.endDateTime;
};

const getAdminRollbackTarget = (resume) => {
  const currentStatus = normalizeWorkflowStatus(resume.currentStatus);
  const derivedStatus =
    currentStatus === "selected" && resume.currentJoiningDate
      ? "pending_joining"
      : currentStatus;

  if (derivedStatus === "rejected") {
    return resume.currentWalkInDate ? "walk_in" : CANONICAL_VERIFY_STATUS;
  }

  return ADMIN_ROLLBACK_TARGETS[derivedStatus] || "";
};

router.post(
  "/api/admin/resumes/:resId/advance-status",
  parseAdminAdvanceStatusRequest,
  async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const normalizedResId = String(req.params.resId || "").trim();
  if (!normalizedResId) {
    return res.status(400).json({ message: "resId is required." });
  }

  const newStatus = normalizeResumeStatusInput(req.body?.status);
  const rawReasonSource =
    req.body?.reason ??
    req.body?.note ??
    (newStatus === CANONICAL_VERIFY_STATUS
      ? (req.body?.verifiedReason ?? req.body?.verified_reason)
      : undefined);
  const reason =
    rawReasonSource === undefined || rawReasonSource === null
      ? ""
      : String(rawReasonSource).trim();
  const joiningDate = String(req.body?.joining_date || "").trim();
  const joinedReasonSource =
    req.body?.joinedReason ??
    req.body?.joined_reason ??
    req.body?.joiningNote ??
    req.body?.joining_note;
  const joinedReason =
    joinedReasonSource === undefined || joinedReasonSource === null
      ? null
      : String(joinedReasonSource).trim();
  const rawRevenueSource =
    req.body?.revenue ??
    req.body?.revenue_amount ??
    req.body?.revenueAmount ??
    req.body?.pending_revenue ??
    req.body?.pendingRevenue ??
    null;
  const rawRevenue =
    rawRevenueSource === undefined || rawRevenueSource === null
      ? ""
      : String(rawRevenueSource).trim();

  const allowedNewStatuses = new Set([
    "verified",
    "walk_in",
    "further",
    "selected",
    "pending_joining",
    "rejected",
    "joined",
    "dropout",
    "billed",
    "left",
  ]);
  if (!allowedNewStatuses.has(newStatus)) {
    return res.status(400).json({ message: "Invalid target status." });
  }

  const shouldCaptureReason = newStatus !== "pending_joining";
  const effectiveReason = shouldCaptureReason ? reason : "";

  const joiningDateRequiredStatuses = new Set(["pending_joining"]);

  if (joiningDateRequiredStatuses.has(newStatus)) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate)) {
      return res.status(400).json({
        message: "joining_date is required in YYYY-MM-DD format.",
      });
    }
  } else if (newStatus === "selected" && joiningDate) {
    return res.status(400).json({
      message: "joining_date should only be provided for pending_joining.",
    });
  } else if (joiningDate && !/^\d{4}-\d{2}-\d{2}$/.test(joiningDate)) {
    return res.status(400).json({
      message: "joining_date must be in YYYY-MM-DD format.",
    });
  }

  const reasonRequiredStatuses = new Set(["rejected", "dropout", "left"]);
  if (reasonRequiredStatuses.has(newStatus) && !reason) {
    return res
      .status(400)
      .json({ message: "reason is required for this status transition." });
  }

  const revenueRequiredStatuses = new Set(["billed"]);
  const shouldValidateRevenue = revenueRequiredStatuses.has(newStatus);
  const parsedRevenue = shouldValidateRevenue
    ? rawRevenue === ""
      ? null
      : Number.parseInt(rawRevenue, 10)
    : undefined;

  if (shouldValidateRevenue && rawRevenue !== "" && !Number.isFinite(parsedRevenue)) {
    return res.status(400).json({
      message: "revenue must be a valid non-negative number.",
    });
  }
  if (shouldValidateRevenue && parsedRevenue !== null && parsedRevenue < 0) {
    return res.status(400).json({
      message: "revenue must be a valid non-negative number.",
    });
  }
  if (newStatus === "billed" && parsedRevenue === null) {
    return res.status(400).json({
      message: "revenue is required for billed status.",
    });
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    // Fetch the resume and its current workflow status
    const [resumeRows] = await connection.query(
      `SELECT
        rd.res_id AS resId,
        rd.rid,
        rd.job_jid AS jobJid,
        rd.ats_raw_json AS atsRawJson,
        c.name AS candidateName,
        c.email AS candidateEmail,
        c.joining_date AS currentJoiningDate,
        COALESCE(jrs.selection_status, '') AS currentStatus
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
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
    const parsedResumePayload = parseJsonField(resume.atsRawJson);
    const adminStatusCandidateSnapshot = extractCandidateSnapshot({
      source: {
        candidate_name:
          req.body?.candidate_name ??
          req.body?.candidateName ??
          resume.candidateName,
        candidate_email:
          req.body?.candidate_email ??
          req.body?.candidateEmail ??
          resume.candidateEmail,
        candidate_phone: req.body?.candidate_phone ?? req.body?.candidatePhone,
        job_jid: resume.jobJid,
        recruiter_rid: resume.rid,
      },
      parsedData:
        parsedResumePayload?.parsed_data ||
        parsedResumePayload?.parsedData ||
        parsedResumePayload,
      fallback: {
        jobJid: resume.jobJid,
        recruiterRid: resume.rid,
      },
    });
    const currentStatus = normalizeWorkflowStatus(resume.currentStatus);
    const currentDerivedStatus =
      currentStatus === "selected" && resume.currentJoiningDate
        ? "pending_joining"
        : currentStatus;

    // Validate transition
    const allowedTransitions = VALID_STATUS_TRANSITIONS[currentDerivedStatus];
    if (!allowedTransitions || !allowedTransitions.has(newStatus)) {
      await connection.rollback();
      return res.status(400).json({
        message: `Invalid status transition from '${currentDerivedStatus}' to '${newStatus}'.`,
      });
    }

    if (newStatus === "billed") {
      if (!req.file) {
        await connection.rollback();
        return res.status(400).json({
          message: "photo PDF attachment is required for billed status.",
        });
      }

      const billedMimeType = String(req.file.mimetype || "")
        .trim()
        .toLowerCase();
      if (billedMimeType !== "application/pdf") {
        await connection.rollback();
        return res.status(400).json({
          message: "Only PDF attachments are allowed for billed status.",
        });
      }
    }

    const persistedStatus =
      newStatus === "pending_joining" ? "selected" : newStatus;
    const effectiveJoiningDate = /^\d{4}-\d{2}-\d{2}$/.test(joiningDate)
      ? joiningDate
      : resume.currentJoiningDate || null;
    const joiningDateValue =
      newStatus === "joined" ? effectiveJoiningDate : null;

    if (resume.jobJid) {
      await connection.query(
        `INSERT INTO job_resume_selection
          (job_jid, res_id, selected_by_admin, selection_status, selection_note)
        VALUES (?, ?, 'admin-panel', ?, ?)
        ON DUPLICATE KEY UPDATE
          selection_status = VALUES(selection_status),
          selection_note = VALUES(selection_note),
          selected_at = CURRENT_TIMESTAMP`,
        [
          resume.jobJid,
          normalizedResId,
          persistedStatus,
          effectiveReason || null,
        ],
      );
    }

    let candidateJoiningDateValue = resume.currentJoiningDate || null;
    if (newStatus === "selected") {
      candidateJoiningDateValue = null;
    } else if (newStatus === "pending_joining") {
      candidateJoiningDateValue = joiningDate;
    } else if (newStatus === "joined" && effectiveJoiningDate) {
      candidateJoiningDateValue = effectiveJoiningDate;
    }

    await upsertCandidateFields(connection, {
      resId: normalizedResId,
      cid: undefined,
      jobJid: resume.jobJid || undefined,
      recruiterRid: resume.rid || undefined,
      name: adminStatusCandidateSnapshot.name || undefined,
      phone: adminStatusCandidateSnapshot.phone || undefined,
      email: adminStatusCandidateSnapshot.email || undefined,
      levelOfEdu: adminStatusCandidateSnapshot.levelOfEdu || undefined,
      boardUni: adminStatusCandidateSnapshot.boardUni || undefined,
      institutionName:
        adminStatusCandidateSnapshot.institutionName || undefined,
      age: adminStatusCandidateSnapshot.age,
      joiningDate: candidateJoiningDateValue,
      revenue: newStatus === "billed" ? parsedRevenue : undefined,
    });

    const statusReasonFieldMap = {
      verified: "verifiedReason",
      walk_in: "walkInReason",
      further: "furtherReason",
      selected: "selectReason",
      rejected: "rejectReason",
      joined: "joinedReason",
      dropout: "dropoutReason",
      billed: "billedReason",
      left: "leftReason",
    };
    const reasonField = statusReasonFieldMap[newStatus];
    const statusReasonValue =
      newStatus === "joined" ? joinedReason : effectiveReason || null;
    const statusTimestampFieldMap = {
      verified: "verifiedAt",
      walk_in: "walkInAt",
      further: "furtherAt",
      selected: "selectedAt",
      pending_joining: "pendingJoiningAt",
      joined: "joinedAt",
      rejected: "rejectedAt",
      dropout: "dropoutAt",
      billed: "billedAt",
      left: "leftAt",
    };

    if (reasonField) {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        [reasonField]: statusReasonValue,
        [statusTimestampFieldMap[newStatus]]: "__CURRENT_TIMESTAMP__",
      });
    } else if (statusTimestampFieldMap[newStatus]) {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        [statusTimestampFieldMap[newStatus]]: "__CURRENT_TIMESTAMP__",
      });
    }

    // Credit points_per_joining to the recruiter when candidate reaches billed status
    if (newStatus === "billed") {
      // Credit points_per_joining to the recruiter when candidate reaches billed status
      if (resume.rid && resume.jobJid) {
        const [jobPtsRows] = await connection.query(
          "SELECT COALESCE(points_per_joining, 0) AS pts FROM jobs WHERE jid = ? LIMIT 1",
          [resume.jobJid],
        );
        const pts = Number(jobPtsRows?.[0]?.pts) || 0;
        if (pts > 0) {
          await connection.query(
            "UPDATE recruiter SET points = COALESCE(points, 0) + ? WHERE rid = ?",
            [pts, resume.rid],
          );
          await connection.query(
            `INSERT INTO recruiter_points_log (recruiter_rid, job_jid, res_id, points, reason)
             VALUES (?, ?, ?, ?, 'billed')`,
            [resume.rid, resume.jobJid, normalizedResId, pts],
          );
        }
      }

      const intakeEntry = await addCandidateBillIntakeEntry(
        connection,
        normalizedResId,
        {
          reason: effectiveReason || "candidate's bill",
          photo: toRevenueAttachmentDataUrl(req.file) || null,
        },
      );
      if (!intakeEntry) {
        throw new Error(
          "Failed to create billed intake entry in money_sum for this candidate.",
        );
      }
    }

    await connection.commit();
    return res.status(200).json({
      message: "Status updated successfully.",
      data: {
        resId: normalizedResId,
        status: newStatus,
        reason: newStatus === "joined" ? joinedReason : effectiveReason || null,
        verifiedReason:
          newStatus === CANONICAL_VERIFY_STATUS
            ? effectiveReason || null
            : null,
        joining_date: joiningDateValue,
        joinedReason: newStatus === "joined" ? joinedReason : null,
        joiningNote: newStatus === "joined" ? joinedReason : null,
      },
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: "Failed to advance resume status.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
});

router.post("/api/admin/resumes/:resId/rollback-status", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const normalizedResId = String(req.params.resId || "").trim();
  if (!normalizedResId) {
    return res.status(400).json({ message: "resId is required." });
  }

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [resumeRows] = await connection.query(
      `SELECT
        rd.res_id AS resId,
        rd.rid,
        rd.job_jid AS jobJid,
        COALESCE(jrs.selection_status, 'pending') AS currentStatus,
        c.joining_date AS currentJoiningDate,
        c.walk_in AS currentWalkInDate
      FROM resumes_data rd
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
      LEFT JOIN candidate c ON c.res_id = rd.res_id
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
    const currentStatus = normalizeWorkflowStatus(resume.currentStatus);
    const currentDerivedStatus =
      currentStatus === "selected" && resume.currentJoiningDate
        ? "pending_joining"
        : currentStatus;
    const rollbackTarget = getAdminRollbackTarget(resume);

    if (!rollbackTarget) {
      await connection.rollback();
      return res.status(400).json({
        message: `Rollback is not supported for '${currentDerivedStatus}'.`,
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
      const persistedStatus =
        rollbackTarget === "pending_joining" ? "selected" : rollbackTarget;
      await connection.query(
        `INSERT INTO job_resume_selection
          (job_jid, res_id, selected_by_admin, selection_status, selection_note)
         VALUES (?, ?, 'admin-rollback', ?, NULL)
         ON DUPLICATE KEY UPDATE
           selected_by_admin = VALUES(selected_by_admin),
           selection_status = VALUES(selection_status),
           selection_note = VALUES(selection_note),
           selected_at = CURRENT_TIMESTAMP`,
        [resume.jobJid, normalizedResId, persistedStatus],
      );
    }

    if (currentDerivedStatus === "verified") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        verifiedReason: null,
      });
    }

    if (currentDerivedStatus === "walk_in") {
      await upsertCandidateFields(connection, {
        resId: normalizedResId,
        walkIn: null,
      });
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        walkInReason: null,
      });
    }

    if (currentDerivedStatus === "selected") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        selectReason: null,
      });
    }

    if (currentDerivedStatus === "rejected") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        rejectReason: null,
      });
    }

    if (currentDerivedStatus === "pending_joining") {
      await upsertCandidateFields(connection, {
        resId: normalizedResId,
        joiningDate: null,
        revenue: null,
      });
    }

    if (currentDerivedStatus === "joined") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        joinedReason: null,
      });
    }

    await connection.commit();
    return res.status(200).json({
      message: "Resume rolled back successfully.",
      data: {
        resId: normalizedResId,
        previousStatus: currentDerivedStatus,
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

// ─── Admin Performance Dashboard ───────────────────────────────────────────────
router.get("/api/admin/resumes/:resId/file", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const resId = String(req.params.resId || "").trim();
  if (!resId) {
    return res.status(400).json({ message: "resId is required." });
  }

  try {
    const [rows] = await pool.query(
      `SELECT
        resume,
        resume_filename AS resumeFilename,
        resume_type AS resumeType
      FROM resumes_data
      WHERE res_id = ?
      LIMIT 1`,
      [resId],
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
});

router.get("/api/admin/performance", async (req, res) => {
  try {
    const dateRange = parseInclusiveDateRange(
      req.query?.startDate,
      req.query?.endDate,
    );
    if (dateRange.error) {
      return res.status(400).json({ message: dateRange.error });
    }

    const teamLeaderDateClause = dateRange.hasDateRange
      ? "WHERE j.created_at >= ? AND j.created_at <= ?"
      : "";
    const teamLeaderQueryParams = dateRange.hasDateRange
      ? [dateRange.startDateTime, dateRange.endDateTime]
      : [];

    // ── Team Leaders: jobs created ──────────────────────────────────────────
    const [teamLeaderRows] = await pool.query(
      `SELECT
        r.rid,
        r.name,
        r.email,
        COALESCE(r.points, 0) AS points,
        COALESCE(jc.jobs_created, 0) AS jobsCreated,
        COALESCE(jc.total_positions, 0) AS totalPositions,
        COALESCE(jc.open_jobs, 0) AS openJobs,
        COALESCE(jc.restricted_jobs, 0) AS restrictedJobs
      FROM recruiter r
      LEFT JOIN (
        SELECT
          j.recruiter_rid,
          COUNT(*) AS jobs_created,
          COALESCE(SUM(j.positions_open), 0) AS total_positions,
          SUM(CASE WHEN j.access_mode = 'open' THEN 1 ELSE 0 END) AS open_jobs,
          SUM(CASE WHEN j.access_mode = 'restricted' THEN 1 ELSE 0 END) AS restricted_jobs
        FROM jobs j
        ${teamLeaderDateClause}
        GROUP BY j.recruiter_rid
      ) jc ON jc.recruiter_rid = r.rid
      WHERE LOWER(TRIM(COALESCE(r.role, 'recruiter'))) IN ('team leader', 'team_leader', 'job creator')
      ORDER BY COALESCE(jc.jobs_created, 0) DESC, r.name ASC`,
      teamLeaderQueryParams,
    );

    const teamLeaders = teamLeaderRows.map((row) => ({
      rid: row.rid,
      name: row.name,
      email: row.email,
      points: Number(row.points) || 0,
      jobsCreated: Number(row.jobsCreated) || 0,
      totalPositions: Number(row.totalPositions) || 0,
      openJobs: Number(row.openJobs) || 0,
      restrictedJobs: Number(row.restrictedJobs) || 0,
    }));

    const [recruiterBaseRows] = await pool.query(
      `SELECT
        r.rid,
        r.name,
        r.email,
        COALESCE(r.points, 0) AS points
      FROM recruiter r
      WHERE LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'
      ORDER BY r.name ASC`,
    );

    const [performanceRows] = await pool.query(
      `SELECT
        rd.res_id AS resId,
        rd.job_jid AS jobJid,
        rd.resume_filename AS resumeFilename,
        DATE_FORMAT(rd.uploaded_at, '%Y-%m-%d %H:%i:%s.%f') AS submittedAt,
        DATE_FORMAT(
          COALESCE(
            ei.verified_at,
            CASE WHEN jrs.selection_status = 'verified' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS verifiedAt,
        DATE_FORMAT(
          COALESCE(
            ei.walk_in_at,
            CASE
              WHEN c.walk_in IS NOT NULL THEN CAST(CONCAT(c.walk_in, ' 00:00:00.000000') AS DATETIME(6))
              WHEN jrs.selection_status = 'walk_in' THEN jrs.selected_at
              ELSE NULL
            END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS walkInAt,
        DATE_FORMAT(
          COALESCE(
            ei.selected_at,
            CASE
              WHEN jrs.selection_status = 'selected' AND c.joining_date IS NULL THEN jrs.selected_at
              ELSE NULL
            END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS selectedAt,
        DATE_FORMAT(
          COALESCE(
            ei.rejected_at,
            CASE WHEN jrs.selection_status = 'rejected' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS rejectedAt,
        DATE_FORMAT(
          COALESCE(
            ei.pending_joining_at,
            CASE
              WHEN jrs.selection_status = 'selected' AND c.joining_date IS NOT NULL
                THEN CAST(CONCAT(c.joining_date, ' 00:00:00.000000') AS DATETIME(6))
              ELSE NULL
            END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS pendingJoiningAt,
        DATE_FORMAT(
          COALESCE(
            ei.joined_at,
            CASE WHEN jrs.selection_status = 'joined' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS joinedAt,
        DATE_FORMAT(
          COALESCE(
            ei.dropout_at,
            CASE WHEN jrs.selection_status = 'dropout' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS dropoutAt,
        DATE_FORMAT(
          COALESCE(
            ei.billed_at,
            CASE WHEN jrs.selection_status = 'billed' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS billedAt,
        DATE_FORMAT(
          COALESCE(
            ei.left_at,
            CASE WHEN jrs.selection_status = 'left' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS leftAt,
        DATE_FORMAT(
          COALESCE(
            ei.further_at,
            CASE WHEN jrs.selection_status = 'further' THEN jrs.selected_at ELSE NULL END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS furtherAt,
        DATE_FORMAT(
          CASE WHEN jrs.selection_status = 'on_hold' THEN jrs.selected_at ELSE NULL END,
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS onHoldAt,
        DATE_FORMAT(c.walk_in, '%Y-%m-%d') AS walkInDate,
        DATE_FORMAT(c.joining_date, '%Y-%m-%d') AS joiningDate,
        recruiter.rid AS recruiterRid,
        recruiter.name AS recruiterName,
        teamLeader.name AS teamLeaderName,
        c.name AS candidateName,
        c.phone AS candidatePhone,
        j.company_name AS companyName,
        j.city AS city
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
      INNER JOIN recruiter recruiter ON recruiter.rid = rd.rid
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
      LEFT JOIN jobs j ON j.jid = rd.job_jid
      LEFT JOIN recruiter teamLeader ON teamLeader.rid = j.recruiter_rid
      LEFT JOIN extra_info ei
        ON ei.res_id = rd.res_id OR ei.resume_id = rd.res_id
      WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
      ORDER BY rd.uploaded_at DESC, rd.res_id DESC`,
    );

    const normalizePerformanceDrilldownItem = (row, statusKey) => ({
      resId: row.resId || null,
      recruiterName: row.recruiterName || null,
      recruiterRid: row.recruiterRid || null,
      teamLeaderName: row.teamLeaderName || null,
      name: row.candidateName || null,
      candidatePhone: row.candidatePhone || null,
      phone: row.candidatePhone || null,
      jobJid:
        row.jobJid === null || row.jobJid === undefined
          ? null
          : String(row.jobJid).trim(),
      companyName: row.companyName || null,
      city: row.city || null,
      status: statusKey,
      resumeFilename: row.resumeFilename || null,
      candidateName: row.candidateName || null,
      walkInDate: row.walkInDate || null,
      joiningDate: row.joiningDate || null,
      eventAt: row.eventAt || null,
    });

    const statusDrilldown = {
      submitted: [],
      verified: [],
      walk_in: [],
      selected: [],
      pending_joining: [],
      joined: [],
      dropout: [],
      rejected: [],
      billed: [],
      left: [],
    };

    const recruiterMap = new Map(
      recruiterBaseRows.map((row) => [
        row.rid,
        {
          rid: row.rid,
          name: row.name,
          email: row.email,
          points: Number(row.points) || 0,
          submitted: 0,
          verified: 0,
          walk_in: 0,
          further: 0,
          selected: 0,
          pending_joining: 0,
          rejected: 0,
          joined: 0,
          dropout: 0,
          billed: 0,
          left: 0,
          on_hold: 0,
          lastUpdated: null,
        },
      ]),
    );

    for (const rawRow of performanceRows) {
      const recruiterStats = recruiterMap.get(rawRow.recruiterRid);
      if (!recruiterStats) continue;

      const row = {
        ...rawRow,
        submittedAt: normalizePerformanceTimestamp(rawRow.submittedAt),
        verifiedAt: normalizePerformanceTimestamp(rawRow.verifiedAt),
        walkInAt: normalizePerformanceTimestamp(rawRow.walkInAt),
        selectedAt: normalizePerformanceTimestamp(rawRow.selectedAt),
        rejectedAt: normalizePerformanceTimestamp(rawRow.rejectedAt),
        pendingJoiningAt: normalizePerformanceTimestamp(rawRow.pendingJoiningAt),
        joinedAt: normalizePerformanceTimestamp(rawRow.joinedAt),
        dropoutAt: normalizePerformanceTimestamp(rawRow.dropoutAt),
        billedAt: normalizePerformanceTimestamp(rawRow.billedAt),
        leftAt: normalizePerformanceTimestamp(rawRow.leftAt),
        furtherAt: normalizePerformanceTimestamp(rawRow.furtherAt),
        onHoldAt: normalizePerformanceTimestamp(rawRow.onHoldAt),
      };

      const eventMap = {
        submitted: row.submittedAt,
        verified: row.verifiedAt,
        walk_in: row.walkInAt,
        selected: row.selectedAt,
        rejected: row.rejectedAt,
        pending_joining: row.pendingJoiningAt,
        joined: row.joinedAt,
        dropout: row.dropoutAt,
        billed: row.billedAt,
        left: row.leftAt,
      };

      for (const metricKey of PERFORMANCE_EVENT_KEYS) {
        const eventAt = eventMap[metricKey];
        if (!isTimestampWithinInclusiveRange(eventAt, dateRange)) continue;
        recruiterStats[PERFORMANCE_EVENT_META[metricKey].recruiterField] += 1;
        row.eventAt = eventAt;
        statusDrilldown[metricKey].push(
          normalizePerformanceDrilldownItem(row, metricKey),
        );
        if (!recruiterStats.lastUpdated || eventAt > recruiterStats.lastUpdated) {
          recruiterStats.lastUpdated = eventAt;
        }
      }

      if (isTimestampWithinInclusiveRange(row.furtherAt, dateRange)) {
        recruiterStats.further += 1;
        if (!recruiterStats.lastUpdated || row.furtherAt > recruiterStats.lastUpdated) {
          recruiterStats.lastUpdated = row.furtherAt;
        }
      }

      if (isTimestampWithinInclusiveRange(row.onHoldAt, dateRange)) {
        recruiterStats.on_hold += 1;
        if (!recruiterStats.lastUpdated || row.onHoldAt > recruiterStats.lastUpdated) {
          recruiterStats.lastUpdated = row.onHoldAt;
        }
      }
    }

    const recruiters = Array.from(recruiterMap.values())
      .map((row) => {
        const submitted = row.submitted;
        const verified = row.verified;
        const selected = row.selected;
        const pendingJoining = row.pending_joining;
        const joined = row.joined;
        const dropout = row.dropout;
        const billed = row.billed;
        const left = row.left;

        return {
          ...row,
          verificationRate:
            submitted > 0
              ? Number(((verified / submitted) * 100).toFixed(1))
              : 0,
          selectionRate:
            verified > 0
              ? Number((((selected + pendingJoining) / verified) * 100).toFixed(1))
              : 0,
          joiningRate:
            pendingJoining > 0
              ? Number(((joined / pendingJoining) * 100).toFixed(1))
              : 0,
          dropoutRate:
            pendingJoining > 0
              ? Number(((dropout / pendingJoining) * 100).toFixed(1))
              : 0,
          billingRate:
            joined > 0 ? Number(((billed / joined) * 100).toFixed(1)) : 0,
          leftRate:
            billed > 0 ? Number(((left / billed) * 100).toFixed(1)) : 0,
        };
      })
      .sort((a, b) => b.submitted - a.submitted || a.name.localeCompare(b.name));

    // ── Summary totals ──────────────────────────────────────────────────────
    const summary = {
      totalTeamLeaders: teamLeaders.length,
      totalRecruiters: recruiters.length,
      totalJobsCreated: teamLeaders.reduce((sum, tl) => sum + tl.jobsCreated, 0),
      totalPositions: teamLeaders.reduce((sum, tl) => sum + tl.totalPositions, 0),
      totalSubmitted: 0,
      totalVerified: 0,
      totalWalkIn: 0,
      totalSelected: 0,
      totalPendingJoining: 0,
      totalJoined: 0,
      totalDropout: 0,
      totalRejected: 0,
      totalBilled: 0,
      totalLeft: 0,
    };

    for (const recruiter of recruiters) {
      for (const metricKey of PERFORMANCE_EVENT_KEYS) {
        summary[PERFORMANCE_EVENT_META[metricKey].summaryField] +=
          recruiter[PERFORMANCE_EVENT_META[metricKey].recruiterField];
      }
    }

    return res.status(200).json({
      teamLeaders,
      recruiters,
      summary,
      statusDrilldown,
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch performance data.",
      error: error.message,
    });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// DELETE /api/admin/recruiters/:rid — Cascade-delete recruiter & all data
// ═══════════════════════════════════════════════════════════════════════════
router.delete(
  "/api/admin/recruiters/:rid",
  requireAuth,
  requireRoles("admin"),
  async (req, res) => {
    const rid = String(req.params.rid || "").trim();
    if (!rid) {
      return res.status(400).json({ message: "Recruiter RID is required." });
    }

    let conn;
    try {
      conn = await pool.getConnection();
      await conn.beginTransaction();

      // Verify recruiter exists
      const [[recruiter]] = await conn.query(
        "SELECT rid, role FROM recruiter WHERE rid = ?",
        [rid],
      );
      if (!recruiter) {
        conn.release();
        return res.status(404).json({ message: `Recruiter ${rid} not found.` });
      }

      // 1. Collect money_sum IDs tied to this recruiter's attendance rows
      const [attendanceRows] = await conn.query(
        "SELECT money_sum_id FROM recruiter_attendance WHERE recruiter_rid = ? AND money_sum_id IS NOT NULL",
        [rid],
      );
      const attendanceMoneySumIds = attendanceRows.map((r) => r.money_sum_id);

      // 2. Collect money_sum IDs tied to this recruiter's reimbursements
      const [reimbursementRows] = await conn.query(
        "SELECT money_sum_id FROM reimbursements WHERE rid = ? AND money_sum_id IS NOT NULL",
        [rid],
      );
      const reimbursementMoneySumIds = reimbursementRows.map(
        (r) => r.money_sum_id,
      );

      // 3. Delete extra_info rows for resumes submitted by this recruiter
      await conn.query(
        "DELETE ei FROM extra_info ei INNER JOIN resumes_data rd ON ei.res_id = rd.res_id WHERE rd.rid = ?",
        [rid],
      );

      // 4. Delete job_resume_selection rows for resumes submitted by this recruiter
      await conn.query(
        "DELETE jrs FROM job_resume_selection jrs INNER JOIN resumes_data rd ON jrs.res_id = rd.res_id WHERE rd.rid = ?",
        [rid],
      );

      // 5. Delete resumes submitted by this recruiter
      await conn.query("DELETE FROM resumes_data WHERE rid = ?", [rid]);

      // 6. Delete reimbursements (must come before money_sum cleanup)
      await conn.query("DELETE FROM reimbursements WHERE rid = ?", [rid]);

      // 7. Delete attendance rows (must come before money_sum cleanup)
      await conn.query(
        "DELETE FROM recruiter_attendance WHERE recruiter_rid = ?",
        [rid],
      );

      // 8. Delete orphaned money_sum entries from attendance & reimbursements
      const allMoneySumIds = [
        ...new Set([...attendanceMoneySumIds, ...reimbursementMoneySumIds]),
      ];
      if (allMoneySumIds.length > 0) {
        await conn.query("DELETE FROM money_sum WHERE id IN (?)", [
          allMoneySumIds,
        ]);
      }

      // 9. Delete job_recruiter_access rows for this recruiter
      await conn.query(
        "DELETE FROM job_recruiter_access WHERE recruiter_rid = ?",
        [rid],
      );

      // 10. If team leader, nullify granted_by references before deleting jobs
      await conn.query(
        "DELETE FROM job_recruiter_access WHERE granted_by = ?",
        [rid],
      );

      // 11. Delete recruiter_candidate_clicks
      await conn.query(
        "DELETE FROM recruiter_candidate_clicks WHERE recruiter_rid = ?",
        [rid],
      );

      // 12. Delete status row
      await conn.query("DELETE FROM status WHERE recruiter_rid = ?", [rid]);

      // 13. If team leader, handle jobs they created:
      //     - Delete extra_info & selections for resumes linked to their jobs
      //     - Delete resumes linked to their jobs
      //     - Delete jobs (CASCADE will also clean applications)
      const isTeamLeader =
        String(recruiter.role || "").toLowerCase() === "team leader" ||
        String(recruiter.role || "").toLowerCase() === "job creator";

      if (isTeamLeader) {
        const [jobRows] = await conn.query(
          "SELECT jid FROM jobs WHERE recruiter_rid = ?",
          [rid],
        );
        const jobIds = jobRows.map((j) => j.jid);

        if (jobIds.length > 0) {
          // Extra info for resumes linked to those jobs
          await conn.query(
            "DELETE ei FROM extra_info ei INNER JOIN resumes_data rd ON ei.res_id = rd.res_id WHERE rd.job_jid IN (?)",
            [jobIds],
          );
          // Selections for resumes linked to those jobs
          await conn.query(
            "DELETE FROM job_resume_selection WHERE job_jid IN (?)",
            [jobIds],
          );
          // Resumes linked to those jobs
          await conn.query("DELETE FROM resumes_data WHERE job_jid IN (?)", [
            jobIds,
          ]);
        }

        // Delete all jobs created by this team leader (CASCADE handles applications)
        await conn.query("DELETE FROM jobs WHERE recruiter_rid = ?", [rid]);
      }

      // 14. Delete the recruiter row itself
      await conn.query("DELETE FROM recruiter WHERE rid = ?", [rid]);

      await conn.commit();
      conn.release();

      return res.status(200).json({
        success: true,
        message: `Recruiter ${rid} and all associated data deleted.`,
      });
    } catch (error) {
      if (conn) {
        try {
          await conn.rollback();
        } catch (_rollbackErr) {
          /* ignore */
        }
        conn.release();
      }
      console.error("DELETE /api/admin/recruiters/:rid error:", error);

      if (error.code === "ER_ROW_IS_REFERENCED_2") {
        return res.status(409).json({
          message:
            "Cannot delete recruiter: a foreign key constraint prevents deletion. Please remove dependent records first.",
          error: error.message,
        });
      }
      return res.status(500).json({
        message: "Failed to delete recruiter.",
        error: error.message,
      });
    }
  },
);

// ═══════════════════════════════════════════════════════════════════════════
// DELETE /api/admin/candidates/:resId — Cascade-delete candidate resume
// ═══════════════════════════════════════════════════════════════════════════
router.delete(
  "/api/admin/candidates/:resId",
  requireAuth,
  requireRoles("admin"),
  async (req, res) => {
    const resId = String(req.params.resId || "").trim();
    if (!resId) {
      return res
        .status(400)
        .json({ message: "Resume ID (resId) is required." });
    }

    let conn;
    try {
      conn = await pool.getConnection();
      await conn.beginTransaction();

      // Verify resume exists
      const [[resume]] = await conn.query(
        "SELECT res_id FROM resumes_data WHERE res_id = ?",
        [resId],
      );
      if (!resume) {
        conn.release();
        return res
          .status(404)
          .json({ message: `Candidate resume ${resId} not found.` });
      }

      // 1. Delete job_resume_selection rows for this resume
      await conn.query("DELETE FROM job_resume_selection WHERE res_id = ?", [
        resId,
      ]);

      // 2. Delete extra_info (ATS scores, phone, email, extra data)
      await conn.query("DELETE FROM extra_info WHERE res_id = ?", [resId]);

      // 3. Delete the resume record itself (LONGBLOB file data is stored inline)
      await conn.query("DELETE FROM resumes_data WHERE res_id = ?", [resId]);

      await conn.commit();
      conn.release();

      return res.status(200).json({
        success: true,
        message: `Candidate resume ${resId} and all associated data deleted.`,
      });
    } catch (error) {
      if (conn) {
        try {
          await conn.rollback();
        } catch (_rollbackErr) {
          /* ignore */
        }
        conn.release();
      }
      console.error("DELETE /api/admin/candidates/:resId error:", error);

      if (error.code === "ER_ROW_IS_REFERENCED_2") {
        return res.status(409).json({
          message:
            "Cannot delete candidate resume: a foreign key constraint prevents deletion. Please remove dependent records first.",
          error: error.message,
        });
      }
      return res.status(500).json({
        message: "Failed to delete candidate resume.",
        error: error.message,
      });
    }
  },
);

module.exports = router;
