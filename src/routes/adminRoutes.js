const crypto = require("crypto");
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
  toMoneyOrNull,
  normalizeJobJid,
  parseJsonField,
  extractCandidateSnapshot,
} = require("../utils/formatters");
const {
  ADMIN_STATUS_TRANSITIONS,
  CANONICAL_WORKFLOW_STATUSES,
  CANONICAL_VERIFY_STATUS,
  DEFAULT_WORKFLOW_STATUS,
  getAllowedNextStatuses,
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
  parseInclusiveDateRange,
} = require("../utils/dateTime");
const {
  TASK_ASSIGNMENT_STATUS,
  ensureTaskTables,
  getTaskAssignmentActionState,
  resolveEffectiveTaskAssignmentStatus,
} = require("../utils/taskAssignments");

const router = express.Router();
const ADMIN_API_KEY = String(process.env.ADMIN_API_KEY || "").trim();
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
      return callback(
        new Error("Only PDF attachments are allowed for billed status."),
      );
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
  return parseAdvanceStatusUpload(req, res, next);
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
  if (!(await columnExists("money_sum", "res_id"))) {
    await pool.query("ALTER TABLE money_sum ADD COLUMN res_id VARCHAR(30) NULL");
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

const ensureRecruiterAccountStatusColumn = async () => {
  const hasColumn = await columnExists("recruiter", "account_status");
  if (!hasColumn) {
    await pool.query(
      "ALTER TABLE recruiter ADD COLUMN account_status VARCHAR(20) NOT NULL DEFAULT 'active'",
    );
  }
};

const buildTaskAssignmentAdminRow = (row) => {
  const effectiveStatus = resolveEffectiveTaskAssignmentStatus(
    row.assignmentStatus,
    row.assignmentDate,
  );
  const actionState = getTaskAssignmentActionState(row.assignmentDate);

  return {
    assignmentId: Number(row.assignmentId) || null,
    recruiterRid: row.recruiterRid || null,
    recruiterName: row.recruiterName || null,
    recruiterEmail: row.recruiterEmail || null,
    recruiterRole: normalizeStaffRole(row.recruiterRole),
    assignedAt: row.assignedAt || null,
    assignmentDate: row.assignmentDate || null,
    rescheduledFromDate: row.rescheduledFromDate || null,
    rescheduledAt: row.rescheduledAt || null,
    rescheduledByRid: row.rescheduledByRid || null,
    rescheduledByName: row.rescheduledByName || null,
    rescheduledByEmail: row.rescheduledByEmail || null,
    actedAt: row.actedAt || null,
    status: effectiveStatus,
    rawStatus: row.assignmentStatus || TASK_ASSIGNMENT_STATUS.PENDING,
    isTimedOut: effectiveStatus === TASK_ASSIGNMENT_STATUS.TIMED_OUT,
    isActionableToday: actionState.isActionableToday,
    isScheduledForFuture: actionState.isScheduledForFuture,
  };
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

const normalizeSalaryEffectiveDate = (value) => {
  const normalized = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : "";
};

const roundMoneyOrNull = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.round(parsed * 100) / 100;
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

const ensureRecruiterSalaryHistoryTable = async () => {
  await pool.query(
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

const getRecruiterSalaryColumnMeta = async () => {
  const hasSalaryColumn = await columnExists("recruiter", "salary");
  const hasMonthlySalaryColumn = await columnExists("recruiter", "monthly_salary");
  const hasDailySalaryColumn = await columnExists("recruiter", "daily_salary");

  return {
    hasSalaryColumn,
    hasMonthlySalaryColumn,
    hasDailySalaryColumn,
    salarySelect: hasSalaryColumn ? "r.salary AS rawSalary," : "NULL AS rawSalary,",
    monthlySalarySelect: hasMonthlySalaryColumn
      ? "r.monthly_salary AS monthlySalary,"
      : "NULL AS monthlySalary,",
    dailySalarySelect: hasDailySalaryColumn
      ? "r.daily_salary AS dailySalary,"
      : "NULL AS dailySalary,",
  };
};

const resolveLegacySalarySnapshot = (row) => {
  const explicitMonthlySalary = toMoneyOrNull(row?.monthlySalary);
  const explicitDailySalary = toMoneyOrNull(row?.dailySalary);
  const rawSalaryAmount = toMoneyOrNull(row?.rawSalary);
  const monthlySalary =
    explicitMonthlySalary ??
    (explicitDailySalary !== null
      ? roundMoneyOrNull(explicitDailySalary * 30)
      : rawSalaryAmount);
  const dailySalary =
    explicitDailySalary ??
    (monthlySalary !== null ? roundMoneyOrNull(monthlySalary / 30) : null);

  return {
    rawSalary:
      row?.rawSalary === undefined || row?.rawSalary === null
        ? null
        : String(row.rawSalary).trim() || null,
    monthlySalary,
    dailySalary,
  };
};

const getRecruiterSalarySnapshot = async (
  connection,
  recruiterRid,
  effectiveDate,
) => {
  await ensureRecruiterSalaryHistoryTable();
  const salaryMeta = await getRecruiterSalaryColumnMeta();
  const [rows] = await connection.query(
    `SELECT
       r.rid,
       r.name,
       r.email,
       COALESCE(r.role, 'recruiter') AS role,
       ${salaryMeta.salarySelect}
       ${salaryMeta.monthlySalarySelect}
       ${salaryMeta.dailySalarySelect}
       (
         SELECT rsh.monthly_salary
         FROM recruiter_salary_history rsh
         WHERE rsh.recruiter_rid = r.rid
           AND rsh.effective_from <= ?
         ORDER BY rsh.effective_from DESC, rsh.id DESC
         LIMIT 1
       ) AS historyMonthlySalary,
       (
         SELECT rsh.daily_salary
         FROM recruiter_salary_history rsh
         WHERE rsh.recruiter_rid = r.rid
           AND rsh.effective_from <= ?
         ORDER BY rsh.effective_from DESC, rsh.id DESC
         LIMIT 1
       ) AS historyDailySalary,
       (
         SELECT rsh.effective_from
         FROM recruiter_salary_history rsh
         WHERE rsh.recruiter_rid = r.rid
           AND rsh.effective_from <= ?
         ORDER BY rsh.effective_from DESC, rsh.id DESC
         LIMIT 1
       ) AS historyEffectiveFrom
     FROM recruiter r
     WHERE r.rid = ?
     LIMIT 1`,
    [effectiveDate, effectiveDate, effectiveDate, recruiterRid],
  );

  if (rows.length === 0) return null;

  const row = rows[0];
  const legacySalary = resolveLegacySalarySnapshot(row);
  const historyMonthlySalary = toMoneyOrNull(row.historyMonthlySalary);
  const historyDailySalary =
    toMoneyOrNull(row.historyDailySalary) ??
    (historyMonthlySalary !== null
      ? roundMoneyOrNull(historyMonthlySalary / 30)
      : null);

  return {
    rid: row.rid,
    name: row.name,
    email: row.email || null,
    role: normalizeStaffRole(row.role),
    rawSalary: legacySalary.rawSalary,
    legacyMonthlySalary: legacySalary.monthlySalary,
    legacyDailySalary: legacySalary.dailySalary,
    currentSalary: historyMonthlySalary ?? legacySalary.monthlySalary,
    currentDailySalary: historyDailySalary ?? legacySalary.dailySalary,
    currentSalaryEffectiveFrom: row.historyEffectiveFrom || null,
  };
};

const syncRecruiterCurrentSalaryColumns = async (
  connection,
  recruiterRid,
  effectiveDate,
) => {
  const salarySnapshot = await getRecruiterSalarySnapshot(
    connection,
    recruiterRid,
    effectiveDate,
  );
  if (!salarySnapshot) return null;

  const salaryMeta = await getRecruiterSalaryColumnMeta();
  const updateFields = [];
  const updateValues = [];

  if (salaryMeta.hasSalaryColumn) {
    updateFields.push("salary = ?");
    updateValues.push(
      salarySnapshot.currentSalary === null
        ? salarySnapshot.rawSalary
        : String(salarySnapshot.currentSalary),
    );
  }
  if (salaryMeta.hasMonthlySalaryColumn) {
    updateFields.push("monthly_salary = ?");
    updateValues.push(salarySnapshot.currentSalary);
  }
  if (salaryMeta.hasDailySalaryColumn) {
    updateFields.push("daily_salary = ?");
    updateValues.push(salarySnapshot.currentDailySalary);
  }

  if (updateFields.length > 0) {
    updateValues.push(recruiterRid);
    await connection.query(
      `UPDATE recruiter SET ${updateFields.join(", ")} WHERE rid = ?`,
      updateValues,
    );
  }

  return salarySnapshot;
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

const extractRevenueResumeId = (entry = {}) => {
  const directResId = String(entry?.resId || "").trim();
  if (directResId) return directResId;

  const reason = String(entry?.reason || "");
  const billedMatch = reason.match(/\[BILLED:([^\]]+)\]/i);
  if (billedMatch?.[1]) {
    return String(billedMatch[1]).trim();
  }

  return "";
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
  if (!ADMIN_API_KEY) {
    return res.status(503).json({
      message: "Admin login is not configured on the server.",
    });
  }

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
          r.role AS recruiterRole,
          ${jobJidSelect}
          teamLeader.rid AS teamLeaderRid,
          teamLeader.name AS teamLeaderName,
          c.name AS candidateName,
          c.phone AS candidatePhone,
          c.joining_date AS joiningDate,
          j.company_name AS companyName,
          j.city AS city,
          ei.office_location_city AS officeLocationCity,
          j.points_per_joining AS pointsPerJoining,
          j.revenue AS revenue,
          COALESCE(jrs.selection_status, 'submitted') AS workflowStatus,
          rd.resume_filename AS resumeFilename,
          rd.resume_type AS resumeType,
          ${acceptedSelect}
          ${acceptedAtSelect}
          ${acceptedByAdminSelect}
          rd.uploaded_at AS uploadedAt
        FROM resumes_data rd
        INNER JOIN recruiter r ON r.rid = rd.rid
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        LEFT JOIN job_resume_selection jrs
          ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
        LEFT JOIN jobs j ON j.jid = rd.job_jid
        LEFT JOIN recruiter teamLeader ON teamLeader.rid = j.recruiter_rid
        LEFT JOIN extra_info ei
          ON ei.res_id = rd.res_id OR ei.resume_id = rd.res_id
        ORDER BY rd.uploaded_at DESC`,
      );

      recruiterResumeUploads = rows.map((row) => {
        const workflowFields = buildWorkflowResponseFields(row, {
          includeSelection: false,
          includeStatusHistory: false,
        });
        const effectiveTeamLeaderRid = isTeamLeaderLikeRole(row.recruiterRole)
          ? row.rid || null
          : null;
        const effectiveTeamLeaderName = isTeamLeaderLikeRole(row.recruiterRole)
          ? row.recruiterName || null
          : null;

        return {
          resId: row.resId || null,
          rid: row.rid || null,
          recruiterName: row.recruiterName || null,
          recruiterEmail: row.recruiterEmail || null,
          jobJid:
            row.jobJid === null || row.jobJid === undefined
              ? null
              : String(row.jobJid).trim(),
          teamLeaderRid: effectiveTeamLeaderRid,
          teamLeaderName: effectiveTeamLeaderName,
          jobOwnerTeamLeaderRid: row.teamLeaderRid || null,
          jobOwnerTeamLeaderName: row.teamLeaderName || null,
          name: row.candidateName || null,
          candidateName: row.candidateName || null,
          candidatePhone: row.candidatePhone || null,
          phone: row.candidatePhone || null,
          companyName: row.companyName || null,
          officeLocationCity: row.officeLocationCity || null,
          city: row.city || null,
          joiningDate: row.joiningDate || null,
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
          ...workflowFields,
        };
      });
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
    const hasOfficeLocationCityColumn =
      hasExtraInfoTable &&
      (await columnExists("extra_info", "office_location_city"));
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
      resumes: rows.map((row) => {
        const workflowFields = buildWorkflowResponseFields({
          workflowStatus: row.selectionStatus,
          selectionStatus: row.selectionStatus,
          selectionNote: row.selectionNote,
          selectedByAdmin: row.selectedByAdmin,
          selectedAt: row.selectedAt,
          walkInDate: row.walkInDate,
          resumeJoiningDate: row.resumeJoiningDate,
          joiningDate: row.joiningDate,
          joinedReason: row.joinedReason,
          joiningNote: row.joinedReason,
          verifiedReason: row.verifiedReason,
          uploadedAt: row.uploadedAt,
        });

        return {
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
          walkInDate: row.walkInDate || null,
          joiningDate: row.joiningDate || null,
          joinedReason: row.joinedReason || null,
          joiningNote: row.joinedReason || null,
          ...workflowFields,
          job: {
            roleName: row.roleName || null,
            companyName: row.companyName || null,
            city: row.city || null,
            jobDescription: row.jobDescription || null,
            skills: row.skills || null,
          },
        };
      }),
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
        ${hasJoinedReasonColumn ? "ei.joined_reason AS joinedReason," : "NULL AS joinedReason,"}
        ${
          hasOfficeLocationCityColumn
            ? "ei.office_location_city AS officeLocationCity"
            : "NULL AS officeLocationCity"
        }
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

          const workflowFields = buildWorkflowResponseFields({
            workflowStatus: row.selectionStatus,
            selectionStatus: row.selectionStatus,
            selectionNote: row.selectionNote,
            selectedByAdmin: row.selectedByAdmin,
            selectedAt: row.selectedAt,
            walkInDate: row.walkInDate,
            resumeJoiningDate: row.resumeJoiningDate,
            joiningDate: row.joiningDate,
            joinedReason: row.joinedReason,
            joiningNote: row.joinedReason,
            verifiedReason: row.verifiedReason,
            uploadedAt: row.uploadedAt,
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
            walkInDate: row.walkInDate || null,
            joiningDate: row.joiningDate || null,
            joinedReason: row.joinedReason || null,
            joiningNote: row.joinedReason || null,
            officeLocationCity: row.officeLocationCity || null,
            ...workflowFields,
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
    await ensureRecruiterSalaryHistoryTable();
    const salaryMeta = await getRecruiterSalaryColumnMeta();

    const [rows] = await pool.query(
      `SELECT
        r.rid,
        r.name,
        CASE
          WHEN LOWER(TRIM(COALESCE(r.role, 'recruiter'))) IN ('team leader', 'team_leader', 'job creator') THEN 'team leader'
          ELSE 'recruiter'
        END AS role,
        COALESCE(
          (
            SELECT rsh.daily_salary
            FROM recruiter_salary_history rsh
            WHERE rsh.recruiter_rid = r.rid
              AND rsh.effective_from <= ?
            ORDER BY rsh.effective_from DESC, rsh.id DESC
            LIMIT 1
          ),
          ${salaryMeta.hasDailySalaryColumn ? "r.daily_salary," : ""}
          ${salaryMeta.hasMonthlySalaryColumn ? "ROUND(r.monthly_salary / 30, 2)," : ""}
          ${
            salaryMeta.hasSalaryColumn
              ? `CASE
            WHEN TRIM(COALESCE(r.salary, '')) REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN ROUND(CAST(TRIM(r.salary) AS DECIMAL(12,2)) / 30, 2)
            ELSE 0
          END`
              : "0"
          }
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
      [attendanceDate, attendanceDate],
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
    await ensureRecruiterSalaryHistoryTable();
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
          (
            SELECT rsh.daily_salary
            FROM recruiter_salary_history rsh
            WHERE rsh.recruiter_rid = recruiter.rid
              AND rsh.effective_from <= ?
            ORDER BY rsh.effective_from DESC, rsh.id DESC
            LIMIT 1
          ),
          daily_salary,
          ROUND(monthly_salary / 30, 2),
          CASE
            WHEN TRIM(COALESCE(salary, '')) REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN ROUND(CAST(TRIM(salary) AS DECIMAL(12,2)) / 30, 2)
            ELSE 0
          END
        ) AS dailySalary
      FROM recruiter recruiter
      WHERE rid = ?
        AND LOWER(TRIM(COALESCE(role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')
      LIMIT 1
      FOR UPDATE`,
      [attendanceDate, recruiterRid],
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
        res_id AS resId,
        entry_type AS entryType,
        created_at AS createdAt
      FROM money_sum
      ORDER BY created_at DESC, id DESC`,
    );

    const revenueResIds = Array.from(
      new Set(
        rows
          .map((row) => extractRevenueResumeId(row))
          .filter((value) => Boolean(String(value || "").trim())),
      ),
    );

    let revenueMetaByResId = new Map();
    if (revenueResIds.length > 0) {
      const placeholders = revenueResIds.map(() => "?").join(", ");
      const [metaRows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.job_jid AS jobJid,
          j.company_name AS companyName,
          j.city AS city
        FROM resumes_data rd
        LEFT JOIN jobs j ON j.jid = rd.job_jid
        WHERE rd.res_id IN (${placeholders})`,
        revenueResIds,
      );
      revenueMetaByResId = new Map(
        (Array.isArray(metaRows) ? metaRows : []).map((row) => [
          String(row.resId),
          {
            resId: row.resId || null,
            jobJid: row.jobJid ?? null,
            companyName: row.companyName || null,
            city: row.city || null,
          },
        ]),
      );
    }

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
      entries: rows.map((row) => {
        const normalizedResId = extractRevenueResumeId(row);
        const revenueMeta = normalizedResId
          ? revenueMetaByResId.get(normalizedResId) || null
          : null;

        return {
          id: Number(row.id),
          companyRev: toMoneyNumber(row.companyRev),
          expense: toMoneyNumber(row.expense),
          profit: toMoneyNumber(row.profit),
          reason: row.reason || "",
          photo: normalizePhotoValue(row.photo),
          resId: normalizedResId || null,
          jobJid: revenueMeta?.jobJid ?? null,
          companyName: revenueMeta?.companyName ?? null,
          city: revenueMeta?.city ?? null,
          entryType:
            normalizeRevenueEntryType(row.entryType) ||
            (toMoneyNumber(row.companyRev) > 0 ? "intake" : "expense"),
          createdAt: row.createdAt,
        };
      }),
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
    await ensureRecruiterSalaryHistoryTable();
    const currentDate = getCurrentDateOnlyInBusinessTimeZone();
    const hasRoleColumn = await columnExists("recruiter", "role");
    const salaryMeta = await getRecruiterSalaryColumnMeta();
    const roleFilter = hasRoleColumn
      ? "WHERE LOWER(TRIM(COALESCE(role, 'recruiter'))) IN ('recruiter', 'team leader', 'team_leader', 'job creator')"
      : "";
    const [rows] = await pool.query(
      `SELECT
         r.rid,
         r.name,
         r.email,
         COALESCE(r.role, 'recruiter') AS role,
         ${salaryMeta.salarySelect}
         ${salaryMeta.monthlySalarySelect}
         ${salaryMeta.dailySalarySelect}
         (
           SELECT rsh.monthly_salary
           FROM recruiter_salary_history rsh
           WHERE rsh.recruiter_rid = r.rid
             AND rsh.effective_from <= ?
           ORDER BY rsh.effective_from DESC, rsh.id DESC
           LIMIT 1
         ) AS historyMonthlySalary,
         (
           SELECT rsh.effective_from
           FROM recruiter_salary_history rsh
           WHERE rsh.recruiter_rid = r.rid
             AND rsh.effective_from <= ?
           ORDER BY rsh.effective_from DESC, rsh.id DESC
           LIMIT 1
         ) AS currentSalaryEffectiveFrom
       FROM recruiter r
       ${roleFilter}
       ORDER BY name ASC, rid ASC`,
      [currentDate, currentDate],
    );

    return res.status(200).json({
      recruiters: rows.map((row) => {
        const legacySalary = resolveLegacySalarySnapshot(row);
        return {
          rid: row.rid,
          name: row.name,
          email: row.email || null,
          role: normalizeStaffRole(row.role),
          currentSalary:
            toMoneyOrNull(row.historyMonthlySalary) ?? legacySalary.monthlySalary,
          currentSalaryEffectiveFrom: row.currentSalaryEffectiveFrom || null,
        };
      }),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch recruiters list.",
      error: error.message,
    });
  }
});

router.get("/api/admin/recruiters/:rid/salary-history", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const recruiterRid = String(req.params?.rid || "").trim();
  if (!recruiterRid) {
    return res.status(400).json({ message: "Recruiter ID is required." });
  }

  try {
    await ensureRecruiterSalaryHistoryTable();
    const currentDate = getCurrentDateOnlyInBusinessTimeZone();
    const salarySnapshot = await getRecruiterSalarySnapshot(
      pool,
      recruiterRid,
      currentDate,
    );

    if (!salarySnapshot) {
      return res.status(404).json({ message: "Recruiter not found." });
    }

    const [historyRows] = await pool.query(
      `SELECT
         id,
         recruiter_rid AS recruiterRid,
         monthly_salary AS monthlySalary,
         daily_salary AS dailySalary,
         effective_from AS effectiveFrom,
         created_by AS createdBy,
         created_at AS createdAt
       FROM recruiter_salary_history
       WHERE recruiter_rid = ?
       ORDER BY effective_from DESC, id DESC`,
      [recruiterRid],
    );

    return res.status(200).json({
      recruiter: salarySnapshot,
      modifications: historyRows.map((row) => ({
        id: Number(row.id) || null,
        recruiterRid: row.recruiterRid,
        monthlySalary: toMoneyOrNull(row.monthlySalary),
        dailySalary: toMoneyOrNull(row.dailySalary),
        effectiveFrom: row.effectiveFrom || null,
        createdBy: row.createdBy || null,
        createdAt: row.createdAt || null,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch salary history.",
      error: error.message,
    });
  }
});

router.put("/api/admin/recruiters/:rid/salary-history", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const recruiterRid = String(req.params?.rid || "").trim();
  const monthlySalary = toMoneyOrNull(req.body?.monthlySalary);
  const effectiveFrom = normalizeSalaryEffectiveDate(req.body?.effectiveFrom);
  const createdBy =
    String(req.body?.createdBy || "admin-panel").trim() || "admin-panel";

  if (!recruiterRid) {
    return res.status(400).json({ message: "Recruiter ID is required." });
  }
  if (monthlySalary === null || monthlySalary <= 0) {
    return res.status(400).json({
      message: "monthlySalary must be a valid positive number.",
    });
  }
  if (!effectiveFrom) {
    return res.status(400).json({
      message: "effectiveFrom must be in YYYY-MM-DD format.",
    });
  }

  const connection = await pool.getConnection();
  try {
    await ensureRecruiterSalaryHistoryTable();
    await connection.beginTransaction();

    const [recruiterRows] = await connection.query(
      `SELECT rid
       FROM recruiter
       WHERE rid = ?
       LIMIT 1
       FOR UPDATE`,
      [recruiterRid],
    );

    if (recruiterRows.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: "Recruiter not found." });
    }

    const dailySalary = roundMoneyOrNull(monthlySalary / 30) || 0;
    await connection.query(
      `INSERT INTO recruiter_salary_history
        (recruiter_rid, monthly_salary, daily_salary, effective_from, created_by)
       VALUES (?, ?, ?, ?, ?)`,
      [recruiterRid, monthlySalary, dailySalary, effectiveFrom, createdBy],
    );

    const currentDate = getCurrentDateOnlyInBusinessTimeZone();
    const recruiter = await syncRecruiterCurrentSalaryColumns(
      connection,
      recruiterRid,
      currentDate,
    );

    await connection.commit();

    return res.status(200).json({
      message: "Salary updated successfully.",
      recruiter,
      modification: {
        recruiterRid,
        monthlySalary,
        dailySalary,
        effectiveFrom,
        createdBy,
      },
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      message: "Failed to update salary.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
});

router.get("/api/admin/tasks", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  try {
    await ensureTaskTables();

    const [taskRows] = await pool.query(
      `SELECT
        t.id,
        t.heading,
        t.description,
        t.created_by AS createdBy,
        t.created_at AS createdAt,
        t.updated_at AS updatedAt,
        ta.id AS assignmentId,
        ta.recruiter_rid AS recruiterRid,
        r.name AS recruiterName,
        r.email AS recruiterEmail,
        COALESCE(r.role, 'recruiter') AS recruiterRole,
        ta.status AS assignmentStatus,
        ta.assignment_date AS assignmentDate,
        ta.rescheduled_from_date AS rescheduledFromDate,
        ta.rescheduled_at AS rescheduledAt,
        ta.rescheduled_by_rid AS rescheduledByRid,
        rb.name AS rescheduledByName,
        rb.email AS rescheduledByEmail,
        ta.acted_at AS actedAt,
        ta.created_at AS assignedAt
      FROM tasks t
      LEFT JOIN task_assignments ta ON ta.task_id = t.id
      LEFT JOIN recruiter r ON r.rid = ta.recruiter_rid
      LEFT JOIN recruiter rb ON rb.rid = ta.rescheduled_by_rid
      ORDER BY t.created_at DESC, ta.created_at DESC, ta.id DESC`,
    );

    const taskMap = new Map();
    for (const row of taskRows) {
      if (!taskMap.has(row.id)) {
        taskMap.set(row.id, {
          id: Number(row.id) || null,
          heading: row.heading || "",
          description: row.description || "",
          createdBy: row.createdBy || "admin",
          createdAt: row.createdAt || null,
          updatedAt: row.updatedAt || null,
          assignments: [],
        });
      }

      if (row.assignmentId) {
        taskMap.get(row.id).assignments.push(buildTaskAssignmentAdminRow(row));
      }
    }

    return res.status(200).json({
      tasks: Array.from(taskMap.values()).map((task) => ({
        ...task,
        totalAssignments: task.assignments.length,
        completedCount: task.assignments.filter(
          (item) => item.status === TASK_ASSIGNMENT_STATUS.COMPLETED,
        ).length,
        rejectedCount: task.assignments.filter(
          (item) => item.status === TASK_ASSIGNMENT_STATUS.REJECTED,
        ).length,
        timedOutCount: task.assignments.filter(
          (item) => item.status === TASK_ASSIGNMENT_STATUS.TIMED_OUT,
        ).length,
        pendingCount: task.assignments.filter(
          (item) => item.status === TASK_ASSIGNMENT_STATUS.PENDING,
        ).length,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to fetch tasks.",
      error: error.message,
    });
  }
});

router.post("/api/admin/tasks", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const heading = String(req.body?.heading || "").trim();
  const description = String(req.body?.description || "").trim();
  const recruiterRid = String(req.body?.recruiterRid || "").trim();

  if (!heading) {
    return res.status(400).json({ message: "Task heading is required." });
  }

  if (!recruiterRid) {
    return res.status(400).json({ message: "Assignee selection is required." });
  }

  let connection;
  try {
    await ensureTaskTables();
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [recruiterRows] = await connection.query(
      `SELECT rid, name, email, COALESCE(role, 'recruiter') AS role
       FROM recruiter
       WHERE rid = ?
       LIMIT 1`,
      [recruiterRid],
    );

    if (recruiterRows.length === 0) {
      await connection.rollback();
      return res.status(404).json({ message: "Assignee not found." });
    }

    const [taskResult] = await connection.query(
      `INSERT INTO tasks (heading, description, created_by)
       VALUES (?, ?, 'admin')`,
      [heading, description || null],
    );

    const assignmentDate = getCurrentDateOnlyInBusinessTimeZone();
    const [assignmentResult] = await connection.query(
      `INSERT INTO task_assignments
        (task_id, recruiter_rid, status, assignment_date)
       VALUES (?, ?, 'pending', ?)`,
      [taskResult.insertId, recruiterRid, assignmentDate],
    );

    await connection.commit();

    return res.status(201).json({
      message: "Task created and assigned successfully.",
      task: {
        id: Number(taskResult.insertId) || null,
        heading,
        description,
        assignments: [
          {
            assignmentId: Number(assignmentResult.insertId) || null,
            recruiterRid,
            recruiterName: recruiterRows[0].name || null,
            recruiterEmail: recruiterRows[0].email || null,
            recruiterRole: normalizeStaffRole(recruiterRows[0].role),
            assignmentDate,
            rescheduledFromDate: null,
            rescheduledAt: null,
            rescheduledByRid: null,
            rescheduledByName: null,
            rescheduledByEmail: null,
            status: TASK_ASSIGNMENT_STATUS.PENDING,
            rawStatus: TASK_ASSIGNMENT_STATUS.PENDING,
            assignedAt: null,
            actedAt: null,
            isTimedOut: false,
            isActionableToday: true,
            isScheduledForFuture: false,
          },
        ],
      },
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_rollbackError) {
        // ignore rollback errors
      }
    }
    return res.status(500).json({
      message: "Failed to create task.",
      error: error.message,
    });
  } finally {
    if (connection) connection.release();
  }
});

router.post("/api/admin/tasks/:taskId/assign", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const taskId = Number(req.params?.taskId);
  const recruiterRid = String(req.body?.recruiterRid || "").trim();

  if (!Number.isInteger(taskId) || taskId <= 0) {
    return res.status(400).json({ message: "Valid task ID is required." });
  }

  if (!recruiterRid) {
    return res.status(400).json({ message: "Assignee selection is required." });
  }

  let connection;
  try {
    await ensureTaskTables();
    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [[taskRow]] = await connection.query(
      "SELECT id, heading, description FROM tasks WHERE id = ? LIMIT 1",
      [taskId],
    );
    if (!taskRow) {
      await connection.rollback();
      return res.status(404).json({ message: "Task not found." });
    }

    const [[recruiterRow]] = await connection.query(
      "SELECT rid, name, email, COALESCE(role, 'recruiter') AS role FROM recruiter WHERE rid = ? LIMIT 1",
      [recruiterRid],
    );
    if (!recruiterRow) {
      await connection.rollback();
      return res.status(404).json({ message: "Assignee not found." });
    }

    const assignmentDate = getCurrentDateOnlyInBusinessTimeZone();
    const [assignmentResult] = await connection.query(
      `INSERT INTO task_assignments
        (task_id, recruiter_rid, status, assignment_date)
       VALUES (?, ?, 'pending', ?)`,
      [taskId, recruiterRid, assignmentDate],
    );

    await connection.commit();

    return res.status(201).json({
      message: "Task assigned successfully.",
      task: {
        id: Number(taskRow.id) || null,
        heading: taskRow.heading || "",
        description: taskRow.description || "",
      },
      assignment: {
        assignmentId: Number(assignmentResult.insertId) || null,
        recruiterRid,
        recruiterName: recruiterRow.name || null,
        recruiterEmail: recruiterRow.email || null,
        recruiterRole: normalizeStaffRole(recruiterRow.role),
        assignmentDate,
        rescheduledFromDate: null,
        rescheduledAt: null,
        rescheduledByRid: null,
        rescheduledByName: null,
        rescheduledByEmail: null,
        status: TASK_ASSIGNMENT_STATUS.PENDING,
        rawStatus: TASK_ASSIGNMENT_STATUS.PENDING,
        assignedAt: null,
        actedAt: null,
        isTimedOut: false,
        isActionableToday: true,
        isScheduledForFuture: false,
      },
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_rollbackError) {
        // ignore rollback errors
      }
    }
    const message =
      error?.code === "ER_DUP_ENTRY"
        ? "This team member already has this task for today."
        : "Failed to assign task.";
    return res.status(
      error?.code === "ER_DUP_ENTRY" ? 409 : 500,
    ).json({
      message,
      error: error.message,
    });
  } finally {
    if (connection) connection.release();
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
const PERFORMANCE_EVENT_KEYS = [...CANONICAL_WORKFLOW_STATUSES];

const PERFORMANCE_EVENT_META = {
  submitted: { recruiterField: "submitted", summaryField: "totalSubmitted" },
  verified: { recruiterField: "verified", summaryField: "totalVerified" },
  walk_in: { recruiterField: "walk_in", summaryField: "totalWalkIn" },
  selected: { recruiterField: "selected", summaryField: "totalSelected" },
  rejected: { recruiterField: "rejected", summaryField: "totalRejected" },
  shortlisted: {
    recruiterField: "shortlisted",
    summaryField: "totalShortlisted",
  },
  joined: { recruiterField: "joined", summaryField: "totalJoined" },
  dropout: { recruiterField: "dropout", summaryField: "totalDropout" },
  billed: { recruiterField: "billed", summaryField: "totalBilled" },
  left: { recruiterField: "left", summaryField: "totalLeft" },
};

const normalizePerformanceTimestamp = (value) =>
  value == null ? null : String(value).trim() || null;

const isTeamLeaderLikeRole = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return (
    normalized === "team leader" ||
    normalized === "team_leader" ||
    normalized === "job creator"
  );
};

const hasNonEmptyValue = (value) =>
  value !== undefined && value !== null && String(value).trim() !== "";

const isCanonicalWorkflowStatus = (value) =>
  CANONICAL_WORKFLOW_STATUSES.includes(normalizeWorkflowStatus(value));

const firstNonEmptyText = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const normalized = String(value).trim();
    if (normalized) return normalized;
  }
  return null;
};

const resolveLatestPerformanceNote = (row = {}) => {
  const workflowStatus = resolveCanonicalWorkflowStatus({
    workflowStatus: row.workflowStatus,
    selectionStatus: row.selectionStatus,
    status: row.status,
    joiningDate: row.joiningDate,
  });

  const byStatus = {
    submitted: firstNonEmptyText(row.submittedReason),
    verified: firstNonEmptyText(row.verifiedReason, row.selectionNote),
    walk_in: firstNonEmptyText(row.walkInReason, row.selectionNote),
    shortlisted: firstNonEmptyText(row.shortlistedReason, row.selectionNote),
    selected: firstNonEmptyText(row.selectReason, row.selectionNote),
    joined: firstNonEmptyText(row.joinedReason, row.joiningNote, row.selectionNote),
    billed: firstNonEmptyText(row.billedReason, row.selectionNote),
    left: firstNonEmptyText(row.leftReason, row.selectionNote),
    dropout: firstNonEmptyText(row.dropoutReason, row.selectionNote),
    rejected: firstNonEmptyText(row.rejectReason, row.selectionNote),
  };

  if (byStatus[workflowStatus]) {
    return byStatus[workflowStatus];
  }

  return firstNonEmptyText(
    row.leftReason,
    row.billedReason,
    row.dropoutReason,
    row.joinedReason,
    row.joiningNote,
    row.selectReason,
    row.shortlistedReason,
    row.rejectReason,
    row.walkInReason,
    row.verifiedReason,
    row.submittedReason,
    row.selectionNote,
    row.note,
    row.reason,
  );
};

const resolveCanonicalWorkflowStatus = ({
  workflowStatus,
  selectionStatus,
  status,
  joiningDate,
} = {}) => {
    const candidates = [workflowStatus, selectionStatus, status];
  for (const candidate of candidates) {
    const normalized = normalizeResumeStatusInput(candidate);
    if (CANONICAL_WORKFLOW_STATUSES.includes(normalized)) {
      if (normalized === "shortlisted" && hasNonEmptyValue(joiningDate)) {
        return "selected";
      }
      return normalized;
    }
  }

  return DEFAULT_WORKFLOW_STATUS;
};

const resolveRollbackSourceStatus = (resume) => {
  const currentStatus = resolveCanonicalWorkflowStatus({
    workflowStatus: resume.workflowStatus,
    selectionStatus: resume.selectionStatus,
    status: resume.currentStatus,
    joiningDate: resume.currentJoiningDate,
  });

  if (currentStatus === "rejected") {
    if (hasNonEmptyValue(resume.currentWalkInDate) || hasNonEmptyValue(resume.walkInAt)) {
      return "walk_in";
    }
    if (
      hasNonEmptyValue(resume.verifiedAt) ||
      hasNonEmptyValue(resume.verifiedReason)
    ) {
      return CANONICAL_VERIFY_STATUS;
    }
    return DEFAULT_WORKFLOW_STATUS;
  }

  if (currentStatus === "dropout") {
    return hasNonEmptyValue(resume.currentJoiningDate) ||
      hasNonEmptyValue(resume.selectedAtHistory) ||
      hasNonEmptyValue(resume.selectedAt)
      ? "selected"
      : "shortlisted";
  }

  return getPreviousWorkflowStatus(currentStatus);
};

const buildInvalidTransitionPayload = (currentStatus, requestedStatus) => ({
  error: "INVALID_STATUS_TRANSITION",
  message: `Invalid status transition from '${currentStatus}' to '${requestedStatus}'.`,
  currentStatus,
  requestedStatus,
  allowedNextStatuses: getAllowedNextStatuses(currentStatus),
});

const buildStatusHistory = (row = {}) => {
  const entries = [
    {
      status: "submitted",
      changedAt: normalizePerformanceTimestamp(row.submittedAt || row.uploadedAt),
      changedBy: row.submittedBy || null,
    },
    {
      status: "verified",
      changedAt: normalizePerformanceTimestamp(row.verifiedAt),
      changedBy: row.verifiedBy || null,
    },
    {
      status: "walk_in",
      changedAt: normalizePerformanceTimestamp(row.walkInAt),
      changedBy: row.walkInBy || null,
    },
    {
      status: "selected",
      changedAt: normalizePerformanceTimestamp(
        row.selectedAtHistory || row.selectedAt,
      ),
      changedBy: row.selectedByAdmin || null,
    },
    {
      status: "shortlisted",
      changedAt: normalizePerformanceTimestamp(row.shortlistedAt),
      changedBy: row.shortlistedBy || row.selectedByAdmin || null,
    },
    {
      status: "joined",
      changedAt: normalizePerformanceTimestamp(row.joinedAt),
      changedBy: row.joinedBy || row.selectedByAdmin || null,
    },
    {
      status: "billed",
      changedAt: normalizePerformanceTimestamp(row.billedAt),
      changedBy: row.billedBy || row.selectedByAdmin || null,
    },
    {
      status: "left",
      changedAt: normalizePerformanceTimestamp(row.leftAt),
      changedBy: row.leftBy || row.selectedByAdmin || null,
    },
    {
      status: "dropout",
      changedAt: normalizePerformanceTimestamp(row.dropoutAt),
      changedBy: row.dropoutBy || row.selectedByAdmin || null,
    },
    {
      status: "rejected",
      changedAt: normalizePerformanceTimestamp(row.rejectedAt),
      changedBy: row.rejectedBy || row.selectedByAdmin || null,
    },
  ]
    .filter((entry) => entry.changedAt)
    .sort((a, b) => a.changedAt.localeCompare(b.changedAt));

  return entries;
};

const buildWorkflowResponseFields = (row = {}, options = {}) => {
  const workflowStatus = resolveCanonicalWorkflowStatus({
    workflowStatus: row.workflowStatus,
    selectionStatus: row.selectionStatus,
    status: row.status,
    joiningDate:
      row.joiningDate ??
      row.currentJoiningDate ??
      row.resumeJoiningDate ??
      row.selectionJoiningDate,
  });
  const allowedNextStatuses = getAllowedNextStatuses(workflowStatus);
  const canRollback = Boolean(resolveRollbackSourceStatus({
    ...row,
    workflowStatus,
  }));
  const includeSelection =
    options.includeSelection !== undefined
      ? options.includeSelection
      : workflowStatus !== DEFAULT_WORKFLOW_STATUS ||
        hasNonEmptyValue(row.selectionStatus) ||
        hasNonEmptyValue(row.selectionNote) ||
        hasNonEmptyValue(row.selectedAt);

  const selection = includeSelection
    ? {
        ...(row.selection || {}),
        status: workflowStatus,
        note:
          row.selection?.note ??
          row.selectionNote ??
          row.note ??
          null,
        selectedByAdmin:
          row.selection?.selectedByAdmin ?? row.selectedByAdmin ?? null,
        selectedAt: row.selection?.selectedAt ?? row.selectedAt ?? null,
        walkInDate:
          row.selection?.walkInDate ?? row.walkInDate ?? row.currentWalkInDate ?? null,
        resumeJoiningDate:
          row.selection?.resumeJoiningDate ??
          row.resumeJoiningDate ??
          row.currentJoiningDate ??
          null,
        joiningDate:
          row.selection?.joiningDate ?? row.joiningDate ?? row.currentJoiningDate ?? null,
        joinedReason:
          row.selection?.joinedReason ?? row.joinedReason ?? row.joiningNote ?? null,
        joiningNote:
          row.selection?.joiningNote ?? row.joinedReason ?? row.joiningNote ?? null,
        dropoutReason:
          row.selection?.dropoutReason ?? row.dropoutReason ?? null,
      }
    : null;

  return {
    workflowStatus,
    status: workflowStatus,
    selection,
    allowedNextStatuses,
    canRollback,
    statusHistory:
      options.includeStatusHistory === false ? undefined : buildStatusHistory(row),
  };
};

const normalizePositiveRevenue = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.round(parsed * 100) / 100;
};

const resolveRevenueAmount = (...candidates) => {
  for (const candidate of candidates) {
    const normalized = normalizePositiveRevenue(candidate);
    if (normalized !== null) return normalized;
  }
  return null;
};

const isTimestampWithinInclusiveRange = (value, range) => {
  if (!value) return false;
  if (!range?.hasDateRange) return true;
  return value >= range.startDateTime && value <= range.endDateTime;
};

const getAdminRollbackTarget = (resume) => {
  return resolveRollbackSourceStatus(resume) || "";
};

const fetchAdminResumeWorkflowPayload = async (connection, resId) => {
  const [rows] = await connection.query(
    `SELECT
      rd.res_id AS resId,
      rd.job_jid AS jobJid,
      rd.rid AS recruiterRid,
      rd.uploaded_at AS uploadedAt,
      rd.ats_raw_json AS atsRawJson,
      c.name AS candidateName,
      c.email AS candidateEmail,
      c.phone AS candidatePhone,
      DATE_FORMAT(c.walk_in, '%Y-%m-%d') AS walkInDate,
      DATE_FORMAT(c.joining_date, '%Y-%m-%d') AS joiningDate,
      DATE_FORMAT(c.walk_in, '%Y-%m-%d') AS currentWalkInDate,
      DATE_FORMAT(c.joining_date, '%Y-%m-%d') AS currentJoiningDate,
      jrs.selection_status AS selectionStatus,
      jrs.selection_note AS selectionNote,
      jrs.selected_by_admin AS selectedByAdmin,
      jrs.selected_at AS selectedAt,
      ei.submitted_reason AS submittedReason,
      ei.verified_reason AS verifiedReason,
      ei.walk_in_reason AS walkInReason,
      ei.select_reason AS selectReason,
      ei.shortlisted_reason AS shortlistedReason,
      ei.reject_reason AS rejectReason,
      ei.joined_reason AS joinedReason,
      ei.dropout_reason AS dropoutReason,
      ei.billed_reason AS billedReason,
      ei.left_reason AS leftReason,
      ei.verified_at AS verifiedAt,
      ei.walk_in_at AS walkInAt,
      ei.selected_at AS selectedAtHistory,
      ei.shortlisted_at AS shortlistedAt,
      ei.joined_at AS joinedAt,
      ei.billed_at AS billedAt,
      ei.left_at AS leftAt,
      ei.dropout_at AS dropoutAt,
      ei.rejected_at AS rejectedAt
    FROM resumes_data rd
    LEFT JOIN candidate c ON c.res_id = rd.res_id
    LEFT JOIN job_resume_selection jrs
      ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
    LEFT JOIN extra_info ei
      ON ei.res_id = rd.res_id OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)
    WHERE rd.res_id = ?
    LIMIT 1`,
    [resId],
  );

  if (rows.length === 0) return null;

  const row = rows[0];
  const parsedResumePayload = parseJsonField(row.atsRawJson);
  const candidateSnapshot = extractCandidateSnapshot({
    source: {
      candidate_name: row.candidateName,
      candidate_email: row.candidateEmail,
      candidate_phone: row.candidatePhone,
      job_jid: row.jobJid,
      recruiter_rid: row.recruiterRid,
    },
    parsedData:
      parsedResumePayload?.parsed_data ||
      parsedResumePayload?.parsedData ||
      parsedResumePayload,
    fallback: {
      jobJid: row.jobJid,
      recruiterRid: row.recruiterRid,
    },
  });
  const workflowFields = buildWorkflowResponseFields({
    ...row,
    workflowStatus: row.selectionStatus,
    selectedAt: row.selectedAt,
  });
  const compatibilityFields = buildResumeCompatibilityFields({
    ...row,
    ...workflowFields,
    candidateName: candidateSnapshot.name || row.candidateName || null,
    candidatePhone: candidateSnapshot.phone || row.candidatePhone || null,
    reason: row.selectionNote || null,
    note: row.selectionNote || null,
  });

  return {
    ...compatibilityFields,
    recruiterRid: row.recruiterRid || null,
    candidateEmail: candidateSnapshot.email || row.candidateEmail || null,
    name: compatibilityFields.candidateName,
    email: candidateSnapshot.email || row.candidateEmail || null,
    phone: compatibilityFields.candidatePhone,
    ...workflowFields,
  };
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
  const reason = resolveStatusReasonInput(req.body, newStatus);
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
  const submittedRevenueAmount = resolveRevenueAmount(
    req.body?.revenue,
    req.body?.revenueAmount,
    req.body?.candidateRevenue,
    req.body?.companyRevenue,
    req.body?.companyRev,
    req.body?.company_rev,
  );

  const allowedNewStatuses = new Set([
    "verified",
    "walk_in",
    "selected",
    "shortlisted",
    "rejected",
    "joined",
    "dropout",
    "billed",
    "left",
  ]);
  if (!allowedNewStatuses.has(newStatus)) {
    return res.status(400).json({ message: "Invalid target status." });
  }

  const effectiveReason = reason;

  const joiningDateRequiredStatuses = new Set(["selected"]);

  if (joiningDateRequiredStatuses.has(newStatus)) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate)) {
      return res.status(400).json({
        message: "joining_date is required in YYYY-MM-DD format.",
      });
    }
  } else if (newStatus === "shortlisted" && joiningDate) {
    return res.status(400).json({
      message: "joining_date should only be provided for selected.",
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

  const isBilledStatus = newStatus === "billed";

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
        c.revenue AS candidateRevenue,
        j.revenue AS companyRevenue,
        c.joining_date AS currentJoiningDate,
        COALESCE(jrs.selection_status, '') AS currentStatus
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
      LEFT JOIN jobs j ON j.jid = rd.job_jid
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
    const currentDerivedStatus = resolveCanonicalWorkflowStatus({
      workflowStatus: currentStatus,
      joiningDate: resume.currentJoiningDate,
    });
    const resolvedRevenueAmount = resolveRevenueAmount(
      resume.candidateRevenue,
      resume.companyRevenue,
    );
    const joinedRevenueAmount =
      newStatus === "joined" ? submittedRevenueAmount : undefined;
    const billedRevenueAmount = isBilledStatus
      ? resolvedRevenueAmount
      : undefined;

    if (
      newStatus === "joined" &&
      (!Number.isFinite(joinedRevenueAmount) || joinedRevenueAmount <= 0)
    ) {
      await connection.rollback();
      return res.status(422).json({
        message: "Revenue amount is required before moving candidate to joined.",
      });
    }

    // Validate transition. Allow billed->billed as an idempotent admin retry.
    const isIdempotentBilledRetry =
      currentDerivedStatus === "billed" && newStatus === "billed";
    if (!isIdempotentBilledRetry) {
      const allowedTransitions = getAllowedNextStatuses(currentDerivedStatus);
      if (!allowedTransitions.includes(newStatus)) {
        await connection.rollback();
        return res
          .status(400)
          .json(buildInvalidTransitionPayload(currentDerivedStatus, newStatus));
      }
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

    const effectiveJoiningDate = /^\d{4}-\d{2}-\d{2}$/.test(joiningDate)
      ? joiningDate
      : resume.currentJoiningDate || null;
    const joiningDateValue =
      newStatus === "selected" || newStatus === "joined"
        ? effectiveJoiningDate
        : null;
    const selectionNoteValue =
      newStatus === "joined" ? joinedReason || effectiveReason || null : effectiveReason || null;

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
          newStatus,
          selectionNoteValue,
        ],
      );
    }

    let candidateJoiningDateValue = resume.currentJoiningDate || null;
    if (newStatus === "shortlisted") {
      candidateJoiningDateValue = null;
    } else if (newStatus === "selected") {
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
      revenue:
        newStatus === "joined"
          ? joinedRevenueAmount
          : newStatus === "billed"
            ? billedRevenueAmount
            : undefined,
    });

    const reasonField = STATUS_REASON_FIELD_MAP[newStatus];
    const statusReasonValue =
      newStatus === "joined" ? joinedReason : effectiveReason || null;
    const statusTimestampFieldMap = {
      verified: "verifiedAt",
      walk_in: "walkInAt",
      further: "furtherAt",
      selected: "selectedAt",
      shortlisted: "shortlistedAt",
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

    if (newStatus === "joined") {
      const intakeEntry = await addCandidateBillIntakeEntry(
        connection,
        normalizedResId,
        {
          amount: joinedRevenueAmount,
          reason:
            joinedReason ||
            effectiveReason ||
            "candidate joined revenue",
        },
      );
      if (!intakeEntry) {
        throw new Error(
          "Failed to create joined intake entry in money_sum for this candidate.",
        );
      }
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
          amount: billedRevenueAmount,
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

    const updatedResumePayload = await fetchAdminResumeWorkflowPayload(
      connection,
      normalizedResId,
    );
    const responseFields = buildResumeCompatibilityFields({
      ...updatedResumePayload,
      reason: newStatus === "joined" ? joinedReason : effectiveReason || null,
      note: newStatus === "joined" ? joinedReason : effectiveReason || null,
    });

    await connection.commit();
    return res.status(200).json({
      message: "Status updated successfully.",
      data: {
        ...updatedResumePayload,
        ...responseFields,
        reason: newStatus === "joined" ? joinedReason : effectiveReason || null,
        verifiedReason:
          newStatus === CANONICAL_VERIFY_STATUS
            ? responseFields.verifiedReason
            : updatedResumePayload?.verifiedReason ?? null,
        joining_date: responseFields.joiningDate ?? joiningDateValue,
        revenue:
          newStatus === "joined"
            ? joinedRevenueAmount
            : resolvedRevenueAmount,
        company_rev:
          newStatus === "billed"
            ? billedRevenueAmount
            : resolvedRevenueAmount,
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
        COALESCE(jrs.selection_status, 'submitted') AS currentStatus,
        c.joining_date AS currentJoiningDate,
        c.walk_in AS currentWalkInDate,
        ei.verified_reason AS verifiedReason,
        ei.verified_at AS verifiedAt,
        ei.walk_in_at AS walkInAt,
        ei.shortlisted_at AS shortlistedAt
      FROM resumes_data rd
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
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
    const currentStatus = normalizeWorkflowStatus(resume.currentStatus);
    const currentDerivedStatus = resolveCanonicalWorkflowStatus({
      workflowStatus: currentStatus,
      joiningDate: resume.currentJoiningDate,
    });
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
      await connection.query(
        `INSERT INTO job_resume_selection
          (job_jid, res_id, selected_by_admin, selection_status, selection_note)
         VALUES (?, ?, 'admin-rollback', ?, NULL)
         ON DUPLICATE KEY UPDATE
          selected_by_admin = VALUES(selected_by_admin),
          selection_status = VALUES(selection_status),
          selection_note = VALUES(selection_note),
          selected_at = CURRENT_TIMESTAMP`,
        [resume.jobJid, normalizedResId, rollbackTarget],
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
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        joinedReason: null,
      });
    }

    if (currentDerivedStatus === "dropout") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        dropoutReason: null,
      });
    }

    if (currentDerivedStatus === "left") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        leftReason: null,
      });
    }

    if (currentDerivedStatus === "billed") {
      await upsertExtraInfoFields(connection, {
        resId: normalizedResId,
        jobJid: resume.jobJid || undefined,
        recruiterRid: resume.rid || undefined,
        billedReason: null,
      });
    }

    const updatedResumePayload = await fetchAdminResumeWorkflowPayload(
      connection,
      normalizedResId,
    );

    await connection.commit();
    return res.status(200).json({
      message: "Resume rolled back successfully.",
      data: {
        previousStatus: currentDerivedStatus,
        ...updatedResumePayload,
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
});

router.get("/api/admin/performance", async (req, res) => {
  try {
    await ensureRecruiterAccountStatusColumn();
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
        COALESCE(NULLIF(TRIM(r.account_status), ''), 'active') AS accountStatus,
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
      accountStatus:
        String(row.accountStatus || "active").trim().toLowerCase() === "inactive"
          ? "inactive"
          : "active",
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
        COALESCE(NULLIF(TRIM(r.account_status), ''), 'active') AS accountStatus,
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
            ei.shortlisted_at,
            CASE
              WHEN jrs.selection_status = 'selected' AND c.joining_date IS NOT NULL
                THEN CAST(CONCAT(c.joining_date, ' 00:00:00.000000') AS DATETIME(6))
              ELSE NULL
            END
          ),
          '%Y-%m-%d %H:%i:%s.%f'
        ) AS shortlistedAt,
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
        COALESCE(jrs.selection_status, 'submitted') AS workflowStatus,
        recruiter.rid AS recruiterRid,
        recruiter.name AS recruiterName,
        recruiter.role AS recruiterRole,
        teamLeader.rid AS teamLeaderRid,
        teamLeader.name AS teamLeaderName,
        statusActor.rid AS statusActorRid,
        statusActor.name AS statusActorName,
        statusActor.role AS statusActorRole,
        jrs.selected_by_admin AS selectedByAdmin,
        jrs.selection_note AS selectionNote,
        c.name AS candidateName,
        c.phone AS candidatePhone,
        c.revenue AS candidateRevenue,
        j.revenue AS companyRevenue,
        j.company_name AS companyName,
        j.city AS city,
        ei.office_location_city AS officeLocationCity,
        ei.submitted_reason AS submittedReason,
        ei.verified_reason AS verifiedReason,
        ei.walk_in_reason AS walkInReason,
        ei.select_reason AS selectReason,
        ei.shortlisted_reason AS shortlistedReason,
        ei.reject_reason AS rejectReason,
        ei.joined_reason AS joinedReason,
        ei.dropout_reason AS dropoutReason,
        ei.billed_reason AS billedReason,
        ei.left_reason AS leftReason
      FROM resumes_data rd
      LEFT JOIN candidate c ON c.res_id = rd.res_id
      INNER JOIN recruiter recruiter ON recruiter.rid = rd.rid
      LEFT JOIN job_resume_selection jrs
        ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id
      LEFT JOIN jobs j ON j.jid = rd.job_jid
      LEFT JOIN recruiter teamLeader ON teamLeader.rid = j.recruiter_rid
      LEFT JOIN recruiter statusActor ON statusActor.rid = jrs.selected_by_admin
      LEFT JOIN extra_info ei
        ON ei.res_id = rd.res_id OR ei.resume_id = rd.res_id
      WHERE COALESCE(LOWER(TRIM(rd.submitted_by_role)), 'recruiter') <> 'candidate'
        AND LOWER(TRIM(COALESCE(recruiter.role, 'recruiter'))) IN (
          'recruiter',
          'team leader',
          'team_leader',
          'job creator'
        )
      ORDER BY rd.uploaded_at DESC, rd.res_id DESC`,
    );

    const normalizePerformanceDrilldownItem = (row) => {
      const workflowFields = buildWorkflowResponseFields(row);
      return {
        resId: row.resId || null,
        recruiterName: row.recruiterName || null,
        recruiterRid: row.recruiterRid || null,
        teamLeaderRid: row.teamLeaderRid || null,
        teamLeaderName: row.teamLeaderName || null,
        statusActorRid: row.statusActorRid || null,
        statusActorName: row.statusActorName || null,
        selectedByAdmin: row.selectedByAdmin || null,
        name: row.candidateName || null,
        candidatePhone: row.candidatePhone || null,
        phone: row.candidatePhone || null,
        jobJid:
          row.jobJid === null || row.jobJid === undefined
            ? null
            : String(row.jobJid).trim(),
        companyName: row.companyName || null,
        officeLocationCity: row.officeLocationCity || null,
        city: row.city || null,
        resumeFilename: row.resumeFilename || null,
        candidateName: row.candidateName || null,
        walkInDate: row.walkInDate || null,
        joiningDate: row.joiningDate || null,
        selectionNote: row.selectionNote || null,
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        walkInReason: row.walkInReason || null,
        selectReason: row.selectReason || null,
        shortlistedReason: row.shortlistedReason || null,
        rejectReason: row.rejectReason || null,
        joinedReason: row.joinedReason || null,
        joiningNote: row.joinedReason || null,
        dropoutReason: row.dropoutReason || null,
        billedReason: row.billedReason || null,
        leftReason: row.leftReason || null,
        latestNote: resolveLatestPerformanceNote(row),
        note: resolveLatestPerformanceNote(row),
        eventAt: row.eventAt || null,
        revenue: row.revenue,
        company_rev: row.company_rev,
        ...workflowFields,
      };
    };

    const statusDrilldown = {
      submitted: [],
      verified: [],
      walk_in: [],
      selected: [],
      shortlisted: [],
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
          accountStatus:
            String(row.accountStatus || "active").trim().toLowerCase() ===
            "inactive"
              ? "inactive"
              : "active",
          points: Number(row.points) || 0,
          submitted: 0,
          verified: 0,
          walk_in: 0,
          further: 0,
          selected: 0,
          shortlisted: 0,
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

      const row = {
        ...rawRow,
        submittedAt: normalizePerformanceTimestamp(rawRow.submittedAt),
        verifiedAt: normalizePerformanceTimestamp(rawRow.verifiedAt),
        walkInAt: normalizePerformanceTimestamp(rawRow.walkInAt),
        selectedAt: normalizePerformanceTimestamp(rawRow.selectedAt),
        rejectedAt: normalizePerformanceTimestamp(rawRow.rejectedAt),
        shortlistedAt: normalizePerformanceTimestamp(rawRow.shortlistedAt),
        joinedAt: normalizePerformanceTimestamp(rawRow.joinedAt),
        dropoutAt: normalizePerformanceTimestamp(rawRow.dropoutAt),
        billedAt: normalizePerformanceTimestamp(rawRow.billedAt),
        leftAt: normalizePerformanceTimestamp(rawRow.leftAt),
        furtherAt: normalizePerformanceTimestamp(rawRow.furtherAt),
        onHoldAt: normalizePerformanceTimestamp(rawRow.onHoldAt),
        revenue: resolveRevenueAmount(
          rawRow.candidateRevenue,
          rawRow.companyRevenue,
        ),
      };
      row.company_rev = row.revenue;
      row.workflowStatus = resolveCanonicalWorkflowStatus({
        workflowStatus: row.workflowStatus,
        joiningDate: row.joiningDate,
      });

      const currentEventAtMap = {
        submitted: row.submittedAt,
        verified: row.verifiedAt,
        walk_in: row.walkInAt,
        selected: row.selectedAt,
        rejected: row.rejectedAt,
        shortlisted: row.shortlistedAt,
        joined: row.joinedAt,
        dropout: row.dropoutAt,
        billed: row.billedAt,
        left: row.leftAt,
      };

      const eventTeamLeaderByMetric = {
        submitted: isTeamLeaderLikeRole(row.recruiterRole)
          ? {
              rid: row.recruiterRid || null,
              name: row.recruiterName || null,
            }
          : null,
        verified: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        walk_in: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        selected: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        rejected: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        shortlisted: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        joined: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        dropout: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        billed: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
        left: isTeamLeaderLikeRole(row.statusActorRole)
          ? {
              rid: row.statusActorRid || null,
              name: row.statusActorName || null,
            }
          : null,
      };

      for (const metricKey of PERFORMANCE_EVENT_KEYS) {
        const eventAt = currentEventAtMap[metricKey];
        if (!isTimestampWithinInclusiveRange(eventAt, dateRange)) continue;
        const effectiveTeamLeader = eventTeamLeaderByMetric[metricKey];

        statusDrilldown[metricKey].push(
          normalizePerformanceDrilldownItem({
            ...row,
            eventAt,
            teamLeaderRid: effectiveTeamLeader?.rid || null,
            teamLeaderName: effectiveTeamLeader?.name || null,
          }),
        );
        if (recruiterStats) {
          recruiterStats[PERFORMANCE_EVENT_META[metricKey].recruiterField] += 1;
          if (!recruiterStats.lastUpdated || eventAt > recruiterStats.lastUpdated) {
            recruiterStats.lastUpdated = eventAt;
          }
        }
      }
    }

    const recruiters = Array.from(recruiterMap.values())
      .map((row) => {
        const submitted = row.submitted;
        const verified = row.verified;
        const selected = row.selected;
        const shortlisted = row.shortlisted;
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
              ? Number(((shortlisted / verified) * 100).toFixed(1))
              : 0,
          joiningRate:
            selected > 0
              ? Number(((joined / selected) * 100).toFixed(1))
              : 0,
          dropoutRate:
            selected > 0
              ? Number(((dropout / selected) * 100).toFixed(1))
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
      totalShortlisted: 0,
      totalJoined: 0,
      totalDropout: 0,
      totalRejected: 0,
      totalBilled: 0,
      totalLeft: 0,
    };

    for (const metricKey of PERFORMANCE_EVENT_KEYS) {
      summary[PERFORMANCE_EVENT_META[metricKey].summaryField] =
        statusDrilldown[metricKey].length;
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
router.patch("/api/admin/recruiters/:rid/account-status", async (req, res) => {
  if (!ensureAdminAuthorized(req, res)) return;

  const rid = String(req.params?.rid || "").trim();
  const requestedStatus = String(req.body?.status || "")
    .trim()
    .toLowerCase();

  if (!rid) {
    return res.status(400).json({ message: "Recruiter ID is required." });
  }

  if (!["active", "inactive"].includes(requestedStatus)) {
    return res.status(400).json({
      message: "status must be either 'active' or 'inactive'.",
    });
  }

  try {
    await ensureRecruiterAccountStatusColumn();
    const hasPasswordChangedColumn = await columnExists(
      "recruiter",
      "password_changed",
    );
    const [recruiterRows] = await pool.query(
      `SELECT rid, name, email, role,
              COALESCE(NULLIF(TRIM(account_status), ''), 'active') AS accountStatus
       FROM recruiter
       WHERE rid = ?
       LIMIT 1`,
      [rid],
    );

    if (recruiterRows.length === 0) {
      return res.status(404).json({ message: "Recruiter not found." });
    }

    const nextPassword =
      requestedStatus === "inactive"
        ? crypto.randomBytes(24).toString("hex")
        : "12345678";
    const updateFields = ["password = ?", "account_status = ?"];
    const updateParams = [nextPassword, requestedStatus];

    if (hasPasswordChangedColumn) {
      updateFields.push("password_changed = ?");
      updateParams.push(requestedStatus === "active" ? false : true);
    }

    updateParams.push(rid);

    await pool.query(
      `UPDATE recruiter SET ${updateFields.join(", ")} WHERE rid = ?`,
      updateParams,
    );

    return res.status(200).json({
      message:
        requestedStatus === "inactive"
          ? "Account deactivated successfully."
          : "Account activated successfully.",
      recruiter: {
        rid: recruiterRows[0].rid,
        name: recruiterRows[0].name,
        email: recruiterRows[0].email,
        role: recruiterRows[0].role,
        accountStatus: requestedStatus,
      },
    });
  } catch (error) {
    return res.status(500).json({
      message: "Failed to update recruiter account status.",
      error: error.message,
    });
  }
});

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
