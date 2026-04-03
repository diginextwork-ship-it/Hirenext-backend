const express = require("express");
const pool = require("../config/db");
const { requireAuth, requireRoles } = require("../middleware/auth");
const { escapeLike } = require("../utils/formatters");
const { tableExists } = require("../utils/dbHelpers");
const { parseInclusiveDateRange } = require("../utils/dateTime");

const router = express.Router();

const toRole = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();
const toRid = (value) => String(value || "").trim();
const toNullableNumber = (value) =>
  value === null || value === undefined ? null : Number(value);
const isValidDateOnly = (value) => {
  const normalized = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return false;

  const [yearText, monthText, dayText] = normalized.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  if (
    !Number.isInteger(year) ||
    !Number.isInteger(month) ||
    !Number.isInteger(day)
  )
    return false;

  const parsed = new Date(Date.UTC(year, month - 1, day));
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() === month - 1 &&
    parsed.getUTCDate() === day
  );
};
const addDaysDateOnly = (value, days) => {
  const [yearText, monthText, dayText] = String(value).split("-");
  const parsed = new Date(
    Date.UTC(Number(yearText), Number(monthText) - 1, Number(dayText)),
  );
  parsed.setUTCDate(parsed.getUTCDate() + Number(days || 0));
  return parsed.toISOString().slice(0, 10);
};
const parseDateRange = (startRaw, endRaw) => {
  const startDate = String(startRaw || "").trim();
  const endDate = String(endRaw || "").trim();

  if (!startDate && !endDate) {
    return {
      startDate: null,
      endDate: null,
      startDateTime: null,
      endExclusiveDateTime: null,
      hasDateRange: false,
      error: null,
    };
  }

  if (!startDate || !endDate) {
    return {
      error: "Both startDate and endDate are required when filtering by date.",
    };
  }

  if (!isValidDateOnly(startDate) || !isValidDateOnly(endDate)) {
    return {
      error: "Invalid date format. Use YYYY-MM-DD for startDate and endDate.",
    };
  }

  if (startDate > endDate) {
    return {
      error: "startDate cannot be after endDate.",
    };
  }

  return {
    startDate,
    endDate,
    startDateTime: `${startDate} 00:00:00`,
    endExclusiveDateTime: `${addDaysDateOnly(endDate, 1)} 00:00:00`,
    hasDateRange: true,
    error: null,
  };
};
const recruiterStatsSubquery = `
  SELECT
    rd.rid AS recruiter_rid,
    COUNT(*) AS submitted,
    SUM(CASE WHEN jrs.selection_status = 'verified' THEN 1 ELSE 0 END) AS verified,
    SUM(CASE WHEN jrs.selection_status = 'walk_in' THEN 1 ELSE 0 END) AS walk_in,
    SUM(CASE WHEN jrs.selection_status = 'further' THEN 1 ELSE 0 END) AS further,
    SUM(CASE WHEN jrs.selection_status = 'selected' THEN 1 ELSE 0 END) AS selected,
    SUM(CASE WHEN jrs.selection_status IN ('shortlisted', 'pending_joining') THEN 1 ELSE 0 END) AS shortlisted,
    SUM(CASE WHEN jrs.selection_status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
    SUM(CASE WHEN jrs.selection_status = 'joined' THEN 1 ELSE 0 END) AS joined,
    SUM(CASE WHEN jrs.selection_status = 'dropout' THEN 1 ELSE 0 END) AS dropout,
    SUM(CASE WHEN jrs.selection_status = 'billed' THEN 1 ELSE 0 END) AS billed,
    SUM(CASE WHEN jrs.selection_status = 'left' THEN 1 ELSE 0 END) AS \`left\`,
    MAX(COALESCE(jrs.selected_at, rd.uploaded_at)) AS last_updated,
    MIN(rd.uploaded_at) AS created_at
  FROM resumes_data rd
  LEFT JOIN job_resume_selection jrs
    ON jrs.job_jid = rd.job_jid
   AND jrs.res_id = rd.res_id
  WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
  GROUP BY rd.rid
`;

const isTeamLeaderRole = (role) => {
  const normalized = toRole(role);
  return normalized === "team leader" || normalized === "team_leader";
};

const isRecruiterRole = (role) => toRole(role) === "recruiter";

const assertOwnRidOrTeamLeader = (req, res) => {
  const authRole = toRole(req.auth?.role);
  const authRid = toRid(req.auth?.rid);
  const requestedRid = toRid(req.params?.rid);

  if (isTeamLeaderRole(authRole)) return true;
  if (
    isRecruiterRole(authRole) &&
    authRid &&
    requestedRid &&
    authRid === requestedRid
  )
    return true;

  res.status(403).json({
    error: "Forbidden: You can only access your own data",
  });
  return false;
};

const buildCalculatedMetrics = (stats) => {
  const submitted = toNullableNumber(stats.submitted);
  const verified = toNullableNumber(stats.verified);
  const selected = toNullableNumber(stats.select);
  const shortlisted = toNullableNumber(stats.shortlisted);
  const joined = toNullableNumber(stats.joined);
  const dropout = toNullableNumber(stats.dropout);
  const billed = toNullableNumber(stats.billed);
  const left = toNullableNumber(stats.left);

  return {
    verificationRate:
      submitted && submitted > 0 && verified !== null
        ? Number(((verified / submitted) * 100).toFixed(2))
        : null,
    selectionRate:
      verified && verified > 0 && shortlisted !== null
        ? Number(((shortlisted / verified) * 100).toFixed(2))
        : null,
    joiningRate:
      selected && selected > 0 && joined !== null
        ? Number(((joined / selected) * 100).toFixed(2))
        : null,
    dropoutRate:
      selected && selected > 0 && dropout !== null
        ? Number(((dropout / selected) * 100).toFixed(2))
        : null,
    billingRate:
      joined && joined > 0 && billed !== null
        ? Number(((billed / (joined + billed + (left || 0))) * 100).toFixed(2))
        : null,
    leftRate:
      billed && billed > 0 && left !== null
        ? Number(((left / (billed + left)) * 100).toFixed(2))
        : null,
  };
};

const mapStats = (row) => ({
  submitted: Number(row.submitted) || 0,
  verified: Number(row.verified) || 0,
  walk_in: Number(row.walk_in) || 0,
  further: Number(row.further) || 0,
  shortlisted: Number(row.shortlisted) || 0,
  select: Number(row.select) || 0,
  reject: Number(row.reject) || 0,
  joined: Number(row.joined) || 0,
  dropout: Number(row.dropout) || 0,
  billed: Number(row.billed) || 0,
  left: Number(row.left) || 0,
  last_updated: row.last_updated || null,
  created_at: row.created_at || null,
});

const TEAM_LEADER_PERFORMANCE_EVENT_KEYS = [
  "submitted",
  "verified",
  "walk_in",
  "shortlisted",
  "selected",
  "rejected",
  "joined",
  "dropout",
  "billed",
  "left",
];

const TEAM_LEADER_PERFORMANCE_EVENT_META = {
  submitted: { summaryField: "totalSubmitted" },
  verified: { summaryField: "totalVerified" },
  walk_in: { summaryField: "totalWalkIn" },
  shortlisted: { summaryField: "totalShortlisted" },
  selected: { summaryField: "totalSelected" },
  rejected: { summaryField: "totalRejected" },
  joined: { summaryField: "totalJoined" },
  dropout: { summaryField: "totalDropout" },
  billed: { summaryField: "totalBilled" },
  left: { summaryField: "totalLeft" },
};

const normalizePerformanceTimestamp = (value) =>
  value == null ? null : String(value).trim() || null;

const normalizeWorkflowStatus = (value, joiningDate = null) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");

  if (!normalized) return "submitted";
  if (normalized === "walkin") return "walk_in";
  if (normalized === "select") return "selected";
  if (normalized === "pendingjoining" || normalized === "pending_joining") {
    return "shortlisted";
  }
  if (normalized === "shortlisted" && joiningDate) return "selected";

  return TEAM_LEADER_PERFORMANCE_EVENT_KEYS.includes(normalized)
    ? normalized
    : "submitted";
};

const isTimestampWithinInclusiveRange = (value, range) => {
  if (!value) return false;
  if (!range?.hasDateRange) return true;
  return value >= range.startDateTime && value <= range.endDateTime;
};

router.get(
  "/api/status/recruiter/:rid",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    if (!assertOwnRidOrTeamLeader(req, res)) return;

    const rid = toRid(req.params.rid);
    if (!rid) return res.status(400).json({ error: "rid is required." });

    try {
      const [rows] = await pool.query(
        `SELECT
          r.rid,
          r.name,
          r.email,
          COALESCE(r.points, 0) AS points,
          COALESCE(rs.submitted, 0) AS submitted,
          COALESCE(rs.verified, 0) AS verified,
          COALESCE(rs.walk_in, 0) AS walk_in,
          COALESCE(rs.further, 0) AS further,
          COALESCE(rs.shortlisted, 0) AS shortlisted,
          COALESCE(rs.selected, 0) AS \`select\`,
          COALESCE(rs.rejected, 0) AS reject,
          COALESCE(rs.joined, 0) AS joined,
          COALESCE(rs.dropout, 0) AS dropout,
          COALESCE(rs.billed, 0) AS billed,
          COALESCE(rs.\`left\`, 0) AS \`left\`,
          rs.last_updated,
          rs.created_at
        FROM recruiter r
        LEFT JOIN (${recruiterStatsSubquery}) rs ON r.rid = rs.recruiter_rid
        WHERE r.rid = ?
          AND LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'
        LIMIT 1`,
        [rid],
      );

      if (rows.length === 0) {
        return res.status(404).json({ error: "Recruiter not found." });
      }

      const row = rows[0];
      const stats = mapStats(row);
      return res.status(200).json({
        recruiter: {
          rid: row.rid,
          name: row.name,
          email: row.email,
          points: Number(row.points) || 0,
        },
        stats,
        calculatedMetrics: buildCalculatedMetrics(stats),
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch recruiter performance stats.",
        details: error.message,
      });
    }
  },
);

router.get(
  "/api/status/all",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  async (req, res) => {
    const search = String(req.query?.search || "").trim();
    const sortBy = String(req.query?.sortBy || "submitted")
      .trim()
      .toLowerCase();
    const sortOrder =
      String(req.query?.sortOrder || "desc")
        .trim()
        .toLowerCase() === "asc"
        ? "ASC"
        : "DESC";

    const sortMap = {
      name: "r.name",
      email: "r.email",
      submitted: "COALESCE(rs.submitted, 0)",
      verified: "COALESCE(rs.verified, 0)",
      walk_in: "COALESCE(rs.walk_in, 0)",
      shortlisted: "COALESCE(rs.shortlisted, 0)",
      select: "COALESCE(rs.selected, 0)",
      reject: "COALESCE(rs.rejected, 0)",
      joined: "COALESCE(rs.joined, 0)",
      dropout: "COALESCE(rs.dropout, 0)",
      billed: "COALESCE(rs.billed, 0)",
      left: "COALESCE(rs.`left`, 0)",
      points: "COALESCE(r.points, 0)",
      last_updated: "rs.last_updated",
    };

    const orderBySql = sortMap[sortBy] || sortMap.submitted;
    const whereClauses = [
      "LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'",
    ];
    const params = [];

    if (search) {
      const safeLike = `%${escapeLike(search)}%`;
      whereClauses.push(
        "(r.name LIKE ? ESCAPE '\\\\' OR r.email LIKE ? ESCAPE '\\\\')",
      );
      params.push(safeLike, safeLike);
    }

    const whereSql = whereClauses.join(" AND ");

    try {
      const [rows] = await pool.query(
        `SELECT
          r.rid,
          r.name,
          r.email,
          COALESCE(r.points, 0) AS points,
          COALESCE(rs.submitted, 0) AS submitted,
          COALESCE(rs.verified, 0) AS verified,
          COALESCE(rs.walk_in, 0) AS walk_in,
          COALESCE(rs.further, 0) AS further,
          COALESCE(rs.shortlisted, 0) AS shortlisted,
          COALESCE(rs.selected, 0) AS \`select\`,
          COALESCE(rs.rejected, 0) AS reject,
          COALESCE(rs.joined, 0) AS joined,
          COALESCE(rs.dropout, 0) AS dropout,
          COALESCE(rs.billed, 0) AS billed,
          COALESCE(rs.\`left\`, 0) AS \`left\`,
          rs.last_updated
        FROM recruiter r
        LEFT JOIN (${recruiterStatsSubquery}) rs ON r.rid = rs.recruiter_rid
        WHERE ${whereSql}
        ORDER BY ${orderBySql} ${sortOrder}, r.name ASC`,
        params,
      );

      const recruiters = rows.map((row) => {
        const stats = mapStats(row);
        return {
          rid: row.rid,
          name: row.name,
          email: row.email,
          points: Number(row.points) || 0,
          stats,
          calculatedMetrics: buildCalculatedMetrics(stats),
        };
      });

      const totalSubmitted = recruiters.reduce(
        (sum, item) => sum + item.stats.submitted,
        0,
      );
      const totalVerified = recruiters.reduce(
        (sum, item) => sum + item.stats.verified,
        0,
      );
      const totalJoined = recruiters.reduce(
        (sum, item) => sum + item.stats.joined,
        0,
      );
      const totalBilled = recruiters.reduce(
        (sum, item) => sum + item.stats.billed,
        0,
      );
      const totalLeft = recruiters.reduce(
        (sum, item) => sum + item.stats.left,
        0,
      );

      return res.status(200).json({
        recruiters,
        total: recruiters.length,
        summary: {
          totalSubmitted,
          totalVerified,
          totalJoined,
          totalBilled,
          totalLeft,
          avgSubmittedPerRecruiter: recruiters.length
            ? Number((totalSubmitted / recruiters.length).toFixed(2))
            : 0,
        },
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch recruiter statistics.",
        details: error.message,
      });
    }
  },
);

const getTeamLeaderDashboard = async (_req, res) => {
  try {
    const [[jobsOverview]] = await pool.query(
      `SELECT
        COUNT(*) AS totalJobs,
        SUM(CASE WHEN access_mode = 'open' THEN 1 ELSE 0 END) AS openJobs,
        SUM(CASE WHEN access_mode = 'restricted' THEN 1 ELSE 0 END) AS restrictedJobs
      FROM jobs`,
    );

    const [[recruiterOverview]] = await pool.query(
      `SELECT
        COUNT(*) AS totalRecruiters
      FROM recruiter
      WHERE LOWER(TRIM(COALESCE(role, 'recruiter'))) = 'recruiter'`,
    );

    const [[activeOverview]] = await pool.query(
      `SELECT
        COUNT(*) AS activeRecruiters,
        COALESCE(SUM(stats.submitted), 0) AS totalSubmissions
      FROM (
        SELECT rd.rid, COUNT(*) AS submitted
        FROM resumes_data rd
        WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
        GROUP BY rd.rid
      ) stats`,
    );

    const [topPerformersRows] = await pool.query(
      `SELECT
        r.rid,
        r.name,
        COALESCE(stats.submitted, 0) AS submitted,
        COALESCE(r.points, 0) AS points
      FROM recruiter r
      LEFT JOIN (
        SELECT rd.rid, COUNT(*) AS submitted
        FROM resumes_data rd
        WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
        GROUP BY rd.rid
      ) stats ON stats.rid = r.rid
      WHERE LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'
      ORDER BY COALESCE(stats.submitted, 0) DESC, COALESCE(r.points, 0) DESC, r.name ASC
      LIMIT 8`,
    );

    return res.status(200).json({
      overview: {
        totalJobs: Number(jobsOverview?.totalJobs) || 0,
        openJobs: Number(jobsOverview?.openJobs) || 0,
        restrictedJobs: Number(jobsOverview?.restrictedJobs) || 0,
        totalRecruiters: Number(recruiterOverview?.totalRecruiters) || 0,
        activeRecruiters: Number(activeOverview?.activeRecruiters) || 0,
        totalSubmissions: Number(activeOverview?.totalSubmissions) || 0,
      },
      topPerformers: topPerformersRows.map((row) => ({
        rid: row.rid,
        name: row.name,
        submitted: Number(row.submitted) || 0,
        points: Number(row.points) || 0,
      })),
    });
  } catch (error) {
    return res.status(500).json({
      error: "Failed to fetch team leader dashboard data.",
      details: error.message,
    });
  }
};

router.get(
  "/api/dashboard/team-leader",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  getTeamLeaderDashboard,
);

router.get(
  "/api/dashboard/team-leader/performance",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  async (req, res) => {
    const teamLeaderRid = toRid(req.auth?.rid);
    if (!teamLeaderRid) {
      return res.status(400).json({ error: "Authenticated team leader RID is required." });
    }

    try {
      const dateRange = parseInclusiveDateRange(
        req.query?.startDate,
        req.query?.endDate,
      );
      if (dateRange.error) {
        return res.status(400).json({ error: dateRange.error });
      }

      const [[jobsOverview]] = await pool.query(
        `SELECT
          COUNT(*) AS totalJobs,
          SUM(CASE WHEN access_mode = 'open' THEN 1 ELSE 0 END) AS openJobs,
          SUM(CASE WHEN access_mode = 'restricted' THEN 1 ELSE 0 END) AS restrictedJobs
        FROM jobs
        WHERE recruiter_rid = ?`,
        [teamLeaderRid],
      );

      const [[recruiterOverview]] = await pool.query(
        `SELECT
          COUNT(DISTINCT rd.rid) AS totalRecruiters
        FROM resumes_data rd
        INNER JOIN jobs j ON j.jid = rd.job_jid
        WHERE j.recruiter_rid = ?
          AND COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'`,
        [teamLeaderRid],
      );

      const [performanceRows] = await pool.query(
        `SELECT
          rd.res_id AS resId,
          rd.job_jid AS jobJid,
          rd.resume_filename AS resumeFilename,
          DATE_FORMAT(rd.uploaded_at, '%Y-%m-%d %H:%i:%s.%f') AS submittedAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'verified' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS verifiedAt,
          DATE_FORMAT(
            CASE
              WHEN c.walk_in IS NOT NULL THEN CAST(CONCAT(c.walk_in, ' 00:00:00.000000') AS DATETIME(6))
              WHEN jrs.selection_status = 'walk_in' THEN jrs.selected_at
              ELSE NULL
            END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS walkInAt,
          DATE_FORMAT(
            CASE
              WHEN jrs.selection_status = 'selected' AND c.joining_date IS NULL THEN jrs.selected_at
              ELSE NULL
            END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS selectedAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'rejected' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS rejectedAt,
          DATE_FORMAT(
            CASE
              WHEN jrs.selection_status = 'selected' AND c.joining_date IS NOT NULL
                THEN CAST(CONCAT(c.joining_date, ' 00:00:00.000000') AS DATETIME(6))
              WHEN jrs.selection_status IN ('shortlisted', 'pending_joining') THEN jrs.selected_at
              ELSE NULL
            END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS shortlistedAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'joined' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS joinedAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'dropout' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS dropoutAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'billed' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS billedAt,
          DATE_FORMAT(
            CASE WHEN jrs.selection_status = 'left' THEN jrs.selected_at ELSE NULL END,
            '%Y-%m-%d %H:%i:%s.%f'
          ) AS leftAt,
          DATE_FORMAT(c.walk_in, '%Y-%m-%d') AS walkInDate,
          DATE_FORMAT(c.joining_date, '%Y-%m-%d') AS joiningDate,
          COALESCE(jrs.selection_status, 'submitted') AS workflowStatus,
          recruiter.rid AS recruiterRid,
          recruiter.name AS recruiterName,
          teamLeader.name AS teamLeaderName,
          c.name AS candidateName,
          c.phone AS candidatePhone,
          j.company_name AS companyName,
          j.city AS city
        FROM resumes_data rd
        INNER JOIN jobs j
          ON j.jid = rd.job_jid
         AND j.recruiter_rid = ?
        LEFT JOIN candidate c ON c.res_id = rd.res_id
        INNER JOIN recruiter recruiter ON recruiter.rid = rd.rid
        LEFT JOIN recruiter teamLeader ON teamLeader.rid = j.recruiter_rid
        LEFT JOIN job_resume_selection jrs
          ON jrs.job_jid = rd.job_jid
         AND jrs.res_id = rd.res_id
        WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
        ORDER BY rd.uploaded_at DESC, rd.res_id DESC`,
        [teamLeaderRid],
      );

      const statusDrilldown = {
        submitted: [],
        verified: [],
        walk_in: [],
        shortlisted: [],
        selected: [],
        rejected: [],
        joined: [],
        dropout: [],
        billed: [],
        left: [],
      };

      for (const rawRow of performanceRows) {
        const row = {
          ...rawRow,
          submittedAt: normalizePerformanceTimestamp(rawRow.submittedAt),
          verifiedAt: normalizePerformanceTimestamp(rawRow.verifiedAt),
          walkInAt: normalizePerformanceTimestamp(rawRow.walkInAt),
          shortlistedAt: normalizePerformanceTimestamp(rawRow.shortlistedAt),
          selectedAt: normalizePerformanceTimestamp(rawRow.selectedAt),
          rejectedAt: normalizePerformanceTimestamp(rawRow.rejectedAt),
          joinedAt: normalizePerformanceTimestamp(rawRow.joinedAt),
          dropoutAt: normalizePerformanceTimestamp(rawRow.dropoutAt),
          billedAt: normalizePerformanceTimestamp(rawRow.billedAt),
          leftAt: normalizePerformanceTimestamp(rawRow.leftAt),
        };

        const workflowStatus = normalizeWorkflowStatus(
          row.workflowStatus,
          row.joiningDate,
        );
        const eventAtMap = {
          submitted: row.submittedAt,
          verified: row.verifiedAt,
          walk_in: row.walkInAt,
          shortlisted: row.shortlistedAt,
          selected: row.selectedAt,
          rejected: row.rejectedAt,
          joined: row.joinedAt,
          dropout: row.dropoutAt,
          billed: row.billedAt,
          left: row.leftAt,
        };

        for (const metricKey of TEAM_LEADER_PERFORMANCE_EVENT_KEYS) {
          const eventAt = eventAtMap[metricKey];
          if (!isTimestampWithinInclusiveRange(eventAt, dateRange)) continue;

          statusDrilldown[metricKey].push({
            resId: row.resId || null,
            recruiterName: row.recruiterName || null,
            recruiterRid: row.recruiterRid || null,
            teamLeaderName: row.teamLeaderName || null,
            candidateName: row.candidateName || null,
            candidatePhone: row.candidatePhone || null,
            phone: row.candidatePhone || null,
            jobJid:
              row.jobJid === null || row.jobJid === undefined
                ? null
                : String(row.jobJid).trim(),
            companyName: row.companyName || null,
            city: row.city || null,
            resumeFilename: row.resumeFilename || null,
            walkInDate: row.walkInDate || null,
            joiningDate: row.joiningDate || null,
            status: workflowStatus,
            workflowStatus,
            eventAt,
          });
        }
      }

      const summary = {
        totalJobs: Number(jobsOverview?.totalJobs) || 0,
        openJobs: Number(jobsOverview?.openJobs) || 0,
        restrictedJobs: Number(jobsOverview?.restrictedJobs) || 0,
        totalRecruiters: Number(recruiterOverview?.totalRecruiters) || 0,
        totalSubmitted: 0,
        totalVerified: 0,
        totalWalkIn: 0,
        totalShortlisted: 0,
        totalSelected: 0,
        totalRejected: 0,
        totalJoined: 0,
        totalDropout: 0,
        totalBilled: 0,
        totalLeft: 0,
      };

      for (const metricKey of TEAM_LEADER_PERFORMANCE_EVENT_KEYS) {
        summary[TEAM_LEADER_PERFORMANCE_EVENT_META[metricKey].summaryField] =
          statusDrilldown[metricKey].length;
      }

      return res.status(200).json({
        teamLeader: { rid: teamLeaderRid },
        dateRange: dateRange.hasDateRange
          ? { startDate: dateRange.startDate, endDate: dateRange.endDate }
          : null,
        summary,
        statusDrilldown,
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch team leader performance dashboard.",
        details: error.message,
      });
    }
  },
);

router.get(
  "/api/dashboard/team-leader/resumes/:resId/file",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  async (req, res) => {
    const teamLeaderRid = toRid(req.auth?.rid);
    const resId = String(req.params?.resId || "").trim();

    if (!teamLeaderRid) {
      return res.status(400).json({ message: "Authenticated team leader RID is required." });
    }
    if (!resId) {
      return res.status(400).json({ message: "resId is required." });
    }

    try {
      const [rows] = await pool.query(
        `SELECT
          rd.resume,
          rd.resume_filename AS resumeFilename,
          rd.resume_type AS resumeType
        FROM resumes_data rd
        INNER JOIN jobs j
          ON j.jid = rd.job_jid
         AND j.recruiter_rid = ?
        WHERE rd.res_id = ?
        LIMIT 1`,
        [teamLeaderRid, resId],
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

router.get(
  "/api/dashboard/recruiter/:rid",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    if (!assertOwnRidOrTeamLeader(req, res)) return;

    const rid = toRid(req.params.rid);
    if (!rid) return res.status(400).json({ error: "rid is required." });
    const {
      startDate,
      endDate,
      startDateTime,
      endExclusiveDateTime,
      hasDateRange,
      error: dateError,
    } = parseDateRange(req.query?.startDate, req.query?.endDate);
    if (dateError) {
      return res.status(400).json({ error: dateError });
    }

    try {
      const submittedRangeCondition = hasDateRange
        ? "rd.uploaded_at >= ? AND rd.uploaded_at < ?"
        : "1=1";
      const statusRangeCondition = hasDateRange
        ? "jrs.selected_at >= ? AND jrs.selected_at < ?"
        : "1=1";
      const activityRangeCondition = hasDateRange
        ? "((rd.uploaded_at >= ? AND rd.uploaded_at < ?) OR (jrs.selected_at >= ? AND jrs.selected_at < ?))"
        : "1=1";
      const statsQueryParams = [];
      if (hasDateRange) {
        for (let idx = 0; idx < 12; idx += 1) {
          statsQueryParams.push(startDateTime, endExclusiveDateTime);
        }
      }
      statsQueryParams.push(rid);

      const [recruiterRows] = await pool.query(
        `SELECT
          r.rid,
          r.name,
          r.email,
          COALESCE(r.points, 0) AS points,
          COALESCE(rs.submitted, 0) AS submitted,
          COALESCE(rs.verified, 0) AS verified,
          COALESCE(rs.walk_in, 0) AS walk_in,
          COALESCE(rs.further, 0) AS further,
          COALESCE(rs.shortlisted, 0) AS shortlisted,
          COALESCE(rs.selected, 0) AS \`select\`,
          COALESCE(rs.rejected, 0) AS reject,
          COALESCE(rs.joined, 0) AS joined,
          COALESCE(rs.dropout, 0) AS dropout,
          COALESCE(rs.billed, 0) AS billed,
          COALESCE(rs.\`left\`, 0) AS \`left\`,
          rs.last_updated
        FROM recruiter r
        LEFT JOIN (
          SELECT
            rd.rid AS recruiter_rid,
            SUM(CASE WHEN ${submittedRangeCondition} THEN 1 ELSE 0 END) AS submitted,
            SUM(CASE WHEN jrs.selection_status = 'verified' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS verified,
            SUM(CASE WHEN jrs.selection_status = 'walk_in' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS walk_in,
            SUM(CASE WHEN jrs.selection_status = 'further' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS further,
            SUM(CASE WHEN jrs.selection_status = 'selected' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS selected,
            SUM(CASE WHEN jrs.selection_status IN ('shortlisted', 'pending_joining') AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS shortlisted,
            SUM(CASE WHEN jrs.selection_status = 'rejected' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS rejected,
            SUM(CASE WHEN jrs.selection_status = 'joined' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS joined,
            SUM(CASE WHEN jrs.selection_status = 'dropout' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS dropout,
            SUM(CASE WHEN jrs.selection_status = 'billed' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS billed,
            SUM(CASE WHEN jrs.selection_status = 'left' AND ${statusRangeCondition} THEN 1 ELSE 0 END) AS \`left\`,
            MAX(CASE WHEN ${activityRangeCondition} THEN COALESCE(jrs.selected_at, rd.uploaded_at) ELSE NULL END) AS last_updated,
            MIN(CASE WHEN ${submittedRangeCondition} THEN rd.uploaded_at ELSE NULL END) AS created_at
          FROM resumes_data rd
          LEFT JOIN job_resume_selection jrs
            ON jrs.job_jid = rd.job_jid
           AND jrs.res_id = rd.res_id
          WHERE COALESCE(rd.submitted_by_role, 'recruiter') = 'recruiter'
          GROUP BY rd.rid
        ) rs ON rs.recruiter_rid = r.rid
        WHERE r.rid = ?
          AND LOWER(TRIM(COALESCE(r.role, 'recruiter'))) = 'recruiter'
        LIMIT 1`,
        statsQueryParams,
      );

      if (recruiterRows.length === 0) {
        return res.status(404).json({ error: "Recruiter not found." });
      }

      const recruiterRow = recruiterRows[0];

      const [[accessibleJobsCountRow]] = await pool.query(
        `SELECT COUNT(DISTINCT j.jid) AS total
         FROM jobs j
         LEFT JOIN job_recruiter_access jra
           ON j.jid = jra.job_jid
          AND jra.recruiter_rid = ?
          AND jra.is_active = TRUE
         WHERE j.access_mode = 'open'
            OR (j.access_mode = 'restricted' AND jra.id IS NOT NULL)`,
        [rid],
      );

      const stats = mapStats(recruiterRow);

      return res.status(200).json({
        recruiter: {
          rid: recruiterRow.rid,
          name: recruiterRow.name,
          email: recruiterRow.email,
          points: Number(recruiterRow.points) || 0,
        },
        stats,
        accessibleJobsCount: Number(accessibleJobsCountRow?.total) || 0,
        dateRange: hasDateRange ? { startDate, endDate } : null,
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch recruiter dashboard data.",
        details: error.message,
      });
    }
  },
);

// ─── Performance Points Dashboard ───────────────────────────────────────────────

router.get(
  "/api/status/recruiter/:rid/points",
  requireAuth,
  requireRoles("recruiter", "team leader", "team_leader"),
  async (req, res) => {
    if (!assertOwnRidOrTeamLeader(req, res)) return;

    const rid = toRid(req.params.rid);
    if (!rid) return res.status(400).json({ error: "rid is required." });

    try {
      const hasLogTable = await tableExists("recruiter_points_log");

      // Fetch recruiter total points
      const [[recruiterRow]] = await pool.query(
        `SELECT rid, name, email, COALESCE(points, 0) AS points
         FROM recruiter WHERE rid = ? LIMIT 1`,
        [rid],
      );

      if (!recruiterRow) {
        return res.status(404).json({ error: "Recruiter not found." });
      }

      let pointsLog = [];
      let totalBilledPoints = 0;

      if (hasLogTable) {
        const [logRows] = await pool.query(
          `SELECT
            pl.id,
            pl.job_jid AS jobJid,
            pl.res_id AS resId,
            pl.points,
            pl.reason,
            pl.created_at AS creditedAt,
            j.company_name AS companyName,
            j.role_name AS roleName,
            j.points_per_joining AS pointsPerJoining,
            c.name AS candidateName
          FROM recruiter_points_log pl
          LEFT JOIN jobs j ON j.jid = pl.job_jid
          LEFT JOIN candidate c ON c.res_id = pl.res_id
          WHERE pl.recruiter_rid = ?
          ORDER BY pl.created_at DESC`,
          [rid],
        );

        pointsLog = logRows.map((row) => ({
          id: row.id,
          jobJid: row.jobJid,
          resId: row.resId,
          points: Number(row.points) || 0,
          reason: row.reason || "billed",
          creditedAt: row.creditedAt,
          companyName: row.companyName || null,
          roleName: row.roleName || null,
          candidateName: row.candidateName || null,
        }));

        totalBilledPoints = pointsLog.reduce(
          (sum, entry) => sum + entry.points,
          0,
        );
      }

      return res.status(200).json({
        recruiter: {
          rid: recruiterRow.rid,
          name: recruiterRow.name,
          email: recruiterRow.email,
          totalPoints: Number(recruiterRow.points) || 0,
        },
        totalBilledPoints,
        pointsLog,
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch performance points.",
        details: error.message,
      });
    }
  },
);

router.get(
  "/api/status/points/all",
  requireAuth,
  requireRoles("team leader", "team_leader"),
  async (_req, res) => {
    try {
      const hasLogTable = await tableExists("recruiter_points_log");

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
        ORDER BY COALESCE(r.points, 0) DESC, r.name ASC`,
      );

      const recruiters = rows.map((row) => ({
        rid: row.rid,
        name: row.name,
        email: row.email,
        totalPoints: Number(row.totalPoints) || 0,
        billedPoints: Number(row.billedPoints) || 0,
        billedCount: Number(row.billedCount) || 0,
      }));

      const totalPoints = recruiters.reduce((s, r) => s + r.totalPoints, 0);
      const totalBilledPoints = recruiters.reduce(
        (s, r) => s + r.billedPoints,
        0,
      );

      return res.status(200).json({
        recruiters,
        total: recruiters.length,
        summary: {
          totalPoints,
          totalBilledPoints,
        },
      });
    } catch (error) {
      return res.status(500).json({
        error: "Failed to fetch performance points dashboard.",
        details: error.message,
      });
    }
  },
);

module.exports = router;
