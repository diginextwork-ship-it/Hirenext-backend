const DEFAULT_BUSINESS_TIME_ZONE =
  String(process.env.APP_TIME_ZONE || process.env.BUSINESS_TIME_ZONE || "")
    .trim() || "Asia/Kolkata";

const DATE_ONLY_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DEFAULT_BUSINESS_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

const SQL_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DEFAULT_BUSINESS_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  hourCycle: "h23",
});

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
  ) {
    return false;
  }

  const parsed = new Date(Date.UTC(year, month - 1, day));
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() === month - 1 &&
    parsed.getUTCDate() === day
  );
};

const getCurrentDateOnlyInBusinessTimeZone = () =>
  DATE_ONLY_FORMATTER.format(new Date());

const formatSqlDateTimeInBusinessTimeZone = (value = new Date()) => {
  const date = value instanceof Date ? value : new Date(value);
  const safeDate = Number.isNaN(date.getTime()) ? new Date() : date;
  const parts = SQL_DATE_TIME_FORMATTER.formatToParts(safeDate).reduce(
    (acc, part) => {
      if (part.type !== "literal") acc[part.type] = part.value;
      return acc;
    },
    {},
  );

  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
};

const parseInclusiveDateRange = (startRaw, endRaw) => {
  const startDate = String(startRaw || "").trim();
  const endDate = String(endRaw || "").trim();

  if (!startDate && !endDate) {
    return {
      startDate: null,
      endDate: null,
      startDateTime: null,
      endDateTime: null,
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
    startDateTime: `${startDate} 00:00:00.000000`,
    endDateTime: `${endDate} 23:59:59.999999`,
    hasDateRange: true,
    error: null,
  };
};

const SQL_HISTORICAL_CUTOFF_RES_NUM = 9071;

/**
 * Resumes uploaded before commit 9fd2973 (up to res_9071) were inserted under
 * MySQL session time_zone = '+00:00' with literal IST datetime strings, so MySQL
 * stored them as UTC = real_IST. With the session now in '+05:30', MySQL converts
 * that UTC by adding +5:30, requiring DATE_SUB(..., INTERVAL 330 MINUTE) to display true IST.
 *
 * Resumes uploaded starting from res_9072 onwards were inserted while the MySQL session
 * was already in '+05:30', so MySQL converted the IST string to UTC properly on insert.
 * Reading them back in '+05:30' produces true IST directly, requiring NO subtraction.
 */
const buildSqlEffectiveSubmittedAt = (
  resIdCol = "rd.res_id",
  uploadedAtCol = "rd.uploaded_at",
  submittedAtCol = "ei.submitted_at",
) => `CASE
  WHEN ${resIdCol} REGEXP '^res_[0-9]+$' AND CAST(SUBSTRING(${resIdCol}, 5) AS UNSIGNED) <= ${SQL_HISTORICAL_CUTOFF_RES_NUM}
  THEN DATE_SUB(COALESCE(${uploadedAtCol}, ${submittedAtCol}), INTERVAL 330 MINUTE)
  ELSE COALESCE(${uploadedAtCol}, ${submittedAtCol})
END`;

const buildSqlEffectiveUploadedAt = (
  resIdCol = "rd.res_id",
  uploadedAtCol = "rd.uploaded_at",
) => `CASE
  WHEN ${resIdCol} REGEXP '^res_[0-9]+$' AND CAST(SUBSTRING(${resIdCol}, 5) AS UNSIGNED) <= ${SQL_HISTORICAL_CUTOFF_RES_NUM}
  THEN DATE_SUB(${uploadedAtCol}, INTERVAL 330 MINUTE)
  ELSE ${uploadedAtCol}
END`;

module.exports = {
  DEFAULT_BUSINESS_TIME_ZONE,
  SQL_HISTORICAL_CUTOFF_RES_NUM,
  buildSqlEffectiveSubmittedAt,
  buildSqlEffectiveUploadedAt,
  formatSqlDateTimeInBusinessTimeZone,
  getCurrentDateOnlyInBusinessTimeZone,
  isValidDateOnly,
  parseInclusiveDateRange,
};
