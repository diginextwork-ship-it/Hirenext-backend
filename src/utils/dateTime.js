const DEFAULT_BUSINESS_TIME_ZONE =
  String(process.env.APP_TIME_ZONE || process.env.BUSINESS_TIME_ZONE || "")
    .trim() || "Asia/Kolkata";

const DATE_ONLY_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DEFAULT_BUSINESS_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
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

module.exports = {
  DEFAULT_BUSINESS_TIME_ZONE,
  getCurrentDateOnlyInBusinessTimeZone,
  isValidDateOnly,
  parseInclusiveDateRange,
};
