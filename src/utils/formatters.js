const crypto = require("crypto");

const toNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toMoneyNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed * 100) / 100;
};

const toMoneyOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return Math.round(parsed * 100) / 100;
};

const normalizeJobJid = (value) => {
  const normalized = String(value || "").trim();
  return normalized || null;
};

const normalizeAccessMode = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "open" || normalized === "restricted") {
    return normalized;
  }
  return "";
};

const normalizePhoneForStorage = (phone) => {
  const digits = String(phone || "").replace(/\D/g, "");
  if (digits.length === 10) return digits;
  if (digits.length > 10) return digits.slice(-10);
  return digits;
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

const escapeLike = (value) => String(value || "").replace(/[\\%_]/g, "\\$&");

const sha256Hex = (buffer) =>
  crypto.createHash("sha256").update(buffer).digest("hex");

const dedupeStringList = (values) => {
  const unique = new Set();
  const result = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const normalized = String(raw || "").trim();
    if (!normalized || unique.has(normalized)) continue;
    unique.add(normalized);
    result.push(normalized);
  }
  return result;
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

module.exports = {
  toNumberOrNull,
  toMoneyNumber,
  toMoneyOrNull,
  normalizeJobJid,
  normalizeAccessMode,
  normalizePhoneForStorage,
  safeJsonOrNull,
  parseJsonField,
  escapeLike,
  sha256Hex,
  dedupeStringList,
  buildAutofillFromParsedData,
  buildJobAtsContext,
};
