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

const toTrimmedString = (value) => {
  if (value === undefined || value === null) return "";
  return String(value).trim();
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

const unwrapRepeatedFieldValue = (value) => {
  if (!Array.isArray(value)) return value;

  for (let index = value.length - 1; index >= 0; index -= 1) {
    const candidate = unwrapRepeatedFieldValue(value[index]);
    if (candidate === undefined || candidate === null) continue;
    if (typeof candidate === "string") {
      if (candidate.trim()) return candidate;
      continue;
    }
    return candidate;
  }

  return undefined;
};

const pickFirstFilled = (...values) => {
  for (const value of values) {
    const normalizedValue = unwrapRepeatedFieldValue(value);
    if (normalizedValue === undefined || normalizedValue === null) continue;
    if (typeof normalizedValue === "string") {
      const trimmed = normalizedValue.trim();
      if (trimmed) return trimmed;
      continue;
    }
    if (typeof normalizedValue === "number" && Number.isFinite(normalizedValue)) {
      return normalizedValue;
    }
    const normalized = String(normalizedValue).trim();
    if (normalized) return normalizedValue;
  }
  return undefined;
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
    name: pickString(
      safeData.full_name,
      safeData.fullName,
      safeData.name,
      safeData.candidate_name,
      safeData.candidateName,
      safeData.applicant_name,
      safeData.applicantName,
      safeData.personal_info?.name,
      safeData.personalInfo?.name,
    ),
    phone: normalizePhoneForStorage(
      pickString(
        safeData.phone,
        safeData.phone_number,
        safeData.phoneNumber,
        safeData.mobile,
        safeData.mobile_number,
        safeData.mobileNumber,
      ),
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

const extractCandidateSnapshot = ({ source, parsedData, fallback } = {}) => {
  const safeSource =
    source && typeof source === "object" && !Array.isArray(source) ? source : {};
  const safeFallback =
    fallback && typeof fallback === "object" && !Array.isArray(fallback)
      ? fallback
      : {};
  const autofill = buildAutofillFromParsedData(parsedData);
  const pickAlias = (...keys) =>
    pickFirstFilled(...keys.map((key) => safeSource[key]));
  const rawAge = pickFirstFilled(
    pickAlias("age"),
    autofill.age,
    safeFallback.age,
  );

  return {
    name: String(
      pickFirstFilled(
        pickAlias(
          "candidate_name",
          "candidateName",
          "applicant_name",
          "applicantName",
          "name",
        ),
        autofill.name,
        safeFallback.name,
      ) || "",
    ).trim(),
    phone: normalizePhoneForStorage(
      pickFirstFilled(
        pickAlias(
          "candidate_phone",
          "candidatePhone",
          "phone",
          "phone_number",
          "phoneNumber",
          "mobile",
          "mobile_number",
          "mobileNumber",
        ),
        autofill.phone,
        safeFallback.phone,
      ) || "",
    ),
    email: String(
      pickFirstFilled(
        pickAlias(
          "candidate_email",
          "candidateEmail",
          "applicant_email",
          "applicantEmail",
          "email",
        ),
        autofill.email,
        safeFallback.email,
      ) || "",
    )
      .trim()
      .toLowerCase(),
    levelOfEdu: String(
      pickFirstFilled(
        pickAlias("latest_education_level"),
        autofill.latestEducationLevel,
        safeFallback.levelOfEdu,
      ) || "",
    ).trim(),
    boardUni: String(
      pickFirstFilled(
        pickAlias("board_university"),
        autofill.boardUniversity,
        safeFallback.boardUni,
      ) || "",
    ).trim(),
    institutionName: String(
      pickFirstFilled(
        pickAlias("institution_name"),
        autofill.institutionName,
        safeFallback.institutionName,
      ) || "",
    ).trim(),
    location: String(
      pickFirstFilled(
        pickAlias("candidate_location", "candidateLocation", "location"),
        safeFallback.location,
      ) || "",
    ).trim(),
    age: toNumberOrNull(rawAge),
    jobJid:
      String(
        pickFirstFilled(
          pickAlias("job_jid", "jid"),
          safeFallback.jobJid,
        ) || "",
      ).trim() || null,
    recruiterRid:
      String(
        pickFirstFilled(
          pickAlias("recruiter_rid"),
          safeFallback.recruiterRid,
        ) || "",
      ).trim() || null,
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
  toTrimmedString,
  normalizeAccessMode,
  normalizePhoneForStorage,
  safeJsonOrNull,
  parseJsonField,
  escapeLike,
  sha256Hex,
  dedupeStringList,
  pickFirstFilled,
  buildAutofillFromParsedData,
  extractCandidateSnapshot,
  buildJobAtsContext,
};
