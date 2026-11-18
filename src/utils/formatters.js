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

const normalizeCandidateName = (value) => {
  if (value === undefined || value === null) return "";
  const cleaned = String(value).trim().replace(/\s+/g, " ");
  if (!cleaned) return "";

  return cleaned
    .toLowerCase()
    .split(" ")
    .map((word) => {
      if (!word) return "";
      return word.replace(/(?:^|[-'])\p{L}/gu, (char) => char.toUpperCase());
    })
    .join(" ");
};

const normalizeEducationLevel = (value) => {
  if (!value) return "";
  const cleaned = String(value).trim().toLowerCase();
  if (["phd", "doctorate", "ph.d", "doctor"].some((k) => cleaned.includes(k))) {
    return "PhD";
  }
  if (
    [
      "master",
      "mtech",
      "m.tech",
      "me",
      "m.e",
      "mca",
      "mba",
      "msc",
      "m.sc",
      "post graduate",
      "pg",
    ].some((k) => cleaned.includes(k))
  ) {
    return "Master's";
  }
  if (
    ["undergraduate", "diploma", "polytechnic"].some((k) => cleaned.includes(k))
  ) {
    return "Undergraduate";
  }
  if (
    [
      "bachelor",
      "btech",
      "b.tech",
      "be",
      "b.e",
      "bca",
      "bsc",
      "b.sc",
      "bcom",
      "b.com",
      "ba",
      "b.a",
      "graduate",
      "degree",
    ].some((k) => cleaned.includes(k))
  ) {
    return "Bachelor's";
  }
  if (["12th", "intermediate", "higher secondary"].some((k) => cleaned.includes(k))) {
    return "12th Standard";
  }
  if (["10th", "secondary", "matriculation"].some((k) => cleaned.includes(k))) {
    return "10th Standard";
  }
  return String(value).trim();
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

const cleanInstitutionName = (value) => {
  if (value === undefined || value === null) return "";
  let text = String(value).trim();
  if (!text) return "";

  // If text contains something like "Completed ... from [College]" or "graduated from [College]"
  const fromMatch = text.match(/\b(?:from|at)\s+\[?([^\]\r\n]+)\]?/i);
  if (fromMatch && fromMatch[1]) {
    text = fromMatch[1].trim();
  }

  // Remove leading/trailing brackets, parentheses, quotes
  text = text.replace(/^[\[\("']+|[\]\)"']+$/g, "").trim();

  // Strip prefixes like "College:", "Institute:", "University:"
  text = text.replace(/^(?:college|institute|institution|university)\s*:\s*/i, "").trim();

  // Safe slice to 190 characters
  if (text.length > 190) {
    text = text.slice(0, 190).trim();
  }
  return text;
};

const safeTruncate = (val, maxLen) => {
  if (val === undefined || val === null) return val;
  const str = String(val).trim();
  if (!str) return null;
  return str.length > maxLen ? str.slice(0, maxLen).trim() : str;
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
    name: normalizeCandidateName(
      pickString(
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
    institutionName: cleanInstitutionName(
      pickFromEducation(
        "institution_name",
        "institutionName",
        "college_name",
        "college",
        "school_name",
        "school",
      ),
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
    name: safeTruncate(
      normalizeCandidateName(
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
      ),
      100,
    ) || "",
    phone: safeTruncate(
      normalizePhoneForStorage(
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
      15,
    ) || "",
    email: safeTruncate(
      String(
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
      100,
    ) || "",
    levelOfEdu: safeTruncate(
      String(
        pickFirstFilled(
          pickAlias("latest_education_level"),
          autofill.latestEducationLevel,
          safeFallback.levelOfEdu,
        ) || "",
      ).trim(),
      50,
    ) || "",
    boardUni: safeTruncate(
      String(
        pickFirstFilled(
          pickAlias("board_university"),
          autofill.boardUniversity,
          safeFallback.boardUni,
        ) || "",
      ).trim(),
      100,
    ) || "",
    institutionName: cleanInstitutionName(
      pickFirstFilled(
        pickAlias("institution_name", "institutionName", "college_name", "college"),
        autofill.institutionName,
        safeFallback.institutionName,
      ),
    ),
    location: safeTruncate(
      String(
        pickFirstFilled(
          pickAlias("candidate_location", "candidateLocation", "location"),
          safeFallback.location,
        ) || "",
      ).trim(),
      150,
    ) || "",
    age: toNumberOrNull(rawAge),
    jobJid:
      String(
        pickFirstFilled(
          pickAlias("job_jid", "jid"),
          safeFallback.jobJid,
        ) || "",
      ).trim() || null,
    recruiterRid: safeTruncate(
      String(
        pickFirstFilled(
          pickAlias("recruiter_rid"),
          safeFallback.recruiterRid,
        ) || "",
      ).trim() || "",
      20,
    ) || null,
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

const renderCandidateProfileHtml = (cand = {}) => `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Candidate Profile - ${cand.name || "Candidate"}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #f1f5f9; padding: 40px 20px; color: #1e293b; }
    .card { max-width: 680px; margin: 0 auto; background: #ffffff; border-radius: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.08); padding: 32px; border: 1px solid #e2e8f0; }
    .header { border-bottom: 2px solid #e2e8f0; padding-bottom: 16px; margin-bottom: 24px; }
    h1 { font-size: 24px; margin: 0 0 6px 0; color: #0f172a; font-weight: 700; }
    .badge { display: inline-block; padding: 4px 12px; background: #fef3c7; color: #92400e; border-radius: 20px; font-size: 12px; font-weight: 600; border: 1px solid #fde68a; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
    .field { margin-bottom: 8px; }
    .label { font-size: 11px; text-transform: uppercase; color: #64748b; font-weight: 600; letter-spacing: 0.5px; margin-bottom: 4px; }
    .value { font-size: 15px; color: #0f172a; font-weight: 500; word-break: break-word; }
    .note-box { background: #f8fafc; padding: 14px 18px; border-radius: 8px; font-size: 14px; margin-top: 16px; border: 1px solid #e2e8f0; border-left: 4px solid #ef4444; }
    .footer { margin-top: 24px; font-size: 12px; color: #94a3b8; text-align: center; border-top: 1px solid #f1f5f9; padding-top: 16px; }
    @media print { body { background: #fff; padding: 0; } .card { box-shadow: none; border: none; } }
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <h1>${cand.name || "Candidate Profile"}</h1>
      <span class="badge">Submitted without Resume File (Manual Entry)</span>
    </div>
    <div class="grid">
      <div class="field"><div class="label">Candidate Name</div><div class="value">${cand.name || "N/A"}</div></div>
      <div class="field"><div class="label">Contact Phone</div><div class="value">${cand.phone || "N/A"}</div></div>
      <div class="field"><div class="label">Email Address</div><div class="value">${cand.email || "N/A"}</div></div>
      <div class="field"><div class="label">Current Location</div><div class="value">${cand.location || "N/A"}</div></div>
      <div class="field"><div class="label">Office Location</div><div class="value">${cand.office_location_city || "As per JD"}</div></div>
      <div class="field"><div class="label">Education Level</div><div class="value">${cand.level_of_edu || "N/A"}</div></div>
      <div class="field"><div class="label">Institution / College</div><div class="value">${cand.institution_name || "N/A"}</div></div>
      <div class="field"><div class="label">Board / University</div><div class="value">${cand.board_uni || "N/A"}</div></div>
      <div class="field"><div class="label">Age</div><div class="value">${cand.age ? cand.age + " years" : "N/A"}</div></div>
      <div class="field"><div class="label">Job Applied</div><div class="value">${cand.role_name || "N/A"}${cand.company_name ? ` (${cand.company_name})` : ""}</div></div>
      <div class="field"><div class="label">Submitted By (RID)</div><div class="value">${cand.recruiter_rid || cand.rid || "N/A"}</div></div>
      <div class="field"><div class="label">Candidate ID / Res ID</div><div class="value">${cand.cid || ""} / ${cand.res_id || ""}</div></div>
    </div>
    ${cand.submitted_reason ? `<div class="note-box"><div class="label">Candidate Availability / Notes</div><div class="value" style="font-size: 14px; margin-top: 4px;">${cand.submitted_reason}</div></div>` : ""}
    <div class="footer">HireNext India ATS &bull; Candidate Record</div>
  </div>
</body>
</html>`;

module.exports = {
  toNumberOrNull,
  toMoneyNumber,
  toMoneyOrNull,
  normalizeJobJid,
  toTrimmedString,
  normalizeAccessMode,
  normalizePhoneForStorage,
  normalizeCandidateName,
  safeJsonOrNull,
  parseJsonField,
  escapeLike,
  sha256Hex,
  dedupeStringList,
  pickFirstFilled,
  normalizeEducationLevel,
  buildAutofillFromParsedData,
  extractCandidateSnapshot,
  buildJobAtsContext,
  cleanInstitutionName,
  safeTruncate,
  renderCandidateProfileHtml,
};
