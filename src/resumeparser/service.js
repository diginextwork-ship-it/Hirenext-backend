const { atsExtractor, calculateAtsScore } = require("./resumeparser");
const { extractTextFromBuffer } = require("../utils/textExtractor");
const { toNumberOrNull } = require("../utils/formatters");

const SUPPORTED_RESUME_TYPES = new Set(["pdf", "docx"]);
const IMAGE_RESUME_TYPES = new Set(["jpg", "jpeg", "png", "webp"]);

const isImageResumeType = (extension) => IMAGE_RESUME_TYPES.has(String(extension || "").toLowerCase());

const toPercentageNumber = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const numeric = toNumberOrNull(value);
  const fallback =
    numeric !== null
      ? numeric
      : toNumberOrNull(String(value).replace(/[^0-9.]/g, ""));
  if (fallback === null) return null;
  return Math.max(0, Math.min(100, Number(fallback)));
};

const getResumeExtension = (filename) => {
  const match = String(filename || "")
    .trim()
    .match(/\.([a-z0-9]+)$/i);
  return match ? match[1].toLowerCase() : "";
};

const decodeResumeBuffer = (resumeBase64) => {
  const base64Payload = String(resumeBase64 || "").includes(",")
    ? String(resumeBase64).split(",").pop()
    : String(resumeBase64 || "");
  return Buffer.from(base64Payload, "base64");
};

const safeJson = (rawValue, fallbackKey) => {
  if (!rawValue) {
    return { error: `Empty ${fallbackKey} response` };
  }

  if (typeof rawValue === "object") {
    return rawValue;
  }

  try {
    return JSON.parse(rawValue);
  } catch {
    return {
      error: `Could not parse ${fallbackKey}`,
      raw: rawValue,
    };
  }
};

const pickFirstNonEmpty = (...values) => {
  for (const value of values) {
    const candidate =
      value === undefined || value === null ? "" : String(value).trim();
    if (candidate) return candidate;
  }
  return "";
};

const normalizeComparableText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const normalizePhoneDigits = (value) => {
  const digits = String(value || "").replace(/\D/g, "");
  if (digits.length < 10) return "";
  return digits.slice(-10);
};

const pickStructuredEducation = (primary, fallback) => {
  const primaryEducation = Array.isArray(primary?.education)
    ? primary.education.filter((item) => item && typeof item === "object")
    : primary?.education && typeof primary.education === "object"
      ? [primary.education]
      : [];
  const fallbackEducation = Array.isArray(fallback?.education)
    ? fallback.education.filter((item) => item && typeof item === "object")
    : fallback?.education && typeof fallback.education === "object"
      ? [fallback.education]
      : [];

  return primaryEducation.length ? primaryEducation : fallbackEducation;
};

const extractApplicantName = (parsedData) => {
  if (
    !parsedData ||
    typeof parsedData !== "object" ||
    Array.isArray(parsedData)
  ) {
    return null;
  }

  const candidate = pickFirstNonEmpty(
    parsedData.full_name,
    parsedData.fullName,
    parsedData.name,
    parsedData.candidate_name,
    parsedData.candidateName,
    parsedData.applicant_name,
    parsedData.applicantName,
    parsedData.personal_info?.name,
    parsedData.personalInfo?.name,
  );

  return candidate || null;
};

const CONTACT_LINE_WINDOW = 12;
const PHONE_PATTERN =
  /(?:\+?\d{1,3}[\s\-().]*)?(?:\(?\d{3,5}\)?[\s\-().]*)\d{3,5}[\s\-().]*\d{3,5}/g;
const EMAIL_PATTERN = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;

const collectTextMatches = ({ text, lines, pattern, normalizeValue, scoreLine }) => {
  const matches = [];
  const seen = new Set();

  for (const [index, line] of lines.entries()) {
    pattern.lastIndex = 0;
    let match = pattern.exec(line);
    while (match) {
      const rawValue = String(match[0] || "").trim();
      const normalizedValue = normalizeValue(rawValue);
      if (normalizedValue && !seen.has(normalizedValue)) {
        seen.add(normalizedValue);
        matches.push({
          raw: rawValue,
          value: normalizedValue,
          lineIndex: index,
          line,
          score: scoreLine({ line, lineIndex: index, rawValue, normalizedValue }),
        });
      }

      match = pattern.exec(line);
    }
  }

  return matches.sort((left, right) => right.score - left.score);
};

const findBestEmail = (lines, text) => {
  const matches = collectTextMatches({
    text,
    lines,
    pattern: EMAIL_PATTERN,
    normalizeValue: (value) => String(value || "").trim().toLowerCase(),
    scoreLine: ({ line, lineIndex, normalizedValue }) => {
      const lower = line.toLowerCase();
      const localPart = normalizedValue.split("@")[0] || "";
      let score = 0;

      if (lineIndex < 5) score += 40 - lineIndex * 4;
      else if (lineIndex < CONTACT_LINE_WINDOW) score += 12;

      if (/\b(email|mail|e-mail)\b/i.test(line)) score += 45;
      if (/^(contact|connect|reach me)\b/i.test(lower)) score += 12;
      if (/\blinkedin|github|portfolio|www\.|http\b/i.test(lower)) score -= 25;
      if (/\breference|referee|manager|supervisor\b/i.test(lower)) score -= 18;
      if (/[|/]/.test(line)) score += 6;
      if (localPart.includes("hr") || localPart.includes("recruit")) score -= 10;

      return score;
    },
  });

  return matches[0]?.value || null;
};

const findBestPhone = (lines, text) => {
  const matches = collectTextMatches({
    text,
    lines,
    pattern: PHONE_PATTERN,
    normalizeValue: normalizePhoneDigits,
    scoreLine: ({ line, lineIndex, normalizedValue }) => {
      const lower = line.toLowerCase();
      let score = 0;

      if (!normalizedValue) return -100;
      if (lineIndex < 5) score += 35 - lineIndex * 4;
      else if (lineIndex < CONTACT_LINE_WINDOW) score += 10;

      if (/\b(phone|mobile|mob|contact|call|tel)\b/i.test(line)) score += 55;
      if (/\bwhatsapp\b/i.test(line)) score += 20;
      if (/\bfax\b/i.test(line)) score -= 50;
      if (/\bexperience|salary|year|years|dob|age\b/i.test(lower)) score -= 18;
      if (/[|/]/.test(line)) score += 4;

      return score;
    },
  });

  return matches[0]?.value || null;
};

const scoreNameCandidate = (line, index) => {
  const trimmed = String(line || "").trim();
  const lower = trimmed.toLowerCase();
  const tokens = trimmed.split(/\s+/).filter(Boolean);

  if (!trimmed) return -100;
  if (tokens.length < 2 || tokens.length > 5) return -100;
  if (!/^[a-z .'-]+$/i.test(trimmed)) return -100;
  if (lower.includes("@") || /\d/.test(trimmed)) return -100;
  if (
    /^(resume|curriculum vitae|cv|profile|summary|objective|education|experience|skills|projects|declaration|references?)$/i.test(
      lower,
    )
  )
    return -100;
  if (
    /\b(email|phone|mobile|address|contact|linkedin|github|portfolio|engineer|developer|manager|analyst|specialist|consultant)\b/i.test(
      lower,
    )
  )
    return -50;

  let score = 0;
  if (index < 3) score += 60 - index * 10;
  else if (index < 8) score += 20 - index;

  if (tokens.length === 2 || tokens.length === 3) score += 30;
  if (tokens.every((token) => /^[A-Z][a-z'-]+$/.test(token))) score += 40;
  else if (tokens.every((token) => /^[A-Z][A-Z'-]+$/.test(token))) score += 28;
  if (trimmed.length >= 8 && trimmed.length <= 32) score += 18;
  if (/^[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+$/.test(trimmed)) score += 18;
  if (/\b(and|with|for|at|in|to)\b/i.test(lower)) score -= 25;

  return score;
};

const findBestName = (lines) => {
  const candidates = lines
    .slice(0, 15)
    .map((line, index) => ({
      line,
      score: scoreNameCandidate(line, index),
    }))
    .filter((item) => item.score > 0)
    .sort((left, right) => right.score - left.score);

  return candidates[0]?.line || null;
};

const isLikelyNamePresentInText = (name, lines, text) => {
  const normalizedName = normalizeComparableText(name);
  if (!normalizedName) return false;

  const topSection = lines.slice(0, 20).map(normalizeComparableText).join(" ");
  if (topSection.includes(normalizedName)) return true;

  return normalizeComparableText(text).includes(normalizedName);
};

const mergeParsedDataWithFallback = ({ aiParsedData, fallbackParsedData, resumeText }) => {
  const safeAi =
    aiParsedData && typeof aiParsedData === "object" && !Array.isArray(aiParsedData)
      ? aiParsedData
      : {};
  const safeFallback =
    fallbackParsedData &&
    typeof fallbackParsedData === "object" &&
    !Array.isArray(fallbackParsedData)
      ? fallbackParsedData
      : {};
  const text = String(resumeText || "");
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const aiName = extractApplicantName(safeAi);
  const fallbackName = extractApplicantName(safeFallback);
  const mergedName = isLikelyNamePresentInText(aiName, lines, text)
    ? aiName
    : fallbackName;

  const aiEmail = pickFirstNonEmpty(safeAi.email, safeAi.mail).toLowerCase();
  const fallbackEmail = pickFirstNonEmpty(safeFallback.email, safeFallback.mail).toLowerCase();
  const mergedEmail =
    aiEmail && text.toLowerCase().includes(aiEmail) ? aiEmail : fallbackEmail;

  const aiPhone = normalizePhoneDigits(
    pickFirstNonEmpty(
      safeAi.phone,
      safeAi.phone_number,
      safeAi.phoneNumber,
      safeAi.mobile,
      safeAi.mobile_number,
      safeAi.mobileNumber,
    ),
  );
  const fallbackPhone = normalizePhoneDigits(
    pickFirstNonEmpty(
      safeFallback.phone,
      safeFallback.phone_number,
      safeFallback.phoneNumber,
      safeFallback.mobile,
      safeFallback.mobile_number,
      safeFallback.mobileNumber,
    ),
  );
  const mergedPhone = aiPhone && text.replace(/\D/g, "").includes(aiPhone) ? aiPhone : fallbackPhone;

  const merged = {
    ...safeFallback,
    ...safeAi,
    full_name: mergedName || null,
    email: mergedEmail || null,
    phone: mergedPhone || null,
    education: pickStructuredEducation(safeAi, safeFallback),
    age: pickFirstNonEmpty(safeAi.age, safeAi.current_age, safeFallback.age) || null,
  };

  if (!merged.education?.length) {
    merged.education = [
      {
        latest_education_level: null,
        board_university: null,
        institution_name: null,
      },
    ];
  }

  return merged;
};

const extractAutofillFallbackFromText = (resumeText) => {
  const text = String(resumeText || "");
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const email = findBestEmail(lines, text);
  const phone = findBestPhone(lines, text);
  const ageMatch = text.match(/\bage\s*[:\-]?\s*(\d{2})\b/i);
  const dobMatch =
    text.match(
      /\b(?:dob|date of birth)\s*[:\-]?\s*([0-3]?\d[\/\-][01]?\d[\/\-](?:19|20)\d{2})\b/i,
    ) || text.match(/\b([0-3]?\d[\/\-][01]?\d[\/\-](?:19|20)\d{2})\b/);

  const name = findBestName(lines) || "";

  const toAgeFromDob = (dobText) => {
    const normalized = String(dobText || "").trim();
    if (!normalized) return null;
    const parts = normalized.split(/[\/\-]/).map((item) => Number(item));
    if (parts.length !== 3 || parts.some((item) => !Number.isFinite(item)))
      return null;
    const [day, month, year] = parts;
    const dob = new Date(year, month - 1, day);
    if (Number.isNaN(dob.getTime())) return null;
    const now = new Date();
    let ageYears = now.getFullYear() - dob.getFullYear();
    const monthDiff = now.getMonth() - dob.getMonth();
    const dayDiff = now.getDate() - dob.getDate();
    if (monthDiff < 0 || (monthDiff === 0 && dayDiff < 0)) {
      ageYears -= 1;
    }
    if (ageYears < 16 || ageYears > 100) return null;
    return String(ageYears);
  };

  const educationSectionLines = lines.filter((line) =>
    /(university|college|institute|institution|school|board|education|bachelor|master|degree|gpa|percentage)/i.test(
      line,
    ),
  );

  const boardUniversity =
    educationSectionLines.find((line) => /(university|board)/i.test(line)) ||
    null;
  const institutionName =
    educationSectionLines.find((line) =>
      /(college|institute|institution|school)/i.test(line),
    ) || null;

  const degreeHints = [
    { pattern: /\b(phd|doctorate)\b/i, level: "phd" },
    {
      pattern: /\b(master|m\.?tech|m\.?e|mba|mca|m\.?sc)\b/i,
      level: "masters",
    },
    {
      pattern: /\b(bachelor|b\.?tech|b\.?e|bca|b\.?sc|bcom|ba)\b/i,
      level: "bachelors",
    },
    { pattern: /\b(12th|higher secondary|intermediate)\b/i, level: "12th" },
    { pattern: /\b(10th|secondary school)\b/i, level: "10th" },
  ];
  const matchedDegree = degreeHints.find((item) => item.pattern.test(text));

  return {
    full_name: name || null,
    email: email || null,
    phone: phone || null,
    education: [
      {
        latest_education_level: matchedDegree ? matchedDegree.level : null,
        board_university: boardUniversity,
        institution_name: institutionName,
      },
    ],
    age: ageMatch ? ageMatch[1] : toAgeFromDob(dobMatch?.[1]),
  };
};

const hasParsedAutofillSignal = (parsedData) => {
  if (
    !parsedData ||
    typeof parsedData !== "object" ||
    Array.isArray(parsedData)
  )
    return false;

  const educationCandidate = Array.isArray(parsedData.education)
    ? parsedData.education[0] || null
    : parsedData.education && typeof parsedData.education === "object"
      ? parsedData.education
      : null;

  return Boolean(
    pickFirstNonEmpty(
      parsedData.full_name,
      parsedData.fullName,
      parsedData.name,
      parsedData.email,
      parsedData.phone,
      parsedData.phone_number,
      educationCandidate?.latest_education_level,
      educationCandidate?.latestEducationLevel,
      educationCandidate?.institution_name,
      educationCandidate?.institutionName,
    ),
  );
};

const uniqueWords = (text) =>
  Array.from(
    new Set(
      String(text || "")
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, " ")
        .split(/\s+/)
        .map((item) => item.trim())
        .filter((item) => item.length >= 3),
    ),
  );

const calculateFallbackAts = (resumeText, jobDescription) => {
  const resumeWords = uniqueWords(resumeText);
  const jobWords = uniqueWords(jobDescription);

  if (!jobWords.length) {
    return {
      ats_score: null,
      match_percentage: null,
      matching_keywords: [],
      missing_keywords: [],
      strengths: [],
      weaknesses: [],
      recommendations: [],
      overall_assessment:
        "ATS could not be calculated because job description is unavailable.",
    };
  }

  const resumeSet = new Set(resumeWords);
  const matchingKeywords = jobWords
    .filter((word) => resumeSet.has(word))
    .slice(0, 25);
  const missingKeywords = jobWords
    .filter((word) => !resumeSet.has(word))
    .slice(0, 25);

  const ratio = jobWords.length ? matchingKeywords.length / jobWords.length : 0;
  const percentage = Number((ratio * 100).toFixed(2));

  return {
    ats_score: percentage,
    match_percentage: `${percentage}%`,
    matching_keywords: matchingKeywords,
    missing_keywords: missingKeywords,
    strengths:
      matchingKeywords.length > 0
        ? ["Resume includes relevant job keywords."]
        : ["Resume appears weakly aligned with key job terms."],
    weaknesses:
      missingKeywords.length > 0
        ? ["Several job-relevant keywords are missing from resume."]
        : [],
    recommendations:
      missingKeywords.length > 0
        ? [
            `Consider adding measurable experience with: ${missingKeywords.slice(0, 6).join(", ")}.`,
          ]
        : [
            "Maintain keyword alignment while improving role-specific achievements.",
          ],
    overall_assessment:
      percentage >= 75
        ? "Strong keyword alignment with the role."
        : percentage >= 50
          ? "Moderate keyword alignment with room for improvement."
          : "Low keyword alignment; resume tailoring recommended.",
  };
};

const parseResumeWithAts = async ({
  resumeBuffer,
  resumeFilename,
  jobDescription,
}) => {
  try {
    const extension = getResumeExtension(resumeFilename);
    if (isImageResumeType(extension)) {
      return {
        ok: true,
        message: "",
        parsedData: null,
        applicantName: null,
        atsScore: null,
        atsMatchPercentage: null,
        atsRawJson: null,
        parserMeta: {
          parsedDataSource: "skipped_image_resume",
          atsSource: "skipped_image_resume",
          manualEntryRequired: true,
        },
      };
    }

    if (!SUPPORTED_RESUME_TYPES.has(extension)) {
      return {
        ok: false,
        message: "Only PDF, DOCX, JPG, JPEG, PNG, and WEBP resumes are supported.",
        parsedData: null,
        atsScore: null,
        atsMatchPercentage: null,
        atsRawJson: null,
      };
    }

    const resumeText = await extractTextFromBuffer(resumeBuffer, extension);
    const normalizedJobDescription = String(jobDescription || "").trim();
    const [rawAiParsedData, rawAiAtsData] = await Promise.all([
      atsExtractor(resumeText),
      normalizedJobDescription
        ? calculateAtsScore(resumeText, normalizedJobDescription)
        : Promise.resolve(null),
    ]);

    const aiParsedData = safeJson(rawAiParsedData, "resume data");
    const fallbackParsedData = extractAutofillFallbackFromText(resumeText);
    const parsedData = hasParsedAutofillSignal(aiParsedData)
      ? mergeParsedDataWithFallback({
          aiParsedData,
          fallbackParsedData,
          resumeText,
        })
      : fallbackParsedData;
    const aiAtsRawJson = normalizedJobDescription
      ? safeJson(rawAiAtsData, "ATS score")
      : null;
    const fallbackAtsRawJson = calculateFallbackAts(
      resumeText,
      normalizedJobDescription,
    );
    const atsRawJson =
      aiAtsRawJson && typeof aiAtsRawJson === "object" && !aiAtsRawJson.error
        ? aiAtsRawJson
        : fallbackAtsRawJson;

    const atsScoreFromModel = toPercentageNumber(atsRawJson?.ats_score);
    const atsMatchFromModel = toPercentageNumber(atsRawJson?.match_percentage);
    const atsScore = atsScoreFromModel ?? atsMatchFromModel;
    const atsMatchPercentage = atsMatchFromModel ?? atsScoreFromModel;

    return {
      ok: true,
      message: "",
      parsedData,
      applicantName: extractApplicantName(parsedData),
      atsScore,
      atsMatchPercentage,
      atsRawJson,
      parserMeta: {
        parsedDataSource: hasParsedAutofillSignal(aiParsedData)
          ? "hybrid_ai_fallback"
          : "fallback",
        atsSource:
          aiAtsRawJson &&
          typeof aiAtsRawJson === "object" &&
          !aiAtsRawJson.error
            ? "ai"
            : "fallback",
      },
    };
  } catch (error) {
    return {
      ok: false,
      message: `Failed to parse resume: ${error.message}`,
      parsedData: null,
      applicantName: null,
      atsScore: null,
      atsMatchPercentage: null,
      atsRawJson: null,
    };
  }
};

const extractResumeAts = async ({
  resumeBuffer,
  resumeFilename,
  jobDescription,
}) => {
  const extension = getResumeExtension(resumeFilename);
  if (isImageResumeType(extension)) {
    return {
      atsScore: null,
      atsMatchPercentage: null,
      atsRawJson: null,
      applicantName: null,
      atsStatus: "manual_entry_required",
    };
  }

  if (!SUPPORTED_RESUME_TYPES.has(extension)) {
    return {
      atsScore: null,
      atsMatchPercentage: null,
      atsRawJson: null,
      applicantName: null,
      atsStatus: "unsupported_file_type",
    };
  }

  const parsed = await parseResumeWithAts({
    resumeBuffer,
    resumeFilename,
    jobDescription,
  });

  if (!parsed.ok) {
    return {
      atsScore: null,
      atsMatchPercentage: null,
      atsRawJson: null,
      applicantName: null,
      atsStatus: "service_error",
    };
  }

  return {
    atsScore: parsed.atsScore,
    atsMatchPercentage: parsed.atsMatchPercentage,
    atsRawJson: parsed.atsRawJson,
    applicantName: parsed.applicantName,
    atsStatus: "scored",
  };
};

module.exports = {
  SUPPORTED_RESUME_TYPES,
  getResumeExtension,
  decodeResumeBuffer,
  parseResumeWithAts,
  extractResumeAts,
  extractApplicantName,
  extractAutofillFallbackFromText,
  mergeParsedDataWithFallback,
  isImageResumeType,
};
