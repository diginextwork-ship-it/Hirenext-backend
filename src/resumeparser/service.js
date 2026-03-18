const pdfParse = require("pdf-parse");
const mammoth = require("mammoth");
const { atsExtractor, calculateAtsScore } = require("./resumeparser");

const SUPPORTED_RESUME_TYPES = new Set(["pdf", "docx"]);

const toNumberOrNull = (value) => {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

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
  const match = String(filename || "").trim().match(/\.([a-z0-9]+)$/i);
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
    const candidate = value === undefined || value === null ? "" : String(value).trim();
    if (candidate) return candidate;
  }
  return "";
};

const extractApplicantName = (parsedData) => {
  if (!parsedData || typeof parsedData !== "object" || Array.isArray(parsedData)) {
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
    parsedData.personalInfo?.name
  );

  return candidate || null;
};

const extractAutofillFallbackFromText = (resumeText) => {
  const text = String(resumeText || "");
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const emailMatch = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneMatch =
    text.match(/(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{3,5}\)?[\s-]?)\d{3,5}[\s-]?\d{3,5}/) || null;
  const ageMatch = text.match(/\bage\s*[:\-]?\s*(\d{2})\b/i);
  const dobMatch =
    text.match(/\b(?:dob|date of birth)\s*[:\-]?\s*([0-3]?\d[\/\-][01]?\d[\/\-](?:19|20)\d{2})\b/i) ||
    text.match(/\b([0-3]?\d[\/\-][01]?\d[\/\-](?:19|20)\d{2})\b/);

  const normalizedPhoneDigits = String(phoneMatch?.[0] || "").replace(/\D/g, "");
  const phone =
    normalizedPhoneDigits.length >= 10
      ? normalizedPhoneDigits.slice(normalizedPhoneDigits.length - 10)
      : "";

  const ignoreLine = (line) => {
    const lower = line.toLowerCase();
    return (
      lower.includes("@") ||
      /^(resume|curriculum vitae|cv)$/i.test(line) ||
      /^(phone|mobile|email|address|contact)\b/i.test(lower) ||
      /\d{5,}/.test(line)
    );
  };

  const name =
    lines.find((line) => {
      if (ignoreLine(line)) return false;
      const tokens = line.split(/\s+/).filter(Boolean);
      return tokens.length >= 2 && tokens.length <= 5 && /^[a-z .'-]+$/i.test(line);
    }) || "";

  const toAgeFromDob = (dobText) => {
    const normalized = String(dobText || "").trim();
    if (!normalized) return null;
    const parts = normalized.split(/[\/\-]/).map((item) => Number(item));
    if (parts.length !== 3 || parts.some((item) => !Number.isFinite(item))) return null;
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
      line
    )
  );

  const boardUniversity =
    educationSectionLines.find((line) => /(university|board)/i.test(line)) || null;
  const institutionName =
    educationSectionLines.find((line) => /(college|institute|institution|school)/i.test(line)) ||
    null;

  const degreeHints = [
    { pattern: /\b(phd|doctorate)\b/i, level: "phd" },
    { pattern: /\b(master|m\.?tech|m\.?e|mba|mca|m\.?sc)\b/i, level: "masters" },
    { pattern: /\b(bachelor|b\.?tech|b\.?e|bca|b\.?sc|bcom|ba)\b/i, level: "bachelors" },
    { pattern: /\b(12th|higher secondary|intermediate)\b/i, level: "12th" },
    { pattern: /\b(10th|secondary school)\b/i, level: "10th" },
  ];
  const matchedDegree = degreeHints.find((item) => item.pattern.test(text));

  return {
    full_name: name || null,
    email: emailMatch ? emailMatch[0] : null,
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
  if (!parsedData || typeof parsedData !== "object" || Array.isArray(parsedData)) return false;

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
      educationCandidate?.institutionName
    )
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
        .filter((item) => item.length >= 3)
    )
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
      overall_assessment: "ATS could not be calculated because job description is unavailable.",
    };
  }

  const resumeSet = new Set(resumeWords);
  const matchingKeywords = jobWords.filter((word) => resumeSet.has(word)).slice(0, 25);
  const missingKeywords = jobWords.filter((word) => !resumeSet.has(word)).slice(0, 25);

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
        ? [`Consider adding measurable experience with: ${missingKeywords.slice(0, 6).join(", ")}.`]
        : ["Maintain keyword alignment while improving role-specific achievements."],
    overall_assessment:
      percentage >= 75
        ? "Strong keyword alignment with the role."
        : percentage >= 50
        ? "Moderate keyword alignment with room for improvement."
        : "Low keyword alignment; resume tailoring recommended.",
  };
};

const readResumeText = async (resumeBuffer, extension) => {
  if (extension === "pdf") {
    const parsed = await pdfParse(resumeBuffer);
    return parsed.text || "";
  }

  if (extension === "docx") {
    const parsed = await mammoth.extractRawText({ buffer: resumeBuffer });
    return parsed.value || "";
  }

  throw new Error(`Unsupported resume format: ${extension}`);
};

const parseResumeWithAts = async ({ resumeBuffer, resumeFilename, jobDescription }) => {
  try {
    const extension = getResumeExtension(resumeFilename);
    if (!SUPPORTED_RESUME_TYPES.has(extension)) {
      return {
        ok: false,
        message: "Only PDF and DOCX resumes are supported.",
        parsedData: null,
        atsScore: null,
        atsMatchPercentage: null,
        atsRawJson: null,
      };
    }

    const resumeText = await readResumeText(resumeBuffer, extension);
    const aiParsedData = safeJson(await atsExtractor(resumeText), "resume data");
    const fallbackParsedData = extractAutofillFallbackFromText(resumeText);
    const parsedData = hasParsedAutofillSignal(aiParsedData) ? aiParsedData : fallbackParsedData;
    const aiAtsRawJson = String(jobDescription || "").trim()
      ? safeJson(await calculateAtsScore(resumeText, String(jobDescription).trim()), "ATS score")
      : null;
    const fallbackAtsRawJson = calculateFallbackAts(resumeText, String(jobDescription || "").trim());
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
        parsedDataSource: hasParsedAutofillSignal(aiParsedData) ? "ai" : "fallback",
        atsSource:
          aiAtsRawJson && typeof aiAtsRawJson === "object" && !aiAtsRawJson.error
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

const extractResumeAts = async ({ resumeBuffer, resumeFilename, jobDescription }) => {
  const extension = getResumeExtension(resumeFilename);
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
};
