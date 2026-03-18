const { GoogleGenerativeAI } = require("@google/generative-ai");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");
const dotenv = require("dotenv");

dotenv.config();

// Load API key from environment or config file
let apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || process.env.API_KEY;
let apiKeySource = "environment";

if (!apiKey) {
  apiKeySource = "config_yaml";
  try {
    const configPath = path.join(__dirname, "config.yaml");
    const fileContents = fs.readFileSync(configPath, "utf8");
    const config = yaml.load(fileContents);
    apiKey = config?.GEMINI_API_KEY;
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.error("ERROR loading config.yaml:", error.message);
    }
  }
}

if (!apiKey) {
  apiKeySource = "missing";
  console.error(
    "ERROR: GEMINI_API_KEY not found in environment or config.yaml",
  );
  console.error("Set GEMINI_API_KEY or add it to config.yaml");
  console.error(
    "Get your free API key at: https://makersuite.google.com/app/apikey",
  );
} else {
  console.log("Gemini API key loaded successfully");
}

// Initialize Gemini AI
const genAI = apiKey ? new GoogleGenerativeAI(apiKey) : null;
const DEFAULT_GEMINI_MODELS = ["gemini-2.5-flash"];
const unsupportedModels = new Set();
let rateLimitRetryAtMs = 0;

const getGeminiModelCandidates = () => {
  const rawModels = String(process.env.GEMINI_MODEL || "").trim();
  const envModels = rawModels
    ? rawModels
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    : [];
  const configured = envModels.length
    ? [
        ...envModels,
        ...DEFAULT_GEMINI_MODELS.filter(
          (modelName) => !envModels.includes(modelName),
        ),
      ]
    : DEFAULT_GEMINI_MODELS;

  return configured.filter((modelName) => !unsupportedModels.has(modelName));
};

const toTimeoutMs = () => {
  // Increased default timeout from 12s to 30s
  const configured = Number(process.env.GEMINI_TIMEOUT_MS || 30000);
  if (!Number.isFinite(configured) || configured < 1000) return 30000;
  return Math.floor(configured);
};

const isGeminiEnabled = () => {
  return (
    String(process.env.GEMINI_ENABLED || "true")
      .trim()
      .toLowerCase() !== "false"
  );
};

const parseRetryDelayMs = (error) => {
  const rawMessage = String(error?.message || "");
  const retryInfoMatch = rawMessage.match(/"retryDelay":"(\d+)s"/i);
  if (retryInfoMatch) {
    const seconds = Number(retryInfoMatch[1]);
    if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000;
  }

  const plainMatch = rawMessage.match(/Please retry in\s+([\d.]+)s/i);
  if (plainMatch) {
    const seconds = Number(plainMatch[1]);
    if (Number.isFinite(seconds) && seconds > 0)
      return Math.ceil(seconds * 1000);
  }

  return 15000;
};

/**
 * Generate content with retry logic for timeout errors
 */
const generateWithRetry = async (model, prompt, maxRetries = 2) => {
  const timeoutMs = toTimeoutMs();
  let lastError = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      console.log(
        `Attempting Gemini API call (attempt ${attempt + 1}/${maxRetries + 1}, timeout: ${timeoutMs}ms)...`,
      );

      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(
          () =>
            reject(new Error(`Gemini request timeout after ${timeoutMs}ms`)),
          timeoutMs,
        );
      });

      const result = await Promise.race([
        model.generateContent(prompt),
        timeoutPromise,
      ]);
      const response = await result.response;

      console.log(`Gemini API call succeeded on attempt ${attempt + 1}`);
      return response.text();
    } catch (error) {
      lastError = error;
      const errorText = String(error?.message || "").toLowerCase();

      // Don't retry on non-timeout errors
      if (!errorText.includes("timeout")) {
        throw error;
      }

      // On last attempt, throw the error
      if (attempt === maxRetries) {
        console.error(`All ${maxRetries + 1} attempts failed with timeout`);
        throw error;
      }

      // Wait before retrying (exponential backoff)
      const waitMs = 1000 * Math.pow(2, attempt);
      console.log(
        `Timeout on attempt ${attempt + 1}, retrying in ${waitMs}ms...`,
      );
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  throw lastError;
};

const generateWithFallbackModels = async (prompt, options = {}) => {
  const { maxRetries = 2, chunkSize = null } = options;

  if (!isGeminiEnabled()) {
    throw new Error("Gemini disabled by GEMINI_ENABLED=false");
  }

  if (!genAI) {
    throw new Error("Gemini API not configured");
  }

  if (rateLimitRetryAtMs > Date.now()) {
    throw new Error(
      `Gemini temporarily rate-limited until ${new Date(rateLimitRetryAtMs).toISOString()}`,
    );
  }

  const modelCandidates = getGeminiModelCandidates();
  if (modelCandidates.length === 0) {
    throw new Error(
      "No supported Gemini models available. Set GEMINI_MODEL to a valid model.",
    );
  }

  let lastError = null;

  for (const modelName of modelCandidates) {
    try {
      const model = genAI.getGenerativeModel({ model: modelName });

      // If prompt is very large and chunkSize specified, consider truncating
      let processedPrompt = prompt;
      if (chunkSize && prompt.length > chunkSize) {
        console.log(
          `Large prompt detected (${prompt.length} chars), truncating to ${chunkSize} chars`,
        );
        processedPrompt =
          prompt.substring(0, chunkSize) +
          "\n\n[Content truncated due to length]";
      }

      return await generateWithRetry(model, processedPrompt, maxRetries);
    } catch (error) {
      lastError = error;
      const errorText = String(error?.message || "").toLowerCase();

      if (
        errorText.includes("404 not found") ||
        errorText.includes("is not found for api version")
      ) {
        unsupportedModels.add(modelName);
      }

      if (
        errorText.includes("429 too many requests") ||
        errorText.includes("quota exceeded")
      ) {
        rateLimitRetryAtMs = Date.now() + parseRetryDelayMs(error);
      }

      console.error(
        `Gemini call failed for model '${modelName}':`,
        error.message,
      );
    }
  }

  throw (
    lastError ||
    new Error("Failed to generate content with configured Gemini models")
  );
};

const getGeminiStatus = () => ({
  configured: Boolean(apiKey),
  enabled: isGeminiEnabled(),
  keySource: apiKeySource,
  modelCandidates: getGeminiModelCandidates(),
  timeoutMs: toTimeoutMs(),
  unsupportedModels: Array.from(unsupportedModels),
  rateLimitedUntil:
    rateLimitRetryAtMs > Date.now()
      ? new Date(rateLimitRetryAtMs).toISOString()
      : null,
});

/**
 * Clean JSON text by removing markdown code blocks
 */
function cleanJsonText(rawText) {
  if (!rawText) {
    return null;
  }

  let cleaned = rawText.trim();

  // Remove ```json ... ``` blocks
  if (cleaned.includes("```json")) {
    const parts = cleaned.split("```json");
    if (parts.length > 1) {
      cleaned = parts[1].split("```")[0].trim();
    }
  } else if (cleaned.includes("```")) {
    // Remove generic ``` blocks
    const parts = cleaned.split("```");
    if (parts.length > 1) {
      cleaned = parts[1].split("```")[0].trim();
    }
  }

  return cleaned;
}

/**
 * Extract structured data from resume with optimized prompting
 */
async function atsExtractor(resumeData) {
  // Truncate very large resumes to avoid timeout
  const maxResumeLength = 15000;
  const truncatedResume =
    resumeData.length > maxResumeLength
      ? resumeData.substring(0, maxResumeLength) +
        "\n\n[Resume truncated for processing]"
      : resumeData;

  const prompt = `
    You are an AI assistant that parses resumes into strict JSON.

    Extract these fields from the resume:
    1. full_name
    2. email
    3. phone
    4. github_portfolio
    5. linkedin_id
    6. employment_details (array of short strings)
    7. technical_skills (array of short strings)
    8. soft_skills (array of short strings)
    9. education (array of objects) where each object has:
       - latest_education_level
       - board_university
       - institution_name
    10. age

    Return valid JSON only. Do not include markdown or code blocks.
    Use null when a field is missing.

    Resume:
    ${truncatedResume}
  `;

  try {
    console.log(`Extracting resume data (${resumeData.length} chars)...`);
    const text = await generateWithFallbackModels(prompt, {
      maxRetries: 2,
      chunkSize: 20000,
    });

    return cleanJsonText(text);
  } catch (error) {
    console.error("Error calling Gemini API in atsExtractor:", error.message);
    return null;
  }
}

/**
 * Calculate ATS score by comparing resume with job description
 */
async function calculateAtsScore(resumeData, jobDescription) {
  // Truncate large inputs
  const maxResumeLength = 10000;
  const maxJdLength = 5000;

  const truncatedResume =
    resumeData.length > maxResumeLength
      ? resumeData.substring(0, maxResumeLength) + "\n\n[Resume truncated]"
      : resumeData;

  const truncatedJd =
    jobDescription.length > maxJdLength
      ? jobDescription.substring(0, maxJdLength) +
        "\n\n[Job description truncated]"
      : jobDescription;

  const prompt = `
    You are an ATS analyzer. Compare the resume with the job description and return strict JSON.

    Include:
    1. ats_score (0-100 number)
    2. match_percentage (string like "83%")
    3. matching_keywords (array)
    4. missing_keywords (array)
    5. strengths (array)
    6. weaknesses (array)
    7. recommendations (array)
    8. overall_assessment (short string)

    JSON format:
    {
        "ats_score": 0,
        "match_percentage": "0%",
        "matching_keywords": [],
        "missing_keywords": [],
        "strengths": [],
        "weaknesses": [],
        "recommendations": [],
        "overall_assessment": ""
    }

    JOB DESCRIPTION:
    ${truncatedJd}

    RESUME:
    ${truncatedResume}
  `;

  try {
    console.log(
      `Calculating ATS score (Resume: ${resumeData.length} chars, JD: ${jobDescription.length} chars)...`,
    );
    const text = await generateWithFallbackModels(prompt, {
      maxRetries: 2,
      chunkSize: 20000,
    });

    return cleanJsonText(text);
  } catch (error) {
    console.error("Error calculating ATS score with Gemini:", error.message);
    return null;
  }
}

module.exports = {
  atsExtractor,
  calculateAtsScore,
  getGeminiStatus,
};
