const pdfParse = require("pdf-parse");
const mammoth = require("mammoth");

const extractTextFromBuffer = async (buffer, extension) => {
  const ext = String(extension || "").toLowerCase();

  if (ext === "pdf") {
    const parsed = await pdfParse(buffer);
    return parsed.text || "";
  }

  if (ext === "docx") {
    const parsed = await mammoth.extractRawText({ buffer });
    return parsed.value || "";
  }

  if (ext === "txt") {
    return buffer.toString("utf-8");
  }

  throw new Error(`Unsupported file format: ${extension}`);
};

function cleanJsonText(rawText) {
  if (!rawText) {
    return null;
  }

  let cleaned = rawText.trim();

  if (cleaned.includes("```json")) {
    const parts = cleaned.split("```json");
    if (parts.length > 1) {
      cleaned = parts[1].split("```")[0].trim();
    }
  } else if (cleaned.includes("```")) {
    const parts = cleaned.split("```");
    if (parts.length > 1) {
      cleaned = parts[1].split("```")[0].trim();
    }
  }

  return cleaned;
}

module.exports = {
  extractTextFromBuffer,
  cleanJsonText,
};
