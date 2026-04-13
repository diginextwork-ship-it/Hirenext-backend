const pdfParse = require("pdf-parse");
const mammoth = require("mammoth");

const normalizeExtractedText = (value) =>
  String(value || "")
    .replace(/\u0000/g, " ")
    .replace(/\r/g, "\n")
    .replace(/[^\S\n]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .join("\n")
    .trim();

const decodePdfLiteralString = (value) => {
  let decoded = "";
  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if (char !== "\\") {
      decoded += char;
      continue;
    }

    const next = value[index + 1];
    if (next === undefined) break;

    if (/[0-7]/.test(next)) {
      let octal = next;
      let offset = 1;
      while (
        offset < 3 &&
        /[0-7]/.test(value[index + offset + 1] || "")
      ) {
        octal += value[index + offset + 1];
        offset += 1;
      }
      decoded += String.fromCharCode(parseInt(octal, 8));
      index += offset;
      continue;
    }

    const replacements = {
      n: "\n",
      r: "\r",
      t: "\t",
      b: "\b",
      f: "\f",
      "\\": "\\",
      "(": "(",
      ")": ")",
    };
    decoded += replacements[next] ?? next;
    index += 1;
  }
  return decoded;
};

const decodePdfHexString = (value) => {
  const cleaned = String(value || "").replace(/[^0-9a-f]/gi, "");
  if (!cleaned) return "";
  const evenHex = cleaned.length % 2 === 0 ? cleaned : `${cleaned}0`;
  const bytes = Buffer.from(evenHex, "hex");

  // Many PDF generators store text as UTF-16BE hex strings.
  const looksUtf16Be =
    bytes.length >= 4 &&
    bytes.length % 2 === 0 &&
    bytes.filter((byte, index) => index % 2 === 0 && byte === 0).length >=
      Math.floor(bytes.length / 4);

  if (looksUtf16Be) {
    let decoded = "";
    for (let index = 0; index < bytes.length; index += 2) {
      decoded += String.fromCharCode((bytes[index] << 8) | bytes[index + 1]);
    }
    return decoded;
  }

  return bytes.toString("latin1");
};

const extractPdfLiteralStrings = (rawPdf) => {
  const chunks = [];
  let current = "";
  let depth = 0;

  for (let index = 0; index < rawPdf.length; index += 1) {
    const char = rawPdf[index];
    const escaped = rawPdf[index - 1] === "\\";

    if (char === "(" && !escaped) {
      if (depth === 0) current = "";
      else current += char;
      depth += 1;
      continue;
    }

    if (char === ")" && !escaped && depth > 0) {
      depth -= 1;
      if (depth === 0) {
        chunks.push(current);
        current = "";
      } else {
        current += char;
      }
      continue;
    }

    if (depth > 0) {
      current += char;
    }
  }

  return chunks.map(decodePdfLiteralString);
};

const extractPdfHexStrings = (rawPdf) =>
  Array.from(rawPdf.matchAll(/<([0-9a-f\s]{8,})>/gi), (match) =>
    decodePdfHexString(match[1]),
  );

const extractPrintableRuns = (buffer) =>
  Array.from(
    buffer
      .toString("latin1")
      .matchAll(/[A-Za-z0-9@][A-Za-z0-9@&(),./:+\-_ ]{5,}/g),
    (match) => match[0],
  );

const buildPdfFallbackText = (buffer) => {
  const rawPdf = buffer.toString("latin1");
  const candidates = [
    ...extractPdfLiteralStrings(rawPdf),
    ...extractPdfHexStrings(rawPdf),
    ...extractPrintableRuns(buffer),
  ]
    .map((item) => normalizeExtractedText(item))
    .filter((item) => /[A-Za-z]/.test(item) && item.length >= 3);

  const deduped = [];
  const seen = new Set();
  for (const item of candidates) {
    const key = item.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }

  return normalizeExtractedText(deduped.join("\n"));
};

const extractTextFromBuffer = async (buffer, extension) => {
  const ext = String(extension || "").toLowerCase();

  if (ext === "pdf") {
    try {
      const parsed = await pdfParse(buffer);
      const normalized = normalizeExtractedText(parsed.text);
      if (normalized) {
        return normalized;
      }
    } catch (error) {
      const fallbackText = buildPdfFallbackText(buffer);
      if (fallbackText) {
        return fallbackText;
      }
      throw error;
    }

    return buildPdfFallbackText(buffer);
  }

  if (ext === "docx") {
    const parsed = await mammoth.extractRawText({ buffer });
    return normalizeExtractedText(parsed.value);
  }

  if (ext === "txt") {
    return normalizeExtractedText(buffer.toString("utf-8"));
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
  buildPdfFallbackText,
};
