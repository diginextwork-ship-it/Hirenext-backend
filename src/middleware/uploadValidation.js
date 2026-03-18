const ALLOWED_MIME_TYPES = new Set([
  "application/pdf",
  "application/msword",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]);

const ALLOWED_EXTENSIONS = new Set(["pdf", "doc", "docx"]);

const getExtension = (filename) => {
  const match = String(filename || "").trim().toLowerCase().match(/\.([a-z0-9]+)$/);
  return match ? match[1] : "";
};

const looksLikePdf = (buffer) => buffer.length >= 4 && buffer.subarray(0, 4).toString() === "%PDF";

const looksLikeDocx = (buffer) => {
  if (buffer.length < 4) return false;
  return (
    buffer[0] === 0x50 &&
    buffer[1] === 0x4b &&
    buffer[2] === 0x03 &&
    buffer[3] === 0x04
  );
};

const looksLikeDoc = (buffer) => {
  if (buffer.length < 8) return false;
  return (
    buffer[0] === 0xd0 &&
    buffer[1] === 0xcf &&
    buffer[2] === 0x11 &&
    buffer[3] === 0xe0 &&
    buffer[4] === 0xa1 &&
    buffer[5] === 0xb1 &&
    buffer[6] === 0x1a &&
    buffer[7] === 0xe1
  );
};

const validateFileSignature = (extension, buffer) => {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return false;
  if (extension === "pdf") return looksLikePdf(buffer);
  if (extension === "docx") return looksLikeDocx(buffer);
  if (extension === "doc") return looksLikeDoc(buffer);
  return false;
};

const validateResumeFile = ({ filename, mimetype, buffer, maxBytes = 5 * 1024 * 1024 }) => {
  const extension = getExtension(filename);
  const normalizedMime = String(mimetype || "").trim().toLowerCase();

  if (!ALLOWED_EXTENSIONS.has(extension)) {
    return { ok: false, message: "Only PDF, DOC, or DOCX files are allowed." };
  }

  if (normalizedMime && !ALLOWED_MIME_TYPES.has(normalizedMime)) {
    return { ok: false, message: "Invalid resume MIME type." };
  }

  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    return { ok: false, message: "Resume file content is invalid." };
  }

  if (buffer.length > maxBytes) {
    return { ok: false, message: "Resume file size must be 5MB or less." };
  }

  if (!validateFileSignature(extension, buffer)) {
    return { ok: false, message: "Resume file signature does not match file extension." };
  }

  return { ok: true, extension, mimeType: normalizedMime || null };
};

module.exports = { validateResumeFile };
