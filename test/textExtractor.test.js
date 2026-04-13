const { test } = require("node:test");
const assert = require("node:assert/strict");

const {
  buildPdfFallbackText,
  extractTextFromBuffer,
} = require("../src/utils/textExtractor");

test("buildPdfFallbackText extracts literal strings from malformed pdf content", () => {
  const malformedPdfBuffer = Buffer.from(
    "%PDF-1.4\n1 0 obj\n<< /Type /Page >>\nstream\nBT\n/F1 12 Tf\n72 720 Td\n(John Doe) Tj\n(email@example.com) Tj\n(9876543210) Tj\nendstream\nxref\nbad entry\n%%EOF",
    "latin1",
  );

  const extracted = buildPdfFallbackText(malformedPdfBuffer);

  assert.match(extracted, /John Doe/);
  assert.match(extracted, /email@example\.com/);
  assert.match(extracted, /9876543210/);
});

test("extractTextFromBuffer falls back when pdf parsing fails", async () => {
  const malformedPdfBuffer = Buffer.from(
    "%PDF-1.4\n1 0 obj\n<< /Length 44 >>\nstream\nBT\n(Resume Candidate) Tj\n(candidate@example.com) Tj\nendstream\nxref\n0 1\nbad Xref entry\n%%EOF",
    "latin1",
  );

  const extracted = await extractTextFromBuffer(malformedPdfBuffer, "pdf");

  assert.match(extracted, /Resume Candidate/);
  assert.match(extracted, /candidate@example\.com/);
});
