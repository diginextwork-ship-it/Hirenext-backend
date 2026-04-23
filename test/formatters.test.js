const test = require("node:test");
const assert = require("node:assert/strict");

const {
  extractCandidateSnapshot,
  normalizePhoneForStorage,
} = require("../src/utils/formatters");

test("normalizePhoneForStorage keeps the recruiter-edited 10 digit value", () => {
  assert.equal(normalizePhoneForStorage(["9876543210", "9123456789"]), "9123456789");
});

test("extractCandidateSnapshot prefers recruiter-entered repeated multipart fields", () => {
  const snapshot = extractCandidateSnapshot({
    source: {
      candidate_name: ["Wrong Parsed Name", "Correct Recruiter Name"],
      phone: ["9000011111", "9876543210"],
      email: ["wrong@example.com", "correct@example.com"],
      latest_education_level: ["Bachelor's"],
      institution_name: ["Correct College"],
      age: ["24"],
    },
    parsedData: {
      full_name: "Wrong Parsed Name",
      phone: "9000011111",
      email: "wrong@example.com",
      education: [
        {
          latest_education_level: "Master's",
          institution_name: "Wrong College",
        },
      ],
      age: "31",
    },
  });

  assert.equal(snapshot.name, "Correct Recruiter Name");
  assert.equal(snapshot.phone, "9876543210");
  assert.equal(snapshot.email, "correct@example.com");
  assert.equal(snapshot.levelOfEdu, "Bachelor's");
  assert.equal(snapshot.institutionName, "Correct College");
  assert.equal(snapshot.age, 24);
});
