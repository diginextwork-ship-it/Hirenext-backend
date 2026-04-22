const { test } = require("node:test");
const assert = require("node:assert/strict");

const {
  extractAutofillFallbackFromText,
  mergeParsedDataWithFallback,
} = require("../src/resumeparser/service");

test("fallback parser prefers top-of-resume candidate contact details", () => {
  const resumeText = `
    Rahul Sharma
    Mobile: 9876543210 | Email: rahul.sharma@gmail.com
    Bengaluru

    Professional Summary
    Java Developer with 4 years of experience.

    References
    HR Team
    contact@company.com
    +91 9123456789
  `;

  const parsed = extractAutofillFallbackFromText(resumeText);

  assert.equal(parsed.full_name, "Rahul Sharma");
  assert.equal(parsed.email, "rahul.sharma@gmail.com");
  assert.equal(parsed.phone, "9876543210");
});

test("merged parser rejects AI contact details not present in resume text", () => {
  const resumeText = `
    Priya Nair
    Email: priya.nair@gmail.com
    Phone: 9988776655
    Kochi
  `;

  const merged = mergeParsedDataWithFallback({
    aiParsedData: {
      full_name: "Recruiter Person",
      email: "wrong.person@agency.com",
      phone: "9000011111",
      education: [
        {
          latest_education_level: "bachelors",
          board_university: "Anna University",
          institution_name: "ABC College",
        },
      ],
    },
    fallbackParsedData: extractAutofillFallbackFromText(resumeText),
    resumeText,
  });

  assert.equal(merged.full_name, "Priya Nair");
  assert.equal(merged.email, "priya.nair@gmail.com");
  assert.equal(merged.phone, "9988776655");
  assert.equal(merged.education[0].latest_education_level, "bachelors");
});

test("merged parser keeps valid AI education while using text-backed identity fields", () => {
  const resumeText = `
    Sandeep Kumar
    Email: sandeep.kumar@gmail.com
    Mobile: +91 9090909090

    Education
    B.Tech, National Institute of Technology
  `;

  const merged = mergeParsedDataWithFallback({
    aiParsedData: {
      full_name: "Sandeep Kumar",
      email: "sandeep.kumar@gmail.com",
      phone: "9090909090",
      education: [
        {
          latest_education_level: "bachelors",
          board_university: "NIT",
          institution_name: "National Institute of Technology",
        },
      ],
      age: "24",
    },
    fallbackParsedData: extractAutofillFallbackFromText(resumeText),
    resumeText,
  });

  assert.equal(merged.full_name, "Sandeep Kumar");
  assert.equal(merged.email, "sandeep.kumar@gmail.com");
  assert.equal(merged.phone, "9090909090");
  assert.equal(merged.education[0].institution_name, "National Institute of Technology");
  assert.equal(merged.age, "24");
});
