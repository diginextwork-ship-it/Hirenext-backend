const express = require("express");
const router = express.Router();
const multer = require("multer");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const pdfParse = require("pdf-parse");
const mammoth = require("mammoth");

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      "application/pdf",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      "text/plain",
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(
        new Error(
          "Invalid file type. Only PDF, DOCX, and TXT files are allowed.",
        ),
      );
    }
  },
});

// Initialize Gemini AI
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// Extract text from different file types
async function extractTextFromFile(file) {
  try {
    console.log(
      `Extracting text from file: ${file.originalname} (${file.mimetype})`,
    );

    if (file.mimetype === "application/pdf") {
      const data = await pdfParse(file.buffer);
      const text = data.text;
      console.log(`Extracted ${text.length} characters from PDF`);
      return text;
    } else if (
      file.mimetype ===
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ) {
      const result = await mammoth.extractRawText({ buffer: file.buffer });
      const text = result.value;
      console.log(`Extracted ${text.length} characters from DOCX`);
      return text;
    } else if (file.mimetype === "text/plain") {
      const text = file.buffer.toString("utf-8");
      console.log(`Extracted ${text.length} characters from TXT`);
      return text;
    }
    throw new Error(`Unsupported file type: ${file.mimetype}`);
  } catch (error) {
    console.error("Text extraction error:", error);
    throw new Error(`Failed to extract text: ${error.message}`);
  }
}

// Parse JD using Gemini AI
async function parseJDWithAI(jdText) {
  try {
    console.log("Starting JD parsing with AI...");
    if (!process.env.GEMINI_API_KEY) {
      throw new Error(
        "GEMINI_API_KEY is not configured in environment variables",
      );
    }

    const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });

    const prompt = `You are an expert HR data extraction system. Extract the following information from the job description below and return it as valid JSON only (no markdown, no explanation).

Required fields:
- company_name (string): Extract company name if mentioned, otherwise return "Not Specified"
- role_name (string): Job title/designation
- city (string): City location
- state (string): State location
- pincode (string): PIN code if mentioned, otherwise "000000"
- positions_open (number): Number of openings, default to 1 if not specified
- skills (string): Comma-separated list of required skills
- experience (string): Experience required (e.g., "1-3 years", "Fresher")
- salary (string): Salary range mentioned
- qualification (string): Educational qualifications required
- benefits (string): Any benefits/perks mentioned
- job_description (string): Full detailed job description preserving all important information

Rules:
1. If a field is not found, use sensible defaults (empty string for text, 1 for positions_open, "000000" for pincode)
2. For location fields, if only state is mentioned, leave city as empty string
3. Extract skills from both explicit skills sections and job responsibilities
4. Preserve formatting in job_description but keep it clean
5. Return ONLY valid JSON, no additional text

Job Description:
${jdText}

Return the extracted data as JSON:`;

    const result = await model.generateContent(prompt);
    const response = result.response;
    const text = response.text();
    console.log("Raw Gemini response received, cleaning...");

    // Clean the response - remove markdown code blocks if present
    let cleanedText = text.trim();
    if (cleanedText.startsWith("```json")) {
      cleanedText = cleanedText
        .replace(/```json\n?/g, "")
        .replace(/```\n?$/g, "");
    } else if (cleanedText.startsWith("```")) {
      cleanedText = cleanedText.replace(/```\n?/g, "").replace(/```\n?$/g, "");
    }

    console.log("Cleaned response:", cleanedText.substring(0, 100) + "...");

    let parsedData;
    try {
      parsedData = JSON.parse(cleanedText);
    } catch (jsonError) {
      console.error("JSON Parse Error:", jsonError.message);
      console.error("Failed to parse response:", cleanedText);
      throw new Error(`Invalid JSON response from AI: ${jsonError.message}`);
    }

    // Validate and set defaults
    return {
      company_name: parsedData.company_name || "Not Specified",
      role_name: parsedData.role_name || "Not Specified",
      city: parsedData.city || "",
      state: parsedData.state || "",
      pincode: parsedData.pincode || "000000",
      positions_open: parseInt(parsedData.positions_open) || 1,
      skills: parsedData.skills || "",
      experience: parsedData.experience || "Not Specified",
      salary: parsedData.salary || "Not Specified",
      qualification: parsedData.qualification || "",
      benefits: parsedData.benefits || "",
      job_description: parsedData.job_description || jdText,
      revenue: 0, // Default revenue
      access_mode: "open", // Default access mode
    };
  } catch (error) {
    console.error("AI Parsing Error:", error);
    throw new Error(`Failed to parse JD with AI: ${error.message}`);
  }
}

// Route: Upload and parse JD file
router.post("/upload", upload.single("jdFile"), async (req, res) => {
  try {
    console.log("[JD Parser] Upload request received");

    if (!req.file) {
      console.log("[JD Parser] No file uploaded");
      return res.status(400).json({ error: "No file uploaded" });
    }

    console.log(`[JD Parser] Processing file: ${req.file.originalname}`);

    // Extract text from file
    const jdText = await extractTextFromFile(req.file);

    if (!jdText || jdText.trim().length === 0) {
      console.log("[JD Parser] Extracted text is empty");
      return res
        .status(400)
        .json({ error: "Could not extract text from file" });
    }

    console.log("[JD Parser] Text extraction successful, starting AI parsing");

    // Parse with AI
    const parsedData = await parseJDWithAI(jdText);

    console.log("[JD Parser] Parsing successful");

    res.json({
      success: true,
      data: parsedData,
      originalFileName: req.file.originalname,
    });
  } catch (error) {
    console.error("[JD Parser] Upload/Parse Error:", error);
    res.status(500).json({
      error: "Failed to process JD file",
      details: error.message,
    });
  }
});

// Route: Parse text directly (for testing or manual input)
router.post("/parse-text", async (req, res) => {
  try {
    const { jdText } = req.body;

    if (!jdText || jdText.trim().length === 0) {
      return res.status(400).json({ error: "No JD text provided" });
    }

    const parsedData = await parseJDWithAI(jdText);

    res.json({
      success: true,
      data: parsedData,
    });
  } catch (error) {
    console.error("Parse Text Error:", error);
    res.status(500).json({
      error: "Failed to parse JD text",
      details: error.message,
    });
  }
});

// Route: Save parsed JD to database
router.post("/save", async (req, res) => {
  try {
    const db = req.app.get("db");
    const { recruiter_rid, parsedData } = req.body;

    if (!parsedData) {
      return res.status(400).json({ error: "No parsed data provided" });
    }

    // Generate JID
    const jid = `JOB${Date.now()}${Math.random().toString(36).substr(2, 5).toUpperCase()}`;

    // Insert into database
    const query = `
      INSERT INTO jobs (
        jid, recruiter_rid, city, state, pincode, company_name, 
        role_name, positions_open, skills, job_description, 
        experience, salary, qualification, benefits, revenue, access_mode
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    const values = [
      jid,
      recruiter_rid || null,
      parsedData.city,
      parsedData.state,
      parsedData.pincode,
      parsedData.company_name,
      parsedData.role_name,
      parsedData.positions_open,
      parsedData.skills,
      parsedData.job_description,
      parsedData.experience,
      parsedData.salary,
      parsedData.qualification,
      parsedData.benefits,
      parsedData.revenue || 0,
      parsedData.access_mode || "open",
    ];

    const [result] = await db.execute(query, values);

    res.json({
      success: true,
      jid: jid,
      message: "Job posting created successfully",
    });
  } catch (error) {
    console.error("Save JD Error:", error);
    res.status(500).json({
      error: "Failed to save job posting",
      details: error.message,
    });
  }
});

module.exports = router;
