const pool = require("../config/db");
const { normalizeWorkflowStatus } = require("./resumeStatusFlow");
const { buildResumeCompatibilityFields } = require("./resumeCompatibility");

const DUPLICATE_RESUME_ALLOWED_STATUSES = new Set([
  "rejected",
  "dropout",
  "left",
]);

const tableExists = async (tableName) => {
  try {
    const [rows] = await pool.query(
      `SELECT 1
       FROM information_schema.tables
       WHERE table_schema = DATABASE() AND table_name = ?
       LIMIT 1`,
      [tableName],
    );
    if (rows.length > 0) return true;
  } catch {}

  try {
    await pool.query(`SELECT 1 FROM \`${tableName}\` LIMIT 1`);
    return true;
  } catch {
    return false;
  }
};

const columnExists = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  return rows.length > 0;
};

const getTableColumns = async (tableName, connection = pool) => {
  const [rows] = await connection.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ?`,
    [tableName],
  );
  return new Set(
    rows.map((row) =>
      String(
        row.column_name ?? row.COLUMN_NAME ?? Object.values(row)[0] ?? "",
      )
        .trim()
        .toLowerCase(),
    ),
  );
};

const getColumnMetadata = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT
      COLUMN_TYPE AS columnType,
      DATA_TYPE AS dataType,
      IS_NULLABLE AS isNullable,
      CHARACTER_SET_NAME AS charset,
      COLLATION_NAME AS collation
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  return rows[0] || null;
};

const constraintExists = async (tableName, constraintName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.table_constraints
     WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = ?
     LIMIT 1`,
    [tableName, constraintName],
  );
  return rows.length > 0;
};

const getColumnMaxLength = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT CHARACTER_MAXIMUM_LENGTH AS maxLength
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  if (rows.length === 0) return null;
  const parsed = Number(rows[0].maxLength);
  return Number.isFinite(parsed) ? parsed : null;
};

const fetchExtraInfoByResumeIds = async (resumeIds, connection = pool) => {
  const normalizedResumeIds = Array.from(
    new Set(
      (Array.isArray(resumeIds) ? resumeIds : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );
  if (normalizedResumeIds.length === 0) return new Map();
  if (!(await tableExists("extra_info"))) return new Map();

  const columns = await getTableColumns("extra_info", connection);
  const resumeIdColumns = [];
  if (columns.has("res_id")) resumeIdColumns.push("res_id");
  if (columns.has("resume_id")) resumeIdColumns.push("resume_id");
  const hasSubmittedReason = columns.has("submitted_reason");
  const hasVerifiedReason = columns.has("verified_reason");
  const hasOthersReason = columns.has("others_reason");
  const hasWalkInReason = columns.has("walk_in_reason");
  const hasFurtherReason = columns.has("further_reason");
  const hasSelectReason = columns.has("select_reason");
  const hasShortlistedReason =
    columns.has("shortlisted_reason") || columns.has("pending_joining_reason");
  const hasJoinedReason = columns.has("joined_reason");
  const hasDropoutReason = columns.has("dropout_reason");
  const hasRejectReason = columns.has("reject_reason");
  const hasLeftReason = columns.has("left_reason");
  const hasBilledReason = columns.has("billed_reason");
  const hasOfficeLocationCity = columns.has("office_location_city");

  const hasAnyReason =
    hasSubmittedReason ||
    hasVerifiedReason ||
    hasOthersReason ||
    hasWalkInReason ||
    hasFurtherReason ||
    hasSelectReason ||
    hasShortlistedReason ||
    hasJoinedReason ||
    hasDropoutReason ||
    hasRejectReason ||
    hasLeftReason ||
    hasBilledReason ||
    hasOfficeLocationCity;

  if (resumeIdColumns.length === 0 || !hasAnyReason) return new Map();

  const selectColumns = [];
  if (columns.has("res_id")) selectColumns.push("res_id AS resId");
  if (columns.has("resume_id")) selectColumns.push("resume_id AS resumeId");
  if (hasSubmittedReason)
    selectColumns.push("submitted_reason AS submittedReason");
  if (hasVerifiedReason)
    selectColumns.push("verified_reason AS verifiedReason");
  if (hasOthersReason) selectColumns.push("others_reason AS othersReason");
  if (hasWalkInReason) selectColumns.push("walk_in_reason AS walkInReason");
  if (hasFurtherReason) selectColumns.push("further_reason AS furtherReason");
  if (hasSelectReason) selectColumns.push("select_reason AS selectReason");
  if (columns.has("shortlisted_reason")) {
    selectColumns.push("shortlisted_reason AS shortlistedReason");
  } else if (columns.has("pending_joining_reason")) {
    selectColumns.push("pending_joining_reason AS shortlistedReason");
  }
  if (hasJoinedReason) selectColumns.push("joined_reason AS joinedReason");
  if (hasDropoutReason) selectColumns.push("dropout_reason AS dropoutReason");
  if (hasRejectReason) selectColumns.push("reject_reason AS rejectReason");
  if (hasLeftReason) selectColumns.push("left_reason AS leftReason");
  if (hasBilledReason) selectColumns.push("billed_reason AS billedReason");
  if (hasOfficeLocationCity) {
    selectColumns.push("office_location_city AS officeLocationCity");
  }

  const whereConditions = resumeIdColumns.map((column) => `${column} IN (?)`);
  const queryParams = resumeIdColumns.map(() => normalizedResumeIds);
  const [rows] = await connection.query(
    `SELECT ${selectColumns.join(", ")}
     FROM extra_info
     WHERE ${whereConditions.join(" OR ")}`,
    queryParams,
  );

  const isPresent = (value) =>
    value !== null &&
    value !== undefined &&
    !(typeof value === "string" && value.trim() === "");
  const mergeByPresence = (current, next) => {
    const merged = { ...current };
    for (const [key, value] of Object.entries(next)) {
      if (!isPresent(merged[key]) && isPresent(value)) {
        merged[key] = value;
      }
    }
    return merged;
  };

  const extraInfoMap = new Map();
  for (const row of rows) {
    const resumeKey = String(row.resId || row.resumeId || "").trim();
    if (!resumeKey) continue;
    const normalizedRow = {
      ...buildResumeCompatibilityFields({
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        othersReason: row.othersReason || null,
        walkInReason: row.walkInReason || null,
        furtherReason: row.furtherReason || null,
        selectReason: row.selectReason || null,
        shortlistedReason: row.shortlistedReason || null,
        joinedReason: row.joinedReason || null,
        joiningNote: row.joinedReason || null,
        dropoutReason: row.dropoutReason || null,
        rejectReason: row.rejectReason || null,
        leftReason: row.leftReason || null,
        billedReason: row.billedReason || null,
      }),
      furtherReason: row.furtherReason || null,
      officeLocationCity: row.officeLocationCity || null,
      office_location_city: row.officeLocationCity || null,
    };
    const existing = extraInfoMap.get(resumeKey);
    extraInfoMap.set(
      resumeKey,
      existing ? mergeByPresence(existing, normalizedRow) : normalizedRow,
    );
  }

  return extraInfoMap;
};

const findExistingResumeMatches = async (
  connection,
  { candidateName, phone, email } = {},
) => {
  const normalizedName = String(candidateName || "").trim().toLowerCase();
  const normalizedPhone = String(phone || "").trim();
  const normalizedEmail = String(email || "")
    .trim()
    .toLowerCase();

  if (!normalizedName || !normalizedPhone || !normalizedEmail) {
    return [];
  }
  if (!(await tableExists("candidate"))) {
    return [];
  }

  const [rows] = await connection.query(
    `SELECT
      rd.res_id AS resId,
      rd.job_jid AS jobJid,
      rd.uploaded_at AS uploadedAt,
      COALESCE(jrs.selection_status, 'submitted') AS workflowStatus
    FROM candidate c
    INNER JOIN resumes_data rd
      ON rd.res_id = c.res_id
    LEFT JOIN job_resume_selection jrs
      ON jrs.res_id = rd.res_id
     AND (jrs.job_jid = rd.job_jid OR (jrs.job_jid IS NULL AND rd.job_jid IS NULL))
    WHERE LOWER(TRIM(COALESCE(c.name, ''))) = ?
      AND TRIM(COALESCE(c.phone, '')) = ?
      AND LOWER(TRIM(COALESCE(c.email, ''))) = ?
    ORDER BY rd.uploaded_at DESC, rd.res_id DESC`,
    [normalizedName, normalizedPhone, normalizedEmail],
  );

  return rows.map((row) => ({
    resId: row.resId ? String(row.resId).trim() : null,
    jobJid:
      row.jobJid === null || row.jobJid === undefined
        ? null
        : String(row.jobJid).trim(),
    uploadedAt: row.uploadedAt || null,
    workflowStatus: normalizeWorkflowStatus(row.workflowStatus),
  }));
};

const evaluateResumeDuplicateDecision = (matches = []) => {
  const normalizedMatches = Array.isArray(matches) ? matches : [];
  const latestMatch = normalizedMatches[0] || null;
  const blockingMatch =
    latestMatch &&
    !DUPLICATE_RESUME_ALLOWED_STATUSES.has(
      normalizeWorkflowStatus(latestMatch.workflowStatus),
    )
      ? latestMatch
      : null;

  return {
    hasMatch: normalizedMatches.length > 0,
    latestMatch,
    allowSubmission: !blockingMatch,
    blockingMatch,
    matches: normalizedMatches,
  };
};

const findResumeDuplicateDecision = async (connection, candidateIdentity) =>
  evaluateResumeDuplicateDecision(
    await findExistingResumeMatches(connection, candidateIdentity),
  );

const resolveResumeBinaryStorage = async (connection = pool) => {
  const [hasBlobTable, hasInlineResumeColumn] = await Promise.all([
    tableExists("resumes_blob"),
    columnExists("resumes_data", "resume"),
  ]);

  return {
    hasBlobTable,
    hasInlineResumeColumn,
  };
};

const buildResumeBinarySelect = async (
  connection = pool,
  { resumesAlias = "rd", blobAlias = "rb" } = {},
) => {
  const storage = await resolveResumeBinaryStorage(connection);

  if (storage.hasBlobTable) {
    return {
      resumeSelectSql: `${blobAlias}.resume AS resume`,
      resumeJoinSql: `LEFT JOIN resumes_blob ${blobAlias} ON ${blobAlias}.res_id = ${resumesAlias}.res_id`,
      storage,
    };
  }

  if (storage.hasInlineResumeColumn) {
    return {
      resumeSelectSql: `${resumesAlias}.resume AS resume`,
      resumeJoinSql: "",
      storage,
    };
  }

  throw new Error(
    "Resume binary storage is unavailable. Neither resumes_blob nor resumes_data.resume exists.",
  );
};

const storeResumeBinary = async (connection, resId, resumeBuffer) => {
  const normalizedResId = String(resId || "").trim();
  if (!normalizedResId) {
    throw new Error("resId is required to store the resume binary.");
  }

  const storage = await resolveResumeBinaryStorage(connection);
  let stored = false;

  if (storage.hasBlobTable) {
    await connection.query(
      `INSERT INTO resumes_blob (res_id, resume)
       VALUES (?, ?)
       ON DUPLICATE KEY UPDATE resume = VALUES(resume)`,
      [normalizedResId, resumeBuffer],
    );
    stored = true;
  }

  if (storage.hasInlineResumeColumn) {
    await connection.query(
      `UPDATE resumes_data
       SET resume = ?
       WHERE res_id = ?`,
      [resumeBuffer, normalizedResId],
    );
    stored = true;
  }

  if (!stored) {
    throw new Error(
      "Resume binary storage is unavailable. Neither resumes_blob nor resumes_data.resume exists.",
    );
  }

  return storage;
};

const upsertExtraInfoFields = async (connection, payload) => {
  if (!(await tableExists("extra_info"))) return;

  const columns = await getTableColumns("extra_info", connection);
  if (!columns.has("res_id") && !columns.has("resume_id")) {
    if (String(process.env.DEBUG_EXTRA_INFO || "").trim() === "1") {
      console.warn(
        "[extra_info] Skipping upsert because extra_info has no res_id/resume_id column.",
      );
    }
    return;
  }
  const updates = [];
  const insertColumns = [];
  const insertValues = [];
  const placeholders = [];

  const addColumnValue = (columnName, value) => {
    if (!columns.has(columnName) || value === undefined) return;
    insertColumns.push(columnName);
    insertValues.push(value);
    placeholders.push("?");
  };

  addColumnValue("res_id", payload.resId);
  addColumnValue("resume_id", payload.resId);
  addColumnValue("job_jid", payload.jobJid);
  addColumnValue("recruiter_rid", payload.recruiterRid);
  addColumnValue("rid", payload.recruiterRid);
  if (
    payload.officeLocationCity !== undefined &&
    columns.has("office_location_city")
  ) {
    insertColumns.push("office_location_city");
    insertValues.push(payload.officeLocationCity);
    placeholders.push("?");
    updates.push("office_location_city = VALUES(office_location_city)");
  }
  if (
    payload.submittedReason !== undefined &&
    columns.has("submitted_reason")
  ) {
    insertColumns.push("submitted_reason");
    insertValues.push(payload.submittedReason);
    placeholders.push("?");
    updates.push("submitted_reason = VALUES(submitted_reason)");
  }

  if (payload.verifiedReason !== undefined && columns.has("verified_reason")) {
    insertColumns.push("verified_reason");
    insertValues.push(payload.verifiedReason);
    placeholders.push("?");
    updates.push("verified_reason = VALUES(verified_reason)");
  }

  if (payload.othersReason !== undefined && columns.has("others_reason")) {
    insertColumns.push("others_reason");
    insertValues.push(payload.othersReason);
    placeholders.push("?");
    updates.push("others_reason = VALUES(others_reason)");
  }

  if (payload.walkInReason !== undefined && columns.has("walk_in_reason")) {
    insertColumns.push("walk_in_reason");
    insertValues.push(payload.walkInReason);
    placeholders.push("?");
    updates.push("walk_in_reason = VALUES(walk_in_reason)");
  }

  if (payload.furtherReason !== undefined && columns.has("further_reason")) {
    insertColumns.push("further_reason");
    insertValues.push(payload.furtherReason);
    placeholders.push("?");
    updates.push("further_reason = VALUES(further_reason)");
  }

  if (payload.selectReason !== undefined && columns.has("select_reason")) {
    insertColumns.push("select_reason");
    insertValues.push(payload.selectReason);
    placeholders.push("?");
    updates.push("select_reason = VALUES(select_reason)");
  }

  if (
    payload.shortlistedReason !== undefined &&
    columns.has("shortlisted_reason")
  ) {
    insertColumns.push("shortlisted_reason");
    insertValues.push(payload.shortlistedReason);
    placeholders.push("?");
    updates.push("shortlisted_reason = VALUES(shortlisted_reason)");
  } else if (
    payload.shortlistedReason !== undefined &&
    columns.has("pending_joining_reason")
  ) {
    insertColumns.push("pending_joining_reason");
    insertValues.push(payload.shortlistedReason);
    placeholders.push("?");
    updates.push("pending_joining_reason = VALUES(pending_joining_reason)");
  }

  if (payload.joinedReason !== undefined && columns.has("joined_reason")) {
    insertColumns.push("joined_reason");
    insertValues.push(payload.joinedReason);
    placeholders.push("?");
    updates.push("joined_reason = VALUES(joined_reason)");
  }

  if (payload.dropoutReason !== undefined && columns.has("dropout_reason")) {
    insertColumns.push("dropout_reason");
    insertValues.push(payload.dropoutReason);
    placeholders.push("?");
    updates.push("dropout_reason = VALUES(dropout_reason)");
  }

  if (payload.rejectReason !== undefined && columns.has("reject_reason")) {
    insertColumns.push("reject_reason");
    insertValues.push(payload.rejectReason);
    placeholders.push("?");
    updates.push("reject_reason = VALUES(reject_reason)");
  }

  if (payload.leftReason !== undefined && columns.has("left_reason")) {
    insertColumns.push("left_reason");
    insertValues.push(payload.leftReason);
    placeholders.push("?");
    updates.push("left_reason = VALUES(left_reason)");
  }

  if (payload.billedReason !== undefined && columns.has("billed_reason")) {
    insertColumns.push("billed_reason");
    insertValues.push(payload.billedReason);
    placeholders.push("?");
    updates.push("billed_reason = VALUES(billed_reason)");
  }

  const timestampFieldMap = {
    submittedAt: "submitted_at",
    verifiedAt: "verified_at",
    othersAt: "others_at",
    walkInAt: "walk_in_at",
    furtherAt: "further_at",
    selectedAt: "selected_at",
    shortlistedAt: columns.has("shortlisted_at")
      ? "shortlisted_at"
      : "pending_joining_at",
    joinedAt: "joined_at",
    dropoutAt: "dropout_at",
    rejectedAt: "rejected_at",
    billedAt: "billed_at",
    leftAt: "left_at",
  };

  for (const [payloadKey, columnName] of Object.entries(timestampFieldMap)) {
    if (payload[payloadKey] === undefined || !columns.has(columnName)) continue;
    insertColumns.push(columnName);
    placeholders.push(
      payload[payloadKey] === "__CURRENT_TIMESTAMP__" ? "CURRENT_TIMESTAMP" : "?",
    );
    if (payload[payloadKey] !== "__CURRENT_TIMESTAMP__") {
      insertValues.push(payload[payloadKey]);
    }
    updates.push(
      `${columnName} = ${payload[payloadKey] === "__CURRENT_TIMESTAMP__" ? "CURRENT_TIMESTAMP" : `VALUES(${columnName})`}`,
    );
  }

  if (insertColumns.length === 0 || updates.length === 0) return;
  if (columns.has("updated_at")) {
    updates.push("updated_at = CURRENT_TIMESTAMP");
  }

  const [result] = await connection.query(
    `INSERT INTO extra_info (${insertColumns.map((column) => `\`${column}\``).join(", ")})
     VALUES (${placeholders.join(", ")})
     ON DUPLICATE KEY UPDATE ${updates.join(", ")}`,
    insertValues,
  );

  if (String(process.env.DEBUG_EXTRA_INFO || "").trim() === "1") {
    console.log("[extra_info] Upserted extra_info fields", {
      resId: payload.resId,
      affectedRows: result?.affectedRows,
      changedRows: result?.changedRows,
      warningStatus: result?.warningStatus,
      updates: updates.slice(0, 4),
    });
  }
};

const upsertCandidateFields = async (connection, payload) => {
  if (!(await tableExists("candidate"))) return;

  const columns = await getTableColumns("candidate", connection);
  if (!columns.has("cid") || !columns.has("res_id")) return;

  const normalizedResId = String(payload?.resId || "").trim();
  const derivedCid = normalizedResId
    ? normalizedResId.startsWith("res_")
      ? `c_${normalizedResId.slice(4)}`
      : ""
    : "";
  const effectiveCid =
    payload?.cid !== undefined && payload?.cid !== null && String(payload.cid).trim()
      ? String(payload.cid).trim()
      : derivedCid;

  const updates = [];
  const insertColumns = [];
  const insertValues = [];
  const placeholders = [];
  const updateAssignments = [];
  const updateValues = [];

  const addColumnValue = (columnName, value, { update = true } = {}) => {
    if (!columns.has(columnName) || value === undefined) return;
    insertColumns.push(columnName);
    insertValues.push(value);
    placeholders.push("?");
    if (update) {
      updates.push(`\`${columnName}\` = VALUES(\`${columnName}\`)`);
      updateAssignments.push(`\`${columnName}\` = ?`);
      updateValues.push(value);
    }
  };

  addColumnValue("cid", effectiveCid, { update: false });
  addColumnValue("res_id", normalizedResId || payload.resId, { update: false });
  addColumnValue("job_jid", payload.jobJid);
  addColumnValue("recruiter_rid", payload.recruiterRid);
  addColumnValue("rid", payload.recruiterRid);
  addColumnValue("name", payload.name);
  addColumnValue("phone", payload.phone);
  addColumnValue("email", payload.email);
  addColumnValue("level_of_edu", payload.levelOfEdu);
  addColumnValue("board_uni", payload.boardUni);
  addColumnValue("institution_name", payload.institutionName);
  addColumnValue("location", payload.location);
  addColumnValue("marks", payload.marks);
  addColumnValue("age", payload.age);
  addColumnValue("industry", payload.industry);
  addColumnValue("expected_sal", payload.expectedSal);
  addColumnValue("prev_sal", payload.prevSal);
  addColumnValue("notice_period", payload.noticePeriod);
  addColumnValue("experience", payload.experience);
  addColumnValue("years_of_exp", payload.yearsOfExp);
  addColumnValue("joining_date", payload.joiningDate);
  addColumnValue("walk_in", payload.walkIn);
  addColumnValue("revenue", payload.revenue);

  if (columns.has("updated_at")) {
    updates.push("updated_at = CURRENT_TIMESTAMP");
    updateAssignments.push("updated_at = CURRENT_TIMESTAMP");
  }

  if (
    insertColumns.length === 0 ||
    (updates.length === 0 && updateAssignments.length === 0)
  ) {
    return;
  }

  const canInsertCandidate =
    Boolean(effectiveCid) &&
    Boolean(normalizedResId) &&
    insertColumns.includes("name");

  try {
    if (!canInsertCandidate) {
      if (!normalizedResId || updateAssignments.length === 0) return;
      const [result] = await connection.query(
        `UPDATE candidate
         SET ${updateAssignments.join(", ")}
         WHERE res_id = ?`,
        [...updateValues, normalizedResId],
      );

      if (
        String(process.env.DEBUG_CANDIDATE_UPSERT || "").trim() === "1" &&
        result
      ) {
        console.log("[candidate] Updated candidate by res_id", {
          resId: normalizedResId,
          affectedRows: result.affectedRows,
          changedRows: result.changedRows,
        });
      }
      return;
    }

    const [result] = await connection.query(
      `INSERT INTO candidate (${insertColumns.map((column) => `\`${column}\``).join(", ")})
       VALUES (${placeholders.join(", ")})
       ON DUPLICATE KEY UPDATE ${updates.join(", ")}`,
      insertValues,
    );

    if (String(process.env.DEBUG_CANDIDATE_UPSERT || "").trim() === "1") {
      console.log("[candidate] Upserted candidate", {
        cid: effectiveCid,
        resId: normalizedResId,
        affectedRows: result?.affectedRows,
        changedRows: result?.changedRows,
        warningStatus: result?.warningStatus,
      });
    }
  } catch (error) {
    console.error("[candidate] Failed to upsert candidate", {
      resId: normalizedResId || null,
      cid: effectiveCid || null,
      jobJid: payload?.jobJid || null,
      recruiterRid: payload?.recruiterRid || null,
      message: error.message,
      code: error.code || null,
      sqlMessage: error.sqlMessage || null,
    });
    throw error;
  }
};

const addCandidateBillIntakeEntry = async (
  connection,
  resId,
  { amount = null, reason = "candidate's bill", photo = null, moneySumId = null } = {},
) => {
  const normalizedResId = String(resId || "").trim();
  if (!normalizedResId) return null;
  if (!(await tableExists("money_sum"))) {
    return null;
  }

  const explicitAmount = Number(amount);
  const hasExplicitAmount = Number.isFinite(explicitAmount) && explicitAmount >= 0;

  let normalizedAmount = 0;
  if (hasExplicitAmount) {
    normalizedAmount = Math.round(explicitAmount * 100) / 100;
  } else {
    if (!(await tableExists("candidate"))) return null;
    const candidateColumns = await getTableColumns("candidate", connection);
    if (!candidateColumns.has("res_id") || !candidateColumns.has("revenue")) {
      return null;
    }

    const [candidateRows] = await connection.query(
      `SELECT revenue
       FROM candidate
       WHERE res_id = ?
       LIMIT 1`,
      [normalizedResId],
    );
    const rawAmount = Number(candidateRows?.[0]?.revenue);
    normalizedAmount =
      Number.isFinite(rawAmount) && rawAmount >= 0 ? rawAmount : 0;
  }

  const moneySumColumns = await getTableColumns("money_sum", connection);
  const hasMoneySumResId = moneySumColumns.has("res_id");
  const normalizedMoneySumId = Number(moneySumId);
  let existingEntry = null;

  if (Number.isInteger(normalizedMoneySumId) && normalizedMoneySumId > 0) {
    const [existingRows] = await connection.query(
      `SELECT id, company_rev AS companyRev, profit, photo
       FROM money_sum
       WHERE id = ?
       LIMIT 1`,
      [normalizedMoneySumId],
    );
    existingEntry = existingRows?.[0] || null;
  } else if (hasMoneySumResId) {
    const [existingRows] = await connection.query(
      `SELECT id, company_rev AS companyRev, profit, photo
       FROM money_sum
       WHERE res_id = ?
       ORDER BY created_at DESC, id DESC
       LIMIT 1`,
      [normalizedResId],
    );
    existingEntry = existingRows?.[0] || null;
  }

  const [profitRows] = moneySumColumns.has("profit")
    ? await connection.query(
        `SELECT COALESCE(profit, 0) AS lastProfit
         FROM money_sum
         ORDER BY created_at DESC, id DESC
         LIMIT 1`,
      )
    : [[{ lastProfit: 0 }]];
  const lastProfit = Number(profitRows?.[0]?.lastProfit) || 0;
  const nextProfit = Math.round((lastProfit + normalizedAmount) * 100) / 100;

  const safeReason = String(reason || "").trim() || "candidate's bill";
  const safePhoto =
    photo === undefined || photo === null || photo === "" ? null : String(photo);
  const storedPhoto =
    safePhoto === null && existingEntry?.photo ? String(existingEntry.photo) : safePhoto;

  if (existingEntry?.id) {
    const existingAmount = Number(existingEntry.companyRev);
    const existingProfit = Number(existingEntry.profit);
    const finalAmount =
      Number.isFinite(existingAmount) && existingAmount > 0
        ? existingAmount
        : normalizedAmount;
    const finalProfit =
      Number.isFinite(existingProfit) && existingProfit >= 0
        ? existingProfit
        : nextProfit;

    await connection.query(
      `UPDATE money_sum
       SET company_rev = ?,
           expense = 0,
           profit = ?,
           reason = ?,
           photo = ?,
           entry_type = 'intake'
           ${hasMoneySumResId ? ", res_id = ?" : ""},
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      hasMoneySumResId
        ? [
            finalAmount,
            finalProfit,
            safeReason,
            storedPhoto,
            normalizedResId,
            existingEntry.id,
          ]
        : [finalAmount, finalProfit, safeReason, storedPhoto, existingEntry.id],
    );

    return {
      id: existingEntry.id,
      amount: finalAmount,
      profit: finalProfit,
      reason: safeReason,
      photo: storedPhoto,
    };
  }

  const [insertResult] = await connection.query(
    `INSERT INTO money_sum (${hasMoneySumResId ? "res_id, " : ""}company_rev, expense, profit, reason, photo, entry_type)
     VALUES (${hasMoneySumResId ? "?, " : ""}?, 0, ?, ?, ?, 'intake')`,
    hasMoneySumResId
      ? [normalizedResId, normalizedAmount, nextProfit, safeReason, safePhoto]
      : [normalizedAmount, nextProfit, safeReason, safePhoto],
  );

  return {
    id: Number(insertResult?.insertId) || null,
    amount: normalizedAmount,
    profit: nextProfit,
    reason: safeReason,
    photo: safePhoto,
  };
};

module.exports = {
  tableExists,
  columnExists,
  getTableColumns,
  getColumnMetadata,
  constraintExists,
  getColumnMaxLength,
  findResumeDuplicateDecision,
  resolveResumeBinaryStorage,
  buildResumeBinarySelect,
  storeResumeBinary,
  fetchExtraInfoByResumeIds,
  upsertExtraInfoFields,
  upsertCandidateFields,
  addCandidateBillIntakeEntry,
  buildResumeCompatibilityFields,
};
