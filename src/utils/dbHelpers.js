const pool = require("../config/db");

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
  const resumeIdColumn = columns.has("res_id")
    ? "res_id"
    : columns.has("resume_id")
      ? "resume_id"
      : "";
  const hasSubmittedReason = columns.has("submitted_reason");
  const hasVerifiedReason = columns.has("verified_reason");
  const hasWalkInReason = columns.has("walk_in_reason");
  const hasFurtherReason = columns.has("further_reason");
  const hasSelectReason = columns.has("select_reason");
  const hasJoinedReason = columns.has("joined_reason");
  const hasDropoutReason = columns.has("dropout_reason");
  const hasRejectReason = columns.has("reject_reason");
  const hasLeftReason = columns.has("left_reason");
  const hasBilledReason = columns.has("billed_reason");

  const hasAnyReason =
    hasSubmittedReason ||
    hasVerifiedReason ||
    hasWalkInReason ||
    hasFurtherReason ||
    hasSelectReason ||
    hasJoinedReason ||
    hasDropoutReason ||
    hasRejectReason ||
    hasLeftReason ||
    hasBilledReason;

  if (!resumeIdColumn || !hasAnyReason) return new Map();

  const selectColumns = [`${resumeIdColumn} AS resumeId`];
  if (hasSubmittedReason)
    selectColumns.push("submitted_reason AS submittedReason");
  if (hasVerifiedReason)
    selectColumns.push("verified_reason AS verifiedReason");
  if (hasWalkInReason) selectColumns.push("walk_in_reason AS walkInReason");
  if (hasFurtherReason) selectColumns.push("further_reason AS furtherReason");
  if (hasSelectReason) selectColumns.push("select_reason AS selectReason");
  if (hasJoinedReason) selectColumns.push("joined_reason AS joinedReason");
  if (hasDropoutReason) selectColumns.push("dropout_reason AS dropoutReason");
  if (hasRejectReason) selectColumns.push("reject_reason AS rejectReason");
  if (hasLeftReason) selectColumns.push("left_reason AS leftReason");
  if (hasBilledReason) selectColumns.push("billed_reason AS billedReason");

  const [rows] = await connection.query(
    `SELECT ${selectColumns.join(", ")}
     FROM extra_info
     WHERE ${resumeIdColumn} IN (?)`,
    [normalizedResumeIds],
  );

  return new Map(
    rows.map((row) => [
      String(row.resumeId || "").trim(),
      {
        submittedReason: row.submittedReason || null,
        verifiedReason: row.verifiedReason || null,
        walkInReason: row.walkInReason || null,
        furtherReason: row.furtherReason || null,
        selectReason: row.selectReason || null,
        joinedReason: row.joinedReason || null,
        joiningNote: row.joinedReason || null,
        dropoutReason: row.dropoutReason || null,
        rejectReason: row.rejectReason || null,
        leftReason: row.leftReason || null,
        billedReason: row.billedReason || null,
      },
    ]),
  );
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
    walkInAt: "walk_in_at",
    furtherAt: "further_at",
    selectedAt: "selected_at",
    pendingJoiningAt: "pending_joining_at",
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

const addCandidateBillIntakeEntry = async (connection, resId) => {
  const normalizedResId = String(resId || "").trim();
  if (!normalizedResId) return null;
  if (!(await tableExists("candidate")) || !(await tableExists("money_sum"))) {
    return null;
  }

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
  const amount = Number(candidateRows?.[0]?.revenue);
  if (!Number.isFinite(amount) || amount < 0) {
    return null;
  }

  const moneySumColumns = await getTableColumns("money_sum", connection);
  const [profitRows] = moneySumColumns.has("profit")
    ? await connection.query(
        `SELECT COALESCE(profit, 0) AS lastProfit
         FROM money_sum
         ORDER BY created_at DESC, id DESC
         LIMIT 1`,
      )
    : [[{ lastProfit: 0 }]];
  const lastProfit = Number(profitRows?.[0]?.lastProfit) || 0;
  const nextProfit = Math.round((lastProfit + amount) * 100) / 100;

  await connection.query(
    `INSERT INTO money_sum (company_rev, expense, profit, reason, entry_type)
     VALUES (?, 0, ?, ?, 'intake')`,
    [amount, nextProfit, "candidate's bill"],
  );

  return {
    amount,
    profit: nextProfit,
  };
};

module.exports = {
  tableExists,
  columnExists,
  getTableColumns,
  getColumnMetadata,
  constraintExists,
  getColumnMaxLength,
  fetchExtraInfoByResumeIds,
  upsertExtraInfoFields,
  upsertCandidateFields,
  addCandidateBillIntakeEntry,
};
