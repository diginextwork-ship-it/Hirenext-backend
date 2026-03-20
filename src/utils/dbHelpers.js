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
      ).trim(),
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
        selectReason: row.selectReason || null,
        joinedReason: row.joinedReason || null,
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
  addColumnValue("candidate_name", payload.candidateName);
  addColumnValue("applicant_name", payload.candidateName);
  addColumnValue("candidate_email", payload.email);
  addColumnValue("applicant_email", payload.email);
  addColumnValue("email", payload.email);
  addColumnValue("phone", payload.phone);

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

module.exports = {
  tableExists,
  columnExists,
  getTableColumns,
  getColumnMetadata,
  constraintExists,
  getColumnMaxLength,
  fetchExtraInfoByResumeIds,
  upsertExtraInfoFields,
};
