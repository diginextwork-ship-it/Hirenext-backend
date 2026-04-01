const mysql = require("mysql2/promise");

const parseBooleanEnv = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
};

const resolveSslConfig = (host) => {
  const explicitSsl = process.env.DB_SSL ?? process.env.MYSQL_SSL;
  const useSsl =
    explicitSsl != null
      ? parseBooleanEnv(explicitSsl)
      : /aivencloud\.com$/i.test(host);
  if (!useSsl) return undefined;

  const rawCa = String(
    process.env.DB_SSL_CA || process.env.AIVEN_CA_CERT || "",
  ).trim();
  if (!rawCa) {
    // For Aiven, use rejectUnauthorized: false to bypass certificate validation
    return { rejectUnauthorized: false };
  }

  return {
    ca: rawCa.replace(/\\n/g, "\n"),
    rejectUnauthorized: true,
  };
};

const getDbConfig = () => {
  const connectionUrl = String(
    process.env.DATABASE_URL ||
      process.env.MYSQL_URL ||
      process.env.JAWSDB_URL ||
      "",
  ).trim();

  if (connectionUrl) {
    const parsedUrl = new URL(connectionUrl);
    const host = parsedUrl.hostname;
    return {
      host,
      port: parsedUrl.port ? Number(parsedUrl.port) : 3306,
      user: decodeURIComponent(parsedUrl.username || ""),
      password: decodeURIComponent(parsedUrl.password || ""),
      database: decodeURIComponent(
        String(parsedUrl.pathname || "").replace(/^\//, ""),
      ),
      ssl: resolveSslConfig(host),
      connectTimeout: 60000, // 60 seconds for initial connection
    };
  }

  const requiredEnvVars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"];
  const missingEnvVars = requiredEnvVars.filter((key) => !process.env[key]);
  if (missingEnvVars.length > 0) {
    throw new Error(
      `Missing required database environment variables: ${missingEnvVars.join(", ")}`,
    );
  }

  const host = process.env.DB_HOST;
  return {
    host,
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: resolveSslConfig(host),
    connectTimeout: 60000, // 60 seconds for initial connection
  };
};

const dbConfig = getDbConfig();

const pool = mysql.createPool({
  ...dbConfig,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

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

const indexExists = async (tableName, indexName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.statistics
     WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
     LIMIT 1`,
    [tableName, indexName],
  );
  return rows.length > 0;
};

const getIndexColumns = async (tableName, indexName) => {
  const [rows] = await pool.query(
    `SELECT column_name AS columnName
     FROM information_schema.statistics
     WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
     ORDER BY seq_in_index`,
    [tableName, indexName],
  );
  return rows.map((row) => String(row.columnName || "").trim());
};

const getIndexesForColumn = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT index_name AS indexName, column_name AS columnName, non_unique AS nonUnique, seq_in_index AS seqInIndex
     FROM information_schema.statistics
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     ORDER BY index_name, seq_in_index`,
    [tableName, columnName],
  );

  const indexes = new Map();
  for (const row of rows) {
    const indexName = String(row.indexName || "").trim();
    if (!indexName) continue;
    if (!indexes.has(indexName)) {
      indexes.set(indexName, {
        indexName,
        nonUnique: Number(row.nonUnique) === 1,
        columns: [],
      });
    }
    indexes.get(indexName).columns.push(String(row.columnName || "").trim());
  }

  return Array.from(indexes.values());
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

const tableExists = async (tableName) => {
  const [rows] = await pool.query(
    `SELECT 1
     FROM information_schema.tables
     WHERE table_schema = DATABASE() AND table_name = ?
     LIMIT 1`,
    [tableName],
  );
  return rows.length > 0;
};

const getColumnMetadata = async (tableName, columnName) => {
  const [rows] = await pool.query(
    `SELECT
      COLUMN_TYPE AS columnType,
      DATA_TYPE AS dataType,
      CHARACTER_SET_NAME AS characterSetName,
      COLLATION_NAME AS collationName
     FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
     LIMIT 1`,
    [tableName, columnName],
  );
  return rows.length > 0 ? rows[0] : null;
};

const buildColumnSql = (metadata, fallbackSql) => {
  if (!metadata || !metadata.columnType) return fallbackSql;
  const baseType = String(metadata.columnType).trim();
  const isCharLike = [
    "char",
    "varchar",
    "tinytext",
    "text",
    "mediumtext",
    "longtext",
  ].includes(String(metadata.dataType || "").toLowerCase());
  const collationClause =
    isCharLike && metadata.collationName
      ? ` COLLATE ${metadata.collationName}`
      : "";
  return `${baseType}${collationClause}`;
};

const ensureColumnMatchesReference = async ({
  tableName,
  columnName,
  referenceTableName,
  referenceColumnName,
  fallbackSql,
  nullable = true,
  constraintName,
  onDelete = "SET NULL",
  onUpdate = "CASCADE",
}) => {
  const referenceMetadata = await getColumnMetadata(
    referenceTableName,
    referenceColumnName,
  );
  const targetColumnSql = buildColumnSql(referenceMetadata, fallbackSql);
  const nullSql = nullable ? "NULL" : "NOT NULL";
  const currentMetadata = await getColumnMetadata(tableName, columnName);
  const currentColumnSql = buildColumnSql(currentMetadata, fallbackSql);
  const needsColumnSync =
    !currentMetadata ||
    String(currentColumnSql).toLowerCase() !==
      String(targetColumnSql).toLowerCase();

  if (
    needsColumnSync &&
    constraintName &&
    (await constraintExists(tableName, constraintName))
  ) {
    await pool.query(
      `ALTER TABLE ${tableName} DROP FOREIGN KEY ${constraintName}`,
    );
  }

  if (!currentMetadata) {
    await pool.query(
      `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${targetColumnSql} ${nullSql}`,
    );
  } else if (needsColumnSync) {
    await pool.query(
      `ALTER TABLE ${tableName} MODIFY COLUMN ${columnName} ${targetColumnSql} ${nullSql}`,
    );
  }

  if (constraintName && !(await constraintExists(tableName, constraintName))) {
    await pool.query(
      `ALTER TABLE ${tableName}
       ADD CONSTRAINT ${constraintName}
       FOREIGN KEY (${columnName}) REFERENCES ${referenceTableName}(${referenceColumnName})
       ON DELETE ${onDelete}
       ON UPDATE ${onUpdate}`,
    );
  }
};

const ensureJobsTableColumns = async () => {
  if (!(await tableExists("jobs"))) return;

  if (!(await columnExists("jobs", "positions_open"))) {
    await pool.query(
      "ALTER TABLE jobs ADD COLUMN positions_open INT NOT NULL DEFAULT 1 AFTER role_name",
    );
  }

  if (!(await columnExists("jobs", "created_at"))) {
    await pool.query(
      "ALTER TABLE jobs ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("jobs", "revenue"))) {
    await pool.query("ALTER TABLE jobs ADD COLUMN revenue DECIMAL(12,2) NULL");
  }

  if (!(await columnExists("jobs", "points_per_joining"))) {
    await pool.query(
      "ALTER TABLE jobs ADD COLUMN points_per_joining INT NOT NULL DEFAULT 0",
    );
  }

  if (!(await indexExists("jobs", "idx_jobs_created_at"))) {
    await pool.query("CREATE INDEX idx_jobs_created_at ON jobs (created_at)");
  }

  if (!(await indexExists("jobs", "idx_jobs_access_mode"))) {
    await pool.query("CREATE INDEX idx_jobs_access_mode ON jobs (access_mode)");
  }

  if (!(await indexExists("jobs", "idx_jobs_recruiter_rid"))) {
    await pool.query(
      "CREATE INDEX idx_jobs_recruiter_rid ON jobs (recruiter_rid)",
    );
  }

  if (await columnExists("jobs", "qualification")) {
    const qualificationMetadata = await getColumnMetadata(
      "jobs",
      "qualification",
    );
    const qualificationType = String(
      qualificationMetadata?.dataType || "",
    ).toLowerCase();
    if (qualificationType !== "longtext") {
      await pool.query(
        "ALTER TABLE jobs MODIFY COLUMN qualification LONGTEXT NULL",
      );
    }
  }
};

const ensureRecruiterTableColumns = async () => {
  if (!(await tableExists("recruiter"))) return;

  if (!(await columnExists("recruiter", "salary"))) {
    await pool.query(
      "ALTER TABLE recruiter ADD COLUMN salary VARCHAR(120) NULL",
    );
  }

  if (!(await columnExists("recruiter", "monthly_salary"))) {
    await pool.query(
      "ALTER TABLE recruiter ADD COLUMN monthly_salary DECIMAL(12,2) NULL",
    );
  }

  if (!(await columnExists("recruiter", "daily_salary"))) {
    await pool.query(
      "ALTER TABLE recruiter ADD COLUMN daily_salary DECIMAL(12,2) NULL",
    );
  }

  if (!(await columnExists("recruiter", "points"))) {
    await pool.query(
      "ALTER TABLE recruiter ADD COLUMN points INT NOT NULL DEFAULT 0",
    );
  }

  await pool.query("UPDATE recruiter SET points = 0 WHERE points IS NULL");
  await pool.query(
    "ALTER TABLE recruiter MODIFY COLUMN points INT NOT NULL DEFAULT 0",
  );
};

const ensureRecruiterAttendanceTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS recruiter_attendance (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      recruiter_rid VARCHAR(20) NOT NULL,
      attendance_date DATE NOT NULL,
      status ENUM('present', 'absent', 'half_day') NOT NULL DEFAULT 'absent',
      salary_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
      money_sum_id BIGINT NULL,
      marked_by VARCHAR(50) NOT NULL DEFAULT 'admin',
      marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_recruiter_attendance_day (recruiter_rid, attendance_date),
      INDEX idx_recruiter_attendance_date_status (attendance_date, status),
      INDEX idx_recruiter_attendance_money_sum_id (money_sum_id),
      CONSTRAINT fk_recruiter_attendance_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_recruiter_attendance_money_sum
        FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
    )`,
  );

  if (!(await columnExists("recruiter_attendance", "salary_amount"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN salary_amount DECIMAL(12,2) NOT NULL DEFAULT 0",
    );
  }

  if (!(await columnExists("recruiter_attendance", "money_sum_id"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN money_sum_id BIGINT NULL",
    );
  }

  await pool.query(
    "ALTER TABLE recruiter_attendance MODIFY COLUMN money_sum_id BIGINT NULL",
  );

  if (!(await columnExists("recruiter_attendance", "marked_by"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN marked_by VARCHAR(50) NOT NULL DEFAULT 'admin'",
    );
  }

  if (!(await columnExists("recruiter_attendance", "marked_at"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN marked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("recruiter_attendance", "updated_at"))) {
    await pool.query(
      "ALTER TABLE recruiter_attendance ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (
    !(await indexExists(
      "recruiter_attendance",
      "idx_recruiter_attendance_date_status",
    ))
  ) {
    await pool.query(
      "CREATE INDEX idx_recruiter_attendance_date_status ON recruiter_attendance (attendance_date, status)",
    );
  }

  if (
    !(await indexExists(
      "recruiter_attendance",
      "idx_recruiter_attendance_money_sum_id",
    ))
  ) {
    await pool.query(
      "CREATE INDEX idx_recruiter_attendance_money_sum_id ON recruiter_attendance (money_sum_id)",
    );
  }

  if (
    !(await constraintExists(
      "recruiter_attendance",
      "fk_recruiter_attendance_money_sum",
    ))
  ) {
    await pool.query(
      `ALTER TABLE recruiter_attendance
       ADD CONSTRAINT fk_recruiter_attendance_money_sum
       FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
       ON DELETE SET NULL
       ON UPDATE CASCADE`,
    );
  }
};

const ensureResumesDataTable = async () => {
  const jobJidMetadata = await getColumnMetadata("jobs", "jid");
  const jobJidColumnSql = buildColumnSql(jobJidMetadata, "VARCHAR(30)");

  await pool.query(
    `CREATE TABLE IF NOT EXISTS resumes_data (
      res_id VARCHAR(30) PRIMARY KEY,
      rid VARCHAR(20) NOT NULL,
      job_jid ${jobJidColumnSql} NULL,
      source VARCHAR(50) NULL,
      resume LONGBLOB NOT NULL,
      resume_filename VARCHAR(255) NOT NULL,
      resume_type VARCHAR(10) NOT NULL,
      ats_score DECIMAL(5,2) NULL,
      ats_match_percentage DECIMAL(5,2) NULL,
      ats_raw_json JSON NULL,
      file_hash CHAR(64) NULL,
      uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE INDEX idx_resumes_data_file_hash (job_jid, file_hash),
      INDEX idx_resumes_data_rid (rid),
      INDEX idx_resumes_data_job_jid (job_jid),
      INDEX idx_resumes_data_uploaded_at (uploaded_at),
      CONSTRAINT fk_resumes_data_recruiter
        FOREIGN KEY (rid) REFERENCES recruiter(rid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_resumes_data_job
        FOREIGN KEY (job_jid) REFERENCES jobs(jid)
        ON DELETE SET NULL
        ON UPDATE CASCADE
    )`,
  );

  await ensureColumnMatchesReference({
    tableName: "resumes_data",
    columnName: "job_jid",
    referenceTableName: "jobs",
    referenceColumnName: "jid",
    fallbackSql: "VARCHAR(30)",
    nullable: true,
    constraintName: "fk_resumes_data_job",
    onDelete: "SET NULL",
    onUpdate: "CASCADE",
  });

  if (!(await columnExists("resumes_data", "ats_score"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN ats_score DECIMAL(5,2) NULL",
    );
  }

  if (!(await columnExists("resumes_data", "ats_match_percentage"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN ats_match_percentage DECIMAL(5,2) NULL",
    );
  }

  if (!(await columnExists("resumes_data", "ats_raw_json"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN ats_raw_json JSON NULL",
    );
  }

  if (!(await columnExists("resumes_data", "submitted_by_role"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN submitted_by_role VARCHAR(30) NULL DEFAULT 'recruiter'",
    );
  }

  if (!(await columnExists("resumes_data", "source"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN source VARCHAR(50) NULL",
    );
  }

  if (!(await columnExists("resumes_data", "is_accepted"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN is_accepted BOOLEAN NOT NULL DEFAULT FALSE",
    );
  }

  if (!(await columnExists("resumes_data", "accepted_at"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN accepted_at TIMESTAMP NULL DEFAULT NULL",
    );
  }

  if (!(await columnExists("resumes_data", "accepted_by_admin"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN accepted_by_admin VARCHAR(50) NULL",
    );
  }

  // Used to detect duplicate resume uploads for the same job only.
  if (!(await columnExists("resumes_data", "file_hash"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN file_hash CHAR(64) NULL",
    );
  }
  const fileHashIndexes = await getIndexesForColumn("resumes_data", "file_hash");
  for (const index of fileHashIndexes) {
    const isLegacyGlobalUniqueIndex =
      !index.nonUnique &&
      index.columns.length === 1 &&
      index.columns[0] === "file_hash";
    if (isLegacyGlobalUniqueIndex) {
      await pool.query(`ALTER TABLE resumes_data DROP INDEX ${index.indexName}`);
    }
  }
  const hasFileHashIndex = await indexExists(
    "resumes_data",
    "idx_resumes_data_file_hash",
  );
  if (hasFileHashIndex) {
    const fileHashIndexColumns = await getIndexColumns(
      "resumes_data",
      "idx_resumes_data_file_hash",
    );
    const isJobScopedIndex =
      fileHashIndexColumns.length === 2 &&
      fileHashIndexColumns[0] === "job_jid" &&
      fileHashIndexColumns[1] === "file_hash";
    if (!isJobScopedIndex) {
      await pool.query("ALTER TABLE resumes_data DROP INDEX idx_resumes_data_file_hash");
    }
  }
  if (!(await indexExists("resumes_data", "idx_resumes_data_file_hash"))) {
    await pool.query(
      "CREATE UNIQUE INDEX idx_resumes_data_file_hash ON resumes_data (job_jid, file_hash)",
    );
  }
};

const ensureCandidateTable = async () => {
  const jobJidMetadata = await getColumnMetadata("jobs", "jid");
  const recruiterRidMetadata = await getColumnMetadata("recruiter", "rid");
  const resumeIdMetadata = await getColumnMetadata("resumes_data", "res_id");
  const jobJidColumnSql = buildColumnSql(jobJidMetadata, "VARCHAR(30)");
  const recruiterRidColumnSql = buildColumnSql(
    recruiterRidMetadata,
    "VARCHAR(20)",
  );
  const resumeIdColumnSql = buildColumnSql(resumeIdMetadata, "VARCHAR(30)");

  await pool.query(
    `CREATE TABLE IF NOT EXISTS candidate (
      cid VARCHAR(20) PRIMARY KEY,
      res_id ${resumeIdColumnSql} NOT NULL,
      job_jid ${jobJidColumnSql} NULL,
      recruiter_rid ${recruiterRidColumnSql} NULL,
      rid ${recruiterRidColumnSql} NULL,
      name VARCHAR(100) NOT NULL,
      phone VARCHAR(15) NULL,
      email VARCHAR(100) NULL,
      level_of_edu VARCHAR(50) NULL,
      board_uni VARCHAR(100) NULL,
      institution_name VARCHAR(190) NULL,
      marks DECIMAL(5,2) NULL,
      age INT NULL,
      industry VARCHAR(50) NULL,
      expected_sal INT NULL,
      prev_sal INT NULL,
      notice_period INT NULL,
      experience TINYINT(1) NULL,
      years_of_exp VARCHAR(20) NULL,
      joining_date DATE NULL,
      walk_in DATE NULL,
      revenue INT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_candidate_res_id (res_id),
      INDEX idx_candidate_job_jid (job_jid),
      INDEX idx_candidate_recruiter_rid (recruiter_rid),
      CONSTRAINT fk_candidate_resume
        FOREIGN KEY (res_id) REFERENCES resumes_data(res_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_candidate_job
        FOREIGN KEY (job_jid) REFERENCES jobs(jid)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
      CONSTRAINT fk_candidate_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON DELETE SET NULL
        ON UPDATE CASCADE
    )`,
  );

  // Migrate candidate.revenue from DECIMAL to INT if needed
  const revenueMetadata = await getColumnMetadata("candidate", "revenue");
  const revenueDataType = String(revenueMetadata?.dataType || "").toLowerCase();
  if (revenueDataType && revenueDataType !== "int") {
    await pool.query("ALTER TABLE candidate MODIFY COLUMN revenue INT NULL");
  }

  // Legacy databases may have candidate rows without res_id.
  if (!(await columnExists("candidate", "res_id"))) {
    await pool.query(
      `ALTER TABLE candidate ADD COLUMN res_id ${resumeIdColumnSql} NULL AFTER cid`,
    );
  }

  if (await columnExists("candidate", "resume_id")) {
    await pool.query(
      "UPDATE candidate SET res_id = resume_id WHERE res_id IS NULL AND resume_id IS NOT NULL",
    );
  }

  const legacyCandidateColumns = [
    { name: "job_jid", sql: `${jobJidColumnSql} NULL` },
    { name: "recruiter_rid", sql: `${recruiterRidColumnSql} NULL` },
    { name: "rid", sql: `${recruiterRidColumnSql} NULL` },
    { name: "name", sql: "VARCHAR(100) NULL" },
    { name: "phone", sql: "VARCHAR(15) NULL" },
    { name: "email", sql: "VARCHAR(100) NULL" },
    { name: "level_of_edu", sql: "VARCHAR(50) NULL" },
    { name: "board_uni", sql: "VARCHAR(100) NULL" },
    { name: "institution_name", sql: "VARCHAR(190) NULL" },
    { name: "age", sql: "INT NULL" },
    { name: "industry", sql: "VARCHAR(50) NULL" },
    { name: "expected_sal", sql: "INT NULL" },
    { name: "prev_sal", sql: "INT NULL" },
    { name: "notice_period", sql: "INT NULL" },
    { name: "experience", sql: "TINYINT(1) NULL" },
    { name: "years_of_exp", sql: "VARCHAR(20) NULL" },
    { name: "joining_date", sql: "DATE NULL" },
    { name: "walk_in", sql: "DATE NULL" },
    { name: "revenue", sql: "INT NULL" },
  ];

  for (const column of legacyCandidateColumns) {
    if (!(await columnExists("candidate", column.name))) {
      await pool.query(
        `ALTER TABLE candidate ADD COLUMN ${column.name} ${column.sql}`,
      );
    }
  }

  await pool.query(
    `UPDATE candidate c
     SET c.res_id = CASE
       WHEN LEFT(c.cid, 2) = 'c_' THEN
         CASE
           WHEN SUBSTRING(c.cid, 3) REGEXP '^[0-9]+$' THEN CONCAT('res_', SUBSTRING(c.cid, 3))
           ELSE SUBSTRING(c.cid, 3)
         END
       ELSE c.res_id
     END
     WHERE c.res_id IS NULL`,
  );

  const hasApplicationsTable = await tableExists("applications");
  const hasExtraInfoTable = await tableExists("extra_info");
  const hasSelectionTable = await tableExists("job_resume_selection");
  const hasResumeApplicantName = await columnExists(
    "resumes_data",
    "applicant_name",
  );
  const hasResumeApplicantEmail = await columnExists(
    "resumes_data",
    "applicant_email",
  );
  const hasResumeWalkIn = await columnExists("resumes_data", "walk_in");
  const hasResumeJoiningDate = await columnExists(
    "resumes_data",
    "joining_date",
  );
  const hasResumeRevenue = await columnExists("resumes_data", "revenue");
  const hasAppCandidateName =
    hasApplicationsTable &&
    (await columnExists("applications", "candidate_name"));
  const hasAppPhone =
    hasApplicationsTable && (await columnExists("applications", "phone"));
  const hasAppEmail =
    hasApplicationsTable && (await columnExists("applications", "email"));
  const hasAppEducation =
    hasApplicationsTable &&
    (await columnExists("applications", "latest_education_level"));
  const hasAppBoard =
    hasApplicationsTable &&
    (await columnExists("applications", "board_university"));
  const hasAppInstitution =
    hasApplicationsTable &&
    (await columnExists("applications", "institution_name"));
  const hasAppAge =
    hasApplicationsTable && (await columnExists("applications", "age"));
  const hasAppExperience =
    hasApplicationsTable &&
    (await columnExists("applications", "has_prior_experience"));
  const hasAppIndustry =
    hasApplicationsTable &&
    (await columnExists("applications", "experience_industry"));
  const hasAppExpectedSalary =
    hasApplicationsTable &&
    (await columnExists("applications", "expected_salary"));
  const hasAppPrevSalary =
    hasApplicationsTable &&
    (await columnExists("applications", "current_salary"));
  const hasAppNoticePeriod =
    hasApplicationsTable &&
    (await columnExists("applications", "notice_period"));
  const hasAppYearsOfExp =
    hasApplicationsTable &&
    (await columnExists("applications", "years_of_experience"));
  const hasExtraCandidateName =
    hasExtraInfoTable && (await columnExists("extra_info", "candidate_name"));
  const hasExtraApplicantName =
    hasExtraInfoTable && (await columnExists("extra_info", "applicant_name"));
  const hasExtraCandidateEmail =
    hasExtraInfoTable && (await columnExists("extra_info", "candidate_email"));
  const hasExtraApplicantEmail =
    hasExtraInfoTable && (await columnExists("extra_info", "applicant_email"));
  const hasExtraEmail =
    hasExtraInfoTable && (await columnExists("extra_info", "email"));
  const hasExtraPhone =
    hasExtraInfoTable && (await columnExists("extra_info", "phone"));
  const hasSelectionJoiningDate =
    hasSelectionTable &&
    (await columnExists("job_resume_selection", "joining_date"));

  const applicationMatchSql = hasApplicationsTable
    ? `a.job_jid = rd.job_jid
        AND (
          a.resume_filename = rd.resume_filename
          ${hasResumeApplicantName && hasAppCandidateName ? "OR a.candidate_name = rd.applicant_name" : ""}
          ${hasResumeApplicantEmail && hasAppEmail ? "OR a.email = rd.applicant_email" : ""}
        )`
    : "1 = 0";
  const latestApplicationValue = (columnName) =>
    hasApplicationsTable
      ? `(
          SELECT a.${columnName}
          FROM applications a
          WHERE ${applicationMatchSql}
          ORDER BY a.created_at DESC, a.id DESC
          LIMIT 1
        )`
      : "NULL";

  const legacyNameExpr = [
    hasResumeApplicantName ? "rd.applicant_name" : null,
    hasAppCandidateName ? latestApplicationValue("candidate_name") : null,
    hasExtraCandidateName ? "ei.candidate_name" : null,
    hasExtraApplicantName ? "ei.applicant_name" : null,
  ]
    .filter(Boolean)
    .join(", ");
  const legacyPhoneExpr = [
    hasAppPhone ? latestApplicationValue("phone") : null,
    hasExtraPhone ? "ei.phone" : null,
  ]
    .filter(Boolean)
    .join(", ");
  const legacyEmailExpr = [
    hasResumeApplicantEmail ? "rd.applicant_email" : null,
    hasAppEmail ? latestApplicationValue("email") : null,
    hasExtraCandidateEmail ? "ei.candidate_email" : null,
    hasExtraApplicantEmail ? "ei.applicant_email" : null,
    hasExtraEmail ? "ei.email" : null,
  ]
    .filter(Boolean)
    .join(", ");

  await pool.query(
    `INSERT INTO candidate (
      cid,
      res_id,
      job_jid,
      recruiter_rid,
      rid,
      name,
      phone,
      email,
      level_of_edu,
      board_uni,
      institution_name,
      age,
      industry,
      expected_sal,
      prev_sal,
      notice_period,
      experience,
      years_of_exp,
      joining_date,
      walk_in,
      revenue
    )
    SELECT
      CASE
        WHEN rd.res_id LIKE 'res_%' THEN LEFT(CONCAT('c_', SUBSTRING(rd.res_id, 5)), 20)
        ELSE LEFT(CONCAT('c_', rd.res_id), 20)
      END AS cid,
      rd.res_id,
      rd.job_jid,
      rd.rid,
      rd.rid,
      COALESCE(${legacyNameExpr || "NULL"}, 'Unknown Candidate') AS name,
      ${legacyPhoneExpr ? `COALESCE(${legacyPhoneExpr})` : "NULL"} AS phone,
      ${legacyEmailExpr ? `COALESCE(${legacyEmailExpr})` : "NULL"} AS email,
      ${hasAppEducation ? latestApplicationValue("latest_education_level") : "NULL"} AS level_of_edu,
      ${hasAppBoard ? latestApplicationValue("board_university") : "NULL"} AS board_uni,
      ${hasAppInstitution ? latestApplicationValue("institution_name") : "NULL"} AS institution_name,
      ${hasAppAge ? latestApplicationValue("age") : "NULL"} AS age,
      ${hasAppIndustry ? latestApplicationValue("experience_industry") : "NULL"} AS industry,
      ${hasAppExpectedSalary ? `CAST(${latestApplicationValue("expected_salary")} AS SIGNED)` : "NULL"} AS expected_sal,
      ${hasAppPrevSalary ? `CAST(${latestApplicationValue("current_salary")} AS SIGNED)` : "NULL"} AS prev_sal,
      ${hasAppNoticePeriod ? `CASE WHEN ${latestApplicationValue("notice_period")} REGEXP '^[0-9]+$' THEN CAST(${latestApplicationValue("notice_period")} AS SIGNED) ELSE NULL END` : "NULL"} AS notice_period,
      ${hasAppExperience ? latestApplicationValue("has_prior_experience") : "NULL"} AS experience,
      ${hasAppYearsOfExp ? latestApplicationValue("years_of_experience") : "NULL"} AS years_of_exp,
      COALESCE(${hasResumeJoiningDate ? "rd.joining_date" : "NULL"}${hasSelectionJoiningDate ? ", jrs.joining_date" : ""}) AS joining_date,
      ${hasResumeWalkIn ? "rd.walk_in" : "NULL"} AS walk_in,
      ${hasResumeRevenue ? "rd.revenue" : "NULL"} AS revenue
    FROM resumes_data rd
    ${hasExtraInfoTable ? "LEFT JOIN extra_info ei ON ei.res_id = rd.res_id OR (ei.resume_id = rd.res_id AND ei.res_id IS NULL)" : ""}
    ${hasSelectionTable ? "LEFT JOIN job_resume_selection jrs ON jrs.job_jid = rd.job_jid AND jrs.res_id = rd.res_id" : ""}
    WHERE NOT EXISTS (
      SELECT 1
      FROM candidate c
      WHERE c.res_id = rd.res_id
         OR c.cid = CASE
           WHEN rd.res_id LIKE 'res_%' THEN LEFT(CONCAT('c_', SUBSTRING(rd.res_id, 5)), 20)
           ELSE LEFT(CONCAT('c_', rd.res_id), 20)
         END
    )`,
  );

  if (await columnExists("resumes_data", "applicant_name")) {
    await pool.query("ALTER TABLE resumes_data DROP COLUMN applicant_name");
  }
  if (await columnExists("resumes_data", "applicant_email")) {
    await pool.query("ALTER TABLE resumes_data DROP COLUMN applicant_email");
  }
  if (await columnExists("resumes_data", "walk_in")) {
    await pool.query("ALTER TABLE resumes_data DROP COLUMN walk_in");
  }
  if (await columnExists("resumes_data", "joining_date")) {
    await pool.query("ALTER TABLE resumes_data DROP COLUMN joining_date");
  }
  if (await columnExists("resumes_data", "revenue")) {
    await pool.query("ALTER TABLE resumes_data DROP COLUMN revenue");
  }

  if (hasApplicationsTable) {
    if (!(await columnExists("applications", "res_id"))) {
      await pool.query(
        "ALTER TABLE applications ADD COLUMN res_id VARCHAR(30) NULL",
      );
    }
    await pool.query(
      `UPDATE applications a
       INNER JOIN resumes_data rd
         ON rd.job_jid = a.job_jid
        AND rd.resume_filename = a.resume_filename
       SET a.res_id = rd.res_id
       WHERE a.res_id IS NULL`,
    );
    if (!(await indexExists("applications", "idx_applications_res_id"))) {
      await pool.query(
        "CREATE INDEX idx_applications_res_id ON applications (res_id)",
      );
    }
    for (const columnName of [
      "candidate_name",
      "phone",
      "email",
      "latest_education_level",
      "board_university",
      "institution_name",
      "age",
      "has_prior_experience",
      "experience_industry",
      "experience_industry_other",
      "current_salary",
      "expected_salary",
      "notice_period",
      "years_of_experience",
    ]) {
      if (await columnExists("applications", columnName)) {
        await pool.query(`ALTER TABLE applications DROP COLUMN ${columnName}`);
      }
    }
  }

  if (hasExtraInfoTable) {
    for (const columnName of [
      "candidate_name",
      "applicant_name",
      "candidate_email",
      "applicant_email",
      "email",
      "phone",
    ]) {
      if (await columnExists("extra_info", columnName)) {
        await pool.query(`ALTER TABLE extra_info DROP COLUMN ${columnName}`);
      }
    }
  }

  if (
    hasSelectionTable &&
    (await columnExists("job_resume_selection", "joining_date"))
  ) {
    await pool.query(
      "ALTER TABLE job_resume_selection DROP COLUMN joining_date",
    );
  }
};

const ensureExtraInfoTable = async () => {
  const jobJidMetadata = await getColumnMetadata("jobs", "jid");
  const jobJidColumnSql = buildColumnSql(jobJidMetadata, "VARCHAR(30)");

  await pool.query(
    `CREATE TABLE IF NOT EXISTS extra_info (
      res_id VARCHAR(30) NOT NULL,
      resume_id VARCHAR(30) NULL,
      job_jid ${jobJidColumnSql} NULL,
      recruiter_rid VARCHAR(50) NULL,
      rid VARCHAR(50) NULL,
      submitted_reason TEXT NULL,
      verified_reason TEXT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (res_id),
      UNIQUE KEY uniq_extra_info_resume_id (resume_id),
      INDEX idx_extra_info_job_jid (job_jid),
      INDEX idx_extra_info_recruiter_rid (recruiter_rid),
      INDEX idx_extra_info_rid (rid)
    )`,
  );

  if (!(await columnExists("extra_info", "res_id"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN res_id VARCHAR(30) NULL",
    );
  }
  if (!(await columnExists("extra_info", "resume_id"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN resume_id VARCHAR(30) NULL",
    );
  }
  if (!(await columnExists("extra_info", "job_jid"))) {
    await pool.query(
      `ALTER TABLE extra_info ADD COLUMN job_jid ${jobJidColumnSql} NULL`,
    );
  }
  if (!(await columnExists("extra_info", "recruiter_rid"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN recruiter_rid VARCHAR(50) NULL",
    );
  }
  if (!(await columnExists("extra_info", "rid"))) {
    await pool.query("ALTER TABLE extra_info ADD COLUMN rid VARCHAR(50) NULL");
  }
  if (!(await columnExists("extra_info", "submitted_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN submitted_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "verified_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN verified_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "walk_in_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN walk_in_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "left_reason"))) {
    await pool.query("ALTER TABLE extra_info ADD COLUMN left_reason TEXT NULL");
  }
  if (!(await columnExists("extra_info", "billed_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN billed_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "further_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN further_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "select_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN select_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "joined_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN joined_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "dropout_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN dropout_reason TEXT NULL",
    );
  }
  if (!(await columnExists("extra_info", "reject_reason"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN reject_reason TEXT NULL",
    );
  }
  const extraInfoTimestampColumns = [
    "submitted_at",
    "verified_at",
    "walk_in_at",
    "further_at",
    "selected_at",
    "pending_joining_at",
    "joined_at",
    "dropout_at",
    "rejected_at",
    "billed_at",
    "left_at",
  ];
  for (const columnName of extraInfoTimestampColumns) {
    if (!(await columnExists("extra_info", columnName))) {
      await pool.query(
        `ALTER TABLE extra_info ADD COLUMN ${columnName} TIMESTAMP NULL DEFAULT NULL`,
      );
    }
  }
  if (!(await columnExists("extra_info", "updated_at"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (!(await indexExists("extra_info", "uniq_extra_info_res_id"))) {
    await pool.query(
      "CREATE UNIQUE INDEX uniq_extra_info_res_id ON extra_info (res_id)",
    );
  }
  if (!(await indexExists("extra_info", "uniq_extra_info_resume_id"))) {
    await pool.query(
      "CREATE UNIQUE INDEX uniq_extra_info_resume_id ON extra_info (resume_id)",
    );
  }
  if (!(await indexExists("extra_info", "idx_extra_info_job_jid"))) {
    await pool.query(
      "CREATE INDEX idx_extra_info_job_jid ON extra_info (job_jid)",
    );
  }
  if (!(await indexExists("extra_info", "idx_extra_info_recruiter_rid"))) {
    await pool.query(
      "CREATE INDEX idx_extra_info_recruiter_rid ON extra_info (recruiter_rid)",
    );
  }
  if (!(await indexExists("extra_info", "idx_extra_info_rid"))) {
    await pool.query("CREATE INDEX idx_extra_info_rid ON extra_info (rid)");
  }
};

const ensureResumeIdSequenceTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS resume_id_sequence (
      seq_id BIGINT AUTO_INCREMENT PRIMARY KEY
    )`,
  );
};

const ensureReimbursementsTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS reimbursements (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      rid VARCHAR(50) NOT NULL,
      role VARCHAR(30) NOT NULL,
      amount DECIMAL(14,2) NOT NULL,
      description TEXT NULL,
      status ENUM('pending','accepted','rejected') NOT NULL DEFAULT 'pending',
      money_sum_id BIGINT NULL,
      admin_note TEXT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_reimbursements_rid (rid),
      INDEX idx_reimbursements_status (status),
      INDEX idx_reimbursements_money_sum_id (money_sum_id),
      CONSTRAINT fk_reimbursements_money_sum
        FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
        ON UPDATE CASCADE ON DELETE SET NULL
    )`,
  );

  // Add money_sum_id column if it doesn't exist
  if (!(await columnExists("reimbursements", "money_sum_id"))) {
    await pool.query(
      "ALTER TABLE reimbursements ADD COLUMN money_sum_id BIGINT NULL",
    );
    await pool.query(
      "ALTER TABLE reimbursements ADD INDEX idx_reimbursements_money_sum_id (money_sum_id)",
    );
    try {
      await pool.query(
        `ALTER TABLE reimbursements ADD CONSTRAINT fk_reimbursements_money_sum
         FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
         ON UPDATE CASCADE ON DELETE SET NULL`,
      );
    } catch (err) {
      // Constraint might already exist or money_sum table doesn't exist yet
      console.log(
        "Note: Foreign key constraint might not be created yet:",
        err.message,
      );
    }
  }
};

const ensureApplicationColumns = async () => {
  const hasApplicationsTable = await pool
    .query(
      `SELECT 1
       FROM information_schema.tables
       WHERE table_schema = DATABASE() AND table_name = 'applications'
       LIMIT 1`,
    )
    .then(([rows]) => rows.length > 0)
    .catch(() => false);

  if (!hasApplicationsTable) return;

  await ensureColumnMatchesReference({
    tableName: "applications",
    columnName: "job_jid",
    referenceTableName: "jobs",
    referenceColumnName: "jid",
    fallbackSql: "VARCHAR(30)",
    nullable: false,
    constraintName: "fk_applications_job",
    onDelete: "CASCADE",
    onUpdate: "CASCADE",
  });

  if (await columnExists("applications", "grading_system")) {
    await pool.query("ALTER TABLE applications DROP COLUMN grading_system");
  }

  if (await columnExists("applications", "score")) {
    await pool.query("ALTER TABLE applications DROP COLUMN score");
  }

  if (!(await columnExists("applications", "resume_filename"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN resume_filename VARCHAR(255) NULL",
    );
  }

  if (!(await columnExists("applications", "res_id"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN res_id VARCHAR(30) NULL",
    );
  }

  if (!(await columnExists("applications", "resume_parsed_data"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN resume_parsed_data JSON NULL",
    );
  }

  if (!(await columnExists("applications", "ats_score"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN ats_score DECIMAL(5,2) NULL",
    );
  }

  if (!(await columnExists("applications", "ats_match_percentage"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN ats_match_percentage DECIMAL(5,2) NULL",
    );
  }

  if (!(await columnExists("applications", "ats_raw_json"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN ats_raw_json JSON NULL",
    );
  }
};

const ensureJobResumeSelectionTable = async () => {
  const jobJidMetadata = await getColumnMetadata("jobs", "jid");
  const resumeIdMetadata = await getColumnMetadata("resumes_data", "res_id");
  const jobJidColumnSql = buildColumnSql(jobJidMetadata, "VARCHAR(30)");
  const resumeIdColumnSql = buildColumnSql(resumeIdMetadata, "VARCHAR(30)");

  await pool.query(
    `CREATE TABLE IF NOT EXISTS job_resume_selection (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      job_jid ${jobJidColumnSql} NOT NULL,
      res_id ${resumeIdColumnSql} NOT NULL,
      selected_by_admin VARCHAR(50) NOT NULL,
      selection_status ENUM('selected', 'rejected', 'on_hold') NOT NULL DEFAULT 'selected',
      selection_note TEXT NULL,
      selected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_job_resume_selection (job_jid, res_id),
      INDEX idx_job_resume_selection_job_status_time (job_jid, selection_status, selected_at),
      CONSTRAINT fk_job_resume_selection_job
        FOREIGN KEY (job_jid) REFERENCES jobs(jid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_job_resume_selection_resume
        FOREIGN KEY (res_id) REFERENCES resumes_data(res_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    )`,
  );

  await ensureColumnMatchesReference({
    tableName: "job_resume_selection",
    columnName: "job_jid",
    referenceTableName: "jobs",
    referenceColumnName: "jid",
    fallbackSql: "VARCHAR(30)",
    nullable: false,
    constraintName: "fk_job_resume_selection_job",
    onDelete: "CASCADE",
    onUpdate: "CASCADE",
  });

  const selectionStatusMetadata = await getColumnMetadata(
    "job_resume_selection",
    "selection_status",
  );
  const selectionStatusType = String(
    selectionStatusMetadata?.columnType || "",
  ).toLowerCase();
  const requiredStatuses = [
    "submitted",
    "verified",
    "walk_in",
    "further",
    "selected",
    "pending_joining",
    "rejected",
    "joined",
    "dropout",
    "on_hold",
    "billed",
    "left",
  ];
  const hasAllStatuses = requiredStatuses.every((status) =>
    selectionStatusType.includes(`'${status}'`),
  );

  if (!hasAllStatuses) {
    await pool.query(
      `ALTER TABLE job_resume_selection
       MODIFY COLUMN selection_status
       ENUM('submitted','verified','walk_in','further','selected','pending_joining','rejected','joined','dropout','on_hold','billed','left')
       NOT NULL DEFAULT 'selected'`,
    );
  }

  if (await columnExists("job_resume_selection", "joining_date")) {
    await pool.query(
      "ALTER TABLE job_resume_selection DROP COLUMN joining_date",
    );
  }
};

const ensureMoneySumTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS money_sum (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      company_rev DECIMAL(14,2) NOT NULL DEFAULT 0,
      expense DECIMAL(14,2) NOT NULL DEFAULT 0,
      profit DECIMAL(14,2) NOT NULL DEFAULT 0,
      reason TEXT NULL,
      photo LONGTEXT NULL,
      entry_type VARCHAR(20) NOT NULL DEFAULT 'expense',
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_money_sum_created_at (created_at),
      INDEX idx_money_sum_entry_type (entry_type)
    )`,
  );

  if (!(await columnExists("money_sum", "company_rev"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN company_rev DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }

  if (!(await columnExists("money_sum", "expense"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN expense DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }

  if (!(await columnExists("money_sum", "profit"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN profit DECIMAL(14,2) NOT NULL DEFAULT 0",
    );
  }

  if (!(await columnExists("money_sum", "reason"))) {
    await pool.query("ALTER TABLE money_sum ADD COLUMN reason TEXT NULL");
  }
  const reasonMetadata = await getColumnMetadata("money_sum", "reason");
  const reasonType = String(reasonMetadata?.dataType || "").toLowerCase();
  if (
    reasonType &&
    reasonType !== "text" &&
    reasonType !== "mediumtext" &&
    reasonType !== "longtext"
  ) {
    await pool.query("ALTER TABLE money_sum MODIFY COLUMN reason TEXT NULL");
  }

  if (!(await columnExists("money_sum", "photo"))) {
    await pool.query("ALTER TABLE money_sum ADD COLUMN photo LONGTEXT NULL");
  }
  const photoMetadata = await getColumnMetadata("money_sum", "photo");
  const photoType = String(photoMetadata?.dataType || "").toLowerCase();
  if (photoType && photoType !== "longtext") {
    await pool.query("ALTER TABLE money_sum MODIFY COLUMN photo LONGTEXT NULL");
  }

  if (!(await columnExists("money_sum", "entry_type"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN entry_type VARCHAR(20) NOT NULL DEFAULT 'expense'",
    );
  }

  if (!(await columnExists("money_sum", "created_at"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("money_sum", "updated_at"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("money_sum", "id"))) {
    await pool.query(
      "ALTER TABLE money_sum ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST",
    );
  }

  await pool.query(
    "ALTER TABLE money_sum MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT",
  );

  if (!(await indexExists("money_sum", "idx_money_sum_created_at"))) {
    await pool.query(
      "CREATE INDEX idx_money_sum_created_at ON money_sum (created_at)",
    );
  }

  if (!(await indexExists("money_sum", "idx_money_sum_entry_type"))) {
    await pool.query(
      "CREATE INDEX idx_money_sum_entry_type ON money_sum (entry_type)",
    );
  }
};

const ensureJobAccessControlSchema = async () => {
  const jobJidMetadata = await getColumnMetadata("jobs", "jid");
  const jobJidColumnSql = buildColumnSql(jobJidMetadata, "VARCHAR(30)");

  if (await tableExists("jobs")) {
    if (!(await columnExists("jobs", "access_mode"))) {
      await pool.query(
        "ALTER TABLE jobs ADD COLUMN access_mode ENUM('open','restricted') NOT NULL DEFAULT 'open' AFTER role_name",
      );
    }

    await pool.query(
      "UPDATE jobs SET access_mode = 'open' WHERE access_mode IS NULL OR TRIM(access_mode) = ''",
    );
  }

  await pool.query(
    `CREATE TABLE IF NOT EXISTS job_recruiter_access (
      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
      job_jid ${jobJidColumnSql} NOT NULL,
      recruiter_rid VARCHAR(20) NOT NULL,
      granted_by VARCHAR(20) NOT NULL,
      granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      notes TEXT NULL,
      UNIQUE KEY uniq_job_recruiter_access (job_jid, recruiter_rid),
      INDEX idx_job_recruiter_access_job_active (job_jid, is_active),
      INDEX idx_job_recruiter_access_recruiter_active (recruiter_rid, is_active),
      CONSTRAINT fk_job_recruiter_access_job
        FOREIGN KEY (job_jid) REFERENCES jobs(jid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_job_recruiter_access_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
      CONSTRAINT fk_job_recruiter_access_granted_by
        FOREIGN KEY (granted_by) REFERENCES recruiter(rid)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
    )`,
  );

  await ensureColumnMatchesReference({
    tableName: "job_recruiter_access",
    columnName: "job_jid",
    referenceTableName: "jobs",
    referenceColumnName: "jid",
    fallbackSql: "VARCHAR(30)",
    nullable: false,
    constraintName: "fk_job_recruiter_access_job",
    onDelete: "CASCADE",
    onUpdate: "CASCADE",
  });

  if (!(await columnExists("job_recruiter_access", "is_active"))) {
    await pool.query(
      "ALTER TABLE job_recruiter_access ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE",
    );
  }

  if (!(await columnExists("job_recruiter_access", "notes"))) {
    await pool.query(
      "ALTER TABLE job_recruiter_access ADD COLUMN notes TEXT NULL",
    );
  }

  if (
    !(await indexExists(
      "job_recruiter_access",
      "idx_job_recruiter_access_job_rid",
    ))
  ) {
    await pool.query(
      "CREATE INDEX idx_job_recruiter_access_job_rid ON job_recruiter_access (job_jid, recruiter_rid)",
    );
  }
};

const ensureStatusTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS status (
      recruiter_rid VARCHAR(20) PRIMARY KEY,
      submitted INT NOT NULL DEFAULT 0,
      verified INT NULL,
      walk_in INT NULL,
      \`select\` INT NULL,
      reject INT NULL,
      joined INT NULL,
      dropout INT NULL,
      last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT fk_status_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    )`,
  );

  if (!(await columnExists("status", "submitted"))) {
    await pool.query(
      "ALTER TABLE status ADD COLUMN submitted INT NOT NULL DEFAULT 0",
    );
  }

  if (!(await columnExists("status", "verified"))) {
    await pool.query("ALTER TABLE status ADD COLUMN verified INT NULL");
  }

  if (!(await columnExists("status", "walk_in"))) {
    await pool.query("ALTER TABLE status ADD COLUMN walk_in INT NULL");
  }

  if (!(await columnExists("status", "select"))) {
    await pool.query("ALTER TABLE status ADD COLUMN `select` INT NULL");
  }

  if (!(await columnExists("status", "reject"))) {
    await pool.query("ALTER TABLE status ADD COLUMN reject INT NULL");
  }

  if (!(await columnExists("status", "joined"))) {
    await pool.query("ALTER TABLE status ADD COLUMN joined INT NULL");
  }

  if (!(await columnExists("status", "dropout"))) {
    await pool.query("ALTER TABLE status ADD COLUMN dropout INT NULL");
  }

  if (!(await columnExists("status", "billed"))) {
    await pool.query("ALTER TABLE status ADD COLUMN billed INT NULL");
  }

  if (!(await columnExists("status", "left"))) {
    await pool.query("ALTER TABLE status ADD COLUMN `left` INT NULL");
  }

  if (!(await columnExists("status", "last_updated"))) {
    await pool.query(
      "ALTER TABLE status ADD COLUMN last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    );
  }

  if (!(await columnExists("status", "created_at"))) {
    await pool.query(
      "ALTER TABLE status ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    );
  }

  if (!(await indexExists("status", "idx_status_recruiter_rid"))) {
    await pool.query(
      "CREATE INDEX idx_status_recruiter_rid ON status (recruiter_rid)",
    );
  }
};

const ensurePointsLogTable = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS recruiter_points_log (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      recruiter_rid VARCHAR(20) NOT NULL,
      job_jid INT NULL,
      res_id VARCHAR(30) NULL,
      points INT NOT NULL DEFAULT 0,
      reason VARCHAR(100) NOT NULL DEFAULT 'billed',
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_points_log_rid (recruiter_rid),
      INDEX idx_points_log_rid_created (recruiter_rid, created_at),
      INDEX idx_points_log_job_jid (job_jid),
      CONSTRAINT fk_points_log_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
  );
};

const initDatabase = async () => {
  await ensureResumeIdSequenceTable();
  await ensureRecruiterTableColumns();
  await ensureJobsTableColumns();
  await ensureResumesDataTable();
  await ensureExtraInfoTable();
  await ensureMoneySumTable();
  await ensureReimbursementsTable();
  await ensureApplicationColumns();
  await ensureCandidateTable();
  await ensureJobResumeSelectionTable();
  await ensureRecruiterAttendanceTable();
  await ensureJobAccessControlSchema();
  await ensureStatusTable();
  await ensurePointsLogTable();
};

pool.initDatabase = initDatabase;

module.exports = pool;
