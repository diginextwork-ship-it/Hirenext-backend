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
      applicant_name VARCHAR(255) NULL,
      job_jid ${jobJidColumnSql} NULL,
      resume LONGBLOB NOT NULL,
      resume_filename VARCHAR(255) NOT NULL,
      resume_type VARCHAR(10) NOT NULL,
      ats_score DECIMAL(5,2) NULL,
      ats_match_percentage DECIMAL(5,2) NULL,
      ats_raw_json JSON NULL,
      uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

  if (!(await columnExists("resumes_data", "applicant_name"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN applicant_name VARCHAR(255) NULL",
    );
  }

  if (!(await columnExists("resumes_data", "applicant_email"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN applicant_email VARCHAR(190) NULL",
    );
  }

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

  // Used to detect duplicate resume uploads across the whole table.
  if (!(await columnExists("resumes_data", "file_hash"))) {
    await pool.query(
      "ALTER TABLE resumes_data ADD COLUMN file_hash CHAR(64) NULL",
    );
  }
  if (!(await indexExists("resumes_data", "idx_resumes_data_file_hash"))) {
    await pool.query(
      "CREATE UNIQUE INDEX idx_resumes_data_file_hash ON resumes_data (file_hash)",
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
      candidate_name VARCHAR(255) NULL,
      applicant_name VARCHAR(255) NULL,
      candidate_email VARCHAR(190) NULL,
      applicant_email VARCHAR(190) NULL,
      email VARCHAR(190) NULL,
      phone VARCHAR(20) NULL,
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
  if (!(await columnExists("extra_info", "candidate_name"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN candidate_name VARCHAR(255) NULL",
    );
  }
  if (!(await columnExists("extra_info", "applicant_name"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN applicant_name VARCHAR(255) NULL",
    );
  }
  if (!(await columnExists("extra_info", "candidate_email"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN candidate_email VARCHAR(190) NULL",
    );
  }
  if (!(await columnExists("extra_info", "applicant_email"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN applicant_email VARCHAR(190) NULL",
    );
  }
  if (!(await columnExists("extra_info", "email"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN email VARCHAR(190) NULL",
    );
  }
  if (!(await columnExists("extra_info", "phone"))) {
    await pool.query(
      "ALTER TABLE extra_info ADD COLUMN phone VARCHAR(20) NULL",
    );
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

  if (!(await columnExists("applications", "has_prior_experience"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN has_prior_experience BOOLEAN NOT NULL DEFAULT FALSE",
    );
  }

  if (!(await columnExists("applications", "experience_industry"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN experience_industry VARCHAR(100) NULL",
    );
  }

  if (!(await columnExists("applications", "experience_industry_other"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN experience_industry_other VARCHAR(190) NULL",
    );
  }

  if (!(await columnExists("applications", "current_salary"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN current_salary DECIMAL(12,2) NULL",
    );
  }

  if (!(await columnExists("applications", "expected_salary"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN expected_salary DECIMAL(12,2) NULL",
    );
  }

  if (!(await columnExists("applications", "notice_period"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN notice_period VARCHAR(100) NULL",
    );
  }

  if (!(await columnExists("applications", "years_of_experience"))) {
    await pool.query(
      "ALTER TABLE applications ADD COLUMN years_of_experience DECIMAL(4,1) NULL",
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
    "verified",
    "walk_in",
    "selected",
    "rejected",
    "joined",
    "dropout",
    "on_hold",
  ];
  const hasAllStatuses = requiredStatuses.every((status) =>
    selectionStatusType.includes(`'${status}'`),
  );

  if (!hasAllStatuses) {
    await pool.query(
      `ALTER TABLE job_resume_selection
       MODIFY COLUMN selection_status
       ENUM('verified','walk_in','selected','rejected','joined','dropout','on_hold')
       NOT NULL DEFAULT 'selected'`,
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

const initDatabase = async () => {
  await ensureResumeIdSequenceTable();
  await ensureRecruiterTableColumns();
  await ensureJobsTableColumns();
  await ensureResumesDataTable();
  await ensureExtraInfoTable();
  await ensureMoneySumTable();
  await ensureReimbursementsTable();
  await ensureApplicationColumns();
  await ensureJobResumeSelectionTable();
  await ensureRecruiterAttendanceTable();
  await ensureJobAccessControlSchema();
  await ensureStatusTable();
};

pool.initDatabase = initDatabase;

module.exports = pool;
