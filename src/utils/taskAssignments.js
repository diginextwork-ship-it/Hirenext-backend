const pool = require("../config/db");
const { columnExists } = require("./dbHelpers");
const {
  getCurrentDateOnlyInBusinessTimeZone,
  isValidDateOnly,
} = require("./dateTime");

const TASK_ASSIGNMENT_STATUS = {
  PENDING: "pending",
  COMPLETED: "completed",
  REJECTED: "rejected",
  TIMED_OUT: "timed_out",
};

const normalizeTaskAssignmentStatus = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === TASK_ASSIGNMENT_STATUS.COMPLETED) {
    return TASK_ASSIGNMENT_STATUS.COMPLETED;
  }
  if (normalized === TASK_ASSIGNMENT_STATUS.REJECTED) {
    return TASK_ASSIGNMENT_STATUS.REJECTED;
  }
  return TASK_ASSIGNMENT_STATUS.PENDING;
};

const resolveEffectiveTaskAssignmentStatus = (
  status,
  assignmentDate,
  today = getCurrentDateOnlyInBusinessTimeZone(),
) => {
  const normalizedStatus = normalizeTaskAssignmentStatus(status);
  const safeAssignmentDate = String(assignmentDate || "").trim();

  if (
    normalizedStatus === TASK_ASSIGNMENT_STATUS.PENDING &&
    safeAssignmentDate &&
    safeAssignmentDate < today
  ) {
    return TASK_ASSIGNMENT_STATUS.TIMED_OUT;
  }

  return normalizedStatus;
};

const getTaskAssignmentActionState = (
  assignmentDate,
  today = getCurrentDateOnlyInBusinessTimeZone(),
) => {
  const safeAssignmentDate = String(assignmentDate || "").trim();
  const safeToday = String(today || "").trim();

  if (
    isValidDateOnly(safeAssignmentDate) &&
    isValidDateOnly(safeToday) &&
    safeAssignmentDate > safeToday
  ) {
    return {
      today: safeToday,
      assignmentDate: safeAssignmentDate,
      isActionableToday: false,
      isScheduledForFuture: true,
      isPastDue: false,
    };
  }

  if (
    isValidDateOnly(safeAssignmentDate) &&
    isValidDateOnly(safeToday) &&
    safeAssignmentDate < safeToday
  ) {
    return {
      today: safeToday,
      assignmentDate: safeAssignmentDate,
      isActionableToday: false,
      isScheduledForFuture: false,
      isPastDue: true,
    };
  }

  return {
    today: safeToday,
    assignmentDate: safeAssignmentDate,
    isActionableToday: true,
    isScheduledForFuture: false,
    isPastDue: false,
  };
};

const ensureTaskTables = async () => {
  await pool.query(
    `CREATE TABLE IF NOT EXISTS tasks (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      heading VARCHAR(255) NOT NULL,
      description TEXT NULL,
      created_by VARCHAR(50) NOT NULL DEFAULT 'admin',
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_tasks_created_at (created_at)
    )`,
  );

  if (!(await columnExists("tasks", "created_by"))) {
    await pool.query(
      "ALTER TABLE tasks ADD COLUMN created_by VARCHAR(50) NOT NULL DEFAULT 'admin'",
    );
  }

  await pool.query(
    `CREATE TABLE IF NOT EXISTS task_assignments (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      task_id BIGINT NOT NULL,
      recruiter_rid VARCHAR(20) NOT NULL,
      status ENUM('pending', 'completed', 'rejected') NOT NULL DEFAULT 'pending',
      assignment_date DATE NOT NULL,
      rescheduled_from_date DATE NULL DEFAULT NULL,
      rescheduled_at TIMESTAMP NULL DEFAULT NULL,
      rescheduled_by_rid VARCHAR(20) NULL DEFAULT NULL,
      acted_at TIMESTAMP NULL DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_task_assignment_day (task_id, recruiter_rid, assignment_date),
      INDEX idx_task_assignments_task (task_id),
      INDEX idx_task_assignments_recruiter (recruiter_rid),
      INDEX idx_task_assignments_status_date (status, assignment_date),
      CONSTRAINT fk_task_assignments_task
        FOREIGN KEY (task_id) REFERENCES tasks(id)
        ON UPDATE CASCADE ON DELETE CASCADE,
      CONSTRAINT fk_task_assignments_recruiter
        FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
        ON UPDATE CASCADE ON DELETE CASCADE
    )`,
  );

  if (!(await columnExists("task_assignments", "assignment_date"))) {
    await pool.query(
      `ALTER TABLE task_assignments
       ADD COLUMN assignment_date DATE NOT NULL DEFAULT (CURRENT_DATE)`,
    );
  }
  if (!(await columnExists("task_assignments", "acted_at"))) {
    await pool.query(
      "ALTER TABLE task_assignments ADD COLUMN acted_at TIMESTAMP NULL DEFAULT NULL",
    );
  }
  if (!(await columnExists("task_assignments", "rescheduled_from_date"))) {
    await pool.query(
      "ALTER TABLE task_assignments ADD COLUMN rescheduled_from_date DATE NULL DEFAULT NULL",
    );
  }
  if (!(await columnExists("task_assignments", "rescheduled_at"))) {
    await pool.query(
      "ALTER TABLE task_assignments ADD COLUMN rescheduled_at TIMESTAMP NULL DEFAULT NULL",
    );
  }
  if (!(await columnExists("task_assignments", "rescheduled_by_rid"))) {
    await pool.query(
      "ALTER TABLE task_assignments ADD COLUMN rescheduled_by_rid VARCHAR(20) NULL DEFAULT NULL",
    );
  }
};

module.exports = {
  TASK_ASSIGNMENT_STATUS,
  ensureTaskTables,
  getTaskAssignmentActionState,
  normalizeTaskAssignmentStatus,
  resolveEffectiveTaskAssignmentStatus,
};
