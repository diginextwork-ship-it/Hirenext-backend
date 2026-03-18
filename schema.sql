-- Run this file after selecting the target database/schema.
-- Example (MySQL): USE hirenext;

CREATE TABLE IF NOT EXISTS recruiter (
  rid VARCHAR(20) PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  email VARCHAR(190) NOT NULL UNIQUE,
  password VARCHAR(255) NOT NULL,
  role VARCHAR(30) NOT NULL DEFAULT 'recruiter',
  salary VARCHAR(120) NULL,
  monthly_salary DECIMAL(12,2) NULL,
  daily_salary DECIMAL(12,2) NULL,
  addjob BOOLEAN NOT NULL DEFAULT FALSE,
  success INT NOT NULL DEFAULT 0,
  points INT NOT NULL DEFAULT 0
);

UPDATE recruiter
SET role = CASE WHEN addjob = TRUE THEN 'job creator' ELSE 'recruiter' END
WHERE role IS NULL OR TRIM(role) = '';

CREATE TABLE IF NOT EXISTS jobs (
  jid INT AUTO_INCREMENT PRIMARY KEY,
  recruiter_rid VARCHAR(20) NOT NULL,
  city VARCHAR(120) NOT NULL,
  state VARCHAR(120) NOT NULL,
  pincode VARCHAR(20) NOT NULL,
  company_name VARCHAR(190) NOT NULL,
  role_name VARCHAR(190) NOT NULL,
  positions_open INT NOT NULL DEFAULT 1,
  revenue DECIMAL(12,2) NULL,
  points_per_joining INT NOT NULL DEFAULT 0,
  skills TEXT NULL,
  job_description TEXT NULL,
  experience VARCHAR(80) NULL,
  salary VARCHAR(120) NULL,
  qualification LONGTEXT NULL,
  benefits TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_jobs_recruiter_rid (recruiter_rid),
  CONSTRAINT fk_jobs_recruiter
    FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS positions_open INT NOT NULL DEFAULT 1;
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS revenue DECIMAL(12,2) NULL;
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS points_per_joining INT NOT NULL DEFAULT 0;
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS access_mode ENUM('open', 'restricted') NOT NULL DEFAULT 'open';
CREATE INDEX IF NOT EXISTS idx_jobs_access_mode ON jobs (access_mode);
CREATE INDEX IF NOT EXISTS idx_jobs_recruiter_rid ON jobs (recruiter_rid);

ALTER TABLE recruiter
  ADD COLUMN IF NOT EXISTS points INT NOT NULL DEFAULT 0;
ALTER TABLE recruiter
  ADD COLUMN IF NOT EXISTS salary VARCHAR(120) NULL;
ALTER TABLE recruiter
  ADD COLUMN IF NOT EXISTS monthly_salary DECIMAL(12,2) NULL;
ALTER TABLE recruiter
  ADD COLUMN IF NOT EXISTS daily_salary DECIMAL(12,2) NULL;

CREATE TABLE IF NOT EXISTS applications (
  id INT AUTO_INCREMENT PRIMARY KEY,
  job_jid INT NOT NULL,
  candidate_name VARCHAR(190) NOT NULL,
  phone VARCHAR(20) NOT NULL,
  email VARCHAR(190) NOT NULL,
  has_prior_experience BOOLEAN NOT NULL DEFAULT FALSE,
  experience_industry VARCHAR(100) NULL,
  experience_industry_other VARCHAR(190) NULL,
  current_salary DECIMAL(12,2) NULL,
  expected_salary DECIMAL(12,2) NULL,
  notice_period VARCHAR(100) NULL,
  years_of_experience DECIMAL(4,1) NULL,
  latest_education_level VARCHAR(100) NOT NULL,
  board_university VARCHAR(190) NOT NULL,
  institution_name VARCHAR(190) NOT NULL,
  age INT NOT NULL,
  resume_filename VARCHAR(255) NULL,
  resume_parsed_data JSON NULL,
  ats_score DECIMAL(5,2) NULL,
  ats_match_percentage DECIMAL(5,2) NULL,
  ats_raw_json JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_applications_job
    FOREIGN KEY (job_jid) REFERENCES jobs(jid)
    ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE applications
  DROP COLUMN IF EXISTS grading_system;
ALTER TABLE applications
  DROP COLUMN IF EXISTS score;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS has_prior_experience BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS experience_industry VARCHAR(100) NULL;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS experience_industry_other VARCHAR(190) NULL;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS current_salary DECIMAL(12,2) NULL;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS expected_salary DECIMAL(12,2) NULL;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS notice_period VARCHAR(100) NULL;
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS years_of_experience DECIMAL(4,1) NULL;

CREATE TABLE IF NOT EXISTS recruiter_candidate_clicks (
  id INT AUTO_INCREMENT PRIMARY KEY,
  recruiter_rid VARCHAR(20) NOT NULL,
  candidate_name VARCHAR(190) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_recruiter_clicks_rid_created (recruiter_rid, created_at),
  CONSTRAINT fk_clicks_recruiter
    FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS resumes_data (
  res_id VARCHAR(30) PRIMARY KEY,
  rid VARCHAR(20) NOT NULL,
  job_jid INT NULL,
  submitted_by_role VARCHAR(30) NULL DEFAULT 'recruiter',
  applicant_name VARCHAR(255) NULL,
  applicant_email VARCHAR(190) NULL,
  is_accepted BOOLEAN NOT NULL DEFAULT FALSE,
  accepted_at TIMESTAMP NULL DEFAULT NULL,
  accepted_by_admin VARCHAR(50) NULL,
  resume LONGBLOB NOT NULL,
  resume_filename VARCHAR(255) NOT NULL,
  resume_type VARCHAR(10) NOT NULL,
  ats_score DECIMAL(5,2) NULL,
  ats_match_percentage DECIMAL(5,2) NULL,
  ats_raw_json JSON NULL,
  file_hash VARCHAR(64) NULL,
  uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE INDEX idx_resumes_data_file_hash (file_hash),
  INDEX idx_resumes_data_rid (rid),
  INDEX idx_resumes_data_job_jid (job_jid),
  INDEX idx_resumes_data_uploaded_at (uploaded_at),
  CONSTRAINT fk_resumes_data_recruiter
    FOREIGN KEY (rid) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_resumes_data_job
    FOREIGN KEY (job_jid) REFERENCES jobs(jid)
    ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS extra_info (
  res_id VARCHAR(30) NOT NULL,
  resume_id VARCHAR(30) NULL,
  job_jid INT NULL,
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
);

ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS walk_in_reason TEXT NULL;
ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS select_reason TEXT NULL;
ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS joined_reason TEXT NULL;
ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS dropout_reason TEXT NULL;
ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS reject_reason TEXT NULL;

ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS submitted_by_role VARCHAR(30) NULL DEFAULT 'recruiter';
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS applicant_name VARCHAR(255) NULL;
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS applicant_email VARCHAR(190) NULL;
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS is_accepted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMP NULL DEFAULT NULL;
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS accepted_by_admin VARCHAR(50) NULL;
ALTER TABLE resumes_data
  ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64) NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_resumes_data_file_hash ON resumes_data (file_hash);

CREATE TABLE IF NOT EXISTS resume_id_sequence (
  seq_id BIGINT AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS job_resume_selection (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_jid INT NOT NULL,
  res_id VARCHAR(30) NOT NULL,
  selected_by_admin VARCHAR(50) NOT NULL,
  selection_status ENUM('selected', 'rejected', 'on_hold') NOT NULL DEFAULT 'selected',
  selection_note TEXT NULL,
  selected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_job_resume_selection (job_jid, res_id),
  INDEX idx_job_resume_selection_job_status_time (job_jid, selection_status, selected_at),
  CONSTRAINT fk_job_resume_selection_job
    FOREIGN KEY (job_jid) REFERENCES jobs(jid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_job_resume_selection_resume
    FOREIGN KEY (res_id) REFERENCES resumes_data(res_id)
    ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE job_resume_selection
  MODIFY COLUMN selection_status ENUM('selected', 'rejected', 'on_hold', 'verified', 'walk_in', 'joined', 'dropout', 'pending') NOT NULL DEFAULT 'selected';

CREATE TABLE IF NOT EXISTS money_sum (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  company_rev DECIMAL(14,2) NOT NULL DEFAULT 0,
  expense DECIMAL(14,2) NOT NULL DEFAULT 0,
  profit DECIMAL(14,2) NOT NULL DEFAULT 0,
  reason TEXT NULL,
  entry_type VARCHAR(20) NOT NULL DEFAULT 'expense',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_money_sum_created_at (created_at),
  INDEX idx_money_sum_entry_type (entry_type)
);

CREATE TABLE IF NOT EXISTS reimbursements (
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
);

CREATE TABLE IF NOT EXISTS recruiter_attendance (
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
    ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_recruiter_attendance_money_sum
    FOREIGN KEY (money_sum_id) REFERENCES money_sum(id)
    ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS job_recruiter_access (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_jid INT NOT NULL,
  recruiter_rid VARCHAR(20) NOT NULL,
  granted_by VARCHAR(20) NOT NULL,
  granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  notes TEXT NULL,
  UNIQUE KEY uniq_job_recruiter_access (job_jid, recruiter_rid),
  INDEX idx_job_recruiter_access_job_rid (job_jid, recruiter_rid),
  INDEX idx_job_recruiter_access_job_active (job_jid, is_active),
  INDEX idx_job_recruiter_access_recruiter_active (recruiter_rid, is_active),
  CONSTRAINT fk_job_recruiter_access_job
    FOREIGN KEY (job_jid) REFERENCES jobs(jid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_job_recruiter_access_recruiter
    FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_job_recruiter_access_granted_by
    FOREIGN KEY (granted_by) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS status (
  recruiter_rid VARCHAR(20) PRIMARY KEY,
  submitted INT NOT NULL DEFAULT 0,
  verified INT NULL,
  walk_in INT NULL,
  `select` INT NULL,
  reject INT NULL,
  joined INT NULL,
  dropout INT NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_status_recruiter_rid (recruiter_rid),
  CONSTRAINT fk_status_recruiter
    FOREIGN KEY (recruiter_rid) REFERENCES recruiter(rid)
    ON UPDATE CASCADE ON DELETE CASCADE
);

-- Add password change tracking for first-time login
ALTER TABLE recruiter
  ADD COLUMN IF NOT EXISTS password_changed BOOLEAN NOT NULL DEFAULT FALSE;
