-- Migration: Change job jid and referencing foreign key columns from INT to VARCHAR(30)
-- Reason: Job IDs are formatted as alphanumeric strings like 'JID-40' by allocateNextJobJid

-- Step 1: Temporarily disable foreign key checks
SET FOREIGN_KEY_CHECKS = 0;

-- Step 2: Drop foreign key constraints referencing jobs(jid)
ALTER TABLE applications DROP FOREIGN KEY IF EXISTS fk_applications_job;
ALTER TABLE resumes_data DROP FOREIGN KEY IF EXISTS fk_resumes_data_job;
ALTER TABLE candidate DROP FOREIGN KEY IF EXISTS fk_candidate_job;
ALTER TABLE job_resume_selection DROP FOREIGN KEY IF EXISTS fk_job_resume_selection_job;
ALTER TABLE job_recruiter_access DROP FOREIGN KEY IF EXISTS fk_job_recruiter_access_job;
ALTER TABLE points_log DROP FOREIGN KEY IF EXISTS fk_points_log_job;

-- Step 3: Modify jobs.jid to VARCHAR(30) (removes AUTO_INCREMENT if set)
ALTER TABLE jobs MODIFY COLUMN jid VARCHAR(30) NOT NULL;

-- Step 4: If any existing rows have purely numeric jid (e.g., 1, 2, 3), convert them to 'JID-1', 'JID-2'
UPDATE jobs SET jid = CONCAT('JID-', jid) WHERE jid NOT LIKE 'JID-%';

-- Step 5: Update child table referencing columns to VARCHAR(30)
ALTER TABLE applications MODIFY COLUMN job_jid VARCHAR(30) NOT NULL;
UPDATE applications SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE resumes_data MODIFY COLUMN job_jid VARCHAR(30) NULL;
UPDATE resumes_data SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE candidate MODIFY COLUMN job_jid VARCHAR(30) NULL;
UPDATE candidate SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE job_resume_selection MODIFY COLUMN job_jid VARCHAR(30) NOT NULL;
UPDATE job_resume_selection SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE job_recruiter_access MODIFY COLUMN job_jid VARCHAR(30) NOT NULL;
UPDATE job_recruiter_access SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE extra_info MODIFY COLUMN job_jid VARCHAR(30) NULL;
UPDATE extra_info SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

ALTER TABLE points_log MODIFY COLUMN job_jid VARCHAR(30) NULL;
UPDATE points_log SET job_jid = CONCAT('JID-', job_jid) WHERE job_jid NOT LIKE 'JID-%' AND job_jid IS NOT NULL;

-- Step 6: Re-add foreign key constraints
ALTER TABLE applications
  ADD CONSTRAINT fk_applications_job
  FOREIGN KEY (job_jid) REFERENCES jobs(jid)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE resumes_data
  ADD CONSTRAINT fk_resumes_data_job
  FOREIGN KEY (job_jid) REFERENCES jobs(jid)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE candidate
  ADD CONSTRAINT fk_candidate_job
  FOREIGN KEY (job_jid) REFERENCES jobs(jid)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE job_resume_selection
  ADD CONSTRAINT fk_job_resume_selection_job
  FOREIGN KEY (job_jid) REFERENCES jobs(jid)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE job_recruiter_access
  ADD CONSTRAINT fk_job_recruiter_access_job
  FOREIGN KEY (job_jid) REFERENCES jobs(jid)
  ON UPDATE CASCADE ON DELETE CASCADE;

-- Step 7: Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

