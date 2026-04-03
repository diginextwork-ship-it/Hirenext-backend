ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS shortlisted_reason TEXT NULL;
