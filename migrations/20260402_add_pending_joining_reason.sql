ALTER TABLE extra_info
  ADD COLUMN IF NOT EXISTS pending_joining_reason TEXT NULL;
