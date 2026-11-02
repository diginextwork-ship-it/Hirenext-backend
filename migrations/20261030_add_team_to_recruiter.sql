-- Migration: Add team column to recruiter table
-- Default to 'Team BPO' so all existing recruiters belong to Team BPO initially

ALTER TABLE recruiter 
  ADD COLUMN IF NOT EXISTS team VARCHAR(50) NOT NULL DEFAULT 'Team BPO';

UPDATE recruiter 
  SET team = 'Team BPO' 
  WHERE team IS NULL OR TRIM(team) = '';

