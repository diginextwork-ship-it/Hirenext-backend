const CANONICAL_VERIFY_STATUS = "verified";

const LEGACY_STATUS_ALIASES = new Map([
  ["verify", CANONICAL_VERIFY_STATUS],
  ["verfied", CANONICAL_VERIFY_STATUS],
]);

const CANONICAL_RESUME_STATUSES = new Set([
  CANONICAL_VERIFY_STATUS,
  "walk_in",
  "further",
  "selected",
  "pending_joining",
  "rejected",
  "joined",
  "dropout",
  "on_hold",
  "pending",
  "billed",
  "left",
]);

const normalizeResumeStatusInput = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "";
  return LEGACY_STATUS_ALIASES.get(normalized) || normalized;
};

const normalizeWorkflowStatus = (value, fallback = "pending") => {
  const normalized = normalizeResumeStatusInput(value);
  return normalized || fallback;
};

const isSupportedResumeStatus = (value) =>
  CANONICAL_RESUME_STATUSES.has(normalizeResumeStatusInput(value));

const ADMIN_STATUS_TRANSITIONS = {
  pending: new Set([CANONICAL_VERIFY_STATUS]),
  [CANONICAL_VERIFY_STATUS]: new Set(["walk_in", "rejected"]),
  walk_in: new Set(["further", "selected", "rejected"]),
  further: new Set(["selected", "rejected"]),
  selected: new Set(["pending_joining", "dropout"]),
  pending_joining: new Set(["joined", "dropout"]),
  joined: new Set(["billed", "left"]),
};

const RECRUITER_STATUS_TRANSITIONS = {
  [CANONICAL_VERIFY_STATUS]: ["walk_in", "rejected"],
  walk_in: ["further", "selected", "rejected"],
  further: ["selected", "rejected"],
  selected: ["joined", "dropout", "rejected"],
  joined: ["billed", "left"],
};

module.exports = {
  ADMIN_STATUS_TRANSITIONS,
  CANONICAL_RESUME_STATUSES,
  CANONICAL_VERIFY_STATUS,
  RECRUITER_STATUS_TRANSITIONS,
  isSupportedResumeStatus,
  normalizeResumeStatusInput,
  normalizeWorkflowStatus,
};
