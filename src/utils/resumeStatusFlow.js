const CANONICAL_VERIFY_STATUS = "verified";
const DEFAULT_WORKFLOW_STATUS = "submitted";

const LEGACY_STATUS_ALIASES = new Map([
  ["verify", CANONICAL_VERIFY_STATUS],
  ["verfied", CANONICAL_VERIFY_STATUS],
  ["pending", DEFAULT_WORKFLOW_STATUS],
  ["pending_joining", "shortlisted"],
  ["pending joining", "shortlisted"],
]);

const CANONICAL_WORKFLOW_STATUSES = [
  DEFAULT_WORKFLOW_STATUS,
  CANONICAL_VERIFY_STATUS,
  "walk_in",
  "shortlisted",
  "selected",
  "joined",
  "billed",
  "left",
  "dropout",
  "rejected",
  "others",
];

const CANONICAL_RESUME_STATUSES = new Set([
  ...CANONICAL_WORKFLOW_STATUSES,
  "further",
  "on_hold",
]);

const normalizeResumeStatusInput = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "";
  return LEGACY_STATUS_ALIASES.get(normalized) || normalized;
};

const normalizeWorkflowStatus = (value, fallback = DEFAULT_WORKFLOW_STATUS) => {
  const normalized = normalizeResumeStatusInput(value);
  if (!normalized) return fallback;
  return CANONICAL_WORKFLOW_STATUSES.includes(normalized) ? normalized : fallback;
};

const isSupportedResumeStatus = (value) =>
  CANONICAL_RESUME_STATUSES.has(normalizeResumeStatusInput(value));

const ADMIN_STATUS_TRANSITIONS = {
  [DEFAULT_WORKFLOW_STATUS]: new Set([CANONICAL_VERIFY_STATUS, "rejected"]),
  [CANONICAL_VERIFY_STATUS]: new Set(["walk_in", "others", "rejected"]),
  others: new Set(["walk_in", "rejected"]),
  walk_in: new Set(["shortlisted", "rejected"]),
  shortlisted: new Set(["selected", "dropout"]),
  selected: new Set(["joined", "dropout"]),
  joined: new Set(["billed", "left"]),
  billed: new Set(),
  left: new Set(),
  dropout: new Set(),
  rejected: new Set(),
};

const RECRUITER_STATUS_TRANSITIONS = {
  [CANONICAL_VERIFY_STATUS]: ["walk_in", "others", "rejected"],
  others: ["walk_in", "rejected"],
  walk_in: ["shortlisted", "rejected"],
  shortlisted: ["selected", "dropout", "rejected"],
  selected: ["joined", "dropout", "rejected"],
  joined: ["billed", "left"],
};

const WORKFLOW_PREVIOUS_STATUS = {
  [CANONICAL_VERIFY_STATUS]: DEFAULT_WORKFLOW_STATUS,
  others: CANONICAL_VERIFY_STATUS,
  walk_in: CANONICAL_VERIFY_STATUS,
  shortlisted: "walk_in",
  selected: "shortlisted",
  joined: "selected",
  billed: "joined",
  left: "joined",
};

const getAllowedNextStatuses = (status) => [
  ...(ADMIN_STATUS_TRANSITIONS[normalizeWorkflowStatus(status)] || []),
];

const getPreviousWorkflowStatus = (status) =>
  WORKFLOW_PREVIOUS_STATUS[normalizeWorkflowStatus(status)] || null;

module.exports = {
  ADMIN_STATUS_TRANSITIONS,
  CANONICAL_RESUME_STATUSES,
  CANONICAL_WORKFLOW_STATUSES,
  CANONICAL_VERIFY_STATUS,
  DEFAULT_WORKFLOW_STATUS,
  RECRUITER_STATUS_TRANSITIONS,
  getAllowedNextStatuses,
  getPreviousWorkflowStatus,
  isSupportedResumeStatus,
  normalizeResumeStatusInput,
  normalizeWorkflowStatus,
};
