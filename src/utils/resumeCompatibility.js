const { normalizeResumeStatusInput } = require("./resumeStatusFlow");

const STATUS_REASON_FIELD_MAP = Object.freeze({
  submitted: "submittedReason",
  verified: "verifiedReason",
  walk_in: "walkInReason",
  further: "furtherReason",
  selected: "selectReason",
  rejected: "rejectReason",
  shortlisted: "shortlistedReason",
  joined: "joinedReason",
  dropout: "dropoutReason",
  billed: "billedReason",
  left: "leftReason",
  others: "othersReason",
});

const STATUS_REASON_INPUT_KEYS = Object.freeze({
  submitted: ["submittedReason", "submitted_reason"],
  verified: ["verifiedReason", "verified_reason"],
  walk_in: ["walkInReason", "walk_in_reason"],
  further: ["furtherReason", "further_reason"],
  selected: [
    "selectReason",
    "select_reason",
    "selectionReason",
    "selection_reason",
  ],
  rejected: ["rejectReason", "reject_reason"],
  shortlisted: [
    "shortlistedReason",
    "shortlisted_reason",
    "pendingJoiningReason",
    "pending_joining_reason",
  ],
  joined: ["joinedReason", "joined_reason", "joiningNote", "joining_note"],
  dropout: ["dropoutReason", "dropout_reason"],
  billed: ["billedReason", "billed_reason"],
  left: ["leftReason", "left_reason"],
  others: ["othersReason", "others_reason"],
});

const isPresent = (value) =>
  value !== undefined && value !== null && String(value).trim() !== "";

const firstPresent = (...values) => {
  for (const value of values) {
    if (isPresent(value)) {
      return String(value).trim();
    }
  }
  return null;
};

const normalizeDateOnly = (value) => {
  if (!isPresent(value)) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }

  const normalized = String(value).trim();
  const dateMatch = normalized.match(/\d{4}-\d{2}-\d{2}/);
  return dateMatch ? dateMatch[0] : normalized;
};

const resolveResumeWorkflowStatus = (record = {}) => {
  const joiningDate = firstPresent(
    record.joiningDate,
    record.joining_date,
    record.currentJoiningDate,
    record.resumeJoiningDate,
    record.resume_joining_date,
    record.selectionJoiningDate,
  );
  const workflowStatus =
    normalizeResumeStatusInput(
      record.workflowStatus ??
        record.workflow_status ??
        record.selectionStatus ??
        record.selection_status ??
        record.status,
    ) || "pending";

  return workflowStatus === "shortlisted" && joiningDate
    ? "selected"
    : workflowStatus;
};

const resolveStatusReasonInput = (payload = {}, status) => {
  const normalizedStatus = normalizeResumeStatusInput(status);
  const statusKeys = STATUS_REASON_INPUT_KEYS[normalizedStatus] || [];

  for (const key of statusKeys) {
    if (isPresent(payload?.[key])) {
      return String(payload[key]).trim();
    }
  }

  if (isPresent(payload?.reason)) return String(payload.reason).trim();
  if (isPresent(payload?.note)) return String(payload.note).trim();
  return "";
};

const buildResumeCompatibilityFields = (record = {}) => {
  const workflowStatus = resolveResumeWorkflowStatus(record);
  const genericReason = firstPresent(
    record.reason,
    record.note,
    record.workflowNote,
    record.workflow_note,
    record.selectionNote,
    record.selection_note,
  );
  const resId = firstPresent(record.resId, record.res_id);
  const candidateName = firstPresent(
    record.candidateName,
    record.candidate_name,
    record.name,
  );
  const candidatePhone = firstPresent(
    record.candidatePhone,
    record.candidate_phone,
    record.phone,
  );
  const jobJid = firstPresent(record.jobJid, record.job_jid);
  const companyName = firstPresent(record.companyName, record.company_name);
  const roleName = firstPresent(record.roleName, record.role_name);
  const city = firstPresent(record.city);
  const officeLocationCity = firstPresent(
    record.officeLocationCity,
    record.office_location_city,
  );
  const walkInDate = normalizeDateOnly(
    record.walkInDate ?? record.walk_in_date ?? record.currentWalkInDate,
  );
  const joiningDate = normalizeDateOnly(
    record.joiningDate ??
      record.joining_date ??
      record.currentJoiningDate ??
      record.resumeJoiningDate ??
      record.resume_joining_date,
  );
  const submittedReason = firstPresent(
    record.submittedReason,
    record.submitted_reason,
    workflowStatus === "submitted" ? genericReason : null,
  );
  const verifiedReason = firstPresent(
    record.verifiedReason,
    record.verified_reason,
    workflowStatus === "verified" ? genericReason : null,
  );
  const othersReason = firstPresent(
    record.othersReason,
    record.others_reason,
    workflowStatus === "others" ? genericReason : null,
  );
  const walkInReason = firstPresent(
    record.walkInReason,
    record.walk_in_reason,
    workflowStatus === "walk_in" ? genericReason : null,
  );
  const selectReason = firstPresent(
    record.selectReason,
    record.select_reason,
    record.selectionReason,
    record.selection_reason,
    workflowStatus === "selected"
      ? genericReason
      : null,
  );
  const rejectReason = firstPresent(
    record.rejectReason,
    record.reject_reason,
    workflowStatus === "rejected" ? genericReason : null,
  );
  const joiningNote = firstPresent(
    record.joiningNote,
    record.joining_note,
    record.joinedReason,
    record.joined_reason,
  );
  const shortlistedReason = firstPresent(
    record.shortlistedReason,
    record.shortlisted_reason,
    record.pendingJoiningReason,
    record.pending_joining_reason,
    workflowStatus === "shortlisted" ? genericReason : null,
  );
  const joinedReason = firstPresent(
    record.joinedReason,
    record.joined_reason,
    workflowStatus === "joined" ? genericReason : null,
    joiningNote,
  );
  const dropoutReason = firstPresent(
    record.dropoutReason,
    record.dropout_reason,
    workflowStatus === "dropout" ? genericReason : null,
  );
  const billedReason = firstPresent(
    record.billedReason,
    record.billed_reason,
    workflowStatus === "billed" ? genericReason : null,
  );
  const leftReason = firstPresent(
    record.leftReason,
    record.left_reason,
    workflowStatus === "left" ? genericReason : null,
  );

  return {
    resId,
    res_id: resId,
    candidateName,
    candidate_name: candidateName,
    candidatePhone,
    candidate_phone: candidatePhone,
    workflowStatus,
    workflow_status: workflowStatus,
    status: workflowStatus,
    submittedReason,
    submitted_reason: submittedReason,
    verifiedReason,
    verified_reason: verifiedReason,
    othersReason,
    others_reason: othersReason,
    walkInReason,
    walk_in_reason: walkInReason,
    selectReason,
    select_reason: selectReason,
    selectionReason: selectReason,
    selection_reason: selectReason,
    rejectReason,
    reject_reason: rejectReason,
    shortlistedReason,
    shortlisted_reason: shortlistedReason,
    pendingJoiningReason: shortlistedReason,
    pending_joining_reason: shortlistedReason,
    joinedReason,
    joined_reason: joinedReason,
    dropoutReason,
    dropout_reason: dropoutReason,
    billedReason,
    billed_reason: billedReason,
    leftReason,
    left_reason: leftReason,
    walkInDate,
    walk_in_date: walkInDate,
    joiningDate,
    joining_date: joiningDate,
    joiningNote,
    joining_note: joiningNote,
    jobJid,
    job_jid: jobJid,
    companyName,
    company_name: companyName,
    roleName,
    role_name: roleName,
    officeLocationCity,
    office_location_city: officeLocationCity,
    city,
  };
};

module.exports = {
  STATUS_REASON_FIELD_MAP,
  buildResumeCompatibilityFields,
  resolveStatusReasonInput,
};
