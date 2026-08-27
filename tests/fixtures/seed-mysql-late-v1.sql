-- Rows deliberately inserted only after the initial high-water pass.
-- These rows deliberately use independent sequences: the audit/decision IDs
-- are late, while their outbox IDs are below the first persisted cursors.
INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (205, 'request-205', 'sequence-late', 'hash-sequence-late', 'permit', '{}',
   '{}', NULL, NULL, NULL,
   '{"decision":"permit","policy":"sequence-late","context":{"optional":{}}}',
   '2026-01-01 00:01:05.000000');
INSERT INTO gatekeep_audit_outbox
  (id, decision_id, event_type, payload, claimed_by, claimed_until,
   delivered_at, created_at)
VALUES
  (100, 205, 'gatekeep.decision_audit_recorded',
   '{"decision":"permit","policy":"sequence-late","context":{"optional":{}}}',
   NULL, NULL, NULL, '2026-01-01 00:01:05.000000');
INSERT INTO keepsake_audit_events
  (id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
   event_type, decision, occurred_at, recorded_at)
VALUES
  (106, '00000000-0000-0000-0000-000000000002',
   '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service',
   'writer-ε', 'keepsake.audit_event_recorded',
   '{"codec":"keepsake-audit-v1","event":{"action":"reconstructed","metadata":{}}}',
   '2026-01-01 00:00:06.000000', '2026-01-01 00:00:06.000000');

INSERT INTO keepsake_audit_outbox
  (id, audit_event_id, event_type, payload, claimed_by, claimed_until,
   delivered_at, created_at)
VALUES
  (106, 106, 'keepsake.audit_event_recorded',
   '{"codec":"keepsake-audit-v1","event":{"action":"reconstructed","metadata":{}}}',
   NULL, NULL, NULL, '2026-01-01 00:00:06.000000');

INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (206, 'request-206', 'reports.view', 'hash-reports-v2', 'permit', '{}', '{}',
   NULL, NULL, NULL,
   '{"codec":"gatekeep-decision-audit-v1","decision":{"effect":"permit","context":{}}}',
   '2026-01-01 00:01:06.000000');

INSERT INTO gatekeep_audit_outbox
  (id, decision_id, event_type, payload, claimed_by, claimed_until,
   delivered_at, created_at)
VALUES
  (206, 206, 'gatekeep.decision_audit_recorded',
   '{"codec":"gatekeep-decision-audit-v1","decision":{"effect":"permit","context":{}}}',
   NULL, NULL, NULL, '2026-01-01 00:01:06.000000');

-- This row is after the captured Gatekeep audit/outbox high-waters and must
-- remain outside the imported snapshot.
INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (1001, 'request-1001', 'post-snapshot', 'hash-post-snapshot', 'permit', '{}',
   '{}', NULL, NULL, NULL,
   '{"decision":"permit","policy":"post-snapshot","context":{}}',
   '2026-01-01 00:01:09.000000');
INSERT INTO gatekeep_audit_outbox
  (id, decision_id, event_type, payload, claimed_by, claimed_until,
   delivered_at, created_at)
VALUES
  (207, 1001, 'gatekeep.decision_audit_recorded',
   '{"decision":"permit","policy":"post-snapshot","context":{}}',
   NULL, NULL, NULL, '2026-01-01 00:01:09.000000');
