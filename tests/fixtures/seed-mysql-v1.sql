-- Rows for the published Keepsake 1.1 and Gatekeep 1.0 MySQL/MariaDB
-- schemas. The shell runner applies the real migration files first.

INSERT INTO keepsake_relation_definitions
  (id, kind, `key`, expiry_policy, created_at, updated_at)
VALUES
  ('00000000-0000-0000-0000-000000000001', 'role', 'reader', '{"type":"manual_only"}',
   '2025-12-01 00:00:00.000000', '2025-12-01 00:00:00.000000');

INSERT INTO keepsakes
  (id, subject_kind, subject_id, relation_id, state, expiry_policy, applied_at,
   metadata, created_at, updated_at)
VALUES
  ('00000000-0000-0000-0000-000000000002', 'user', 'münchen',
   '00000000-0000-0000-0000-000000000001', 'applied', '{"type":"manual_only"}',
   '2025-12-01 00:00:00.000000', '{}', '2025-12-01 00:00:00.000000',
   '2025-12-01 00:00:00.000000');

INSERT INTO keepsake_audit_events
  (id, keepsake_id, relation_id, subject_kind, subject_id, actor_kind, actor_id,
   event_type, decision, occurred_at, recorded_at)
VALUES
  (100, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service', 'writer-zero', 'apply', '{"type":"applied","duplicate_prevented":false}', '2026-01-01 00:00:00.000000', '2026-01-01 00:00:00.000000'),
  (101, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service', 'writer-α', 'keepsake.audit_event_recorded', '{"z":1,"text":"café","optional":null}', '2026-01-01 00:00:01.123456', '2026-01-01 00:00:01.123456'),
  (102, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service', 'writer-β', 'keepsake.audit_event_recorded', '{"action":"active","actor":"π","context":{"tenant":"münchen"}}', '2026-01-01 00:00:02.000000', '2026-01-01 00:00:02.000000'),
  (103, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service', 'writer-γ', 'keepsake.audit_event_recorded', '{"action":"expired","empty":[],"absent_is_not_null":true}', '2026-01-01 00:00:03.000001', '2026-01-01 00:00:03.000001'),
  (104, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'user', 'münchen', 'service', 'writer-δ', 'keepsake.audit_event_recorded', '{"action":"delivered","details":{"b":2,"a":1}}', '2026-01-01 00:00:04.000000', '2026-01-01 00:00:04.000000');

INSERT INTO keepsake_audit_outbox
  (id, audit_event_id, event_type, payload, claimed_by, claimed_until, delivered_at, created_at)
VALUES
  (101, 101, 'keepsake.audit_event_recorded', '{"z":1,"text":"café","optional":null}', NULL, NULL, NULL, '2026-01-01 00:00:01.123456'),
  (102, 102, 'keepsake.audit_event_recorded', '{"action":"active","actor":"π","context":{"tenant":"münchen"}}', 'legacy-worker', '2037-01-01 00:00:00.000000', NULL, '2026-01-01 00:00:02.000000'),
  (103, 103, 'keepsake.audit_event_recorded', '{"action":"expired","empty":[],"absent_is_not_null":true}', 'expired-worker', '2020-01-01 00:00:00.000000', NULL, '2026-01-01 00:00:03.000001'),
  (104, 104, 'keepsake.audit_event_recorded', '{"action":"delivered","details":{"b":2,"a":1}}', NULL, NULL, '2026-01-02 03:04:05.654321', '2026-01-01 00:00:04.000000');

INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (100, NULL, 'reports.view', 'hash-reports-overlap', 'permit', '{}', '{}', NULL, NULL, NULL, '{"effect":"permit","context":{"tenant":"東京","optional":[]}}', '2026-01-01 00:00:00.000000'),
  (200, NULL, 'reports.view', 'hash-reports-old', 'permit', '{}', '{}', NULL, NULL, NULL, '{"effect":"permit","context":{}}', '2026-01-01 00:01:00.000000'),
  (201, NULL, 'files.read', 'hash-read', 'permit', '[]', '{}', NULL, NULL, NULL, '{"decision":"permit","policy":"files.read","subjects":[],"facts":null}', '2026-01-01 00:01:01.000000'),
  (202, 'request-202', 'files.write', 'hash-write', 'deny', '{"rule":"deny-write"}', '{"clause":1}', 'forbidden', 'forbidden', '{"reason":"拒绝"}', '{"decision":"deny","policy":"files.write","reason":"拒绝","obligations":{}}', '2026-01-01 00:01:02.000000'),
  (203, 'request-203', 'reports.view', 'hash-reports', 'permit', '[]', '{}', NULL, NULL, NULL, '{"decision":"permit","policy":"reports.view","trace":[],"optional":null}', '2026-01-01 00:01:03.000000'),
  (204, 'request-204', 'admin', 'hash-admin', 'deny', '{"rule":"r-1"}', '{"clause":2}', 'policy', 'hidden', '{"reason":"private"}', '{"decision":"deny","policy":{"id":"admin","version":7},"trace":{"rule":"r-1"}}', '2026-01-01 00:01:04.000000');

INSERT INTO gatekeep_audit_consulted_facts
  (decision_id, position, fact_id, presence)
VALUES
  (201, 0, 'subject.exists', 'present'),
  (202, 0, 'role.admin', 'absent'),
  (203, 0, 'report.visible', 'unknown');

INSERT INTO gatekeep_audit_obligations
  (decision_id, position, obligation_id)
VALUES (202, 0, 'notify-owner');

INSERT INTO gatekeep_audit_request_subjects
  (decision_id, slot, subject_kind, subject_id)
VALUES
  (201, 'principal', 'user', 'münchen'),
  (202, 'principal', 'user', '用户');

INSERT INTO gatekeep_audit_reason_params
  (decision_id, `key`, value)
VALUES
  (202, 'locale', '"zh-Hans"'),
  (204, 'empty', '{}');

INSERT INTO gatekeep_audit_outbox
  (id, decision_id, event_type, payload, claimed_by, claimed_until, delivered_at, created_at)
VALUES
  (201, 201, 'gatekeep.decision_audit_recorded', '{"decision":"permit","policy":"files.read","subjects":[],"facts":null}', NULL, NULL, NULL, '2026-01-01 00:01:01.000000'),
  (202, 202, 'gatekeep.decision_audit_recorded', '{"decision":"deny","policy":"files.write","reason":"拒绝","obligations":{}}', 'legacy-worker', '2037-01-01 00:00:00.000000', NULL, '2026-01-01 00:01:02.000000'),
  (203, 203, 'gatekeep.decision_audit_recorded', '{"decision":"permit","policy":"reports.view","trace":[],"optional":null}', 'expired-worker', '2020-01-01 00:00:00.000000', NULL, '2026-01-01 00:01:03.000000'),
  (204, 204, 'gatekeep.decision_audit_recorded', '{"decision":"deny","policy":{"id":"admin","version":7},"trace":{"rule":"r-1"}}', NULL, NULL, '2026-01-03 04:05:06.000007', '2026-01-01 00:01:04.000000');

INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (500, 'request-500', 'audit-only-after-outbox', 'hash-audit-only', 'permit', '{}',
   '{}', NULL, NULL, NULL,
   '{"effect":"permit","context":{"audit":"only-after-outbox"}}',
   '2026-01-01 00:01:07.000000');
INSERT INTO gatekeep_audit_decisions
  (id, request_id, policy_id, policy_hash, effect, trace, decisive_clause,
   denial_reason_code, denial_reason_shape, denial_reason, entry, recorded_at)
VALUES
  (1000, 'request-1000', 'inverse-sequence', 'hash-inverse', 'permit', '{}',
   '{}', NULL, NULL, NULL,
   '{"decision":"permit","policy":"inverse-sequence","context":{}}',
   '2026-01-01 00:01:08.000000');
INSERT INTO gatekeep_audit_outbox
  (id, decision_id, event_type, payload, claimed_by, claimed_until,
   delivered_at, created_at)
VALUES
  (1, 1000, 'gatekeep.decision_audit_recorded',
   '{"decision":"permit","policy":"inverse-sequence","context":{}}',
   NULL, NULL, NULL, '2026-01-01 00:01:08.000000');
