create table gatekeep_audit_decisions (
  id integer primary key autoincrement,
  request_id text,
  policy_id text not null,
  policy_hash text not null,
  effect text not null check (effect in ('permit', 'deny')),
  trace text not null check (json_valid(trace)),
  decisive_clause text not null check (json_valid(decisive_clause)),
  denial_reason_code text,
  denial_reason_shape text check (denial_reason_shape in ('forbidden', 'hidden') or denial_reason_shape is null),
  denial_reason text check (denial_reason is null or json_valid(denial_reason)),
  entry text not null check (json_valid(entry)),
  recorded_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

create table gatekeep_audit_consulted_facts (
  decision_id integer not null references gatekeep_audit_decisions(id) on delete cascade,
  position integer not null,
  fact_id text not null,
  presence text not null check (presence in ('present', 'absent', 'unknown')),
  primary key (decision_id, position)
);

create table gatekeep_audit_obligations (
  decision_id integer not null references gatekeep_audit_decisions(id) on delete cascade,
  position integer not null,
  obligation_id text not null,
  primary key (decision_id, position)
);

create table gatekeep_audit_request_subjects (
  decision_id integer not null references gatekeep_audit_decisions(id) on delete cascade,
  slot text not null,
  subject_kind text not null,
  subject_id text not null,
  primary key (decision_id, slot)
);

create table gatekeep_audit_reason_params (
  decision_id integer not null references gatekeep_audit_decisions(id) on delete cascade,
  key text not null,
  value text not null check (json_valid(value)),
  primary key (decision_id, key)
);

create table gatekeep_audit_outbox (
  id integer primary key autoincrement,
  decision_id integer not null references gatekeep_audit_decisions(id) on delete cascade,
  event_type text not null default 'gatekeep.decision_audit_recorded',
  payload text not null check (json_valid(payload)),
  claimed_by text,
  claimed_until text,
  delivered_at text,
  created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

create index gatekeep_audit_decisions_by_request
  on gatekeep_audit_decisions (request_id, id);

create index gatekeep_audit_decisions_by_policy
  on gatekeep_audit_decisions (policy_id, policy_hash, id);

create index gatekeep_audit_consulted_fact_lookup
  on gatekeep_audit_consulted_facts (fact_id, presence, decision_id);

create index gatekeep_audit_outbox_claimable
  on gatekeep_audit_outbox (delivered_at, claimed_until, id);
