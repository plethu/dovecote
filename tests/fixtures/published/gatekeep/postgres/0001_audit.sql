create table gatekeep_audit_decisions (
  id bigserial primary key,
  request_id text,
  policy_id text not null,
  policy_hash text not null,
  effect text not null check (effect in ('permit', 'deny')),
  trace jsonb not null,
  decisive_clause jsonb not null,
  denial_reason_code text,
  denial_reason_shape text check (denial_reason_shape in ('forbidden', 'hidden') or denial_reason_shape is null),
  denial_reason jsonb,
  entry jsonb not null,
  recorded_at timestamptz not null default now()
);

create table gatekeep_audit_consulted_facts (
  decision_id bigint not null references gatekeep_audit_decisions(id) on delete cascade,
  position integer not null,
  fact_id text not null,
  presence text not null check (presence in ('present', 'absent', 'unknown')),
  primary key (decision_id, position)
);

create table gatekeep_audit_obligations (
  decision_id bigint not null references gatekeep_audit_decisions(id) on delete cascade,
  position integer not null,
  obligation_id text not null,
  primary key (decision_id, position)
);

create table gatekeep_audit_request_subjects (
  decision_id bigint not null references gatekeep_audit_decisions(id) on delete cascade,
  slot text not null,
  subject_kind text not null,
  subject_id text not null,
  primary key (decision_id, slot)
);

create table gatekeep_audit_reason_params (
  decision_id bigint not null references gatekeep_audit_decisions(id) on delete cascade,
  key text not null,
  value jsonb not null,
  primary key (decision_id, key)
);

create table gatekeep_audit_outbox (
  id bigserial primary key,
  decision_id bigint not null references gatekeep_audit_decisions(id) on delete cascade,
  event_type text not null default 'gatekeep.decision_audit_recorded',
  payload jsonb not null,
  claimed_by text,
  claimed_until timestamptz,
  delivered_at timestamptz,
  created_at timestamptz not null default now()
);

create index gatekeep_audit_decisions_by_request
  on gatekeep_audit_decisions (request_id, id);

create index gatekeep_audit_decisions_by_policy
  on gatekeep_audit_decisions (policy_id, policy_hash, id);

create index gatekeep_audit_consulted_fact_lookup
  on gatekeep_audit_consulted_facts (fact_id, presence, decision_id);

create index gatekeep_audit_outbox_claimable
  on gatekeep_audit_outbox (delivered_at, claimed_until, id);
