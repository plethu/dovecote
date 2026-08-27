create table gatekeep_audit_decisions (
  id bigint not null auto_increment primary key,
  request_id text,
  policy_id text not null,
  policy_hash text not null,
  effect varchar(16) not null,
  trace json not null,
  decisive_clause json not null,
  denial_reason_code text,
  denial_reason_shape varchar(16),
  denial_reason json,
  entry json not null,
  recorded_at timestamp(6) not null default current_timestamp(6),
  check (effect in ('permit', 'deny')),
  check (denial_reason_shape in ('forbidden', 'hidden') or denial_reason_shape is null)
);

create table gatekeep_audit_consulted_facts (
  decision_id bigint not null,
  position integer not null,
  fact_id text not null,
  presence varchar(16) not null,
  primary key (decision_id, position),
  constraint gatekeep_audit_consulted_facts_decision_fk foreign key (decision_id)
    references gatekeep_audit_decisions(id) on delete cascade,
  check (presence in ('present', 'absent', 'unknown'))
);

create table gatekeep_audit_obligations (
  decision_id bigint not null,
  position integer not null,
  obligation_id text not null,
  primary key (decision_id, position),
  constraint gatekeep_audit_obligations_decision_fk foreign key (decision_id)
    references gatekeep_audit_decisions(id) on delete cascade
);

create table gatekeep_audit_request_subjects (
  decision_id bigint not null,
  slot varchar(255) not null,
  subject_kind text not null,
  subject_id text not null,
  primary key (decision_id, slot),
  constraint gatekeep_audit_request_subjects_decision_fk foreign key (decision_id)
    references gatekeep_audit_decisions(id) on delete cascade
);

create table gatekeep_audit_reason_params (
  decision_id bigint not null,
  `key` varchar(255) not null,
  value json not null,
  primary key (decision_id, `key`),
  constraint gatekeep_audit_reason_params_decision_fk foreign key (decision_id)
    references gatekeep_audit_decisions(id) on delete cascade
);

create table gatekeep_audit_outbox (
  id bigint not null auto_increment primary key,
  decision_id bigint not null,
  event_type text not null default ('gatekeep.decision_audit_recorded'),
  payload json not null,
  claimed_by text,
  claimed_until timestamp(6) null,
  delivered_at timestamp(6) null,
  created_at timestamp(6) not null default current_timestamp(6),
  constraint gatekeep_audit_outbox_decision_fk foreign key (decision_id)
    references gatekeep_audit_decisions(id) on delete cascade
);

create index gatekeep_audit_decisions_by_request
  on gatekeep_audit_decisions (request_id(255), id);

create index gatekeep_audit_decisions_by_policy
  on gatekeep_audit_decisions (policy_id(255), policy_hash(255), id);

create index gatekeep_audit_consulted_fact_lookup
  on gatekeep_audit_consulted_facts (fact_id(255), presence, decision_id);

create index gatekeep_audit_outbox_claimable
  on gatekeep_audit_outbox (delivered_at, claimed_until, id);
