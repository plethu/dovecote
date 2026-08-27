create table keepsake_schema_metadata (
  `key` varchar(191) primary key,
  value varchar(191) not null
);

insert into keepsake_schema_metadata (`key`, value)
values ('backend', 'mysql');

create table keepsake_relation_definitions (
  id char(36) primary key,
  kind varchar(191) not null,
  `key` varchar(191) not null,
  enabled boolean not null default true,
  expiry_policy json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  unique (kind, `key`)
);

create table keepsakes (
  id char(36) primary key,
  subject_kind varchar(191) not null,
  subject_id varchar(191) not null,
  relation_id char(36) not null,
  state varchar(16) not null check (state in ('applied', 'revoked', 'expired')),
  expiry_policy json not null,
  applied_at datetime(6) not null,
  expires_at datetime(6),
  fulfilled_at datetime(6),
  revoked_at datetime(6),
  metadata json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  active_relation_key char(36) generated always as (
    case when state = 'applied' then relation_id else null end
  ) stored,
  constraint keepsakes_relation_fk foreign key (relation_id)
    references keepsake_relation_definitions(id),
  unique keepsakes_one_active_relation_per_subject
    (subject_kind, subject_id, active_relation_key)
);

create index keepsakes_active_subject_lookup
  on keepsakes (subject_kind, subject_id, relation_id, id);

create index keepsakes_active_relation_membership
  on keepsakes (relation_id, subject_kind, subject_id, id);

create index keepsakes_due_timed_expiry
  on keepsakes (expires_at, relation_id, subject_kind, subject_id, id);

create table keepsake_fulfillment_counters (
  keepsake_id char(36) not null,
  `key` varchar(191) not null,
  value bigint not null,
  observed_at datetime(6) not null,
  primary key (keepsake_id, `key`),
  constraint keepsake_fulfillment_counters_keepsake_fk foreign key (keepsake_id)
    references keepsakes(id) on delete cascade
);

create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (`key`, value, keepsake_id);

create table keepsake_audit_events (
  id bigint primary key auto_increment,
  keepsake_id char(36) not null,
  relation_id char(36) not null,
  subject_kind varchar(191) not null,
  subject_id varchar(191) not null,
  actor_kind varchar(191) not null,
  actor_id varchar(191) not null,
  event_type varchar(64) not null,
  decision json not null,
  occurred_at datetime(6) not null,
  recorded_at datetime(6) not null default current_timestamp(6)
);

create table keepsake_audit_context_attributes (
  audit_event_id bigint not null,
  `key` varchar(191) not null,
  value text not null,
  primary key (audit_event_id, `key`),
  constraint keepsake_audit_context_attributes_event_fk foreign key (audit_event_id)
    references keepsake_audit_events(id) on delete cascade
);

create index keepsake_audit_by_keepsake
  on keepsake_audit_events (keepsake_id, occurred_at, id);

create index keepsake_audit_by_relation
  on keepsake_audit_events (relation_id, occurred_at, id);

create index keepsake_audit_context_attribute_lookup
  on keepsake_audit_context_attributes (`key`, value(191), audit_event_id);
