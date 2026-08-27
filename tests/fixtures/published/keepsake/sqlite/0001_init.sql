create table keepsake_schema_metadata (
  key text primary key,
  value text not null
);

insert into keepsake_schema_metadata (key, value)
values ('backend', 'sqlite');

create table keepsake_relation_definitions (
  id text primary key,
  kind text not null,
  key text not null,
  enabled integer not null default 1 check (enabled in (0, 1)),
  expiry_policy text not null check (json_valid(expiry_policy)),
  created_at text not null,
  updated_at text not null,
  unique (kind, key)
);

create table keepsakes (
  id text primary key,
  subject_kind text not null,
  subject_id text not null,
  relation_id text not null references keepsake_relation_definitions(id),
  state text not null check (state in ('applied', 'revoked', 'expired')),
  expiry_policy text not null check (json_valid(expiry_policy)),
  applied_at text not null,
  expires_at text,
  fulfilled_at text,
  revoked_at text,
  metadata text not null default '{}' check (json_valid(metadata)),
  created_at text not null,
  updated_at text not null
);

create unique index keepsakes_one_active_relation_per_subject
  on keepsakes (subject_kind, subject_id, relation_id)
  where state = 'applied';

create index keepsakes_active_subject_lookup
  on keepsakes (subject_kind, subject_id, relation_id, id)
  where state = 'applied';

create index keepsakes_active_relation_membership
  on keepsakes (relation_id, subject_kind, subject_id, id)
  where state = 'applied';

create index keepsakes_due_timed_expiry
  on keepsakes (expires_at, relation_id, subject_kind, subject_id, id)
  where state = 'applied' and expires_at is not null;

create table keepsake_fulfillment_counters (
  keepsake_id text not null references keepsakes(id) on delete cascade,
  key text not null,
  value integer not null,
  observed_at text not null,
  primary key (keepsake_id, key)
);

create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (key, value, keepsake_id);

create table keepsake_audit_events (
  id integer primary key autoincrement,
  keepsake_id text not null,
  relation_id text not null,
  subject_kind text not null,
  subject_id text not null,
  actor_kind text not null,
  actor_id text not null,
  event_type text not null,
  decision text not null check (json_valid(decision)),
  occurred_at text not null,
  recorded_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

create table keepsake_audit_context_attributes (
  audit_event_id integer not null references keepsake_audit_events(id) on delete cascade,
  key text not null,
  value text not null,
  primary key (audit_event_id, key)
);

create index keepsake_audit_by_keepsake
  on keepsake_audit_events (keepsake_id, occurred_at, id);

create index keepsake_audit_by_relation
  on keepsake_audit_events (relation_id, occurred_at, id);

create index keepsake_audit_context_attribute_lookup
  on keepsake_audit_context_attributes (key, value, audit_event_id);
