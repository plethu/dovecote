create table keepsake_relation_definitions (
  id uuid primary key,
  kind text not null,
  key text not null,
  enabled boolean not null default true,
  expiry_policy jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (kind, key)
);

create table keepsakes (
  id uuid primary key,
  subject_kind text not null,
  subject_id text not null,
  relation_id uuid not null references keepsake_relation_definitions(id),
  state text not null check (state in ('applied', 'revoked', 'expired')),
  expiry_policy jsonb not null,
  applied_at timestamptz not null,
  expires_at timestamptz,
  fulfilled_at timestamptz,
  revoked_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
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
  keepsake_id uuid not null references keepsakes(id) on delete cascade,
  key text not null,
  value bigint not null,
  observed_at timestamptz not null,
  primary key (keepsake_id, key)
);

create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (key, value, keepsake_id);

create table keepsake_audit_events (
  id bigserial primary key,
  keepsake_id uuid not null,
  relation_id uuid not null,
  subject_kind text not null,
  subject_id text not null,
  actor_kind text not null,
  actor_id text not null,
  event_type text not null,
  decision jsonb not null,
  occurred_at timestamptz not null,
  recorded_at timestamptz not null default now()
);

create table keepsake_audit_context_attributes (
  audit_event_id bigint not null references keepsake_audit_events(id) on delete cascade,
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
