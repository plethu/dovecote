create table keepsake_audit_outbox (
  id bigserial primary key,
  audit_event_id bigint not null references keepsake_audit_events(id) on delete cascade,
  event_type text not null default 'keepsake.audit_event_recorded',
  payload jsonb not null,
  claimed_by text,
  claimed_until timestamptz,
  delivered_at timestamptz,
  created_at timestamptz not null default now()
);

create index keepsake_audit_outbox_export
  on keepsake_audit_outbox (id)
  where delivered_at is null;

create index keepsake_audit_outbox_claim
  on keepsake_audit_outbox (delivered_at, claimed_until, id);
