create table keepsake_audit_outbox (
  id integer primary key autoincrement,
  audit_event_id integer not null references keepsake_audit_events(id) on delete cascade,
  event_type text not null default 'keepsake.audit_event_recorded',
  payload text not null check (json_valid(payload)),
  claimed_by text,
  claimed_until text,
  delivered_at text,
  created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

create index keepsake_audit_outbox_export
  on keepsake_audit_outbox (id)
  where delivered_at is null;

create index keepsake_audit_outbox_claim
  on keepsake_audit_outbox (delivered_at, claimed_until, id);
