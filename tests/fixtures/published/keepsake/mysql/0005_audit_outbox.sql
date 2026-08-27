create table keepsake_audit_outbox (
  id bigint not null auto_increment primary key,
  audit_event_id bigint not null,
  event_type text not null default ('keepsake.audit_event_recorded'),
  payload json not null,
  claimed_by text,
  claimed_until timestamp(6) null,
  delivered_at timestamp(6) null,
  created_at timestamp(6) not null default current_timestamp(6),
  constraint keepsake_audit_outbox_event_fk foreign key (audit_event_id)
    references keepsake_audit_events(id) on delete cascade
);

create index keepsake_audit_outbox_export
  on keepsake_audit_outbox (id);

create index keepsake_audit_outbox_claim
  on keepsake_audit_outbox (delivered_at, claimed_until, id);
