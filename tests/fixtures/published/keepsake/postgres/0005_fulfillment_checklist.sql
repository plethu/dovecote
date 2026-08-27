-- Persists application-owned checklist fulfillment state so `when_fulfilled`
-- policies with a `checklist_complete` rule can be evaluated by the expiry
-- sweep instead of only in application code.
create table keepsake_fulfillment_checklist (
  keepsake_id uuid not null references keepsakes(id) on delete cascade,
  item text not null,
  complete boolean not null,
  observed_at timestamptz not null,
  primary key (keepsake_id, item)
);

create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (item, complete, keepsake_id);
