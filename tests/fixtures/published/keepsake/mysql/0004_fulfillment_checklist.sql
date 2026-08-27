-- Persists application-owned checklist fulfillment state so `when_fulfilled`
-- policies with a `checklist_complete` rule can be evaluated by the expiry
-- sweep instead of only in application code.
create table keepsake_fulfillment_checklist (
  keepsake_id char(36) not null,
  item varchar(191) not null,
  complete tinyint(1) not null,
  observed_at datetime(6) not null,
  primary key (keepsake_id, item),
  constraint keepsake_fulfillment_checklist_keepsake_fk foreign key (keepsake_id)
    references keepsakes(id) on delete cascade
);

create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (item, complete, keepsake_id);
