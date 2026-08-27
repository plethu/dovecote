-- Supports the fulfillment expiry sweep, which scans applied keepsakes whose
-- policy is `when_fulfilled` in stable batch order. MySQL has no partial index,
-- so a stored generated column projects the sweep predicate (mirroring the
-- `active_relation_key` pattern) and the index covers it. The column is null for
-- every row outside the sweep set, keeping the index small.
alter table keepsakes
  add column fulfillment_pending tinyint
    generated always as (
      case
        when state = 'applied'
         and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled'
        then 1
      end
    ) stored;

create index keepsakes_due_fulfilled_expiry
  on keepsakes (fulfillment_pending, relation_id, subject_kind, subject_id, id);
