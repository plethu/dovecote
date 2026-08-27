-- Supports the fulfillment expiry sweep, which scans applied keepsakes whose
-- policy is `when_fulfilled` in stable batch order. Without this partial index
-- the sweep scans every applied keepsake.
create index keepsakes_due_fulfilled_expiry
  on keepsakes (relation_id, subject_kind, subject_id, id)
  where state = 'applied' and json_extract(expiry_policy, '$.type') = 'when_fulfilled';
