-- Prepare a version 1 SQLite file for explicit tenant backfill.
-- SQLite cannot add a NOT NULL column without a value, so preparation is
-- nullable and activation rebuilds the tables after a complete backfill.
-- The v1 global identity index remains in place until activation can replace
-- it with the tenant-scoped identity index.
ALTER TABLE dovecote_events ADD COLUMN tenant_id TEXT;
ALTER TABLE dovecote_deliveries ADD COLUMN tenant_id TEXT;
