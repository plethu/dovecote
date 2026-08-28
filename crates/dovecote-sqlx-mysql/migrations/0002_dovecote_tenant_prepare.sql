-- Prepare a version 1 MySQL/MariaDB deployment for explicit tenant backfill.
-- This adds nullable columns only; operators must populate both columns and
-- verify event/delivery equality before activation.
-- The v1 global identity key remains in place until activation can replace it
-- with the tenant-scoped generated identity key.
ALTER TABLE dovecote_events ADD COLUMN tenant_id VARBINARY(255);
ALTER TABLE dovecote_deliveries ADD COLUMN tenant_id VARBINARY(255);
