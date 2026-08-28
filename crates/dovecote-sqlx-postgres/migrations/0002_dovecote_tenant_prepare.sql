-- Prepare an existing Dovecote schema version 1 for explicit tenant backfill.
--
-- Run this in a maintenance window with the application still on the v1
-- adapter.  It intentionally adds nullable columns only: no tenant value is
-- invented here, and v2 must not be activated until every row is assigned by
-- an operator-owned mapping. The v1 global identity index remains in place
-- until activation can replace it with the tenant-scoped identity index.

ALTER TABLE dovecote_events ADD COLUMN tenant_id VARCHAR(255) COLLATE "C";
ALTER TABLE dovecote_deliveries ADD COLUMN tenant_id VARCHAR(255) COLLATE "C";
