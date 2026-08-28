-- Activate tenant-aware constraints after an operator-owned backfill.
--
-- The backfill must assign the same validated TenantId to each event and its
-- delivery before this script runs. The script refuses to guess a tenant and
-- makes the v2 invariant durable before the new adapter is deployed.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM dovecote_events WHERE tenant_id IS NULL)
       OR EXISTS (SELECT 1 FROM dovecote_deliveries WHERE tenant_id IS NULL) THEN
        RAISE EXCEPTION 'Dovecote tenant backfill is incomplete';
    END IF;
    IF EXISTS (SELECT 1 FROM dovecote_events WHERE tenant_id = '')
       OR EXISTS (SELECT 1 FROM dovecote_deliveries WHERE tenant_id = '') THEN
        RAISE EXCEPTION 'Dovecote tenant backfill contains an empty tenant';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM dovecote_deliveries AS d
        LEFT JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE e.row_id IS NULL OR d.tenant_id <> e.tenant_id
    ) THEN
        RAISE EXCEPTION 'Dovecote event and delivery tenants do not agree';
    END IF;
END
$$;

ALTER TABLE dovecote_events
    ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE dovecote_deliveries
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE dovecote_events
    ADD CONSTRAINT dovecote_events_tenant_size
        CHECK (octet_length(tenant_id) <= 255),
    ADD CONSTRAINT dovecote_events_tenant_nonempty
        CHECK (octet_length(tenant_id) > 0),
    ADD CONSTRAINT dovecote_events_tenant_row_unique
        UNIQUE (tenant_id, row_id);

DROP INDEX IF EXISTS dovecote_events_source_event_id;
CREATE UNIQUE INDEX dovecote_events_tenant_source_event_id
    ON dovecote_events (tenant_id COLLATE "C", source COLLATE "C", event_id COLLATE "C");

ALTER TABLE dovecote_deliveries
    DROP CONSTRAINT IF EXISTS dovecote_deliveries_event_fk;
DROP INDEX IF EXISTS dovecote_deliveries_claimable;
DROP INDEX IF EXISTS dovecote_deliveries_expired_claims;

ALTER TABLE dovecote_deliveries
    ADD CONSTRAINT dovecote_deliveries_tenant_size
        CHECK (octet_length(tenant_id) <= 255),
    ADD CONSTRAINT dovecote_deliveries_tenant_nonempty
        CHECK (octet_length(tenant_id) > 0),
    ADD CONSTRAINT dovecote_deliveries_event_fk
        FOREIGN KEY (tenant_id, event_row_id)
        REFERENCES dovecote_events (tenant_id, row_id)
        ON DELETE RESTRICT;

CREATE INDEX dovecote_deliveries_claimable
    ON dovecote_deliveries (tenant_id COLLATE "C", state, available_at, event_row_id);
CREATE INDEX dovecote_deliveries_expired_claims
    ON dovecote_deliveries (tenant_id COLLATE "C", state, claim_expires_at, event_row_id);

ALTER TABLE dovecote_schema
    DROP CONSTRAINT dovecote_schema_version_supported,
    ADD CONSTRAINT dovecote_schema_version_supported
        CHECK (schema_version = 2);

UPDATE dovecote_schema
SET schema_version = 2,
    minimum_crate_major = 0,
    minimum_crate_minor = 2,
    minimum_crate_patch = 0,
    rolling_compatible = FALSE;

COMMIT;
