-- Optional PostgreSQL RLS profile for an already-installed schema v2.
--
-- This is not part of the ordinary migration: deployments must review role
-- ownership, connection pooling, and BYPASSRLS administration first. Scoped
-- application transactions must call bind_tenant; administrative roles must
-- use BYPASSRLS (or an equivalent reviewed role policy).

ALTER TABLE dovecote_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE dovecote_events FORCE ROW LEVEL SECURITY;
ALTER TABLE dovecote_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE dovecote_deliveries FORCE ROW LEVEL SECURITY;

CREATE POLICY dovecote_events_tenant_isolation ON dovecote_events
    USING (tenant_id = current_setting('dovecote.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('dovecote.tenant_id', true));

CREATE POLICY dovecote_deliveries_tenant_isolation ON dovecote_deliveries
    USING (tenant_id = current_setting('dovecote.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('dovecote.tenant_id', true));
