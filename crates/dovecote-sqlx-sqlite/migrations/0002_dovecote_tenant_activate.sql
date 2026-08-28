-- Activate a prepared version 1 SQLite file after an explicit tenant backfill.
-- Run 0002_dovecote_tenant_prepare.sql first, populate both nullable tenant_id
-- columns in the same file, and execute this artifact on one connection.
-- The guard makes missing, empty, oversized, or mismatched assignments fail
-- before any old table is replaced. The rebuild is transactional.

BEGIN IMMEDIATE;

CREATE TEMP TABLE dovecote_tenant_activation_guard (
    valid INTEGER NOT NULL CHECK (valid = 1)
);
INSERT INTO dovecote_tenant_activation_guard(valid)
SELECT CASE WHEN
    NOT EXISTS (
        SELECT 1 FROM dovecote_events
        WHERE tenant_id IS NULL
           OR length(CAST(tenant_id AS BLOB)) = 0
           OR length(CAST(tenant_id AS BLOB)) > 255
    )
    AND NOT EXISTS (
        SELECT 1 FROM dovecote_deliveries
        WHERE tenant_id IS NULL
           OR length(CAST(tenant_id AS BLOB)) = 0
           OR length(CAST(tenant_id AS BLOB)) > 255
    )
    AND NOT EXISTS (
        SELECT 1
        FROM dovecote_deliveries AS d
        LEFT JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE e.row_id IS NULL OR d.tenant_id <> e.tenant_id
    )
    THEN 1 ELSE 0 END;

CREATE TABLE dovecote_events_v2 (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (row_id > 0),
    tenant_id TEXT NOT NULL CHECK (length(CAST(tenant_id AS BLOB)) > 0 AND length(CAST(tenant_id AS BLOB)) <= 255),
    stream TEXT NOT NULL CHECK (length(CAST(stream AS BLOB)) <= 255),
    specversion TEXT NOT NULL CHECK (specversion = '1.0'),
    event_id TEXT NOT NULL CHECK (length(CAST(event_id AS BLOB)) <= 1024),
    source TEXT NOT NULL CHECK (length(CAST(source AS BLOB)) <= 2048),
    event_type TEXT NOT NULL CHECK (length(CAST(event_type AS BLOB)) <= 1024),
    subject TEXT CHECK (subject IS NULL OR length(CAST(subject AS BLOB)) <= 2048),
    occurred_at TEXT,
    datacontenttype TEXT CHECK (datacontenttype IS NULL OR length(CAST(datacontenttype AS BLOB)) <= 255),
    dataschema TEXT CHECK (dataschema IS NULL OR length(CAST(dataschema AS BLOB)) <= 2048),
    partitionkey TEXT CHECK (partitionkey IS NULL OR length(CAST(partitionkey AS BLOB)) <= 255),
    extensions TEXT NOT NULL DEFAULT '{}',
    data_kind TEXT CHECK (data_kind IS NULL OR data_kind IN ('json', 'binary')),
    data BLOB, enqueued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')),
    CHECK ((data_kind IS NULL) = (data IS NULL)),
    CHECK (length(CAST(stream AS BLOB)) <= 255),
    CHECK (length(CAST(event_id AS BLOB)) <= 1024),
    CHECK (length(CAST(source AS BLOB)) <= 2048),
    CHECK (length(CAST(event_type AS BLOB)) <= 1024),
    CHECK (length(CAST(source AS BLOB)) + length(CAST(event_id AS BLOB)) <= 2048),
    CHECK (data IS NULL OR length(data) = 0 OR datacontenttype IS NOT NULL),
    UNIQUE (tenant_id, row_id)
);

CREATE TABLE dovecote_deliveries_v2 (
    event_row_id INTEGER PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (length(CAST(tenant_id AS BLOB)) > 0 AND length(CAST(tenant_id AS BLOB)) <= 255),
    state TEXT NOT NULL CHECK (state IN ('pending', 'claimed', 'delivered', 'quarantined')),
    available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token BLOB CHECK (claim_token IS NULL OR length(claim_token) = 16),
    claimed_by TEXT, claim_expires_at TEXT, last_failure_code TEXT,
    last_failure_detail TEXT, delivered_at TEXT, quarantined_at TEXT, quarantine_reason TEXT,
    CHECK (claimed_by IS NULL OR length(CAST(claimed_by AS BLOB)) <= 255),
    CHECK (last_failure_code IS NULL OR length(CAST(last_failure_code AS BLOB)) <= 128),
    CHECK (last_failure_detail IS NULL OR length(CAST(last_failure_detail AS BLOB)) <= 2048),
    CHECK (quarantine_reason IS NULL OR length(CAST(quarantine_reason AS BLOB)) <= 2048),
    CHECK ((last_failure_code IS NULL) = (last_failure_detail IS NULL)),
    CHECK (
      (state = 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)
    ),
    FOREIGN KEY (tenant_id, event_row_id) REFERENCES dovecote_events_v2 (tenant_id, row_id) ON DELETE RESTRICT
);

INSERT INTO dovecote_events_v2 (
    row_id, tenant_id, stream, specversion, event_id, source, event_type,
    subject, occurred_at, datacontenttype, dataschema, partitionkey,
    extensions, data_kind, data, enqueued_at
)
SELECT row_id, tenant_id, stream, specversion, event_id, source, event_type,
       subject, occurred_at, datacontenttype, dataschema, partitionkey,
       extensions, data_kind, data, enqueued_at
FROM dovecote_events;

INSERT INTO dovecote_deliveries_v2 (
    event_row_id, tenant_id, state, available_at, attempts, claim_token,
    claimed_by, claim_expires_at, last_failure_code, last_failure_detail,
    delivered_at, quarantined_at, quarantine_reason
)
SELECT event_row_id, tenant_id, state, available_at, attempts, claim_token,
       claimed_by, claim_expires_at, last_failure_code, last_failure_detail,
       delivered_at, quarantined_at, quarantine_reason
FROM dovecote_deliveries;

DROP TABLE dovecote_deliveries;
DROP TABLE dovecote_events;
ALTER TABLE dovecote_events_v2 RENAME TO dovecote_events;
ALTER TABLE dovecote_deliveries_v2 RENAME TO dovecote_deliveries;

DROP INDEX IF EXISTS dovecote_events_source_event_id;
CREATE UNIQUE INDEX dovecote_events_tenant_source_event_id
    ON dovecote_events (tenant_id COLLATE BINARY, source COLLATE BINARY, event_id COLLATE BINARY);
CREATE INDEX dovecote_events_tenant_row
    ON dovecote_events (tenant_id COLLATE BINARY, row_id);
CREATE INDEX dovecote_deliveries_claimable
    ON dovecote_deliveries (tenant_id, state, available_at, event_row_id);
CREATE INDEX dovecote_deliveries_expired_claims
    ON dovecote_deliveries (tenant_id, state, claim_expires_at, event_row_id);

CREATE TABLE dovecote_schema (
    schema_version INTEGER PRIMARY KEY CHECK (schema_version = 2),
    minimum_crate_major INTEGER NOT NULL CHECK (minimum_crate_major >= 0),
    minimum_crate_minor INTEGER NOT NULL CHECK (minimum_crate_minor >= 0),
    minimum_crate_patch INTEGER NOT NULL CHECK (minimum_crate_patch >= 0),
    rolling_compatible INTEGER NOT NULL
);
INSERT INTO dovecote_schema VALUES (2, 0, 2, 0, 0);

DROP TABLE dovecote_tenant_activation_guard;
COMMIT;
