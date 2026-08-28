-- Schema version 2: clean tenant-aware SQLite installation.
-- Version 1 remains immutable; existing files use explicit backfill and
-- activation rather than receiving a guessed tenant.

PRAGMA foreign_keys = ON;

CREATE TABLE dovecote_schema (
    schema_version INTEGER PRIMARY KEY CHECK (schema_version = 2),
    minimum_crate_major INTEGER NOT NULL CHECK (minimum_crate_major >= 0),
    minimum_crate_minor INTEGER NOT NULL CHECK (minimum_crate_minor >= 0),
    minimum_crate_patch INTEGER NOT NULL CHECK (minimum_crate_patch >= 0),
    rolling_compatible INTEGER NOT NULL
);
INSERT INTO dovecote_schema VALUES (2, 0, 2, 0, 0);

CREATE TABLE dovecote_events (
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
CREATE UNIQUE INDEX dovecote_events_tenant_source_event_id ON dovecote_events (tenant_id COLLATE BINARY, source COLLATE BINARY, event_id COLLATE BINARY);
CREATE INDEX dovecote_events_tenant_row ON dovecote_events (tenant_id COLLATE BINARY, row_id);

CREATE TABLE dovecote_deliveries (
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
    CHECK ((state = 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
      OR (state = 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)),
    FOREIGN KEY (tenant_id, event_row_id) REFERENCES dovecote_events (tenant_id, row_id) ON DELETE RESTRICT
);
CREATE INDEX dovecote_deliveries_claimable ON dovecote_deliveries (tenant_id, state, available_at, event_row_id);
CREATE INDEX dovecote_deliveries_expired_claims ON dovecote_deliveries (tenant_id, state, claim_expires_at, event_row_id);
