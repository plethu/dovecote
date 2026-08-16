-- Schema version 1. This file is a release artifact: apply it explicitly and
-- preserve it unchanged so old rows can always be interpreted against it.

PRAGMA foreign_keys = ON;

CREATE TABLE carrier_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (row_id > 0),
    stream TEXT NOT NULL,
    specversion TEXT NOT NULL CHECK (specversion = '1.0'),
    event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    subject TEXT,
    occurred_at TEXT,
    datacontenttype TEXT,
    dataschema TEXT,
    partitionkey TEXT,
    extensions TEXT NOT NULL DEFAULT '{}',
    data_kind TEXT CHECK (data_kind IS NULL OR data_kind IN ('json', 'binary')),
    data BLOB,
    enqueued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(CAST(stream AS BLOB)) <= 255),
    CHECK (length(CAST(event_id AS BLOB)) <= 1024),
    CHECK (length(CAST(source AS BLOB)) <= 2048),
    CHECK (length(CAST(event_type AS BLOB)) <= 1024),
    CHECK (subject IS NULL OR length(CAST(subject AS BLOB)) <= 2048),
    CHECK (datacontenttype IS NULL OR length(CAST(datacontenttype AS BLOB)) <= 255),
    CHECK (dataschema IS NULL OR length(CAST(dataschema AS BLOB)) <= 2048),
    CHECK (partitionkey IS NULL OR length(CAST(partitionkey AS BLOB)) <= 255),
    CHECK (length(CAST(source AS BLOB)) + length(CAST(event_id AS BLOB)) <= 2048),
    CHECK ((data_kind IS NULL) = (data IS NULL)),
    CHECK (data IS NULL OR length(data) = 0 OR datacontenttype IS NOT NULL),
    UNIQUE (source COLLATE BINARY, event_id COLLATE BINARY)
);

CREATE TABLE carrier_deliveries (
    event_row_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('pending', 'claimed', 'delivered', 'quarantined')),
    available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token BLOB CHECK (claim_token IS NULL OR length(claim_token) = 16),
    claimed_by TEXT,
    claim_expires_at TEXT,
    last_failure_code TEXT,
    last_failure_detail TEXT,
    delivered_at TEXT,
    quarantined_at TEXT,
    quarantine_reason TEXT,
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
    FOREIGN KEY (event_row_id) REFERENCES carrier_events (row_id) ON DELETE RESTRICT
);

CREATE INDEX carrier_deliveries_claimable
    ON carrier_deliveries (state, available_at, event_row_id);

CREATE INDEX carrier_deliveries_expired_claims
    ON carrier_deliveries (state, claim_expires_at, event_row_id);
