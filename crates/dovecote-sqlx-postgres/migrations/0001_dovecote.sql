-- Schema version 1. This file is a release artifact: apply it explicitly and
-- preserve it unchanged so old rows can always be interpreted against it.

-- This is migration metadata, not a third Dovecote domain table. It is the
-- durable marker checked by the adapter before it uses either domain table.
CREATE TABLE dovecote_schema (
    schema_version INTEGER PRIMARY KEY,
    minimum_crate_major SMALLINT NOT NULL,
    minimum_crate_minor SMALLINT NOT NULL,
    minimum_crate_patch SMALLINT NOT NULL,
    rolling_compatible BOOLEAN NOT NULL,
    CONSTRAINT dovecote_schema_version_supported CHECK (schema_version = 1),
    CONSTRAINT dovecote_schema_minimum_nonnegative CHECK (
        minimum_crate_major >= 0 AND minimum_crate_minor >= 0 AND minimum_crate_patch >= 0
    )
);

INSERT INTO dovecote_schema
    (schema_version, minimum_crate_major, minimum_crate_minor, minimum_crate_patch, rolling_compatible)
VALUES (1, 0, 1, 0, FALSE);

CREATE TABLE dovecote_events (
    row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream VARCHAR(255) NOT NULL,
    specversion VARCHAR(8) NOT NULL,
    event_id VARCHAR(1024) NOT NULL,
    source VARCHAR(2048) NOT NULL,
    event_type VARCHAR(1024) NOT NULL,
    subject VARCHAR(2048),
    occurred_at TIMESTAMPTZ,
    datacontenttype VARCHAR(255),
    dataschema VARCHAR(2048),
    partitionkey VARCHAR(255),
    extensions TEXT NOT NULL DEFAULT '{}',
    data_kind VARCHAR(6),
    data BYTEA,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dovecote_events_row_id_positive CHECK (row_id > 0),
    CONSTRAINT dovecote_events_specversion CHECK (specversion = '1.0'),
    CONSTRAINT dovecote_events_stream_size CHECK (octet_length(stream) <= 255),
    CONSTRAINT dovecote_events_event_id_size CHECK (octet_length(event_id) <= 1024),
    CONSTRAINT dovecote_events_source_size CHECK (octet_length(source) <= 2048),
    CONSTRAINT dovecote_events_event_type_size CHECK (octet_length(event_type) <= 1024),
    CONSTRAINT dovecote_events_subject_size CHECK (subject IS NULL OR octet_length(subject) <= 2048),
    CONSTRAINT dovecote_events_content_type_size CHECK (datacontenttype IS NULL OR octet_length(datacontenttype) <= 255),
    CONSTRAINT dovecote_events_schema_size CHECK (dataschema IS NULL OR octet_length(dataschema) <= 2048),
    CONSTRAINT dovecote_events_partition_size CHECK (partitionkey IS NULL OR octet_length(partitionkey) <= 255),
    CONSTRAINT dovecote_events_identity_size CHECK (octet_length(source) + octet_length(event_id) <= 2048),
    CONSTRAINT dovecote_events_data_kind CHECK (data_kind IS NULL OR data_kind IN ('json', 'binary')),
    CONSTRAINT dovecote_events_data_pair CHECK ((data_kind IS NULL) = (data IS NULL)),
    CONSTRAINT dovecote_events_content_type CHECK (data IS NULL OR octet_length(data) = 0 OR datacontenttype IS NOT NULL)
);

CREATE UNIQUE INDEX dovecote_events_source_event_id
    ON dovecote_events (source COLLATE "C", event_id COLLATE "C");

CREATE TABLE dovecote_deliveries (
    event_row_id BIGINT PRIMARY KEY REFERENCES dovecote_events (row_id) ON DELETE RESTRICT,
    state VARCHAR(12) NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempts BIGINT NOT NULL DEFAULT 0,
    claim_token BYTEA,
    claimed_by VARCHAR(255),
    claim_expires_at TIMESTAMPTZ,
    last_failure_code VARCHAR(128),
    last_failure_detail VARCHAR(2048),
    delivered_at TIMESTAMPTZ,
    quarantined_at TIMESTAMPTZ,
    quarantine_reason VARCHAR(2048),
    CONSTRAINT dovecote_deliveries_state CHECK (state IN ('pending', 'claimed', 'delivered', 'quarantined')),
    CONSTRAINT dovecote_deliveries_attempts CHECK (attempts >= 0),
    CONSTRAINT dovecote_deliveries_token_size CHECK (claim_token IS NULL OR octet_length(claim_token) = 16),
    CONSTRAINT dovecote_deliveries_worker_size CHECK (claimed_by IS NULL OR octet_length(claimed_by) <= 255),
    CONSTRAINT dovecote_deliveries_failure_code_size CHECK (last_failure_code IS NULL OR octet_length(last_failure_code) <= 128),
    CONSTRAINT dovecote_deliveries_failure_detail_size CHECK (last_failure_detail IS NULL OR octet_length(last_failure_detail) <= 2048),
    CONSTRAINT dovecote_deliveries_quarantine_size CHECK (quarantine_reason IS NULL OR octet_length(quarantine_reason) <= 2048),
    CONSTRAINT dovecote_deliveries_failure_pair CHECK ((last_failure_code IS NULL) = (last_failure_detail IS NULL)),
    CONSTRAINT dovecote_deliveries_state_shape CHECK (
        (state = 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)
    )
);

CREATE INDEX dovecote_deliveries_claimable
    ON dovecote_deliveries (state, available_at, event_row_id);

CREATE INDEX dovecote_deliveries_expired_claims
    ON dovecote_deliveries (state, claim_expires_at, event_row_id);
