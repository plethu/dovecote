-- Schema version 1. This file is a release artifact: apply it explicitly and
-- preserve it unchanged so old rows can always be interpreted against it.
-- Source and event_id are UTF-8 bytes in binary columns so the full identity
-- key remains indexable under MySQL and MariaDB byte limits.

CREATE TABLE dovecote_events (
    row_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    stream VARBINARY(255) NOT NULL,
    specversion VARBINARY(8) NOT NULL,
    event_id VARBINARY(1024) NOT NULL,
    source VARBINARY(2048) NOT NULL,
    event_type VARBINARY(1024) NOT NULL,
    subject VARBINARY(2048),
    occurred_at DATETIME(6),
    datacontenttype VARBINARY(255),
    dataschema VARBINARY(2048),
    partitionkey VARBINARY(255),
    extensions LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    data_kind VARBINARY(6),
    data LONGBLOB,
    enqueued_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT dovecote_events_specversion CHECK (specversion = _binary '1.0'),
    CONSTRAINT dovecote_events_stream_size CHECK (OCTET_LENGTH(stream) <= 255),
    CONSTRAINT dovecote_events_event_id_size CHECK (OCTET_LENGTH(event_id) <= 1024),
    CONSTRAINT dovecote_events_source_size CHECK (OCTET_LENGTH(source) <= 2048),
    CONSTRAINT dovecote_events_event_type_size CHECK (OCTET_LENGTH(event_type) <= 1024),
    CONSTRAINT dovecote_events_subject_size CHECK (subject IS NULL OR OCTET_LENGTH(subject) <= 2048),
    CONSTRAINT dovecote_events_content_type_size CHECK (datacontenttype IS NULL OR OCTET_LENGTH(datacontenttype) <= 255),
    CONSTRAINT dovecote_events_schema_size CHECK (dataschema IS NULL OR OCTET_LENGTH(dataschema) <= 2048),
    CONSTRAINT dovecote_events_partition_size CHECK (partitionkey IS NULL OR OCTET_LENGTH(partitionkey) <= 255),
    CONSTRAINT dovecote_events_identity_size CHECK (OCTET_LENGTH(source) + OCTET_LENGTH(event_id) <= 2048),
    CONSTRAINT dovecote_events_data_kind CHECK (data_kind IS NULL OR data_kind IN (_binary 'json', _binary 'binary')),
    CONSTRAINT dovecote_events_data_pair CHECK ((data_kind IS NULL) = (data IS NULL)),
    CONSTRAINT dovecote_events_content_type CHECK (data IS NULL OR OCTET_LENGTH(data) = 0 OR datacontenttype IS NOT NULL),
    UNIQUE KEY dovecote_events_source_event_id (source, event_id)
) ENGINE = InnoDB;

CREATE TRIGGER dovecote_events_row_id_positive_insert
BEFORE INSERT ON dovecote_events
FOR EACH ROW
BEGIN
    IF NEW.row_id < 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'dovecote row_id must be positive';
    END IF;
END;

CREATE TRIGGER dovecote_events_row_id_positive_update
BEFORE UPDATE ON dovecote_events
FOR EACH ROW
BEGIN
    IF NEW.row_id <= 0 OR NEW.row_id <> OLD.row_id THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'dovecote row_id must be positive';
    END IF;
END;

CREATE TABLE dovecote_deliveries (
    event_row_id BIGINT NOT NULL PRIMARY KEY,
    state VARBINARY(12) NOT NULL,
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempts BIGINT NOT NULL DEFAULT 0,
    claim_token BINARY(16),
    claimed_by VARBINARY(255),
    claim_expires_at DATETIME(6),
    last_failure_code VARBINARY(128),
    last_failure_detail VARBINARY(2048),
    delivered_at DATETIME(6),
    quarantined_at DATETIME(6),
    quarantine_reason VARBINARY(2048),
    CONSTRAINT dovecote_deliveries_state CHECK (state IN (_binary 'pending', _binary 'claimed', _binary 'delivered', _binary 'quarantined')),
    CONSTRAINT dovecote_deliveries_attempts CHECK (attempts >= 0),
    CONSTRAINT dovecote_deliveries_token_size CHECK (claim_token IS NULL OR OCTET_LENGTH(claim_token) = 16),
    CONSTRAINT dovecote_deliveries_worker_size CHECK (claimed_by IS NULL OR OCTET_LENGTH(claimed_by) <= 255),
    CONSTRAINT dovecote_deliveries_failure_code_size CHECK (last_failure_code IS NULL OR OCTET_LENGTH(last_failure_code) <= 128),
    CONSTRAINT dovecote_deliveries_failure_detail_size CHECK (last_failure_detail IS NULL OR OCTET_LENGTH(last_failure_detail) <= 2048),
    CONSTRAINT dovecote_deliveries_quarantine_size CHECK (quarantine_reason IS NULL OR OCTET_LENGTH(quarantine_reason) <= 2048),
    CONSTRAINT dovecote_deliveries_failure_pair CHECK ((last_failure_code IS NULL) = (last_failure_detail IS NULL)),
    CONSTRAINT dovecote_deliveries_state_shape CHECK (
        (state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)
    ),
    CONSTRAINT dovecote_deliveries_event_fk FOREIGN KEY (event_row_id) REFERENCES dovecote_events (row_id) ON DELETE RESTRICT
    ,KEY dovecote_deliveries_claimable (state, available_at, event_row_id)
    ,KEY dovecote_deliveries_expired_claims (state, claim_expires_at, event_row_id)
) ENGINE = InnoDB;
