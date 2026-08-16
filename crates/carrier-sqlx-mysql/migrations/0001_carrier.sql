-- Schema version 1. This file is a release artifact: apply it explicitly and
-- preserve it unchanged so old rows can always be interpreted against it.
-- Source and event_id are UTF-8 bytes in binary columns so the full identity
-- key remains indexable under MySQL and MariaDB byte limits.

CREATE TABLE carrier_events (
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
    CONSTRAINT carrier_events_row_id_positive CHECK (row_id > 0),
    CONSTRAINT carrier_events_specversion CHECK (specversion = _binary '1.0'),
    CONSTRAINT carrier_events_stream_size CHECK (OCTET_LENGTH(stream) <= 255),
    CONSTRAINT carrier_events_event_id_size CHECK (OCTET_LENGTH(event_id) <= 1024),
    CONSTRAINT carrier_events_source_size CHECK (OCTET_LENGTH(source) <= 2048),
    CONSTRAINT carrier_events_event_type_size CHECK (OCTET_LENGTH(event_type) <= 1024),
    CONSTRAINT carrier_events_subject_size CHECK (subject IS NULL OR OCTET_LENGTH(subject) <= 2048),
    CONSTRAINT carrier_events_content_type_size CHECK (datacontenttype IS NULL OR OCTET_LENGTH(datacontenttype) <= 255),
    CONSTRAINT carrier_events_schema_size CHECK (dataschema IS NULL OR OCTET_LENGTH(dataschema) <= 2048),
    CONSTRAINT carrier_events_partition_size CHECK (partitionkey IS NULL OR OCTET_LENGTH(partitionkey) <= 255),
    CONSTRAINT carrier_events_identity_size CHECK (OCTET_LENGTH(source) + OCTET_LENGTH(event_id) <= 2048),
    CONSTRAINT carrier_events_data_kind CHECK (data_kind IS NULL OR data_kind IN (_binary 'json', _binary 'binary')),
    CONSTRAINT carrier_events_data_pair CHECK ((data_kind IS NULL) = (data IS NULL)),
    CONSTRAINT carrier_events_content_type CHECK (data IS NULL OR OCTET_LENGTH(data) = 0 OR datacontenttype IS NOT NULL),
    UNIQUE KEY carrier_events_source_event_id (source, event_id)
) ENGINE = InnoDB;

CREATE TABLE carrier_deliveries (
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
    CONSTRAINT carrier_deliveries_state CHECK (state IN (_binary 'pending', _binary 'claimed', _binary 'delivered', _binary 'quarantined')),
    CONSTRAINT carrier_deliveries_attempts CHECK (attempts >= 0),
    CONSTRAINT carrier_deliveries_worker_size CHECK (claimed_by IS NULL OR OCTET_LENGTH(claimed_by) <= 255),
    CONSTRAINT carrier_deliveries_failure_code_size CHECK (last_failure_code IS NULL OR OCTET_LENGTH(last_failure_code) <= 128),
    CONSTRAINT carrier_deliveries_failure_detail_size CHECK (last_failure_detail IS NULL OR OCTET_LENGTH(last_failure_detail) <= 2048),
    CONSTRAINT carrier_deliveries_quarantine_size CHECK (quarantine_reason IS NULL OR OCTET_LENGTH(quarantine_reason) <= 2048),
    CONSTRAINT carrier_deliveries_failure_pair CHECK ((last_failure_code IS NULL) = (last_failure_detail IS NULL)),
    CONSTRAINT carrier_deliveries_state_shape CHECK (
        (state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)
    ),
    CONSTRAINT carrier_deliveries_event_fk FOREIGN KEY (event_row_id) REFERENCES carrier_events (row_id) ON DELETE RESTRICT
) ENGINE = InnoDB;

CREATE INDEX carrier_deliveries_claimable
    ON carrier_deliveries (state, available_at, event_row_id);

CREATE INDEX carrier_deliveries_expired_claims
    ON carrier_deliveries (state, claim_expires_at, event_row_id);
