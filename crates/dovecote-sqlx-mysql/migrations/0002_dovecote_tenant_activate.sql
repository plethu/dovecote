-- Activate tenant invariants after an operator-owned, complete backfill.
--
-- MySQL and MariaDB implicitly commit most DDL.  The temporary guard is
-- therefore deliberately the first durable-schema operation: a rejected
-- backfill cannot leave a partially activated schema.  The DDL below is
-- conditional and ordered so that an interrupted activation can be rerun.
-- `PREPARE` is used instead of vendor-specific `IF NOT EXISTS` ALTER syntax;
-- this artifact is shared by MySQL 8.4/26.7 and MariaDB 11.8.

DROP TEMPORARY TABLE IF EXISTS dovecote_tenant_activation_guard;
DROP TEMPORARY TABLE IF EXISTS dovecote_tenant_activation_catalog_checks;
DROP TEMPORARY TABLE IF EXISTS dovecote_tenant_activation_checks;
DROP TEMPORARY TABLE IF EXISTS dovecote_tenant_activation_statistics;
CREATE TEMPORARY TABLE dovecote_tenant_activation_guard (
    valid TINYINT NOT NULL CHECK (valid = 1)
);

-- MariaDB can lose information-schema column names while resolving nested
-- aggregate/derived queries.  Project the complete statistics contract once
-- into a connection-local relation and use only these explicit fields below.
CREATE TEMPORARY TABLE dovecote_tenant_activation_statistics (
    table_name VARCHAR(64) NOT NULL,
    index_name VARCHAR(64) NOT NULL,
    non_unique TINYINT NOT NULL,
    seq_in_index INT NOT NULL,
    column_name VARCHAR(64),
    index_type VARCHAR(16) NOT NULL,
    sub_part BIGINT
);
INSERT INTO dovecote_tenant_activation_statistics
    (table_name, index_name, non_unique, seq_in_index, column_name, index_type, sub_part)
SELECT stats_source.TABLE_NAME, stats_source.INDEX_NAME, stats_source.NON_UNIQUE,
    stats_source.SEQ_IN_INDEX, stats_source.COLUMN_NAME, stats_source.INDEX_TYPE,
    stats_source.SUB_PART
FROM information_schema.STATISTICS AS stats_source
WHERE stats_source.TABLE_SCHEMA = DATABASE()
  AND stats_source.TABLE_NAME IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries');

-- Keep the catalog contract in one temporary relation.  The server rewrites
-- CHECK_CLAUSE (quoting identifiers, and in some releases grouping boolean
-- operands), so each row contains the canonical normalized clause and the
-- complete forms emitted by the supported MySQL-family servers.  Required
-- rows are ordinary v1 checks and marker checks; tenant checks are optional
-- while the activation DDL is being replayed.
CREATE TEMPORARY TABLE dovecote_tenant_activation_checks (
    table_name VARCHAR(64) NOT NULL,
    constraint_name VARCHAR(64) NOT NULL,
    required TINYINT NOT NULL,
    expected_clause VARCHAR(2048) NOT NULL
);
INSERT INTO dovecote_tenant_activation_checks
    (table_name, constraint_name, required, expected_clause)
VALUES
    ('dovecote_events', 'dovecote_events_specversion', 1, 'specversion=''1.0'''),
    ('dovecote_events', 'dovecote_events_tenant_size', 0, 'octet_length(tenant_id)<=255'),
    ('dovecote_events', 'dovecote_events_tenant_nonempty', 0, 'octet_length(tenant_id)>0'),
    ('dovecote_events', 'dovecote_events_stream_size', 1, 'octet_length(stream)<=255'),
    ('dovecote_events', 'dovecote_events_event_id_size', 1, 'octet_length(event_id)<=1024'),
    ('dovecote_events', 'dovecote_events_source_size', 1, 'octet_length(source)<=2048'),
    ('dovecote_events', 'dovecote_events_event_type_size', 1, 'octet_length(event_type)<=1024'),
    ('dovecote_events', 'dovecote_events_subject_size', 1, 'subjectisnulloroctet_length(subject)<=2048'),
    ('dovecote_events', 'dovecote_events_content_type_size', 1, 'datacontenttypeisnulloroctet_length(datacontenttype)<=255'),
    ('dovecote_events', 'dovecote_events_schema_size', 1, 'dataschemaisnulloroctet_length(dataschema)<=2048'),
    ('dovecote_events', 'dovecote_events_partition_size', 1, 'partitionkeyisnulloroctet_length(partitionkey)<=255'),
    ('dovecote_events', 'dovecote_events_identity_size', 1, 'octet_length(source)+octet_length(event_id)<=2048'),
    ('dovecote_events', 'dovecote_events_data_kind', 1, 'data_kindisnullordata_kindin(''json'',''binary'')'),
    ('dovecote_events', 'dovecote_events_data_pair', 1, '(data_kindisnull)=(dataisnull)'),
    ('dovecote_events', 'dovecote_events_content_type', 1, 'dataisnulloroctet_length(data)=0ordatacontenttypeisnotnull'),
    ('dovecote_deliveries', 'dovecote_deliveries_state', 1, 'statein(''pending'',''claimed'',''delivered'',''quarantined'')'),
    ('dovecote_deliveries', 'dovecote_deliveries_tenant_size', 0, 'octet_length(tenant_id)<=255'),
    ('dovecote_deliveries', 'dovecote_deliveries_tenant_nonempty', 0, 'octet_length(tenant_id)>0'),
    ('dovecote_deliveries', 'dovecote_deliveries_attempts', 1, 'attempts>=0'),
    ('dovecote_deliveries', 'dovecote_deliveries_token_size', 1, 'claim_tokenisnulloroctet_length(claim_token)=16'),
    ('dovecote_deliveries', 'dovecote_deliveries_worker_size', 1, 'claimed_byisnulloroctet_length(claimed_by)<=255'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_code_size', 1, 'last_failure_codeisnulloroctet_length(last_failure_code)<=128'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_detail_size', 1, 'last_failure_detailisnulloroctet_length(last_failure_detail)<=2048'),
    ('dovecote_deliveries', 'dovecote_deliveries_quarantine_size', 1, 'quarantine_reasonisnulloroctet_length(quarantine_reason)<=2048'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_pair', 1, '(last_failure_codeisnull)=(last_failure_detailisnull)'),
    ('dovecote_deliveries', 'dovecote_deliveries_state_shape', 1, '(state=''pending''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnullandquarantined_atisnullandquarantine_reasonisnull)or(state=''claimed''andclaim_tokenisnotnullandclaimed_byisnotnullandclaim_expires_atisnotnullanddelivered_atisnullandquarantined_atisnullandquarantine_reasonisnull)or(state=''delivered''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnotnullandquarantined_atisnullandquarantine_reasonisnull)or(state=''quarantined''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnullandquarantined_atisnotnullandquarantine_reasonisnotnull)'),
    ('dovecote_schema', 'dovecote_schema_version_supported', 1, 'schema_version=2'),
    ('dovecote_schema', 'dovecote_schema_minimum_nonnegative', 1, 'minimum_crate_major>=0andminimum_crate_minor>=0andminimum_crate_patch>=0');

-- MySQL 8.4 and MariaDB may group nullable predicates, remove redundant
-- outer parentheses, or omit the parentheses around the delivery state
-- alternatives.  These are complete alternatives, never fragments.
INSERT INTO dovecote_tenant_activation_checks
    (table_name, constraint_name, required, expected_clause)
VALUES
    ('dovecote_events', 'dovecote_events_subject_size', 0, '(subjectisnull)or(octet_length(subject)<=2048)'),
    ('dovecote_events', 'dovecote_events_content_type_size', 0, '(datacontenttypeisnull)or(octet_length(datacontenttype)<=255)'),
    ('dovecote_events', 'dovecote_events_schema_size', 0, '(dataschemaisnull)or(octet_length(dataschema)<=2048)'),
    ('dovecote_events', 'dovecote_events_partition_size', 0, '(partitionkeyisnull)or(octet_length(partitionkey)<=255)'),
    ('dovecote_events', 'dovecote_events_identity_size', 0, '(octet_length(source)+octet_length(event_id))<=2048'),
    ('dovecote_events', 'dovecote_events_data_kind', 0, '(data_kindisnull)or(data_kindin(''json'',''binary''))'),
    ('dovecote_events', 'dovecote_events_data_pair', 0, 'data_kindisnull=(dataisnull)'),
    ('dovecote_events', 'dovecote_events_content_type', 0, '(dataisnull)or(octet_length(data)=0)or(datacontenttypeisnotnull)'),
    ('dovecote_deliveries', 'dovecote_deliveries_token_size', 0, '(claim_tokenisnull)or(octet_length(claim_token)=16)'),
    ('dovecote_deliveries', 'dovecote_deliveries_worker_size', 0, '(claimed_byisnull)or(octet_length(claimed_by)<=255)'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_code_size', 0, '(last_failure_codeisnull)or(octet_length(last_failure_code)<=128)'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_detail_size', 0, '(last_failure_detailisnull)or(octet_length(last_failure_detail)<=2048)'),
    ('dovecote_deliveries', 'dovecote_deliveries_quarantine_size', 0, '(quarantine_reasonisnull)or(octet_length(quarantine_reason)<=2048)'),
    ('dovecote_deliveries', 'dovecote_deliveries_failure_pair', 0, 'last_failure_codeisnull=(last_failure_detailisnull)'),
    ('dovecote_deliveries', 'dovecote_deliveries_state_shape', 0, 'state=''pending''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnullandquarantined_atisnullandquarantine_reasonisnullorstate=''claimed''andclaim_tokenisnotnullandclaimed_byisnotnullandclaim_expires_atisnotnullanddelivered_atisnullandquarantined_atisnullandquarantine_reasonisnullorstate=''delivered''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnotnullandquarantined_atisnullandquarantine_reasonisnullorstate=''quarantined''andclaim_tokenisnullandclaimed_byisnullandclaim_expires_atisnullanddelivered_atisnullandquarantined_atisnotnullandquarantine_reasonisnotnull'),
    ('dovecote_deliveries', 'dovecote_deliveries_state_shape', 0, '((state=''pending'')and(claim_tokenisnull)and(claimed_byisnull)and(claim_expires_atisnull)and(delivered_atisnull)and(quarantined_atisnull)and(quarantine_reasonisnull))or((state=''claimed'')and(claim_tokenisnotnull)and(claimed_byisnotnull)and(claim_expires_atisnotnull)and(delivered_atisnull)and(quarantined_atisnull)and(quarantine_reasonisnull))or((state=''delivered'')and(claim_tokenisnull)and(claimed_byisnull)and(claim_expires_atisnull)and(delivered_atisnotnull)and(quarantined_atisnull)and(quarantine_reasonisnull))or((state=''quarantined'')and(claim_tokenisnull)and(claimed_byisnull)and(claim_expires_atisnull)and(delivered_atisnull)and(quarantined_atisnotnull)and(quarantine_reasonisnotnull))'),
    ('dovecote_schema', 'dovecote_schema_minimum_nonnegative', 0, '(minimum_crate_major>=0)and(minimum_crate_minor>=0)and(minimum_crate_patch>=0)');

CREATE TEMPORARY TABLE dovecote_tenant_activation_catalog_checks (
    table_name VARCHAR(64) NOT NULL,
    constraint_name VARCHAR(64) NOT NULL,
    normalized_clause VARCHAR(2048) NOT NULL
);
INSERT INTO dovecote_tenant_activation_catalog_checks
    (table_name, constraint_name, normalized_clause)
SELECT tc.table_name, tc.constraint_name,
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        cc.check_clause, '`', ''), CHAR(92), ''), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''),
                        '_binary', '')),
                    '_utf8mb4', ''),
                'octet_length(', '__dovecote_octet__('),
            'length(', 'octet_length('),
        '__dovecote_octet__(', 'octet_length(')
FROM information_schema.table_constraints AS tc
JOIN information_schema.check_constraints AS cc
  ON cc.constraint_schema = tc.constraint_schema
 AND cc.constraint_name = tc.constraint_name
WHERE tc.constraint_schema = DATABASE()
  AND tc.table_name IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries')
  AND tc.constraint_type = 'CHECK';

-- A marker table is created only by the final activation step.  If a prior
-- attempt already created it, validate its complete catalog and row before
-- touching either domain table.  The dynamic statement avoids referencing an
-- absent table during statement preparation.
SET @dovecote_marker_present = EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'dovecote_schema'
);
SET @dovecote_marker_statistics_valid = (
    SELECT COUNT(*) = 1
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.seq_in_index = 1
           AND stats.column_name = 'schema_version' AND stats.index_type = 'BTREE'
           AND stats.sub_part IS NULL
       ) = 1
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_schema'
);
SET @dovecote_marker_checks_valid = (
    SELECT COUNT(*) = 2 AND SUM(marker_check.matched) = 2
    FROM (
        SELECT expected.constraint_name,
            MAX(
                CASE WHEN actual.normalized_clause IN (
                    expected.expected_clause,
                    CONCAT('(', expected.expected_clause, ')')
                ) THEN 1 ELSE 0 END
            ) AS matched
        FROM dovecote_tenant_activation_checks AS expected
        LEFT JOIN dovecote_tenant_activation_catalog_checks AS actual
          ON actual.table_name = expected.table_name
         AND actual.constraint_name = expected.constraint_name
        WHERE expected.table_name = 'dovecote_schema'
        GROUP BY expected.constraint_name
        HAVING MAX(expected.required) = 1
    ) AS marker_check
);
SET @dovecote_marker_catalog_valid = IF(
    @dovecote_marker_present = 0,
    1,
    (
        SELECT COUNT(*) = 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_schema'
          AND table_type = 'BASE TABLE' AND engine = 'InnoDB'
    )
    AND (
        SELECT COUNT(*) = 5
            AND SUM(column_name = 'schema_version' AND ordinal_position = 1 AND data_type = 'int'
                    AND LOWER(column_type) NOT LIKE '%unsigned%'
                    AND is_nullable = 'NO' AND column_default IS NULL AND extra = '') = 1
            AND SUM(column_name = 'minimum_crate_major' AND ordinal_position = 2 AND data_type = 'smallint'
                    AND LOWER(column_type) NOT LIKE '%unsigned%'
                    AND is_nullable = 'NO' AND column_default IS NULL AND extra = '') = 1
            AND SUM(column_name = 'minimum_crate_minor' AND ordinal_position = 3 AND data_type = 'smallint'
                    AND LOWER(column_type) NOT LIKE '%unsigned%'
                    AND is_nullable = 'NO' AND column_default IS NULL AND extra = '') = 1
            AND SUM(column_name = 'minimum_crate_patch' AND ordinal_position = 4 AND data_type = 'smallint'
                    AND LOWER(column_type) NOT LIKE '%unsigned%'
                    AND is_nullable = 'NO' AND column_default IS NULL AND extra = '') = 1
            AND SUM(column_name = 'rolling_compatible' AND ordinal_position = 5 AND data_type = 'tinyint'
                    AND column_type = 'tinyint(1)' AND is_nullable = 'NO'
                    AND column_default IS NULL AND extra = '') = 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_schema'
    )
    AND (
        SELECT COUNT(*) = 3
            AND SUM(constraint_name = 'PRIMARY' AND constraint_type = 'PRIMARY KEY') = 1
            AND SUM(constraint_name = 'dovecote_schema_version_supported'
                    AND constraint_type = 'CHECK') = 1
            AND SUM(constraint_name = 'dovecote_schema_minimum_nonnegative'
                    AND constraint_type = 'CHECK') = 1
        FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_schema'
    )
    AND @dovecote_marker_statistics_valid = 1
    AND (
        SELECT COUNT(*) = 1
        FROM information_schema.key_column_usage
        WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_schema'
          AND constraint_name = 'PRIMARY' AND column_name = 'schema_version'
          AND ordinal_position = 1
    )
    AND @dovecote_marker_checks_valid = 1
);
SET @dovecote_marker_data_sql = IF(
    @dovecote_marker_present = 0,
    'SELECT 1 INTO @dovecote_marker_data_valid',
    'SELECT IF(COUNT(*) = 0 OR (COUNT(*) = 1 AND MIN(schema_version) = 2 AND MIN(minimum_crate_major) = 0 AND MIN(minimum_crate_minor) = 2 AND MIN(minimum_crate_patch) = 0 AND MIN(rolling_compatible) = 0), 1, 0) INTO @dovecote_marker_data_valid FROM dovecote_schema'
);
PREPARE dovecote_marker_data_statement FROM @dovecote_marker_data_sql;
EXECUTE dovecote_marker_data_statement;
DEALLOCATE PREPARE dovecote_marker_data_statement;

-- MySQL and MariaDB expose generated expressions with catalog quoting and
-- decoration.  This is the exact normalized expression expected below; the
-- length prefixes make the encoding injective even for direct SQL writers.
SET @dovecote_identity_generation_expression =
    'concat(lpad(octet_length(tenant_id),3,''0''),tenant_id,lpad(octet_length(source),4,''0''),source,event_id)';
SET @dovecote_identity_generation_actual = (
    SELECT REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                LOWER(REPLACE(REPLACE(REPLACE(REPLACE(
                                        generation_expression, '`', ''), CHAR(92), ''), ' ', ''),
                                        '_binary', '')),
                                    '_utf8mb4', ''),
                                'octet_length(tenant_id)', '__dovecote_tenant_length__'),
                            'length(tenant_id)', '__dovecote_tenant_length__'),
                        'octet_length(source)', '__dovecote_source_length__'),
                    'length(source)', '__dovecote_source_length__'),
                '__dovecote_tenant_length__', 'octet_length(tenant_id)'),
            '__dovecote_source_length__', 'octet_length(source)')
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
      AND column_name = 'identity_key'
);

-- Compute the complete catalog proof before any durable DDL.  Every variable is
-- deliberately a closed predicate: a missing row, extra object, or malformed
-- target yields zero rather than being silently treated as an old deployment.
SET @dovecote_tables_valid = (
    (SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ('dovecote_events', 'dovecote_deliveries')) = 2
    AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ('dovecote_events', 'dovecote_deliveries')
          AND (table_type <> 'BASE TABLE' OR engine <> 'InnoDB')
    )
);

SET @dovecote_identity_column_valid = (
    SELECT COUNT(*) = 1
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
      AND column_name = 'identity_key'
      AND data_type = 'varbinary' AND character_maximum_length = 2310
      AND column_type = 'varbinary(2310)' AND is_nullable = 'YES'
      AND (column_default IS NULL OR LOWER(column_default) = 'null')
      AND extra = 'STORED GENERATED'
      AND @dovecote_identity_generation_actual = @dovecote_identity_generation_expression
      AND generation_expression IS NOT NULL
);

SET @dovecote_events_columns_valid = (
    (
        (
        (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'dovecote_events') = 16
        AND @dovecote_identity_column_valid = 0
        )
        OR (
        (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = 'dovecote_events') = 17
        AND @dovecote_identity_column_valid = 1
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS c
        WHERE c.table_schema = DATABASE() AND c.table_name = 'dovecote_events'
          AND NOT (
              (c.column_name = 'row_id' AND c.data_type = 'bigint'
               AND LOWER(c.column_type) NOT LIKE '%unsigned%' AND c.is_nullable = 'NO'
               AND c.column_default IS NULL AND LOWER(c.extra) = 'auto_increment'
               AND (c.generation_expression IS NULL OR TRIM(c.generation_expression) = ''))
              OR (c.column_name = 'tenant_id' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND ((c.is_nullable = 'NO' AND c.column_default IS NULL)
                    OR (c.is_nullable = 'YES'
                        AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null')))
               AND c.extra = '' AND (c.generation_expression IS NULL OR TRIM(c.generation_expression) = ''))
              OR (c.column_name = 'stream' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'specversion' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(8)' AND c.character_maximum_length = 8
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'event_id' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(1024)' AND c.character_maximum_length = 1024
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'source' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(2048)' AND c.character_maximum_length = 2048
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'event_type' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(1024)' AND c.character_maximum_length = 1024
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'subject' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(2048)' AND c.character_maximum_length = 2048
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'occurred_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'datacontenttype' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'dataschema' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(2048)' AND c.character_maximum_length = 2048
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'partitionkey' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'extensions' AND c.data_type = 'longtext'
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'data_kind' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(6)' AND c.character_maximum_length = 6
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'data' AND c.data_type = 'longblob'
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'enqueued_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'NO'
               AND LOWER(c.column_default) = 'current_timestamp(6)'
               AND LOWER(c.extra) IN ('', 'default_generated'))
              OR (c.column_name = 'identity_key' AND @dovecote_identity_column_valid = 1)
          )
    )
    AND (
        SELECT COUNT(*) = 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
          AND column_name = 'extensions' AND character_set_name = 'utf8mb4'
          AND collation_name = 'utf8mb4_bin'
    )
);

SET @dovecote_deliveries_columns_valid = (
    (SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries') = 13
    AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS c
        WHERE c.table_schema = DATABASE() AND c.table_name = 'dovecote_deliveries'
          AND NOT (
              (c.column_name = 'event_row_id' AND c.data_type = 'bigint'
               AND LOWER(c.column_type) NOT LIKE '%unsigned%' AND c.is_nullable = 'NO'
               AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'tenant_id' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND ((c.is_nullable = 'NO' AND c.column_default IS NULL)
                    OR (c.is_nullable = 'YES'
                        AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null')))
               AND c.extra = '')
              OR (c.column_name = 'state' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(12)' AND c.character_maximum_length = 12
               AND c.is_nullable = 'NO' AND c.column_default IS NULL AND c.extra = '')
              OR (c.column_name = 'available_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'NO'
               AND LOWER(c.column_default) = 'current_timestamp(6)'
               AND LOWER(c.extra) IN ('', 'default_generated'))
              OR (c.column_name = 'attempts' AND c.data_type = 'bigint'
               AND LOWER(c.column_type) NOT LIKE '%unsigned%' AND c.is_nullable = 'NO'
               AND c.column_default = '0' AND c.extra = '')
              OR (c.column_name = 'claim_token' AND c.data_type = 'binary'
               AND c.column_type = 'binary(16)' AND c.character_maximum_length = 16
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'claimed_by' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(255)' AND c.character_maximum_length = 255
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'claim_expires_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'last_failure_code' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(128)' AND c.character_maximum_length = 128
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'last_failure_detail' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(2048)' AND c.character_maximum_length = 2048
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'delivered_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'quarantined_at' AND c.data_type = 'datetime'
               AND c.column_type = 'datetime(6)' AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
              OR (c.column_name = 'quarantine_reason' AND c.data_type = 'varbinary'
               AND c.column_type = 'varbinary(2048)' AND c.character_maximum_length = 2048
               AND c.is_nullable = 'YES'
               AND (c.column_default IS NULL OR LOWER(c.column_default) = 'null') AND c.extra = '')
          )
    )
);

SET @dovecote_events_tenant_target_valid = (
    SELECT COUNT(*) = 1
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
      AND column_name = 'tenant_id' AND data_type = 'varbinary'
      AND column_type = 'varbinary(255)' AND character_maximum_length = 255
      AND is_nullable = 'NO' AND column_default IS NULL AND extra = ''
      AND (generation_expression IS NULL OR TRIM(generation_expression) = '')
);
SET @dovecote_deliveries_tenant_target_valid = (
    SELECT COUNT(*) = 1
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries'
      AND column_name = 'tenant_id' AND data_type = 'varbinary'
      AND column_type = 'varbinary(255)' AND character_maximum_length = 255
      AND is_nullable = 'NO' AND column_default IS NULL AND extra = ''
      AND (generation_expression IS NULL OR TRIM(generation_expression) = '')
);
SET @dovecote_tenant_columns_target_valid = (
    @dovecote_events_tenant_target_valid = 1
    AND @dovecote_deliveries_tenant_target_valid = 1
);

-- Every statement below reads each temporary relation at most once.  MySQL
-- rejects a statement that reopens the same temporary table through nested
-- subqueries, while MariaDB accepts that form.  The required aggregate checks
-- prove the complete ordinary v1 CHECK set; the two independent predicates
-- reject unknown constraints and known constraints with wrong semantics.
SET @dovecote_required_checks_valid = (
    SELECT COUNT(*) = 22 AND SUM(required_check.matched) = 22
    FROM (
        SELECT expected.table_name, expected.constraint_name,
            MAX(
                CASE WHEN actual.normalized_clause IN (
                    expected.expected_clause,
                    CONCAT('(', expected.expected_clause, ')')
                ) THEN 1 ELSE 0 END
            ) AS matched
        FROM dovecote_tenant_activation_checks AS expected
        LEFT JOIN dovecote_tenant_activation_catalog_checks AS actual
          ON actual.table_name = expected.table_name
         AND actual.constraint_name = expected.constraint_name
        WHERE expected.table_name IN ('dovecote_events', 'dovecote_deliveries')
        GROUP BY expected.table_name, expected.constraint_name
        HAVING MAX(expected.required) = 1
    ) AS required_check
);
SET @dovecote_unexpected_checks_valid = NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints AS tc
    WHERE tc.constraint_schema = DATABASE()
      AND tc.table_name IN ('dovecote_events', 'dovecote_deliveries')
      AND NOT (
          (tc.table_name = 'dovecote_events' AND tc.constraint_name = 'PRIMARY' AND tc.constraint_type = 'PRIMARY KEY')
          OR (tc.table_name = 'dovecote_events' AND tc.constraint_name IN ('dovecote_events_source_event_id', 'dovecote_events_tenant_source_event_id', 'dovecote_events_tenant_row_unique') AND tc.constraint_type = 'UNIQUE')
          OR (tc.table_name = 'dovecote_deliveries' AND tc.constraint_name = 'PRIMARY' AND tc.constraint_type = 'PRIMARY KEY')
          OR (tc.table_name = 'dovecote_deliveries' AND tc.constraint_name = 'dovecote_deliveries_event_fk' AND tc.constraint_type = 'FOREIGN KEY')
          OR (tc.constraint_type = 'CHECK' AND EXISTS (
              SELECT 1 FROM dovecote_tenant_activation_checks AS expected
              WHERE expected.table_name = tc.table_name
                AND expected.constraint_name = tc.constraint_name
          ))
      )
);
SET @dovecote_check_shapes_valid = NOT EXISTS (
    SELECT actual.table_name, actual.constraint_name
    FROM dovecote_tenant_activation_catalog_checks AS actual
    LEFT JOIN dovecote_tenant_activation_checks AS expected
      ON expected.table_name = actual.table_name
     AND expected.constraint_name = actual.constraint_name
    WHERE actual.table_name IN ('dovecote_events', 'dovecote_deliveries')
    GROUP BY actual.table_name, actual.constraint_name, actual.normalized_clause
    HAVING MAX(
        CASE WHEN actual.normalized_clause IN (
            expected.expected_clause,
            CONCAT('(', expected.expected_clause, ')')
        ) THEN 1 ELSE 0 END
    ) = 0
);
SET @dovecote_checks_valid = (
    @dovecote_required_checks_valid = 1
    AND @dovecote_unexpected_checks_valid = 1
    AND @dovecote_check_shapes_valid = 1
);

SET @dovecote_triggers_valid = (
    (SELECT COUNT(*) FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND event_object_table IN ('dovecote_events', 'dovecote_deliveries')) = 2
    AND NOT EXISTS (
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND event_object_table IN ('dovecote_events', 'dovecote_deliveries')
          AND NOT (
              (trigger_name = 'dovecote_events_row_id_positive_insert'
               AND event_manipulation = 'INSERT' AND action_timing = 'BEFORE'
               AND event_object_table = 'dovecote_events'
               AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(action_statement, '`', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')) = 'beginifnew.row_id<0thensignalsqlstate''45000''setmessage_text=''dovecoterow_idmustbepositive'';endif;end')
              OR (trigger_name = 'dovecote_events_row_id_positive_update'
               AND event_manipulation = 'UPDATE' AND action_timing = 'BEFORE'
               AND event_object_table = 'dovecote_events'
               AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(action_statement, '`', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '')) = 'beginifnew.row_id<=0ornew.row_id<>old.row_idthensignalsqlstate''45000''setmessage_text=''dovecoterow_idmustbepositive'';endif;end')
          )
    )
);

SET @dovecote_events_pk_valid = (
    SELECT COUNT(*) = 1
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.seq_in_index = 1
           AND stats.column_name = 'row_id' AND stats.index_type = 'BTREE'
           AND stats.sub_part IS NULL
       ) = 1
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'PRIMARY'
);
SET @dovecote_deliveries_pk_valid = (
    SELECT COUNT(*) = 1
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.seq_in_index = 1
           AND stats.column_name = 'event_row_id' AND stats.index_type = 'BTREE'
           AND stats.sub_part IS NULL
       ) = 1
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'PRIMARY'
);

SET @dovecote_old_identity_valid = (
    SELECT COUNT(*) = 2
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'source')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'event_id'))
       ) = 2
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_source_event_id'
);
SET @dovecote_target_identity_valid = (
    SELECT COUNT(*) = 1
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND stats.seq_in_index = 1 AND stats.column_name = 'identity_key'
       ) = 1
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_tenant_source_event_id'
);
SET @dovecote_old_identity_present = EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_source_event_id'
);
SET @dovecote_target_identity_present = EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_tenant_source_event_id'
);
SET @dovecote_old_identity_shape_valid = (
    @dovecote_old_identity_present = 0 OR @dovecote_old_identity_valid = 1
);
SET @dovecote_target_identity_shape_valid = (
    @dovecote_target_identity_present = 0 OR @dovecote_target_identity_valid = 1
);
SET @dovecote_tenant_row_target_valid = (
    SELECT COUNT(*) = 2
       AND SUM(
           stats.NON_UNIQUE = 0 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'tenant_id')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'row_id'))
       ) = 2
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_tenant_row_unique'
);
SET @dovecote_tenant_row_present = (
    SELECT COUNT(*) > 0
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_events'
      AND stats.index_name = 'dovecote_events_tenant_row_unique'
);
SET @dovecote_tenant_row_valid = (
    @dovecote_tenant_row_present = 0 OR @dovecote_tenant_row_target_valid = 1
);
SET @dovecote_tenant_checks_valid = (
    SELECT COUNT(*) = 4
    FROM information_schema.table_constraints AS tc
    JOIN dovecote_tenant_activation_catalog_checks AS actual
      ON actual.table_name = tc.table_name AND actual.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE()
      AND tc.table_name IN ('dovecote_events', 'dovecote_deliveries')
      AND tc.constraint_name IN ('dovecote_events_tenant_size', 'dovecote_events_tenant_nonempty', 'dovecote_deliveries_tenant_size', 'dovecote_deliveries_tenant_nonempty')
      AND tc.constraint_type = 'CHECK'
      AND EXISTS (
          SELECT 1 FROM dovecote_tenant_activation_checks AS ec
          WHERE ec.table_name = actual.table_name
            AND ec.constraint_name = actual.constraint_name
                      AND actual.normalized_clause IN (
                          ec.expected_clause,
                          CONCAT('(', ec.expected_clause, ')')
                      )
      )
);
SET @dovecote_identity_prerequisites_valid = (
    @dovecote_tenant_columns_target_valid = 1
    AND @dovecote_tenant_row_target_valid = 1
    AND @dovecote_tenant_checks_valid = 1
);
SET @dovecote_identity_ready = (
    @dovecote_identity_prerequisites_valid = 1
    AND @dovecote_identity_column_valid = 1
    AND @dovecote_target_identity_valid = 1
);

SET @dovecote_fk_target_valid = (
    SELECT COUNT(*) = 2
       AND SUM(
           k.ordinal_position = 1 AND k.column_name = 'tenant_id'
           AND k.referenced_column_name = 'tenant_id'
           OR k.ordinal_position = 2 AND k.column_name = 'event_row_id'
           AND k.referenced_column_name = 'row_id'
       ) = 2
    FROM information_schema.key_column_usage AS k
    JOIN information_schema.table_constraints AS tc
      ON tc.constraint_schema = k.constraint_schema
     AND tc.table_name = k.table_name
     AND tc.constraint_name = k.constraint_name
    JOIN information_schema.referential_constraints AS r
      ON r.constraint_schema = tc.constraint_schema
     AND r.table_name = tc.table_name
     AND r.constraint_name = tc.constraint_name
    WHERE k.constraint_schema = DATABASE() AND k.table_name = 'dovecote_deliveries'
      AND k.constraint_name = 'dovecote_deliveries_event_fk'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND r.delete_rule = 'RESTRICT' AND r.update_rule IN ('RESTRICT', 'NO ACTION')
      AND k.referenced_table_schema = DATABASE() AND k.referenced_table_name = 'dovecote_events'
);
SET @dovecote_fk_old_valid = (
    SELECT COUNT(*) = 1
       AND SUM(
           k.ordinal_position = 1 AND k.column_name = 'event_row_id'
           AND k.referenced_column_name = 'row_id'
       ) = 1
    FROM information_schema.key_column_usage AS k
    JOIN information_schema.table_constraints AS tc
      ON tc.constraint_schema = k.constraint_schema
     AND tc.table_name = k.table_name
     AND tc.constraint_name = k.constraint_name
    JOIN information_schema.referential_constraints AS r
      ON r.constraint_schema = tc.constraint_schema
     AND r.table_name = tc.table_name
     AND r.constraint_name = tc.constraint_name
    WHERE k.constraint_schema = DATABASE() AND k.table_name = 'dovecote_deliveries'
      AND k.constraint_name = 'dovecote_deliveries_event_fk'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND r.delete_rule = 'RESTRICT' AND r.update_rule IN ('RESTRICT', 'NO ACTION')
      AND k.referenced_table_schema = DATABASE() AND k.referenced_table_name = 'dovecote_events'
);
SET @dovecote_fk_index_valid = (
    SELECT COUNT(*) = 2
       AND SUM(
           stats.NON_UNIQUE = 1 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'tenant_id')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'event_row_id'))
       ) = 2
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_event_fk'
);
SET @dovecote_fk_index_present = EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_event_fk'
);
SET @dovecote_fk_index_shape_valid = (
    @dovecote_fk_index_present = 0 OR @dovecote_fk_index_valid = 1
);
SET @dovecote_fk_valid = (
    @dovecote_fk_old_valid = 1 OR @dovecote_fk_target_valid = 1
    OR (@dovecote_identity_ready = 1 AND NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_deliveries'
          AND constraint_name = 'dovecote_deliveries_event_fk'))
);

SET @dovecote_old_claimable_valid = (
    SELECT COUNT(*) = 3
       AND SUM(
           stats.NON_UNIQUE = 1 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'state')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'available_at')
             OR (stats.seq_in_index = 3 AND stats.column_name = 'event_row_id'))
       ) = 3
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_claimable'
);
SET @dovecote_target_claimable_valid = (
    SELECT COUNT(*) = 4
       AND SUM(
           stats.NON_UNIQUE = 1 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'tenant_id')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'state')
             OR (stats.seq_in_index = 3 AND stats.column_name = 'available_at')
             OR (stats.seq_in_index = 4 AND stats.column_name = 'event_row_id'))
       ) = 4
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_claimable'
);
SET @dovecote_old_claimable_present = EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_claimable'
);
SET @dovecote_claimable_shape_valid = (
    @dovecote_old_claimable_present = 0
    OR @dovecote_old_claimable_valid = 1
    OR @dovecote_target_claimable_valid = 1
);
SET @dovecote_old_expired_valid = (
    SELECT COUNT(*) = 3
       AND SUM(
           stats.NON_UNIQUE = 1 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'state')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'claim_expires_at')
             OR (stats.seq_in_index = 3 AND stats.column_name = 'event_row_id'))
       ) = 3
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_expired_claims'
);
SET @dovecote_target_expired_valid = (
    SELECT COUNT(*) = 4
       AND SUM(
           stats.NON_UNIQUE = 1 AND stats.index_type = 'BTREE' AND stats.sub_part IS NULL
           AND ((stats.seq_in_index = 1 AND stats.column_name = 'tenant_id')
             OR (stats.seq_in_index = 2 AND stats.column_name = 'state')
             OR (stats.seq_in_index = 3 AND stats.column_name = 'claim_expires_at')
             OR (stats.seq_in_index = 4 AND stats.column_name = 'event_row_id'))
       ) = 4
    FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_expired_claims'
);
SET @dovecote_old_expired_present = EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name = 'dovecote_deliveries'
      AND stats.index_name = 'dovecote_deliveries_expired_claims'
);
SET @dovecote_expired_shape_valid = (
    @dovecote_old_expired_present = 0
    OR @dovecote_old_expired_valid = 1
    OR @dovecote_target_expired_valid = 1
);
SET @dovecote_unknown_index_valid = NOT EXISTS (
    SELECT 1 FROM dovecote_tenant_activation_statistics AS stats
    WHERE stats.table_name IN ('dovecote_events', 'dovecote_deliveries')
      AND stats.index_name NOT IN ('PRIMARY', 'dovecote_events_source_event_id', 'dovecote_events_tenant_source_event_id', 'dovecote_events_tenant_row_unique', 'dovecote_deliveries_event_fk', 'dovecote_deliveries_claimable', 'dovecote_deliveries_expired_claims')
);
SET @dovecote_indexes_valid = (
    @dovecote_unknown_index_valid = 1
    AND @dovecote_events_pk_valid = 1 AND @dovecote_deliveries_pk_valid = 1
    AND @dovecote_old_identity_shape_valid = 1
    AND @dovecote_target_identity_shape_valid = 1
    AND (@dovecote_old_identity_valid = 1 OR @dovecote_target_identity_valid = 1)
    AND @dovecote_tenant_row_valid = 1
    AND (@dovecote_identity_column_valid = 0 OR @dovecote_identity_prerequisites_valid = 1)
    AND (@dovecote_target_identity_valid = 0 OR @dovecote_identity_column_valid = 1)
    AND @dovecote_fk_index_shape_valid = 1
    AND (
        (@dovecote_fk_old_valid = 1 AND @dovecote_fk_index_present = 0)
        OR (@dovecote_fk_target_valid = 1 AND @dovecote_fk_index_valid = 1)
        OR (@dovecote_identity_ready = 1 AND @dovecote_fk_valid = 1 AND @dovecote_fk_index_present = 0)
    )
    AND (@dovecote_fk_target_valid = 0 OR (@dovecote_identity_ready = 1 AND @dovecote_old_identity_present = 0))
    AND @dovecote_claimable_shape_valid = 1
    AND @dovecote_expired_shape_valid = 1
    AND (@dovecote_old_claimable_valid = 1 OR @dovecote_target_claimable_valid = 1 OR (@dovecote_identity_ready = 1 AND @dovecote_fk_target_valid = 1 AND @dovecote_old_claimable_present = 0))
    AND (@dovecote_old_expired_valid = 1 OR @dovecote_target_expired_valid = 1 OR (@dovecote_target_claimable_valid = 1 AND @dovecote_fk_target_valid = 1 AND @dovecote_old_expired_present = 0))
    AND (@dovecote_target_claimable_valid = 0 OR @dovecote_fk_target_valid = 1)
    AND (@dovecote_target_expired_valid = 0 OR @dovecote_target_claimable_valid = 1)
);

SET @dovecote_marker_state_valid = (
    @dovecote_marker_present = 0
    OR (
        @dovecote_identity_ready = 1
        AND @dovecote_old_identity_present = 0
        AND @dovecote_fk_target_valid = 1
        AND @dovecote_target_claimable_valid = 1
        AND @dovecote_target_expired_valid = 1
    )
);
SET @dovecote_events_tenant_data_valid = NOT EXISTS (
    SELECT 1 FROM dovecote_events
    WHERE tenant_id IS NULL OR OCTET_LENGTH(tenant_id) = 0 OR OCTET_LENGTH(tenant_id) > 255
);
SET @dovecote_deliveries_tenant_data_valid = NOT EXISTS (
    SELECT 1 FROM dovecote_deliveries
    WHERE tenant_id IS NULL OR OCTET_LENGTH(tenant_id) = 0 OR OCTET_LENGTH(tenant_id) > 255
);
SET @dovecote_delivery_event_data_valid = NOT EXISTS (
    SELECT 1
    FROM dovecote_deliveries AS d
    LEFT JOIN dovecote_events AS e ON e.row_id = d.event_row_id
    WHERE e.row_id IS NULL OR d.tenant_id <> e.tenant_id
);
SET @dovecote_identity_data_valid = NOT EXISTS (
    SELECT tenant_id, source, event_id
    FROM dovecote_events
    GROUP BY tenant_id, source, event_id
    HAVING COUNT(*) > 1
);
SET @dovecote_catalog_valid = (
    @dovecote_tables_valid = 1
    AND @dovecote_events_columns_valid = 1
    AND @dovecote_deliveries_columns_valid = 1
    AND @dovecote_checks_valid = 1
    AND @dovecote_triggers_valid = 1
    AND @dovecote_fk_valid = 1
    AND @dovecote_indexes_valid = 1
    AND @dovecote_marker_catalog_valid = 1
    AND @dovecote_marker_data_valid = 1
    AND @dovecote_marker_state_valid = 1
);
SET @dovecote_backfill_valid = (
    @dovecote_events_tenant_data_valid = 1
    AND @dovecote_deliveries_tenant_data_valid = 1
    AND @dovecote_delivery_event_data_valid = 1
    AND @dovecote_identity_data_valid = 1
);
SET @dovecote_preflight_valid = (
    @dovecote_catalog_valid = 1 AND @dovecote_backfill_valid = 1
);

-- This is the complete preflight.  Duplicate tenant-scoped identities and
-- every catalog mismatch are rejected before the first durable ALTER.
INSERT INTO dovecote_tenant_activation_guard(valid)
SELECT CASE WHEN
    @dovecote_preflight_valid = 1
    THEN 1 ELSE 0 END;

-- A temporary table is connection-local, so dropping it does not commit or
-- alter the v1 tables.  Every statement after this point is retryable: each
-- conditional ALTER either observes its already-completed target or performs
-- exactly the missing step.
DROP TEMPORARY TABLE dovecote_tenant_activation_guard;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
          AND column_name = 'tenant_id' AND data_type = 'varbinary'
          AND column_type = 'varbinary(255)' AND character_maximum_length = 255
          AND is_nullable = 'NO' AND column_default IS NULL AND extra = ''
          AND (generation_expression IS NULL OR TRIM(generation_expression) = '')),
    'SELECT 1',
    'ALTER TABLE dovecote_events MODIFY tenant_id VARBINARY(255) NOT NULL'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries'
          AND column_name = 'tenant_id' AND data_type = 'varbinary'
          AND column_type = 'varbinary(255)' AND character_maximum_length = 255
          AND is_nullable = 'NO' AND column_default IS NULL AND extra = ''
          AND (generation_expression IS NULL OR TRIM(generation_expression) = '')),
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries MODIFY tenant_id VARBINARY(255) NOT NULL'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM dovecote_tenant_activation_catalog_checks AS actual
        WHERE actual.table_name = 'dovecote_events'
          AND actual.constraint_name = 'dovecote_events_tenant_size'
          AND actual.normalized_clause IN (
              'octet_length(tenant_id)<=255',
              '(octet_length(tenant_id)<=255)')),
    'SELECT 1',
    'ALTER TABLE dovecote_events ADD CONSTRAINT dovecote_events_tenant_size CHECK (OCTET_LENGTH(tenant_id) <= 255)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM dovecote_tenant_activation_catalog_checks AS actual
        WHERE actual.table_name = 'dovecote_events'
          AND actual.constraint_name = 'dovecote_events_tenant_nonempty'
          AND actual.normalized_clause IN (
              'octet_length(tenant_id)>0',
              '(octet_length(tenant_id)>0)')),
    'SELECT 1',
    'ALTER TABLE dovecote_events ADD CONSTRAINT dovecote_events_tenant_nonempty CHECK (OCTET_LENGTH(tenant_id) > 0)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM dovecote_tenant_activation_catalog_checks AS actual
        WHERE actual.table_name = 'dovecote_deliveries'
          AND actual.constraint_name = 'dovecote_deliveries_tenant_size'
          AND actual.normalized_clause IN (
              'octet_length(tenant_id)<=255',
              '(octet_length(tenant_id)<=255)')),
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries ADD CONSTRAINT dovecote_deliveries_tenant_size CHECK (OCTET_LENGTH(tenant_id) <= 255)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM dovecote_tenant_activation_catalog_checks AS actual
        WHERE actual.table_name = 'dovecote_deliveries'
          AND actual.constraint_name = 'dovecote_deliveries_tenant_nonempty'
          AND actual.normalized_clause IN (
              'octet_length(tenant_id)>0',
              '(octet_length(tenant_id)>0)')),
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries ADD CONSTRAINT dovecote_deliveries_tenant_nonempty CHECK (OCTET_LENGTH(tenant_id) > 0)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_tenant_row_target_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_events ADD CONSTRAINT dovecote_events_tenant_row_unique UNIQUE (tenant_id, row_id)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

-- The physical identity is a generated, length-prefixed byte encoding.  Add
-- it before the unique index so an interrupted activation never leaves the
-- table without an enforceable tenant-scoped identity.
SET @dovecote_activation_sql = IF(
    @dovecote_identity_column_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_events ADD COLUMN identity_key VARBINARY(2310) GENERATED ALWAYS AS (CONCAT(LPAD(OCTET_LENGTH(tenant_id), 3, ''0''), tenant_id, LPAD(OCTET_LENGTH(source), 4, ''0''), source, event_id)) STORED'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

-- Add the new identity key before removing the old one.  This preserves an
-- identity uniqueness guarantee across a successful activation step.
SET @dovecote_activation_sql = IF(
    @dovecote_target_identity_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_events ADD UNIQUE KEY dovecote_events_tenant_source_event_id (identity_key)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    EXISTS (SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'
          AND index_name = 'dovecote_events_source_event_id'),
    'ALTER TABLE dovecote_events DROP INDEX dovecote_events_source_event_id',
    'SELECT 1'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

-- The v1 foreign key has one column.  Drop it only when the composite target
-- is not already present, then add the target under the stable constraint
-- name.  A retry after either DDL statement sees the remaining step.
SET @dovecote_activation_sql = IF(
    @dovecote_fk_target_valid = 1,
    'SELECT 1',
    IF(EXISTS (SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_deliveries'
          AND constraint_name = 'dovecote_deliveries_event_fk'),
        'ALTER TABLE dovecote_deliveries DROP FOREIGN KEY dovecote_deliveries_event_fk',
        'SELECT 1')
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_fk_target_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries ADD CONSTRAINT dovecote_deliveries_event_fk FOREIGN KEY (tenant_id, event_row_id) REFERENCES dovecote_events (tenant_id, row_id) ON DELETE RESTRICT'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_target_claimable_valid = 1,
    'SELECT 1',
    IF(EXISTS (SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries'
          AND index_name = 'dovecote_deliveries_claimable'),
        'ALTER TABLE dovecote_deliveries DROP INDEX dovecote_deliveries_claimable',
        'SELECT 1')
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_target_claimable_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries ADD KEY dovecote_deliveries_claimable (tenant_id, state, available_at, event_row_id)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_target_expired_valid = 1,
    'SELECT 1',
    IF(EXISTS (SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries'
          AND index_name = 'dovecote_deliveries_expired_claims'),
        'ALTER TABLE dovecote_deliveries DROP INDEX dovecote_deliveries_expired_claims',
        'SELECT 1')
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

SET @dovecote_activation_sql = IF(
    @dovecote_target_expired_valid = 1,
    'SELECT 1',
    'ALTER TABLE dovecote_deliveries ADD KEY dovecote_deliveries_expired_claims (tenant_id, state, claim_expires_at, event_row_id)'
);
PREPARE dovecote_activation_statement FROM @dovecote_activation_sql;
EXECUTE dovecote_activation_statement;
DEALLOCATE PREPARE dovecote_activation_statement;

-- v1 did not have the marker table.  The upsert makes a completed activation
-- safe to replay while retaining the exact v2 marker values.
CREATE TABLE IF NOT EXISTS dovecote_schema (
    schema_version INT PRIMARY KEY,
    minimum_crate_major SMALLINT NOT NULL,
    minimum_crate_minor SMALLINT NOT NULL,
    minimum_crate_patch SMALLINT NOT NULL,
    rolling_compatible BOOLEAN NOT NULL,
    CONSTRAINT dovecote_schema_version_supported CHECK (schema_version = 2),
    CONSTRAINT dovecote_schema_minimum_nonnegative CHECK (
        minimum_crate_major >= 0 AND minimum_crate_minor >= 0 AND minimum_crate_patch >= 0
    )
) ENGINE = InnoDB;
INSERT INTO dovecote_schema
    (schema_version, minimum_crate_major, minimum_crate_minor, minimum_crate_patch, rolling_compatible)
VALUES (2, 0, 2, 0, FALSE)
ON DUPLICATE KEY UPDATE
    minimum_crate_major = 0,
    minimum_crate_minor = 2,
    minimum_crate_patch = 0,
    rolling_compatible = FALSE;
