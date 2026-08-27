//! MySQL/MariaDB information-schema verification for the installed schema.

use crate::{backend, error::SchemaError, migration::current_migration};
use sqlx::{FromRow, MySqlConnection, MySqlPool, query_as};

pub async fn check_schema(pool: &MySqlPool) -> Result<(), SchemaError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| SchemaError::sql("acquire schema-check connection", source))?;
    check_schema_connection(&mut connection).await
}

/// Performs the complete schema check on an already-owned connection.
pub(crate) async fn check_schema_connection(
    connection: &mut MySqlConnection,
) -> Result<(), SchemaError> {
    let info = backend::detect_on_connection(connection).await?;
    let tables = query_as::<_, TableInfo>("SELECT TABLE_NAME AS table_name, ENGINE AS engine, TABLE_TYPE AS table_type FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries')")
        .fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote tables", source))?;
    for name in ["dovecote_events", "dovecote_deliveries"] {
        let Some(table) = tables.iter().find(|table| table.table_name == name) else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required table {name} is missing"),
            });
        };

        if table.table_type != "BASE TABLE" || table.engine.as_deref() != Some("InnoDB") {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("table {name} must be an InnoDB base table"),
            });
        }
    }

    check_columns(
        connection,
        "dovecote_events",
        &[
            ColumnSpec::required("row_id", "bigint", None, true),
            ColumnSpec::required("stream", "varbinary", Some(255), false),
            ColumnSpec::required("specversion", "varbinary", Some(8), false),
            ColumnSpec::required("event_id", "varbinary", Some(1024), false),
            ColumnSpec::required("source", "varbinary", Some(2048), false),
            ColumnSpec::required("event_type", "varbinary", Some(1024), false),
            ColumnSpec::optional("subject", "varbinary", Some(2048)),
            ColumnSpec::optional("occurred_at", "datetime", None),
            ColumnSpec::optional("datacontenttype", "varbinary", Some(255)),
            ColumnSpec::optional("dataschema", "varbinary", Some(2048)),
            ColumnSpec::optional("partitionkey", "varbinary", Some(255)),
            ColumnSpec::required("extensions", "longtext", None, false),
            ColumnSpec::optional("data_kind", "varbinary", Some(6)),
            ColumnSpec::optional("data", "longblob", None),
            ColumnSpec::required_default("enqueued_at", "datetime", "CURRENT_TIMESTAMP(6)"),
        ],
    )
    .await?;
    let extension_collation = query_as::<_, CollationInfo>("SELECT COLLATION_NAME AS collation_name FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_events' AND COLUMN_NAME = 'extensions'")
        .fetch_optional(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check extension collation", source))?;
    if extension_collation
        .as_ref()
        .map(|value| value.collation_name.as_str())
        != Some("utf8mb4_bin")
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "dovecote_events.extensions must use utf8mb4_bin".to_owned(),
        });
    }

    check_columns(
        connection,
        "dovecote_deliveries",
        &[
            ColumnSpec::required("event_row_id", "bigint", None, false),
            ColumnSpec::required("state", "varbinary", Some(12), false),
            ColumnSpec::required_default("available_at", "datetime", "CURRENT_TIMESTAMP(6)"),
            ColumnSpec::required_default("attempts", "bigint", "0"),
            ColumnSpec::optional("claim_token", "binary", Some(16)),
            ColumnSpec::optional("claimed_by", "varbinary", Some(255)),
            ColumnSpec::optional("claim_expires_at", "datetime", None),
            ColumnSpec::optional("last_failure_code", "varbinary", Some(128)),
            ColumnSpec::optional("last_failure_detail", "varbinary", Some(2048)),
            ColumnSpec::optional("delivered_at", "datetime", None),
            ColumnSpec::optional("quarantined_at", "datetime", None),
            ColumnSpec::optional("quarantine_reason", "varbinary", Some(2048)),
        ],
    )
    .await?;

    current_migration().map_err(|detail| SchemaError::MigrationMismatch { detail })?;
    check_constraints(connection, &info).await?;
    let triggers = query_as::<_, TriggerInfo>("SELECT TRIGGER_NAME AS trigger_name, EVENT_MANIPULATION AS event_manipulation, ACTION_TIMING AS action_timing, EVENT_OBJECT_TABLE AS event_object_table, ACTION_STATEMENT AS action_statement FROM information_schema.triggers WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE IN ('dovecote_events','dovecote_deliveries')")
        .fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote row_id triggers", source))?;
    if triggers.len() != 2
        || triggers
            .iter()
            .any(|trigger| trigger.event_object_table == "dovecote_deliveries")
        || !triggers.iter().any(|trigger| {
            trigger.trigger_name == "dovecote_events_row_id_positive_insert"
                && trigger.event_manipulation == "INSERT"
                && trigger.action_timing == "BEFORE"
                && trigger.event_object_table == "dovecote_events"
                && trigger_action_matches("insert", &trigger.action_statement)
        })
        || !triggers.iter().any(|trigger| {
            trigger.trigger_name == "dovecote_events_row_id_positive_update"
                && trigger.event_manipulation == "UPDATE"
                && trigger.action_timing == "BEFORE"
                && trigger.event_object_table == "dovecote_events"
                && trigger_action_matches("update", &trigger.action_statement)
        })
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "Dovecote row_id positivity triggers are missing".to_owned(),
        });
    }
    check_indexes(connection).await
}

#[derive(Debug, FromRow)]
struct TriggerInfo {
    trigger_name: String,
    event_manipulation: String,
    action_timing: String,
    event_object_table: String,
    action_statement: String,
}
fn normalize_trigger(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .filter(|c| !c.is_ascii_whitespace() && *c != '`')
        .collect()
}
fn trigger_action_matches(kind: &str, action: &str) -> bool {
    let expected = match kind {
        "insert" => {
            "beginifnew.row_id<0thensignal sqlstate'45000'setmessage_text='dovecote row_id must be positive';endif;end"
        }
        "update" => {
            "beginifnew.row_id<=0ornew.row_id<>old.row_idthensignal sqlstate'45000'setmessage_text='dovecote row_id must be positive';endif;end"
        }
        _ => return false,
    };
    normalize_trigger(action) == expected.replace(" ", "")
}

#[derive(Debug, FromRow)]
struct TableInfo {
    table_name: String,
    engine: Option<String>,
    table_type: String,
}
#[derive(Debug, FromRow)]
struct ColumnInfo {
    column_name: String,
    data_type: String,
    column_type: String,
    character_maximum_length: Option<i64>,
    is_nullable: String,
    column_default: Option<String>,
    extra: String,
}
#[derive(Debug, FromRow)]
struct CollationInfo {
    collation_name: String,
}
struct ColumnSpec {
    name: &'static str,
    data_type: &'static str,
    max: Option<i64>,
    nullable: bool,
    identity: bool,
    default: Option<&'static str>,
}
impl ColumnSpec {
    const fn required(
        name: &'static str,
        data_type: &'static str,
        max: Option<i64>,
        identity: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            max,
            nullable: false,
            identity,
            default: None,
        }
    }
    const fn optional(name: &'static str, data_type: &'static str, max: Option<i64>) -> Self {
        Self {
            name,
            data_type,
            max,
            nullable: true,
            identity: false,
            default: None,
        }
    }
    const fn required_default(
        name: &'static str,
        data_type: &'static str,
        default: &'static str,
    ) -> Self {
        Self {
            name,
            data_type,
            max: None,
            nullable: false,
            identity: false,
            default: Some(default),
        }
    }
}
async fn check_columns(
    connection: &mut MySqlConnection,
    table: &str,
    expected: &[ColumnSpec],
) -> Result<(), SchemaError> {
    let columns = query_as::<_, ColumnInfo>("SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED) AS character_maximum_length, IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, EXTRA AS extra FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?").bind(table).fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote columns", source))?;
    if columns.len() != expected.len() {
        return Err(SchemaError::MigrationMismatch {
            detail: format!("table {table} has unexpected columns"),
        });
    }

    for spec in expected {
        let Some(actual) = columns
            .iter()
            .find(|column| column.column_name == spec.name)
        else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required column {table}.{} is missing", spec.name),
            });
        };

        let default_matches = default_matches(spec, actual.column_default.as_deref());
        let has_identity = actual.extra.to_ascii_lowercase().contains("auto_increment");
        let identity_matches = has_identity == spec.identity;
        let size_matches = if spec.data_type == "binary" {
            spec.max.is_some_and(|size| {
                actual
                    .column_type
                    .eq_ignore_ascii_case(&format!("binary({size})"))
            })
        } else {
            spec.max.is_none() || actual.character_maximum_length == spec.max
        };
        let precision_matches = if spec.data_type == "datetime" {
            actual.column_type.eq_ignore_ascii_case("datetime(6)")
        } else {
            true
        };
        if actual.data_type != spec.data_type
            || actual.column_type.to_ascii_lowercase().contains("unsigned")
            || !size_matches
            || !precision_matches
            || (actual.is_nullable == "YES") != spec.nullable
            || !identity_matches
            || !default_matches
        {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("column {table}.{} is incompatible", spec.name),
            });
        }
    }
    Ok(())
}
fn default_matches(spec: &ColumnSpec, actual: Option<&str>) -> bool {
    match spec.default {
        Some(expected) => actual.is_some_and(|value| normalize(value) == normalize(expected)),
        None => actual.is_none_or(|value| spec.nullable && normalize(value) == "null"),
    }
}

#[derive(Debug, FromRow)]
struct ConstraintInfo {
    table_name: String,
    constraint_name: String,
    constraint_type: String,
}
#[derive(Debug, FromRow)]
struct CheckInfo {
    constraint_name: String,
    check_clause: String,
}
#[derive(Debug, FromRow)]
struct KeyColumn {
    constraint_name: String,
    column_name: String,
    referenced_table_name: Option<String>,
    referenced_column_name: Option<String>,
}
async fn check_constraints(
    connection: &mut MySqlConnection,
    _info: &backend::BackendInfo,
) -> Result<(), SchemaError> {
    let constraints = query_as::<_, ConstraintInfo>("SELECT TABLE_NAME AS table_name, CONSTRAINT_NAME AS constraint_name, CONSTRAINT_TYPE AS constraint_type FROM information_schema.table_constraints WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries')").fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote constraints", source))?;
    let required = [
        ("dovecote_events", "PRIMARY", "PRIMARY KEY"),
        ("dovecote_deliveries", "PRIMARY", "PRIMARY KEY"),
        (
            "dovecote_events",
            "dovecote_events_source_event_id",
            "UNIQUE",
        ),
        (
            "dovecote_deliveries",
            "dovecote_deliveries_event_fk",
            "FOREIGN KEY",
        ),
    ];
    for (table, name, kind) in required {
        if !constraints.iter().any(|c| {
            c.table_name == table && c.constraint_name == name && c.constraint_type == kind
        }) {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required constraint {table}.{name} is missing or incompatible"),
            });
        }
    }

    let checks = query_as::<_, CheckInfo>("SELECT CONSTRAINT_NAME AS constraint_name, CHECK_CLAUSE AS check_clause FROM information_schema.check_constraints WHERE CONSTRAINT_SCHEMA = DATABASE()").fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote CHECK constraints", source))?;
    if constraints.len() != CHECK_NAMES.len() + 4 {
        return Err(SchemaError::MigrationMismatch {
            detail: "Dovecote has unexpected constraints".to_owned(),
        });
    }

    for name in CHECK_NAMES {
        let Some(check) = checks.iter().find(|check| check.constraint_name == *name) else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required CHECK constraint {name} is missing"),
            });
        };

        if !check_clause_is_plausible(name, &check.check_clause) {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("CHECK constraint {name} is incompatible"),
            });
        }
    }

    let keys = query_as::<_, KeyColumn>("SELECT CONSTRAINT_NAME AS constraint_name, COLUMN_NAME AS column_name, REFERENCED_TABLE_NAME AS referenced_table_name, REFERENCED_COLUMN_NAME AS referenced_column_name FROM information_schema.key_column_usage WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries') ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION").fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote constraint columns", source))?;
    let identity: Vec<_> = keys
        .iter()
        .filter(|k| k.constraint_name == "dovecote_events_source_event_id")
        .collect();
    if identity.len() != 2
        || identity[0].column_name != "source"
        || identity[1].column_name != "event_id"
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "identity constraint must cover source,event_id in order".to_owned(),
        });
    }

    let fk: Vec<_> = keys
        .iter()
        .filter(|k| k.constraint_name == "dovecote_deliveries_event_fk")
        .collect();
    if fk.len() != 1
        || fk[0].column_name != "event_row_id"
        || fk[0].referenced_table_name.as_deref() != Some("dovecote_events")
        || fk[0].referenced_column_name.as_deref() != Some("row_id")
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "delivery foreign key is incompatible".to_owned(),
        });
    }

    let reference = query_as::<_, ReferenceInfo>("SELECT CONSTRAINT_NAME AS constraint_name, DELETE_RULE AS delete_rule FROM information_schema.referential_constraints WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_deliveries' AND CONSTRAINT_NAME = 'dovecote_deliveries_event_fk'")
        .fetch_optional(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check delivery foreign key action", source))?;
    if reference
        .as_ref()
        .is_none_or(|value| value.delete_rule != "RESTRICT")
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "delivery foreign key must use ON DELETE RESTRICT".to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, FromRow)]
struct ReferenceInfo {
    #[allow(dead_code)]
    constraint_name: String,
    delete_rule: String,
}
const CHECK_NAMES: &[&str] = &[
    "dovecote_events_specversion",
    "dovecote_events_stream_size",
    "dovecote_events_event_id_size",
    "dovecote_events_source_size",
    "dovecote_events_event_type_size",
    "dovecote_events_subject_size",
    "dovecote_events_content_type_size",
    "dovecote_events_schema_size",
    "dovecote_events_partition_size",
    "dovecote_events_identity_size",
    "dovecote_events_data_kind",
    "dovecote_events_data_pair",
    "dovecote_events_content_type",
    "dovecote_deliveries_state",
    "dovecote_deliveries_attempts",
    "dovecote_deliveries_token_size",
    "dovecote_deliveries_worker_size",
    "dovecote_deliveries_failure_code_size",
    "dovecote_deliveries_failure_detail_size",
    "dovecote_deliveries_quarantine_size",
    "dovecote_deliveries_failure_pair",
    "dovecote_deliveries_state_shape",
];
fn check_clause_is_plausible(name: &str, clause: &str) -> bool {
    let Some(expected) = expected_check_clause(name) else {
        return false;
    };

    let actual = normalize_check_clause(name, clause);
    actual == normalize_check_clause(name, expected)
        || catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mariadb_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mysql_unwrapped_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mysql_predicate_grouped_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
}

// These expressions are copied from migrations/0001_dovecote.sql.  The
// migration is the semantic authority; catalog decoration is handled by
// normalize_check_clause, never by looking for a matching fragment.
fn expected_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_events_specversion" => "specversion = _binary '1.0'",
        "dovecote_events_stream_size" => "OCTET_LENGTH(stream) <= 255",
        "dovecote_events_event_id_size" => "OCTET_LENGTH(event_id) <= 1024",
        "dovecote_events_source_size" => "OCTET_LENGTH(source) <= 2048",
        "dovecote_events_event_type_size" => "OCTET_LENGTH(event_type) <= 1024",
        "dovecote_events_subject_size" => "subject IS NULL OR OCTET_LENGTH(subject) <= 2048",
        "dovecote_events_content_type_size" => {
            "datacontenttype IS NULL OR OCTET_LENGTH(datacontenttype) <= 255"
        }
        "dovecote_events_schema_size" => "dataschema IS NULL OR OCTET_LENGTH(dataschema) <= 2048",
        "dovecote_events_partition_size" => {
            "partitionkey IS NULL OR OCTET_LENGTH(partitionkey) <= 255"
        }
        "dovecote_events_identity_size" => "OCTET_LENGTH(source) + OCTET_LENGTH(event_id) <= 2048",
        "dovecote_events_data_kind" => {
            "data_kind IS NULL OR data_kind IN (_binary 'json', _binary 'binary')"
        }
        "dovecote_events_data_pair" => "(data_kind IS NULL) = (data IS NULL)",
        "dovecote_events_content_type" => {
            "data IS NULL OR OCTET_LENGTH(data) = 0 OR datacontenttype IS NOT NULL"
        }
        "dovecote_deliveries_state" => {
            "state IN (_binary 'pending', _binary 'claimed', _binary 'delivered', _binary 'quarantined')"
        }
        "dovecote_deliveries_attempts" => "attempts >= 0",
        "dovecote_deliveries_token_size" => "claim_token IS NULL OR OCTET_LENGTH(claim_token) = 16",
        "dovecote_deliveries_worker_size" => {
            "claimed_by IS NULL OR OCTET_LENGTH(claimed_by) <= 255"
        }
        "dovecote_deliveries_failure_code_size" => {
            "last_failure_code IS NULL OR OCTET_LENGTH(last_failure_code) <= 128"
        }
        "dovecote_deliveries_failure_detail_size" => {
            "last_failure_detail IS NULL OR OCTET_LENGTH(last_failure_detail) <= 2048"
        }
        "dovecote_deliveries_quarantine_size" => {
            "quarantine_reason IS NULL OR OCTET_LENGTH(quarantine_reason) <= 2048"
        }
        "dovecote_deliveries_failure_pair" => {
            "(last_failure_code IS NULL) = (last_failure_detail IS NULL)"
        }
        "dovecote_deliveries_state_shape" => {
            "(state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)"
        }
        _ => return None,
    })
}

// CHECK_CLAUSE is rendered from the server's expression tree.  MySQL 8.4
// preserves the migration expression while adding grouping around each
// boolean operand; these are the complete captured forms that are accepted as
// an alternative to the migration source expression.  Keep this list exact:
// no part of it is used as a substring predicate.
fn catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_events_subject_size" => "(subject IS NULL) OR (OCTET_LENGTH(subject) <= 2048)",
        "dovecote_events_content_type_size" => {
            "(datacontenttype IS NULL) OR (OCTET_LENGTH(datacontenttype) <= 255)"
        }
        "dovecote_events_schema_size" => {
            "(dataschema IS NULL) OR (OCTET_LENGTH(dataschema) <= 2048)"
        }
        "dovecote_events_partition_size" => {
            "(partitionkey IS NULL) OR (OCTET_LENGTH(partitionkey) <= 255)"
        }
        "dovecote_events_identity_size" => {
            "(OCTET_LENGTH(source) + OCTET_LENGTH(event_id)) <= 2048"
        }
        "dovecote_events_data_kind" => {
            "(data_kind IS NULL) OR (data_kind IN (_binary 'json', _binary 'binary'))"
        }
        "dovecote_events_data_pair" => "data_kind IS NULL = (data IS NULL)",
        "dovecote_events_content_type" => {
            "(data IS NULL) OR (OCTET_LENGTH(data) = 0) OR (datacontenttype IS NOT NULL)"
        }
        "dovecote_deliveries_token_size" => {
            "(claim_token IS NULL) OR (OCTET_LENGTH(claim_token) = 16)"
        }
        "dovecote_deliveries_worker_size" => {
            "(claimed_by IS NULL) OR (OCTET_LENGTH(claimed_by) <= 255)"
        }
        "dovecote_deliveries_failure_code_size" => {
            "(last_failure_code IS NULL) OR (OCTET_LENGTH(last_failure_code) <= 128)"
        }
        "dovecote_deliveries_failure_detail_size" => {
            "(last_failure_detail IS NULL) OR (OCTET_LENGTH(last_failure_detail) <= 2048)"
        }
        "dovecote_deliveries_quarantine_size" => {
            "(quarantine_reason IS NULL) OR (OCTET_LENGTH(quarantine_reason) <= 2048)"
        }
        "dovecote_deliveries_failure_pair" => {
            "last_failure_code IS NULL = (last_failure_detail IS NULL)"
        }
        "dovecote_deliveries_state_shape" => {
            "(state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)"
        }
        _ => return None,
    })
}

fn mariadb_catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_deliveries_state_shape" => {
            "state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL"
        }
        _ => return None,
    })
}

// MySQL 8.4 may remove redundant branch grouping when serializing this
// expression. This is a complete, ordered alternative—not a fragment match.
fn mysql_unwrapped_catalog_check_clause(name: &str) -> Option<&'static str> {
    mariadb_catalog_check_clause(name)
}

fn mysql_predicate_grouped_catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_deliveries_state_shape" => {
            "((state = _binary 'pending') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'claimed') AND (claim_token IS NOT NULL) AND (claimed_by IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'delivered') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NOT NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'quarantined') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NOT NULL) AND (quarantine_reason IS NOT NULL))"
        }
        _ => return None,
    })
}

fn normalize_check_clause(name: &str, clause: &str) -> String {
    let mut normalized = normalize(clause);

    // MySQL and MariaDB catalog output may decorate the same ASCII binary
    // literal as _binary'...' or _utf8mb4'...'.  These are the only literal
    // introducers accepted here; the literal and its complete expression must
    // still compare equal below.
    normalized = normalized
        .replace("_binary'", "'")
        .replace("_utf8mb4'", "'");

    // MySQL reports OCTET_LENGTH(binary/blob) as LENGTH(binary/blob) on some
    // releases.  Canonicalize only the binary/blob operands used by this
    // migration; LENGTH on another expression remains a different clause.
    for column in binary_length_columns(name) {
        let length = format!("length({column})");
        let octet_length = format!("octet_length({column})");
        let mut offset = 0;
        let mut rewritten = String::with_capacity(normalized.len());
        while let Some(found) = normalized[offset..].find(&length) {
            let start = offset + found;
            rewritten.push_str(&normalized[offset..start]);
            if normalized.as_bytes()[..start].ends_with(b"octet_") {
                rewritten.push_str(&normalized[start..start + length.len()]);
            } else {
                rewritten.push_str(&octet_length);
            }
            offset = start + length.len();
        }
        rewritten.push_str(&normalized[offset..]);
        normalized = rewritten;
    }

    strip_redundant_outer_parentheses(&normalized)
}

fn binary_length_columns(name: &str) -> &'static [&'static str] {
    match name {
        "dovecote_events_stream_size" => &["stream"],
        "dovecote_events_event_id_size" => &["event_id"],
        "dovecote_events_source_size" => &["source"],
        "dovecote_events_event_type_size" => &["event_type"],
        "dovecote_events_subject_size" => &["subject"],
        "dovecote_events_content_type_size" => &["datacontenttype"],
        "dovecote_events_schema_size" => &["dataschema"],
        "dovecote_events_partition_size" => &["partitionkey"],
        "dovecote_events_identity_size" => &["source", "event_id"],
        "dovecote_events_content_type" => &["data"],
        "dovecote_deliveries_token_size" => &["claim_token"],
        "dovecote_deliveries_worker_size" => &["claimed_by"],
        "dovecote_deliveries_failure_code_size" => &["last_failure_code"],
        "dovecote_deliveries_failure_detail_size" => &["last_failure_detail"],
        "dovecote_deliveries_quarantine_size" => &["quarantine_reason"],
        _ => &[],
    }
}

fn strip_redundant_outer_parentheses(value: &str) -> String {
    let mut value = value;
    while value.starts_with('(')
        && value.ends_with(')')
        && outer_parentheses_enclose_expression(value)
    {
        value = &value[1..value.len() - 1];
    }
    value.to_owned()
}

fn outer_parentheses_enclose_expression(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        // This is a byte-level lexer state machine, not ordered policy.
        // ast-grep-ignore: rust-elseif-cascade
        if in_string {
            let (still_in_string, escaped_next, skip_next) =
                quoted_byte_state(bytes, index, escaped);
            in_string = still_in_string;
            escaped = escaped_next;
            index += usize::from(skip_next);
        // ast-grep-ignore: rust-elseif-cascade
        } else if byte == b'\'' {
            in_string = true;
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            if depth == 0 {
                return false;
            }
            depth -= 1;
            if depth == 0 && index != bytes.len() - 1 {
                return false;
            }
        }
        index += 1;
    }
    !in_string && depth == 0
}

fn quoted_byte_state(bytes: &[u8], index: usize, escaped: bool) -> (bool, bool, bool) {
    match (bytes[index], escaped) {
        (_, true) => (true, false, false),
        (b'\\', false) => (true, true, false),
        (b'\'', false) if bytes.get(index + 1) == Some(&b'\'') => (true, false, true),
        (b'\'', false) => (false, false, false),
        (_, false) => (true, false, false),
    }
}
#[derive(Debug, FromRow)]
struct IndexInfo {
    table_name: String,
    index_name: String,
    non_unique: i64,
    seq_in_index: i64,
    column_name: String,
    index_type: String,
    sub_part: Option<i64>,
}
async fn check_indexes(connection: &mut MySqlConnection) -> Result<(), SchemaError> {
    let indexes=query_as::<_,IndexInfo>("SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, CAST(SEQ_IN_INDEX AS SIGNED) AS seq_in_index, COLUMN_NAME AS column_name, INDEX_TYPE AS index_type, CAST(SUB_PART AS SIGNED) AS sub_part FROM information_schema.statistics WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries') ORDER BY INDEX_NAME, SEQ_IN_INDEX").fetch_all(&mut *connection).await.map_err(|source|SchemaError::sql("check Dovecote indexes",source))?;
    if indexes.iter().any(|i| {
        i.index_name != "PRIMARY"
            && !matches!(
                i.index_name.as_str(),
                "dovecote_events_source_event_id"
                    | "dovecote_deliveries_claimable"
                    | "dovecote_deliveries_expired_claims"
            )
    }) {
        return Err(SchemaError::MigrationMismatch {
            detail: "Dovecote has unexpected indexes".to_owned(),
        });
    }

    for (name, table, unique, columns) in [
        (
            "dovecote_events_source_event_id",
            "dovecote_events",
            true,
            &["source", "event_id"][..],
        ),
        (
            "dovecote_deliveries_claimable",
            "dovecote_deliveries",
            false,
            &["state", "available_at", "event_row_id"][..],
        ),
        (
            "dovecote_deliveries_expired_claims",
            "dovecote_deliveries",
            false,
            &["state", "claim_expires_at", "event_row_id"][..],
        ),
    ] {
        let actual: Vec<_> = indexes.iter().filter(|i| i.index_name == name).collect();
        if actual.len() != columns.len()
            || actual.iter().enumerate().any(|(n, i)| {
                i.table_name != table
                    || i.column_name != columns[n]
                    || i.seq_in_index != n as i64 + 1
                    || i.non_unique != (if unique { 0 } else { 1 })
                    || i.index_type != "BTREE"
                    || i.sub_part.is_some()
            })
        {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("index {name} is missing or incompatible"),
            });
        }
    }

    Ok(())
}
fn normalize(value: &str) -> String {
    // MySQL's catalog serializes the quote delimiters of binary literals as
    // `\'`; unescape that decoration before tracking SQL string boundaries.
    let value = value.replace("\\'", "'");
    let mut normalized = String::with_capacity(value.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in value.chars() {
        match (in_string, escaped, character) {
            (true, true, '\'') => {
                // Preserve a quote escaped inside a literal.
                normalized.push('\'');
                escaped = false;
            }
            (true, true, _) => {
                normalized.push('\\');
                normalized.push(character);
                escaped = false;
            }
            (true, false, '\\') => escaped = true,
            (true, false, '\'') => {
                normalized.push(character);
                in_string = false;
            }
            (true, false, _) => normalized.push(character),
            (false, _, '\'') => {
                in_string = true;
                normalized.push(character);
            }
            (false, _, character) if character.is_ascii_whitespace() || character == '`' => {}
            (false, _, character) => normalized.push(character.to_ascii_lowercase()),
        }
    }

    if escaped {
        normalized.push('\\');
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_ignores_catalog_decoration() {
        assert_eq!(normalize("CHECK ((`x` = 1))"), "check((x=1))");
    }

    #[test]
    fn mariadb_nullable_null_default_is_accepted_only_for_nullable_columns() {
        let nullable = ColumnSpec::optional("subject", "varbinary", Some(2048));
        let required = ColumnSpec::required("row_id", "bigint", None, false);
        assert!(default_matches(&nullable, Some("NULL")));
        assert!(default_matches(&nullable, None));
        assert!(!default_matches(&required, Some("NULL")));
        assert!(default_matches(&required, None));
    }

    #[test]
    fn every_migration_check_clause_matches() {
        for name in CHECK_NAMES {
            let clause = expected_check_clause(name).expect("every name has a migration clause");
            assert!(
                check_clause_is_plausible(name, clause),
                "migration clause should match {name}"
            );
        }
    }

    #[test]
    fn captured_mysql_catalog_clauses_match_as_complete_expressions() {
        for name in CHECK_NAMES {
            let Some(clause) = catalog_check_clause(name) else {
                continue;
            };
            assert!(
                check_clause_is_plausible(name, clause),
                "captured catalog clause should match {name}"
            );
        }
    }

    #[test]
    fn catalog_decoration_and_binary_length_aliases_match() {
        assert!(check_clause_is_plausible(
            "dovecote_events_specversion",
            r#"(`specversion` = _binary\'1.0\')"#
        ));
        assert!(check_clause_is_plausible(
            "dovecote_events_subject_size",
            "((`subject` IS NULL) OR (LENGTH(`subject`) <= 2048))"
        ));
        assert!(check_clause_is_plausible(
            "dovecote_deliveries_state",
            "(`state` IN (_utf8mb4'pending', _utf8mb4'claimed', _utf8mb4'delivered', _utf8mb4'quarantined'))"
        ));
    }

    #[test]
    fn mariadb_state_shape_precedence_form_matches_only_as_a_whole() {
        let clause = mariadb_catalog_check_clause("dovecote_deliveries_state_shape").unwrap();
        assert!(check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            clause
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            &clause.replace(" OR state =", " OR (state =")
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            &clause.replace("quarantine_reason IS NOT NULL", "quarantine_reason IS NULL")
        ));
    }

    #[test]
    fn mysql_unwrapped_state_shape_catalog_form_matches_exactly() {
        let clause = mysql_unwrapped_catalog_check_clause("dovecote_deliveries_state_shape")
            .expect("captured MySQL alternative");
        assert!(check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            clause
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            &format!("{clause} AND 1 = 1")
        ));
    }

    #[test]
    fn altered_or_true_and_false_are_rejected() {
        assert!(!check_clause_is_plausible(
            "dovecote_events_stream_size",
            "OCTET_LENGTH(stream) <= 255 OR true"
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_attempts",
            "attempts >= 0 AND false"
        ));
    }

    #[test]
    fn missing_or_reordered_state_branches_are_rejected() {
        let expected = expected_check_clause("dovecote_deliveries_state_shape").unwrap();
        let branches = expected.split(" OR ").collect::<Vec<_>>();
        assert_eq!(branches.len(), 4);
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            &branches[..3].join(" OR ")
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_deliveries_state_shape",
            &branches
                .iter()
                .rev()
                .copied()
                .collect::<Vec<_>>()
                .join(" OR ")
        ));
    }

    #[test]
    fn changed_bounds_grouping_and_removed_constraints_are_rejected() {
        assert!(!check_clause_is_plausible(
            "dovecote_events_source_size",
            "OCTET_LENGTH(source) <= 2047"
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_events_data_pair",
            "data_kind IS NULL = data IS NULL"
        ));
        assert!(!check_clause_is_plausible(
            "dovecote_events_row_id_positive",
            "row_id > 0"
        ));
    }
}
