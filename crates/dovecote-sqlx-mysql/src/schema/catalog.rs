//! Information-schema catalog reads and checks for Dovecote's tables.

use crate::{backend, error::SchemaError};
use sqlx::{FromRow, MySqlConnection, query_as};

use super::{
    contracts::{
        CHECK_NAMES, ColumnSpec, IDENTITY_KEY_GENERATION_EXPRESSION, MARKER_CHECK_NAMES,
        check_clause_is_plausible, marker_check_clause_is_plausible, marker_columns,
    },
    normalization::{normalize, normalize_generated_expression, trigger_action_matches},
};

#[derive(Debug, FromRow)]
struct TriggerInfo {
    trigger_name: String,
    event_manipulation: String,
    action_timing: String,
    event_object_table: String,
    action_statement: String,
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
    generation_expression: Option<String>,
}

#[derive(Debug, FromRow)]
struct CollationInfo {
    collation_name: String,
}

pub(super) async fn check_tables_and_columns(
    connection: &mut MySqlConnection,
) -> Result<(), crate::error::SchemaError> {
    check_marker_table(connection).await?;
    let tables = query_as::<_, TableInfo>("SELECT TABLE_NAME AS table_name, ENGINE AS engine, TABLE_TYPE AS table_type FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries')")
        .fetch_all(&mut *connection).await.map_err(|source| crate::error::SchemaError::sql("check Dovecote tables", source))?;
    for name in ["dovecote_events", "dovecote_deliveries"] {
        let Some(table) = tables.iter().find(|table| table.table_name == name) else {
            return Err(crate::error::SchemaError::MigrationMismatch {
                detail: format!("required table {name} is missing"),
            });
        };

        if table.table_type != "BASE TABLE" || table.engine.as_deref() != Some("InnoDB") {
            return Err(crate::error::SchemaError::MigrationMismatch {
                detail: format!("table {name} must be an InnoDB base table"),
            });
        }
    }

    check_columns(
        connection,
        "dovecote_events",
        &[
            ColumnSpec::required("row_id", "bigint", None, true),
            ColumnSpec::required("tenant_id", "varbinary", Some(255), false),
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
            ColumnSpec::stored_generated(
                "identity_key",
                "varbinary",
                Some(2310),
                IDENTITY_KEY_GENERATION_EXPRESSION,
            ),
        ],
    )
    .await?;
    let extension_collation = query_as::<_, CollationInfo>("SELECT COLLATION_NAME AS collation_name FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_events' AND COLUMN_NAME = 'extensions'")
        .fetch_optional(&mut *connection)
        .await
        .map_err(|source| crate::error::SchemaError::sql("check extension collation", source))?;
    if extension_collation
        .as_ref()
        .map(|value| value.collation_name.as_str())
        != Some("utf8mb4_bin")
    {
        return Err(crate::error::SchemaError::MigrationMismatch {
            detail: "dovecote_events.extensions must use utf8mb4_bin".to_owned(),
        });
    }

    check_columns(
        connection,
        "dovecote_deliveries",
        &[
            ColumnSpec::required("event_row_id", "bigint", None, false),
            ColumnSpec::required("tenant_id", "varbinary", Some(255), false),
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
    .await
}

async fn check_marker_table(
    connection: &mut MySqlConnection,
) -> Result<(), crate::error::SchemaError> {
    let marker = query_as::<_, TableInfo>(
        "SELECT TABLE_NAME AS table_name, ENGINE AS engine, TABLE_TYPE AS table_type FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_schema'",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check Dovecote schema marker table", source))?;
    let Some(marker) = marker else {
        return Err(SchemaError::MigrationMismatch {
            detail: "required table dovecote_schema is missing".to_owned(),
        });
    };
    if marker.table_type != "BASE TABLE" || marker.engine.as_deref() != Some("InnoDB") {
        return Err(SchemaError::MigrationMismatch {
            detail: "table dovecote_schema must be an InnoDB base table".to_owned(),
        });
    }

    let expected_columns = marker_columns();
    check_columns(connection, "dovecote_schema", &expected_columns).await?;

    let constraints = query_as::<_, ConstraintInfo>(
        "SELECT TABLE_NAME AS table_name, CONSTRAINT_NAME AS constraint_name, CONSTRAINT_TYPE AS constraint_type FROM information_schema.table_constraints WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_schema'",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check Dovecote schema marker constraints", source))?;
    if constraints.len() != MARKER_CHECK_NAMES.len() + 1
        || !constraints.iter().any(|constraint| {
            constraint.constraint_name == "PRIMARY" && constraint.constraint_type == "PRIMARY KEY"
        })
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "Dovecote schema marker has unexpected constraints".to_owned(),
        });
    }

    let checks = query_as::<_, CheckInfo>(
        "SELECT CONSTRAINT_NAME AS constraint_name, CHECK_CLAUSE AS check_clause FROM information_schema.check_constraints WHERE CONSTRAINT_SCHEMA = DATABASE()",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check Dovecote schema marker CHECK constraints", source))?;
    for name in MARKER_CHECK_NAMES {
        let Some(check) = checks.iter().find(|check| check.constraint_name == *name) else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required schema marker CHECK constraint {name} is missing"),
            });
        };
        if !marker_check_clause_is_plausible(name, &check.check_clause) {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("schema marker CHECK constraint {name} is incompatible"),
            });
        }
    }
    Ok(())
}

pub(super) async fn check_columns(
    connection: &mut MySqlConnection,
    table: &str,
    expected: &[ColumnSpec],
) -> Result<(), SchemaError> {
    let columns = query_as::<_, ColumnInfo>("SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED) AS character_maximum_length, IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, EXTRA AS extra, GENERATION_EXPRESSION AS generation_expression FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?").bind(table).fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote columns", source))?;
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
        let has_identity = actual.extra.trim().eq_ignore_ascii_case("auto_increment");
        let identity_matches = has_identity == spec.identity;
        let generated_matches = match (spec.generated, spec.generation_expression) {
            (Some(expected_extra), Some(expected_expression)) => {
                actual.extra.eq_ignore_ascii_case(expected_extra)
                    && actual
                        .generation_expression
                        .as_deref()
                        .is_some_and(|actual| {
                            normalize_generated_expression(actual)
                                == normalize_generated_expression(expected_expression)
                        })
            }
            (None, None) => non_generated_metadata_matches(
                &actual.extra,
                actual.generation_expression.as_deref(),
                spec.identity,
                spec.default.is_some(),
            ),
            _ => false,
        };
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
        let exact_type_matches = spec
            .exact_column_type
            .is_none_or(|expected| actual.column_type.eq_ignore_ascii_case(expected));
        if actual.data_type != spec.data_type
            || actual.column_type.to_ascii_lowercase().contains("unsigned")
            || !size_matches
            || !precision_matches
            || !exact_type_matches
            || (actual.is_nullable == "YES") != spec.nullable
            || !identity_matches
            || !generated_matches
            || !default_matches
        {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("column {table}.{} is incompatible", spec.name),
            });
        }
    }
    Ok(())
}

pub(super) fn non_generated_metadata_matches(
    extra: &str,
    expression: Option<&str>,
    identity: bool,
    has_expected_default: bool,
) -> bool {
    // MySQL reports DEFAULT_GENERATED for ordinary columns whose default is
    // generated by the server (for example CURRENT_TIMESTAMP). That marker
    // is distinct from the STORED GENERATED and VIRTUAL GENERATED markers.
    let extra = extra.trim();
    let extra_matches = extra.is_empty()
        || (has_expected_default && extra.eq_ignore_ascii_case("DEFAULT_GENERATED"))
        || (identity && extra.eq_ignore_ascii_case("AUTO_INCREMENT"));
    extra_matches
        && expression.is_none_or(|value| value.bytes().all(|byte| byte.is_ascii_whitespace()))
}

pub(super) fn default_matches(spec: &ColumnSpec, actual: Option<&str>) -> bool {
    match spec.default {
        Some(expected) => actual.is_some_and(|value| normalize(value) == normalize(expected)),
        None => actual.is_none_or(|value| spec.nullable && normalize(value) == "null"),
    }
}

pub(super) async fn check_constraints(
    connection: &mut MySqlConnection,
    _info: &backend::BackendInfo,
) -> Result<(), crate::error::SchemaError> {
    let constraints = query_as::<_, ConstraintInfo>("SELECT TABLE_NAME AS table_name, CONSTRAINT_NAME AS constraint_name, CONSTRAINT_TYPE AS constraint_type FROM information_schema.table_constraints WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries')").fetch_all(&mut *connection).await.map_err(|source| SchemaError::sql("check Dovecote constraints", source))?;
    let required = [
        ("dovecote_events", "PRIMARY", "PRIMARY KEY"),
        ("dovecote_deliveries", "PRIMARY", "PRIMARY KEY"),
        (
            "dovecote_events",
            "dovecote_events_tenant_source_event_id",
            "UNIQUE",
        ),
        (
            "dovecote_events",
            "dovecote_events_tenant_row_unique",
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
    if constraints.len() != CHECK_NAMES.len() + 5 {
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
        .filter(|k| k.constraint_name == "dovecote_events_tenant_source_event_id")
        .collect();
    if identity.len() != 1 || identity[0].column_name != "identity_key" {
        return Err(SchemaError::MigrationMismatch {
            detail: "identity constraint must cover identity_key".to_owned(),
        });
    }

    let tenant_row: Vec<_> = keys
        .iter()
        .filter(|k| k.constraint_name == "dovecote_events_tenant_row_unique")
        .collect();
    if tenant_row.len() != 2
        || tenant_row[0].column_name != "tenant_id"
        || tenant_row[1].column_name != "row_id"
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "tenant row constraint must cover tenant_id,row_id in order".to_owned(),
        });
    }

    let fk: Vec<_> = keys
        .iter()
        .filter(|k| k.constraint_name == "dovecote_deliveries_event_fk")
        .collect();
    if fk.len() != 2
        || fk[0].column_name != "tenant_id"
        || fk[0].referenced_table_name.as_deref() != Some("dovecote_events")
        || fk[0].referenced_column_name.as_deref() != Some("tenant_id")
        || fk[1].column_name != "event_row_id"
        || fk[1].referenced_table_name.as_deref() != Some("dovecote_events")
        || fk[1].referenced_column_name.as_deref() != Some("row_id")
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "delivery foreign key is incompatible".to_owned(),
        });
    }

    let reference = query_as::<_, ReferenceInfo>("SELECT DELETE_RULE AS delete_rule FROM information_schema.referential_constraints WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'dovecote_deliveries' AND CONSTRAINT_NAME = 'dovecote_deliveries_event_fk'")
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

#[derive(Debug, FromRow)]
struct ReferenceInfo {
    delete_rule: String,
}

pub(super) async fn check_triggers(
    connection: &mut MySqlConnection,
) -> Result<(), crate::error::SchemaError> {
    let triggers = query_as::<_, TriggerInfo>("SELECT TRIGGER_NAME AS trigger_name, EVENT_MANIPULATION AS event_manipulation, ACTION_TIMING AS action_timing, EVENT_OBJECT_TABLE AS event_object_table, ACTION_STATEMENT AS action_statement FROM information_schema.triggers WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE IN ('dovecote_events','dovecote_deliveries')")
        .fetch_all(&mut *connection).await.map_err(|source| crate::error::SchemaError::sql("check Dovecote row_id triggers", source))?;
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
        return Err(crate::error::SchemaError::MigrationMismatch {
            detail: "Dovecote row_id positivity triggers are missing".to_owned(),
        });
    }
    Ok(())
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
pub(super) async fn check_indexes(connection: &mut MySqlConnection) -> Result<(), SchemaError> {
    let indexes=query_as::<_,IndexInfo>("SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, CAST(SEQ_IN_INDEX AS SIGNED) AS seq_in_index, COLUMN_NAME AS column_name, INDEX_TYPE AS index_type, CAST(SUB_PART AS SIGNED) AS sub_part FROM information_schema.statistics WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events','dovecote_deliveries') ORDER BY INDEX_NAME, SEQ_IN_INDEX").fetch_all(&mut *connection).await.map_err(|source|SchemaError::sql("check Dovecote indexes",source))?;
    if indexes.iter().any(|i| {
        i.index_name != "PRIMARY"
            && !matches!(
                i.index_name.as_str(),
                "dovecote_events_tenant_source_event_id"
                    | "dovecote_events_tenant_row_unique"
                    | "dovecote_deliveries_event_fk"
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
            "dovecote_events_tenant_source_event_id",
            "dovecote_events",
            true,
            &["identity_key"][..],
        ),
        (
            "dovecote_events_tenant_row_unique",
            "dovecote_events",
            true,
            &["tenant_id", "row_id"][..],
        ),
        (
            "dovecote_deliveries_event_fk",
            "dovecote_deliveries",
            false,
            &["tenant_id", "event_row_id"][..],
        ),
        (
            "dovecote_deliveries_claimable",
            "dovecote_deliveries",
            false,
            &["tenant_id", "state", "available_at", "event_row_id"][..],
        ),
        (
            "dovecote_deliveries_expired_claims",
            "dovecote_deliveries",
            false,
            &["tenant_id", "state", "claim_expires_at", "event_row_id"][..],
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
