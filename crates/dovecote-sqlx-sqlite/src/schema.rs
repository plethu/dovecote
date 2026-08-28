//! SQLite schema verification.

use crate::{
    error::SchemaError,
    migration::{current_migration, migration_is_usable},
};
use sqlx::{FromRow, Row, SqliteConnection, SqlitePool, query, query_as, query_scalar};

#[derive(Debug, FromRow)]
struct SchemaMarker {
    schema_version: i64,
    minimum_crate_major: i64,
    minimum_crate_minor: i64,
    minimum_crate_patch: i64,
    rolling_compatible: i64,
}

/// Verifies the exact v2 table shape, constraints, indexes, and foreign key of
/// the installed schema. It never applies a migration.
pub async fn check_schema(pool: &SqlitePool) -> Result<(), SchemaError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| SchemaError::sql("acquire schema-check connection", source))?;
    check_schema_connection(&mut connection).await
}

/// Performs the complete schema check on an already-owned connection.
pub(crate) async fn check_schema_connection(
    connection: &mut SqliteConnection,
) -> Result<(), SchemaError> {
    let enabled: i64 = query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check foreign-key enforcement", source))?;
    if enabled != 1 {
        return Err(mismatch("foreign-key enforcement is disabled"));
    }

    let migration = current_migration().map_err(mismatch)?;
    migration_is_usable(migration).map_err(mismatch)?;
    check_schema_marker(connection, migration).await?;

    check_columns(
        connection,
        "dovecote_events",
        &[
            ColumnSpec::required("row_id", "INTEGER", true),
            ColumnSpec::required("tenant_id", "TEXT", false),
            ColumnSpec::required("stream", "TEXT", false),
            ColumnSpec::required("specversion", "TEXT", false),
            ColumnSpec::required("event_id", "TEXT", false),
            ColumnSpec::required("source", "TEXT", false),
            ColumnSpec::required("event_type", "TEXT", false),
            ColumnSpec::optional("subject", "TEXT", false),
            ColumnSpec::optional("occurred_at", "TEXT", false),
            ColumnSpec::optional("datacontenttype", "TEXT", false),
            ColumnSpec::optional("dataschema", "TEXT", false),
            ColumnSpec::optional("partitionkey", "TEXT", false),
            ColumnSpec::required("extensions", "TEXT", false),
            ColumnSpec::optional("data_kind", "TEXT", false),
            ColumnSpec::optional("data", "BLOB", false),
            ColumnSpec::required("enqueued_at", "TEXT", false),
        ],
    )
    .await?;
    check_columns(
        connection,
        "dovecote_deliveries",
        &[
            ColumnSpec::required("event_row_id", "INTEGER", true),
            ColumnSpec::required("tenant_id", "TEXT", false),
            ColumnSpec::required("state", "TEXT", false),
            ColumnSpec::required("available_at", "TEXT", false),
            ColumnSpec::required("attempts", "INTEGER", false),
            ColumnSpec::optional("claim_token", "BLOB", false),
            ColumnSpec::optional("claimed_by", "TEXT", false),
            ColumnSpec::optional("claim_expires_at", "TEXT", false),
            ColumnSpec::optional("last_failure_code", "TEXT", false),
            ColumnSpec::optional("last_failure_detail", "TEXT", false),
            ColumnSpec::optional("delivered_at", "TEXT", false),
            ColumnSpec::optional("quarantined_at", "TEXT", false),
            ColumnSpec::optional("quarantine_reason", "TEXT", false),
        ],
    )
    .await?;

    let sources = query_as::<_, TableSource>(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries')",
    ).fetch_all(&mut *connection).await
        .map_err(|source| SchemaError::sql("read schema definitions", source))?;
    for name in ["dovecote_schema", "dovecote_events", "dovecote_deliveries"] {
        if !sources.iter().any(|source| source.name == name) {
            return Err(mismatch(format!("required table {name} is missing")));
        }
    }

    for name in ["dovecote_schema", "dovecote_events", "dovecote_deliveries"] {
        let source = sources
            .iter()
            .find(|source| source.name == name)
            .expect("checked above");
        let expected = expected_table_source(migration.sql(), name).map_err(mismatch)?;
        if normalize_sql(&source.sql) != normalize_sql(&expected) {
            return Err(mismatch(format!(
                "table {name} definition is incompatible with schema version {}",
                migration.version()
            )));
        }
    }

    // TEMP triggers and indexes can target a main-schema table, so inspect
    // both catalogs.  Leaving sqlite_temp_master out would allow a caller to
    // add an unreviewed trigger that changes durable invariants for this
    // connection while the main schema still appears exact.
    let extra_objects: Vec<SchemaObject> = query_as(
        "SELECT type, name, COALESCE(tbl_name, '') AS tbl_name FROM sqlite_master WHERE (name LIKE 'dovecote_%' OR tbl_name IN ('dovecote_events', 'dovecote_deliveries')) AND NOT (type = 'table' AND name IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries')) AND NOT (type = 'index' AND name IN ('dovecote_events_tenant_source_event_id', 'dovecote_events_tenant_row', 'dovecote_deliveries_claimable', 'dovecote_deliveries_expired_claims', 'sqlite_autoindex_dovecote_events_1')) UNION ALL SELECT type, name, COALESCE(tbl_name, '') AS tbl_name FROM sqlite_temp_master WHERE name LIKE 'dovecote_%' OR tbl_name IN ('dovecote_events', 'dovecote_deliveries')",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check schema object isolation", source))?;
    if let Some(object) = extra_objects.first() {
        return Err(mismatch(format!(
            "unsupported SQLite schema object {} {} on {}",
            object.object_type, object.name, object.table_name
        )));
    }

    check_index(
        connection,
        "dovecote_events",
        "dovecote_events_tenant_source_event_id",
        true,
        &["tenant_id", "source", "event_id"],
        migration.sql(),
    )
    .await?;
    check_index(
        connection,
        "dovecote_events",
        "dovecote_events_tenant_row",
        false,
        &["tenant_id", "row_id"],
        migration.sql(),
    )
    .await?;
    check_index(
        connection,
        "dovecote_deliveries",
        "dovecote_deliveries_claimable",
        false,
        &["tenant_id", "state", "available_at", "event_row_id"],
        migration.sql(),
    )
    .await?;
    check_index(
        connection,
        "dovecote_deliveries",
        "dovecote_deliveries_expired_claims",
        false,
        &["tenant_id", "state", "claim_expires_at", "event_row_id"],
        migration.sql(),
    )
    .await?;
    check_foreign_key(connection).await?;
    let violations = query("PRAGMA foreign_key_check")
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check foreign-key integrity", source))?;
    if !violations.is_empty() {
        return Err(mismatch("installed schema contains foreign-key violations"));
    }
    Ok(())
}

async fn check_schema_marker(
    connection: &mut SqliteConnection,
    migration: crate::migration::Migration,
) -> Result<(), SchemaError> {
    let markers = query_as::<_, SchemaMarker>(
        "SELECT schema_version, minimum_crate_major, minimum_crate_minor, minimum_crate_patch, rolling_compatible FROM dovecote_schema",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check schema marker", source))?;
    if markers.len() != 1 {
        return Err(mismatch(format!(
            "expected exactly one schema marker row, found {}",
            markers.len()
        )));
    }

    let marker = &markers[0];
    let minimum = migration.compatibility().minimum();
    if marker.schema_version != i64::from(migration.version())
        || marker.minimum_crate_major != i64::from(minimum.major())
        || marker.minimum_crate_minor != i64::from(minimum.minor())
        || marker.minimum_crate_patch != i64::from(minimum.patch())
        || marker.rolling_compatible != if migration.rolling_compatible() { 1 } else { 0 }
    {
        return Err(mismatch("schema marker is incompatible with this adapter"));
    }
    Ok(())
}

fn mismatch(detail: impl Into<String>) -> SchemaError {
    SchemaError::MigrationMismatch {
        detail: detail.into(),
    }
}

#[derive(Clone, Copy)]
struct ColumnSpec {
    name: &'static str,
    kind: &'static str,
    primary_key: bool,
    not_null: bool,
}
impl ColumnSpec {
    const fn required(name: &'static str, kind: &'static str, primary_key: bool) -> Self {
        Self {
            name,
            kind,
            primary_key,
            not_null: !primary_key,
        }
    }
    const fn optional(name: &'static str, kind: &'static str, primary_key: bool) -> Self {
        Self {
            name,
            kind,
            primary_key,
            not_null: false,
        }
    }
}

#[derive(Debug, FromRow)]
struct TableSource {
    name: String,
    sql: String,
}

#[derive(Debug, FromRow)]
struct SchemaObject {
    #[sqlx(rename = "type")]
    object_type: String,
    name: String,
    #[sqlx(rename = "tbl_name")]
    table_name: String,
}

async fn check_columns(
    connection: &mut SqliteConnection,
    table: &str,
    expected: &[ColumnSpec],
) -> Result<(), SchemaError> {
    let sql = sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})"));
    let rows = query(sql)
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check table columns", source))?;
    for spec in expected {
        let Some(row) = rows
            .iter()
            .find(|row| row.try_get::<String, _>("name").ok().as_deref() == Some(spec.name))
        else {
            return Err(mismatch(format!(
                "required column {table}.{} is missing",
                spec.name
            )));
        };

        let kind = row
            .try_get::<String, _>("type")
            .map_err(|_| mismatch(format!("column {table}.{} has no type", spec.name)))?;
        if !kind.eq_ignore_ascii_case(spec.kind) {
            return Err(mismatch(format!(
                "column {table}.{} has type {kind}, expected {}",
                spec.name, spec.kind
            )));
        }

        let pk = row.try_get::<i64, _>("pk").unwrap_or_default();
        if spec.primary_key && pk != 1 {
            return Err(mismatch(format!(
                "column {table}.{} is not the primary key",
                spec.name
            )));
        }

        let not_null = row.try_get::<i64, _>("notnull").unwrap_or_default() != 0;
        if spec.not_null && !not_null {
            return Err(mismatch(format!(
                "column {table}.{} must be NOT NULL",
                spec.name
            )));
        }
    }

    Ok(())
}

async fn check_index(
    connection: &mut SqliteConnection,
    table: &str,
    expected_name: &str,
    unique: bool,
    columns: &[&str],
    migration: &str,
) -> Result<(), SchemaError> {
    let sql = sqlx::AssertSqlSafe(format!("PRAGMA index_list({table})"));
    let indexes = query(sql)
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check schema indexes", source))?;
    let Some(index) = indexes
        .iter()
        .find(|row| row.try_get::<String, _>("name").ok().as_deref() == Some(expected_name))
    else {
        return Err(mismatch(format!(
            "required index {expected_name} is missing"
        )));
    };

    let actual_unique = index.try_get::<i64, _>("unique").unwrap_or_default() != 0;
    if actual_unique != unique {
        return Err(mismatch(format!(
            "index {expected_name} uniqueness is incompatible"
        )));
    }

    let source: Option<String> =
        query_scalar("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?")
            .bind(expected_name)
            .fetch_optional(&mut *connection)
            .await
            .map_err(|source| SchemaError::sql("read schema index definition", source))?;
    let Some(source) = source else {
        return Err(mismatch(format!("index {expected_name} has no definition")));
    };

    let expected = expected_index_source(migration, expected_name).map_err(mismatch)?;
    if normalize_sql(&source) != normalize_sql(&expected) {
        return Err(mismatch(format!(
            "index {expected_name} definition is incompatible"
        )));
    }

    let info_sql = sqlx::AssertSqlSafe(format!("PRAGMA index_info({expected_name})"));
    let info = query(info_sql)
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("read schema index columns", source))?;
    let actual = info
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect::<Vec<_>>();
    if actual
        != columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>()
    {
        return Err(mismatch(format!(
            "index {expected_name} columns are incompatible"
        )));
    }

    if expected_name == "dovecote_events_tenant_source_event_id" {
        let xinfo_sql = sqlx::AssertSqlSafe(format!("PRAGMA index_xinfo({expected_name})"));
        let xinfo = query(xinfo_sql)
            .fetch_all(&mut *connection)
            .await
            .map_err(|source| SchemaError::sql("read identity index collation", source))?;
        let collations = xinfo
            .iter()
            .filter_map(|row| row.try_get::<i64, _>("key").ok().filter(|key| *key != 0))
            .zip(
                xinfo
                    .iter()
                    .filter_map(|row| row.try_get::<String, _>("coll").ok()),
            )
            .map(|(_, collation)| collation)
            .collect::<Vec<_>>();
        if collations
            != [
                "BINARY".to_owned(),
                "BINARY".to_owned(),
                "BINARY".to_owned(),
            ]
        {
            return Err(mismatch("identity index collation is not BINARY"));
        }
    }

    Ok(())
}

async fn check_foreign_key(connection: &mut SqliteConnection) -> Result<(), SchemaError> {
    let rows = query("PRAGMA foreign_key_list(dovecote_deliveries)")
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| SchemaError::sql("check delivery foreign key", source))?;
    let matching = rows
        .iter()
        .filter(|row| row.try_get::<String, _>("table").ok().as_deref() == Some("dovecote_events"))
        .collect::<Vec<_>>();
    if matching.len() != 2 {
        return Err(mismatch("delivery foreign key is missing"));
    }

    let columns = matching
        .iter()
        .map(|row| {
            (
                row.try_get::<String, _>("from").unwrap_or_default(),
                row.try_get::<String, _>("to").unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    if !columns.contains(&("tenant_id".to_owned(), "tenant_id".to_owned()))
        || !columns.contains(&("event_row_id".to_owned(), "row_id".to_owned()))
    {
        return Err(mismatch("delivery foreign key is incompatible"));
    }
    Ok(())
}

fn expected_table_source(migration: &str, table: &str) -> Result<String, String> {
    let needle = format!("CREATE TABLE {table}");
    let start = migration
        .find(&needle)
        .ok_or_else(|| format!("migration does not define {table}"))?;
    let mut depth = 0_u32;
    let mut quoted = false;
    for (offset, character) in migration[start..].char_indices() {
        match character {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth = depth.saturating_add(1),
            ')' if !quoted => depth = depth.saturating_sub(1),
            ';' if !quoted && depth == 0 => return Ok(migration[start..start + offset].to_owned()),
            _ => {}
        }
    }
    Err(format!("migration statement for {table} is unterminated"))
}

fn expected_index_source(migration: &str, index: &str) -> Result<String, String> {
    let needle = if migration.contains(&format!("CREATE UNIQUE INDEX {index}")) {
        format!("CREATE UNIQUE INDEX {index}")
    } else {
        format!("CREATE INDEX {index}")
    };
    let start = migration
        .find(&needle)
        .ok_or_else(|| format!("migration does not define {index}"))?;
    let end = migration[start..]
        .find(';')
        .map(|offset| start + offset)
        .ok_or_else(|| format!("migration statement for {index} is unterminated"))?;
    Ok(migration[start..end].to_owned())
}

fn normalize_sql(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut in_string = false;
    for character in value.chars() {
        match (character, in_string) {
            ('\'', _) => {
                in_string = !in_string;
                normalized.push(character);
            }
            ('"', false) => {
                // SQLite quotes a rebuilt table name after ALTER TABLE RENAME;
                // identifier quoting does not change the table contract.
            }
            (character, true) if !character.is_ascii_whitespace() => {
                normalized.push(character);
            }
            (character, false) if !character.is_ascii_whitespace() => {
                normalized.extend(character.to_lowercase());
            }
            _ => {}
        }
    }
    normalized
}
