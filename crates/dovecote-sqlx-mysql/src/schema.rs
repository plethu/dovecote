//! MySQL/MariaDB information-schema verification for the installed schema.

mod catalog;
mod contracts;
mod normalization;

#[cfg(test)]
mod tests;

use crate::{backend, error::SchemaError, migration::current_migration};
use sqlx::{FromRow, MySqlConnection, MySqlPool, query_as};

#[derive(Debug, FromRow)]
struct SchemaMarker {
    schema_version: i32,
    minimum_crate_major: i16,
    minimum_crate_minor: i16,
    minimum_crate_patch: i16,
    rolling_compatible: bool,
}

/// Verifies the active connection against the supported MySQL/MariaDB schema.
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
    check_schema_marker(connection).await?;
    catalog::check_tables_and_columns(connection).await?;
    current_migration().map_err(|detail| SchemaError::MigrationMismatch { detail })?;
    catalog::check_constraints(connection, &info).await?;
    catalog::check_triggers(connection).await?;
    catalog::check_indexes(connection).await
}

async fn check_schema_marker(connection: &mut MySqlConnection) -> Result<(), SchemaError> {
    let markers = query_as::<_, SchemaMarker>(
        "SELECT schema_version, minimum_crate_major, minimum_crate_minor, minimum_crate_patch, rolling_compatible FROM dovecote_schema",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check schema marker", source))?;
    if markers.len() != 1 {
        return Err(SchemaError::MigrationMismatch {
            detail: format!(
                "expected exactly one schema marker row, found {}",
                markers.len()
            ),
        });
    }

    let marker = &markers[0];
    let migration =
        current_migration().map_err(|detail| SchemaError::MigrationMismatch { detail })?;
    let minimum = migration.compatibility().minimum();
    if marker.schema_version != migration.version() as i32
        || marker.minimum_crate_major != minimum.major() as i16
        || marker.minimum_crate_minor != minimum.minor() as i16
        || marker.minimum_crate_patch != minimum.patch() as i16
        || marker.rolling_compatible != migration.rolling_compatible()
    {
        return Err(SchemaError::MigrationMismatch {
            detail: "schema marker is incompatible with this adapter".to_owned(),
        });
    }
    Ok(())
}
