//! PostgreSQL catalog verification for the installed Dovecote schema.

mod catalog;
mod contracts;
mod normalization;
#[cfg(test)]
mod tests;

use crate::{
    error::SchemaError,
    migration::{SchemaMarker, current_migration, marker_matches_migration},
};
use catalog::{ColumnSpec, check_columns, resolve_namespace};
use contracts::{ConstraintInfo, IndexInfo};
use sqlx::{PgConnection, PgPool, query_as};

/// Verifies the tables and all columns required by schema version 2.
pub async fn check_schema(pool: &PgPool) -> Result<(), SchemaError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| SchemaError::sql("acquire schema-check connection", source))?;
    check_schema_connection(&mut connection).await
}

/// Performs the complete schema check on an already-owned connection.
pub(crate) async fn check_schema_connection(
    connection: &mut PgConnection,
) -> Result<(), SchemaError> {
    let namespace = resolve_namespace(connection).await?;

    let marker_columns = [
        ColumnSpec::required("schema_version", "integer", None),
        ColumnSpec::required("minimum_crate_major", "smallint", None),
        ColumnSpec::required("minimum_crate_minor", "smallint", None),
        ColumnSpec::required("minimum_crate_patch", "smallint", None),
        ColumnSpec::required("rolling_compatible", "boolean", None),
    ];
    check_columns(
        connection,
        &namespace.name,
        "dovecote_schema",
        &marker_columns,
    )
    .await?;
    let markers = query_as::<_, SchemaMarker>(
        r#"
        SELECT schema_version, minimum_crate_major, minimum_crate_minor,
               minimum_crate_patch, rolling_compatible
        FROM dovecote_schema
        ORDER BY schema_version DESC
        "#,
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
    if let Err(detail) = marker_matches_migration(marker, migration) {
        return Err(SchemaError::MigrationMismatch { detail });
    }

    let event_columns = [
        ColumnSpec::required_identity("row_id", "bigint", None),
        ColumnSpec::required("tenant_id", "character varying", Some(255)),
        ColumnSpec::required("stream", "character varying", Some(255)),
        ColumnSpec::required("specversion", "character varying", Some(8)),
        ColumnSpec::required("event_id", "character varying", Some(1024)),
        ColumnSpec::required("source", "character varying", Some(2048)),
        ColumnSpec::required("event_type", "character varying", Some(1024)),
        ColumnSpec::optional("subject", "character varying", Some(2048)),
        ColumnSpec::optional("occurred_at", "timestamp with time zone", None),
        ColumnSpec::optional("datacontenttype", "character varying", Some(255)),
        ColumnSpec::optional("dataschema", "character varying", Some(2048)),
        ColumnSpec::optional("partitionkey", "character varying", Some(255)),
        ColumnSpec::required_with_default("extensions", "text", None, "'{}'::text"),
        ColumnSpec::optional("data_kind", "character varying", Some(6)),
        ColumnSpec::optional("data", "bytea", None),
        ColumnSpec::required_with_default(
            "enqueued_at",
            "timestamp with time zone",
            None,
            "current_timestamp",
        ),
    ];
    let delivery_columns = [
        ColumnSpec::required("event_row_id", "bigint", None),
        ColumnSpec::required("tenant_id", "character varying", Some(255)),
        ColumnSpec::required("state", "character varying", Some(12)),
        ColumnSpec::required_with_default(
            "available_at",
            "timestamp with time zone",
            None,
            "current_timestamp",
        ),
        ColumnSpec::required_with_default("attempts", "bigint", None, "0"),
        ColumnSpec::optional("claim_token", "bytea", None),
        ColumnSpec::optional("claimed_by", "character varying", Some(255)),
        ColumnSpec::optional("claim_expires_at", "timestamp with time zone", None),
        ColumnSpec::optional("last_failure_code", "character varying", Some(128)),
        ColumnSpec::optional("last_failure_detail", "character varying", Some(2048)),
        ColumnSpec::optional("delivered_at", "timestamp with time zone", None),
        ColumnSpec::optional("quarantined_at", "timestamp with time zone", None),
        ColumnSpec::optional("quarantine_reason", "character varying", Some(2048)),
    ];
    check_columns(
        connection,
        &namespace.name,
        "dovecote_events",
        &event_columns,
    )
    .await?;
    check_columns(
        connection,
        &namespace.name,
        "dovecote_deliveries",
        &delivery_columns,
    )
    .await?;

    let expected_constraints = contracts::expected_constraints();

    let constraints = query_as::<_, ConstraintInfo>(
        r#"
        SELECT table_class.relname AS table_name,
               constraint_class.conname AS name,
               constraint_class.contype::text AS kind,
               ARRAY(
                   SELECT attribute.attname::text
                   FROM unnest(constraint_class.conkey) WITH ORDINALITY AS key(attnum, ordinality)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = constraint_class.conrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY key.ordinality
               ) AS columns,
               parent_class.relname AS referenced_table,
               ARRAY(
                   SELECT attribute.attname::text
                   FROM unnest(constraint_class.confkey) WITH ORDINALITY AS key(attnum, ordinality)
                   JOIN pg_attribute attribute
                     ON attribute.attrelid = constraint_class.confrelid
                    AND attribute.attnum = key.attnum
                   ORDER BY key.ordinality
               ) AS referenced_columns,
               CASE WHEN constraint_class.contype = 'f'
                    THEN constraint_class.confdeltype::text END AS delete_action,
               constraint_class.convalidated AS validated,
               constraint_class.condeferrable AS deferrable,
               constraint_class.condeferred AS deferred,
               pg_get_constraintdef(constraint_class.oid) AS definition
        FROM pg_constraint constraint_class
        JOIN pg_class table_class ON table_class.oid = constraint_class.conrelid
        LEFT JOIN pg_class parent_class ON parent_class.oid = constraint_class.confrelid
        WHERE table_class.relnamespace::bigint = $1
          AND (constraint_class.confrelid = 0 OR parent_class.relnamespace::bigint = $1)
          AND table_class.relname IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries')
        "#,
    )
    .bind(namespace.oid)
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check constraints", source))?;
    for expected in &expected_constraints {
        let Some(actual) = constraints
            .iter()
            .find(|constraint| constraint.name == expected.name)
        else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required constraint {} is missing", expected.name),
            });
        };

        if !actual.matches(expected) {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required constraint {} is incompatible", expected.name),
            });
        }
    }

    let expected_constraint_names = expected_constraints
        .iter()
        .map(|constraint| constraint.name)
        .collect::<Vec<_>>();
    if let Some(unexpected) = constraints
        .iter()
        .find(|actual| !is_expected_name(&actual.name, &expected_constraint_names))
    {
        return Err(SchemaError::MigrationMismatch {
            detail: format!("unexpected constraint {}", unexpected.name),
        });
    }

    let expected_indexes = contracts::expected_indexes();

    let indexes = query_as::<_, IndexInfo>(
        r#"
        SELECT table_class.relname AS table_name,
               index_class.relname AS name,
               access_method.amname AS access_method,
               i.indisunique AS is_unique,
               i.indisvalid AS is_valid,
               i.indisready AS is_ready,
               i.indpred IS NOT NULL AS has_predicate,
               i.indnkeyatts AS key_columns,
               i.indnatts AS total_columns,
               COALESCE(
                   ARRAY_AGG(i.indoption[keys.ordinality::integer - 1] ORDER BY keys.ordinality)
                       FILTER (WHERE keys.ordinality <= i.indnkeyatts),
                   ARRAY[]::smallint[]
               ) AS options,
               COALESCE(
                   ARRAY_AGG(a.attname::text ORDER BY keys.ordinality)
                       FILTER (WHERE keys.ordinality <= i.indnkeyatts),
                   ARRAY[]::text[]
               ) AS columns,
               COALESCE(
                   ARRAY_AGG(COALESCE(coll.collname::text, 'default') ORDER BY keys.ordinality)
                       FILTER (WHERE keys.ordinality <= i.indnkeyatts),
                   ARRAY[]::text[]
               ) AS collations
        FROM pg_class table_class
        JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
        JOIN pg_index i ON i.indrelid = table_class.oid
        JOIN pg_class index_class ON index_class.oid = i.indexrelid
        JOIN pg_am access_method ON access_method.oid = index_class.relam
        CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
        JOIN pg_attribute a ON a.attrelid = table_class.oid AND a.attnum = keys.attnum
        LEFT JOIN pg_collation coll ON coll.oid = i.indcollation[keys.ordinality::integer - 1]
        WHERE table_class.relnamespace::bigint = $1
          AND table_class.relname IN ('dovecote_schema', 'dovecote_events', 'dovecote_deliveries')
          AND NOT EXISTS (
              SELECT 1
              FROM pg_constraint constraint_class
              WHERE constraint_class.conindid = index_class.oid
          )
        GROUP BY table_class.relname, index_class.relname, access_method.amname,
                 i.indisunique, i.indisvalid, i.indisready, i.indpred IS NOT NULL,
                 i.indnkeyatts, i.indnatts
        "#,
    )
    .bind(namespace.oid)
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check indexes", source))?;
    for expected in &expected_indexes {
        let Some(actual) = indexes.iter().find(|index| index.name == expected.name) else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required index {} is missing", expected.name),
            });
        };

        if !actual.matches(expected) {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("required index {} is incompatible", expected.name),
            });
        }
    }

    let expected_index_names = expected_indexes
        .iter()
        .map(|index| index.name)
        .collect::<Vec<_>>();
    if let Some(unexpected) = indexes
        .iter()
        .find(|actual| !is_expected_name(&actual.name, &expected_index_names))
    {
        return Err(SchemaError::MigrationMismatch {
            detail: format!("unexpected index {}", unexpected.name),
        });
    }

    Ok(())
}

fn is_expected_name(name: &str, expected: &[&str]) -> bool {
    expected.contains(&name)
}
