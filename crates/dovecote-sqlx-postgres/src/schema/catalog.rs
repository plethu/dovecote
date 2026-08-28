use super::normalization::normalize_sql;
use crate::error::SchemaError;
use sqlx::{FromRow, PgConnection, query_as};

#[derive(Debug, FromRow)]
pub(crate) struct NamespaceInfo {
    pub(crate) oid: i64,
    pub(crate) name: String,
}

pub(crate) async fn resolve_namespace(
    connection: &mut PgConnection,
) -> Result<NamespaceInfo, SchemaError> {
    query_as::<_, NamespaceInfo>(
        r#"
        SELECT oid::bigint AS oid, nspname AS name
        FROM pg_namespace
        WHERE nspname = current_schema()
        "#,
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("resolve current schema", source))?
    .ok_or_else(|| SchemaError::MigrationMismatch {
        detail: "the transaction has no resolvable current schema".to_owned(),
    })
}

#[derive(Clone, Copy)]
pub(crate) struct ColumnSpec {
    name: &'static str,
    data_type: &'static str,
    maximum_length: Option<i32>,
    nullable: bool,
    identity: bool,
    default_fragment: Option<&'static str>,
}

impl ColumnSpec {
    pub(crate) const fn required(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
    ) -> Self {
        Self {
            name,
            data_type,
            maximum_length,
            nullable: false,
            identity: false,
            default_fragment: None,
        }
    }

    pub(crate) const fn required_identity(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
    ) -> Self {
        Self {
            identity: true,
            ..Self::required(name, data_type, maximum_length)
        }
    }

    pub(crate) const fn optional(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
    ) -> Self {
        Self {
            nullable: true,
            ..Self::required(name, data_type, maximum_length)
        }
    }

    pub(crate) const fn required_with_default(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
        default_fragment: &'static str,
    ) -> Self {
        Self {
            default_fragment: Some(default_fragment),
            ..Self::required(name, data_type, maximum_length)
        }
    }
}

#[derive(Debug, FromRow)]
struct ColumnInfo {
    column_name: String,
    data_type: String,
    character_maximum_length: Option<i32>,
    is_nullable: String,
    column_default: Option<String>,
    is_identity: String,
    identity_generation: Option<String>,
}

pub(crate) async fn check_columns(
    connection: &mut PgConnection,
    schema_name: &str,
    table: &str,
    expected: &[ColumnSpec],
) -> Result<(), SchemaError> {
    let columns = query_as::<_, ColumnInfo>(
        r#"
        SELECT column_name, data_type, character_maximum_length,
               is_nullable, column_default, is_identity, identity_generation
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        "#,
    )
    .bind(schema_name)
    .bind(table)
    .fetch_all(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check columns", source))?;
    if let Some(column) = columns.iter().find(|column| {
        !expected
            .iter()
            .any(|specification| specification.name == column.column_name)
    }) {
        return Err(SchemaError::MigrationMismatch {
            detail: format!("unexpected column {}.{}", table, column.column_name),
        });
    }

    for specification in expected {
        let Some(column) = columns
            .iter()
            .find(|column| column.column_name == specification.name)
        else {
            return Err(SchemaError::MigrationMismatch {
                detail: format!(
                    "required column {}.{} is missing",
                    table, specification.name
                ),
            });
        };
        let default_matches = specification.default_fragment.is_none_or(|fragment| {
            column
                .column_default
                .as_deref()
                .is_some_and(|default| normalize_sql(default) == normalize_sql(fragment))
        });
        let identity_matches = if specification.identity {
            column.is_identity == "YES" && column.identity_generation.as_deref() == Some("ALWAYS")
        } else {
            column.is_identity == "NO"
        };
        if column.data_type != specification.data_type
            || column.character_maximum_length != specification.maximum_length
            || (column.is_nullable == "YES") != specification.nullable
            || !default_matches
            || !identity_matches
        {
            return Err(SchemaError::MigrationMismatch {
                detail: format!("column {}.{} is incompatible", table, specification.name),
            });
        }
    }
    Ok(())
}
