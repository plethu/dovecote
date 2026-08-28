//! PostgreSQL catalog verification for the installed Dovecote schema.

use crate::{
    error::SchemaError,
    migration::{SchemaMarker, current_migration, marker_matches_migration},
};
use sqlx::{FromRow, PgConnection, PgPool, query_as};

/// Verifies the tables and all columns required by schema version 1.
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
    let marker = query_as::<_, SchemaMarker>(
        r#"
        SELECT schema_version, minimum_crate_major, minimum_crate_minor,
               minimum_crate_patch, rolling_compatible
        FROM dovecote_schema
        ORDER BY schema_version DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("check schema marker", source))?
    .ok_or_else(|| SchemaError::MigrationMismatch {
        detail: "schema marker is missing".to_owned(),
    })?;
    let migration =
        current_migration().map_err(|detail| SchemaError::MigrationMismatch { detail })?;
    if let Err(detail) = marker_matches_migration(&marker, migration) {
        return Err(SchemaError::MigrationMismatch { detail });
    }

    let event_columns = [
        ColumnSpec::required_identity("row_id", "bigint", None),
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

    let expected_constraints = [
        ConstraintContract::check(
            "dovecote_schema_version_supported",
            "dovecote_schema",
            &["CHECK ((schema_version = 1))"],
        ),
        ConstraintContract::check(
            "dovecote_schema_minimum_nonnegative",
            "dovecote_schema",
            &[
                "CHECK (((minimum_crate_major >= 0) AND (minimum_crate_minor >= 0) AND (minimum_crate_patch >= 0)))",
            ],
        ),
        ConstraintContract::primary_key(
            "dovecote_schema_pkey",
            "dovecote_schema",
            &["schema_version"],
            &["PRIMARY KEY (schema_version)"],
        ),
        ConstraintContract::check(
            "dovecote_events_row_id_positive",
            "dovecote_events",
            &["CHECK ((row_id > 0))"],
        ),
        ConstraintContract::primary_key(
            "dovecote_events_pkey",
            "dovecote_events",
            &["row_id"],
            &["PRIMARY KEY (row_id)"],
        ),
        ConstraintContract::check(
            "dovecote_events_specversion",
            "dovecote_events",
            &["CHECK (((specversion) = '1.0'))"],
        ),
        ConstraintContract::check(
            "dovecote_events_stream_size",
            "dovecote_events",
            &["CHECK ((octet_length((stream)) <= 255))"],
        ),
        ConstraintContract::check(
            "dovecote_events_event_id_size",
            "dovecote_events",
            &["CHECK ((octet_length((event_id)) <= 1024))"],
        ),
        ConstraintContract::check(
            "dovecote_events_source_size",
            "dovecote_events",
            &["CHECK ((octet_length((source)) <= 2048))"],
        ),
        ConstraintContract::check(
            "dovecote_events_event_type_size",
            "dovecote_events",
            &["CHECK ((octet_length((event_type)) <= 1024))"],
        ),
        ConstraintContract::check(
            "dovecote_events_subject_size",
            "dovecote_events",
            &["CHECK (((subject IS NULL) OR (octet_length((subject)) <= 2048)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_content_type_size",
            "dovecote_events",
            &["CHECK (((datacontenttype IS NULL) OR (octet_length((datacontenttype)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_schema_size",
            "dovecote_events",
            &["CHECK (((dataschema IS NULL) OR (octet_length((dataschema)) <= 2048)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_partition_size",
            "dovecote_events",
            &["CHECK (((partitionkey IS NULL) OR (octet_length((partitionkey)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_identity_size",
            "dovecote_events",
            &["CHECK (((octet_length((source)) + octet_length((event_id))) <= 2048))"],
        ),
        ConstraintContract::check(
            "dovecote_events_data_kind",
            "dovecote_events",
            &["CHECK (((data_kind IS NULL) OR ((data_kind) = ANY ((ARRAY['json', 'binary'])))))"],
        ),
        ConstraintContract::check(
            "dovecote_events_data_pair",
            "dovecote_events",
            &["CHECK (((data_kind IS NULL) = (data IS NULL)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_content_type",
            "dovecote_events",
            &[
                "CHECK (((data IS NULL) OR (octet_length(data) = 0) OR (datacontenttype IS NOT NULL)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_state",
            "dovecote_deliveries",
            &[
                "CHECK (((state) = ANY ((ARRAY['pending', 'claimed', 'delivered', 'quarantined']))))",
            ],
        ),
        ConstraintContract::primary_key(
            "dovecote_deliveries_pkey",
            "dovecote_deliveries",
            &["event_row_id"],
            &["PRIMARY KEY (event_row_id)"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_attempts",
            "dovecote_deliveries",
            &["CHECK ((attempts >= 0))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_token_size",
            "dovecote_deliveries",
            &["CHECK (((claim_token IS NULL) OR (octet_length(claim_token) = 16)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_worker_size",
            "dovecote_deliveries",
            &["CHECK (((claimed_by IS NULL) OR (octet_length((claimed_by)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_code_size",
            "dovecote_deliveries",
            &[
                "CHECK (((last_failure_code IS NULL) OR (octet_length((last_failure_code)) <= 128)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_detail_size",
            "dovecote_deliveries",
            &[
                "CHECK (((last_failure_detail IS NULL) OR (octet_length((last_failure_detail)) <= 2048)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_quarantine_size",
            "dovecote_deliveries",
            &[
                "CHECK (((quarantine_reason IS NULL) OR (octet_length((quarantine_reason)) <= 2048)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_pair",
            "dovecote_deliveries",
            &["CHECK (((last_failure_code IS NULL) = (last_failure_detail IS NULL)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_state_shape",
            "dovecote_deliveries",
            &[
                "CHECK (((((state) = 'pending') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'claimed') AND (claim_token IS NOT NULL) AND (claimed_by IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'delivered') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NOT NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'quarantined') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NOT NULL) AND (quarantine_reason IS NOT NULL))))",
            ],
        ),
        ConstraintContract::foreign_key(
            "dovecote_deliveries_event_row_id_fkey",
            "dovecote_deliveries",
            &["event_row_id"],
            "dovecote_events",
            &["row_id"],
            "r",
            &["FOREIGN KEY (event_row_id) REFERENCES dovecote_events (row_id) ON DELETE RESTRICT"],
        ),
    ];

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
          AND constraint_class.conname = ANY($2)
        "#,
    )
    .bind(namespace.oid)
    .bind(
        expected_constraints
            .iter()
            .map(|constraint| constraint.name.to_owned())
            .collect::<Vec<_>>(),
    )
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

    let expected_indexes = [
        IndexContract::new(
            "dovecote_events_source_event_id",
            "dovecote_events",
            true,
            &["source", "event_id"],
            Some(&["C", "C"]),
        ),
        IndexContract::new(
            "dovecote_deliveries_claimable",
            "dovecote_deliveries",
            false,
            &["state", "available_at", "event_row_id"],
            None,
        ),
        IndexContract::new(
            "dovecote_deliveries_expired_claims",
            "dovecote_deliveries",
            false,
            &["state", "claim_expires_at", "event_row_id"],
            None,
        ),
    ];
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
          AND index_class.relname = ANY($2)
        GROUP BY table_class.relname, index_class.relname, access_method.amname,
                 i.indisunique, i.indisvalid, i.indisready, i.indpred IS NOT NULL,
                 i.indnkeyatts, i.indnatts
        "#,
    )
    .bind(namespace.oid)
    .bind(
        expected_indexes
            .iter()
            .map(|index| index.name.to_owned())
            .collect::<Vec<_>>(),
    )
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

    Ok(())
}

#[derive(Debug, FromRow)]
struct NamespaceInfo {
    oid: i64,
    name: String,
}

async fn resolve_namespace(connection: &mut PgConnection) -> Result<NamespaceInfo, SchemaError> {
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
struct ColumnSpec {
    name: &'static str,
    data_type: &'static str,
    maximum_length: Option<i32>,
    nullable: bool,
    identity: bool,
    default_fragment: Option<&'static str>,
}

impl ColumnSpec {
    const fn required(
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

    const fn required_identity(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
    ) -> Self {
        Self {
            identity: true,
            ..Self::required(name, data_type, maximum_length)
        }
    }

    const fn optional(
        name: &'static str,
        data_type: &'static str,
        maximum_length: Option<i32>,
    ) -> Self {
        Self {
            nullable: true,
            ..Self::required(name, data_type, maximum_length)
        }
    }

    const fn required_with_default(
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

async fn check_columns(
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

#[derive(Debug, FromRow)]
struct ConstraintInfo {
    table_name: String,
    name: String,
    kind: String,
    columns: Vec<String>,
    referenced_table: Option<String>,
    referenced_columns: Vec<String>,
    delete_action: Option<String>,
    validated: bool,
    deferrable: bool,
    deferred: bool,
    definition: String,
}

struct ConstraintContract {
    name: &'static str,
    table_name: &'static str,
    kind: &'static str,
    columns: &'static [&'static str],
    referenced_table: Option<&'static str>,
    referenced_columns: &'static [&'static str],
    delete_action: Option<&'static str>,
    definition_variants: &'static [&'static str],
}

impl ConstraintContract {
    fn check(
        name: &'static str,
        table_name: &'static str,
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "c",
            columns: &[],
            referenced_table: None,
            referenced_columns: &[],
            delete_action: None,
            definition_variants,
        }
    }

    fn primary_key(
        name: &'static str,
        table_name: &'static str,
        columns: &'static [&'static str],
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "p",
            columns,
            referenced_table: None,
            referenced_columns: &[],
            delete_action: None,
            definition_variants,
        }
    }

    fn foreign_key(
        name: &'static str,
        table_name: &'static str,
        columns: &'static [&'static str],
        referenced_table: &'static str,
        referenced_columns: &'static [&'static str],
        delete_action: &'static str,
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "f",
            columns,
            referenced_table: Some(referenced_table),
            referenced_columns,
            delete_action: Some(delete_action),
            definition_variants,
        }
    }
}

impl ConstraintInfo {
    fn matches(&self, expected: &ConstraintContract) -> bool {
        let definition = normalize_sql(&self.definition);
        let columns_match = self.kind == "c"
            || self.columns
                == expected
                    .columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>();
        self.table_name == expected.table_name
            && self.kind == expected.kind
            && columns_match
            && self.referenced_table.as_deref() == expected.referenced_table
            && self.referenced_columns
                == expected
                    .referenced_columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            && self.delete_action.as_deref() == expected.delete_action
            && self.validated
            && !self.deferrable
            && !self.deferred
            && expected
                .definition_variants
                .iter()
                .any(|variant| definition == normalize_sql(variant))
    }
}

#[derive(Debug, FromRow)]
struct IndexInfo {
    table_name: String,
    name: String,
    access_method: String,
    is_unique: bool,
    is_valid: bool,
    is_ready: bool,
    has_predicate: bool,
    key_columns: i16,
    total_columns: i16,
    options: Vec<i16>,
    columns: Vec<String>,
    collations: Vec<String>,
}

struct IndexContract {
    name: &'static str,
    table_name: &'static str,
    is_unique: bool,
    columns: &'static [&'static str],
    collations: Option<&'static [&'static str]>,
}

impl IndexContract {
    fn new(
        name: &'static str,
        table_name: &'static str,
        is_unique: bool,
        columns: &'static [&'static str],
        collations: Option<&'static [&'static str]>,
    ) -> Self {
        Self {
            name,
            table_name,
            is_unique,
            columns,
            collations,
        }
    }
}

impl IndexInfo {
    fn matches(&self, expected: &IndexContract) -> bool {
        self.table_name == expected.table_name
            && self.access_method == "btree"
            && self.is_unique == expected.is_unique
            && self.is_valid
            && self.is_ready
            && !self.has_predicate
            && self.key_columns == i16::try_from(expected.columns.len()).unwrap_or(i16::MAX)
            && self.total_columns == self.key_columns
            && self.options == vec![0; expected.columns.len()]
            && self.columns
                == expected
                    .columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            && expected.collations.is_none_or(|collations| {
                self.collations
                    == collations
                        .iter()
                        .map(|value| (*value).to_owned())
                        .collect::<Vec<_>>()
            })
    }
}

fn normalize_sql(value: &str) -> String {
    let mut value = value.to_ascii_lowercase();
    for cast in [
        "::character varying[]",
        "::character varying",
        "::timestamp with time zone",
        "::timestamp without time zone",
        "::double precision",
        "::numeric",
        "::bigint",
        "::integer",
        "::smallint",
        "::boolean",
        "::text[]",
        "::text",
    ] {
        value = value.replace(cast, "");
    }
    value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::{
        MIGRATIONS, SCHEMA_VERSION, current_crate_version, marker_compatibility,
    };

    #[test]
    fn schema_marker_uses_the_shipped_compatibility_range() {
        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version() == SCHEMA_VERSION)
            .expect("the v1 migration is shipped");
        let marker = SchemaMarker {
            schema_version: 1,
            minimum_crate_major: 0,
            minimum_crate_minor: 1,
            minimum_crate_patch: 0,
            rolling_compatible: false,
        };
        assert_eq!(marker_compatibility(&marker), Ok(migration.compatibility()));
        assert!(marker_matches_migration(&marker, *migration).is_ok());
        assert!(migration.compatibility().contains(current_crate_version()));

        let wrong_version = SchemaMarker {
            schema_version: 2,
            ..marker
        };
        assert!(marker_matches_migration(&wrong_version, *migration).is_err());

        let too_new = SchemaMarker {
            minimum_crate_major: 9,
            ..marker
        };
        assert!(marker_matches_migration(&too_new, *migration).is_err());

        let malformed = SchemaMarker {
            minimum_crate_minor: -1,
            ..marker
        };
        assert!(marker_compatibility(&malformed).is_err());
    }

    #[test]
    fn constraint_and_index_contracts_require_their_live_semantics() {
        let constraint = ConstraintInfo {
            table_name: "dovecote_events".to_owned(),
            name: "dovecote_events_pkey".to_owned(),
            kind: "p".to_owned(),
            columns: vec!["row_id".to_owned()],
            referenced_table: None,
            referenced_columns: Vec::new(),
            delete_action: None,
            validated: true,
            deferrable: false,
            deferred: false,
            definition: "PRIMARY KEY (row_id)".to_owned(),
        };
        let expected = ConstraintContract::primary_key(
            "dovecote_events_pkey",
            "dovecote_events",
            &["row_id"],
            &["PRIMARY KEY (row_id)"],
        );
        assert!(constraint.matches(&expected));
        let wrong_relation = ConstraintInfo {
            table_name: "other".to_owned(),
            ..constraint
        };
        assert!(!wrong_relation.matches(&expected));

        let check = ConstraintInfo {
            table_name: "dovecote_schema".to_owned(),
            name: "dovecote_schema_version_supported".to_owned(),
            kind: "c".to_owned(),
            columns: vec!["schema_version".to_owned()],
            referenced_table: None,
            referenced_columns: Vec::new(),
            delete_action: None,
            validated: true,
            deferrable: false,
            deferred: false,
            definition: "CHECK ((schema_version = 1))".to_owned(),
        };
        let expected_check = ConstraintContract::check(
            "dovecote_schema_version_supported",
            "dovecote_schema",
            &["CHECK ((schema_version = 1))"],
        );
        assert!(check.matches(&expected_check));

        let index = IndexInfo {
            table_name: "dovecote_events".to_owned(),
            name: "dovecote_events_source_event_id".to_owned(),
            access_method: "btree".to_owned(),
            is_unique: true,
            is_valid: true,
            is_ready: true,
            has_predicate: false,
            key_columns: 2,
            total_columns: 2,
            options: vec![0, 0],
            columns: vec!["source".to_owned(), "event_id".to_owned()],
            collations: vec!["C".to_owned(), "C".to_owned()],
        };
        let expected_index = IndexContract::new(
            "dovecote_events_source_event_id",
            "dovecote_events",
            true,
            &["source", "event_id"],
            Some(&["C", "C"]),
        );
        assert!(index.matches(&expected_index));
        let wrong_order = IndexInfo {
            options: vec![1, 0],
            ..index
        };
        assert!(!wrong_order.matches(&expected_index));
    }

    #[test]
    fn pg17_constraint_renderings_match_the_shipped_contracts() {
        let fixtures = [(
            ConstraintInfo {
                table_name: "dovecote_events".to_owned(),
                name: "dovecote_events_identity_size".to_owned(),
                kind: "c".to_owned(),
                columns: vec!["source".to_owned(), "event_id".to_owned()],
                referenced_table: None,
                referenced_columns: Vec::new(),
                delete_action: None,
                validated: true,
                deferrable: false,
                deferred: false,
                definition: "CHECK (((octet_length((source)::text) + octet_length((event_id)::text)) <= 2048))".to_owned(),
            },
            ConstraintContract::check(
                "dovecote_events_identity_size",
                "dovecote_events",
                &["CHECK (((octet_length((source)) + octet_length((event_id))) <= 2048))"],
            ),
        )];

        for (actual, expected) in fixtures {
            assert!(actual.matches(&expected));
        }
    }

    #[test]
    fn sql_normalization_preserves_boolean_grouping() {
        assert_ne!(
            normalize_sql("CHECK ((left IS NULL OR right IS NULL))"),
            normalize_sql("CHECK (((left IS NULL OR right IS NULL)))")
        );
        assert_ne!(
            normalize_sql("CHECK (((left IS NULL) OR (right IS NULL)))"),
            normalize_sql("CHECK (((left IS NULL) AND (right IS NULL)))")
        );
    }
}
