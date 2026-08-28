//! Migration-only import into the PostgreSQL Dovecote tables.
//!
//! This module deliberately accepts a caller-owned transaction and a
//! validated [`dovecote::NewEvent`]. It does not read or name a legacy schema;
//! Keepsake and Gatekeep own that extraction and may commit their application
//! state and this import together.

use crate::{
    delivery_state::{DeliveryRow, matches_import},
    enqueue::{ExistingEvent, same_event, validate_existing_event},
    error::ImportError,
    schema::check_schema_connection,
};
use dovecote::{ImportOutcome, ImportedDeliveryState, NewEvent, RowId, TenantId};
use sqlx::{FromRow, Postgres, Transaction, query, query_as, query_scalar};
use time::OffsetDateTime;

/// Imports one event and a portable legacy delivery state in the supplied
/// transaction. The caller remains responsible for commit or rollback.
///
/// Pending imports receive database-authoritative operation time as both
/// `enqueued_at` and `available_at`. Delivered imports preserve the supplied
/// authoritative timestamp at the database's microsecond precision. Claims,
/// claim tokens, retries, and quarantines are not importable.
/// An active legacy claim must finish, expire, or be explicitly fenced before
/// the caller maps that source row to `Pending`.
pub(crate) async fn import_for_scope<'c>(
    transaction: &mut Transaction<'c, Postgres>,
    tenant_id: &TenantId,
    event: NewEvent,
    state: ImportedDeliveryState,
) -> Result<ImportOutcome, ImportError> {
    state
        .validate()
        .map_err(|source| ImportError::InvalidState { source })?;
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;

    // CURRENT_TIMESTAMP is transaction-stable in PostgreSQL. Import time is
    // a persistence operation timestamp, so it must observe the statement's
    // actual database time even when the caller held its transaction open.
    let operation_time = query_scalar::<_, OffsetDateTime>("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| ImportError::sql("read import operation time", source))?;
    // PostgreSQL's timestamptz is microsecond-capable; still validate the
    // actual server value so a changed driver/backend cannot silently round it.
    ImportedDeliveryState::delivered(operation_time)
        .map_err(|source| ImportError::InvalidState { source })?;

    let (data_kind, data) = event.data().map_or((None, None), |data| {
        (
            Some(if data.is_json() { "json" } else { "binary" }),
            Some(data.as_bytes().to_vec()),
        )
    });
    let inserted = query_as::<_, InsertedEvent>(
        r#"
        INSERT INTO dovecote_events
            (tenant_id, stream, specversion, event_id, source, event_type, subject,
             occurred_at, datacontenttype, dataschema, partitionkey, extensions,
             data_kind, data, enqueued_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (tenant_id, source, event_id) DO NOTHING
        RETURNING row_id
        "#,
    )
    .bind(tenant_id.as_str())
    .bind(event.stream().as_str())
    .bind(event.specversion())
    .bind(event.id().as_str())
    .bind(event.source().as_str())
    .bind(event.event_type().as_str())
    .bind(event.subject().map(|value| value.as_str()))
    .bind(event.time())
    .bind(event.datacontenttype().map(|value| value.as_str()))
    .bind(event.dataschema().map(|value| value.as_str()))
    .bind(event.partitionkey().map(|value| value.as_str()))
    .bind(event.extensions().canonical_json())
    .bind(data_kind)
    .bind(data)
    .bind(operation_time)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("insert imported event", source))?;

    if let Some(inserted) = inserted {
        let row_id = row_id(inserted.row_id)?;
        insert_delivery(
            transaction,
            tenant_id,
            inserted.row_id,
            operation_time,
            state,
        )
        .await?;
        return Ok(ImportOutcome::Imported { row_id });
    }

    let existing = query_as::<_, ExistingEvent>(
        r#"
        SELECT row_id, stream, specversion, event_id, source, event_type,
               subject, occurred_at, datacontenttype, dataschema,
               partitionkey, extensions, data_kind, data, enqueued_at
        FROM dovecote_events
        WHERE tenant_id = $1 COLLATE "C"
          AND source = $2 COLLATE "C" AND event_id = $3 COLLATE "C"
        "#,
    )
    .bind(tenant_id.as_str())
    .bind(event.source().as_str())
    .bind(event.id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("find imported event", source))?
    .ok_or_else(|| {
        ImportError::sql(
            "resolve imported event",
            sqlx::Error::Protocol("identity insert returned no row".to_owned()),
        )
    })?;
    let existing_id = row_id(existing.row_id)?;
    validate_existing_event(&existing).map_err(ImportError::serialization)?;
    if !same_event(&event, &existing) {
        return Err(ImportError::IdentityConflict {
            existing_row_id: existing_id,
        });
    }

    let delivery = query_as::<_, DeliveryRow>(
        r#"
        SELECT state, attempts, claim_token, claimed_by, claim_expires_at,
               last_failure_code, last_failure_detail, delivered_at,
               quarantined_at, quarantine_reason, available_at
        FROM dovecote_deliveries
        WHERE tenant_id = $1 AND event_row_id = $2
        "#,
    )
    .bind(tenant_id.as_str())
    .bind(existing.row_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("find imported delivery", source))?
    .ok_or_else(|| ImportError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if matches_import(&delivery, state, existing.enqueued_at).map_err(|detail| {
        ImportError::MigrationMismatch {
            detail: detail.to_owned(),
        }
    })? {
        Ok(ImportOutcome::AlreadyImported {
            row_id: existing_id,
        })
    } else {
        Err(ImportError::ImportConflict {
            existing_row_id: existing_id,
        })
    }
}

async fn insert_delivery<'c>(
    transaction: &mut Transaction<'c, Postgres>,
    tenant_id: &TenantId,
    event_row_id: i64,
    operation_time: OffsetDateTime,
    state: ImportedDeliveryState,
) -> Result<(), ImportError> {
    let (state_name, delivered_at) = match state {
        ImportedDeliveryState::Pending => ("pending", None),
        ImportedDeliveryState::Delivered { delivered_at } => ("delivered", Some(delivered_at)),
        _ => {
            return Err(ImportError::MigrationMismatch {
                detail: "adapter does not support this imported delivery state".to_owned(),
            });
        }
    };
    query(
        "INSERT INTO dovecote_deliveries (tenant_id, event_row_id, state, available_at, attempts, delivered_at) VALUES ($1, $2, $3, $4, 0, $5)",
    )
    .bind(tenant_id.as_str())
    .bind(event_row_id)
    .bind(state_name)
    .bind(operation_time)
    .bind(delivered_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("insert imported delivery", source))?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct InsertedEvent {
    row_id: i64,
}

fn row_id(value: i64) -> Result<RowId, ImportError> {
    RowId::new(value).map_err(|error| ImportError::serialization(error.to_string()))
}

fn map_schema_error(error: crate::SchemaError) -> ImportError {
    match error {
        crate::SchemaError::MigrationMismatch { detail } => {
            ImportError::MigrationMismatch { detail }
        }
        crate::SchemaError::Sql { operation, source } => ImportError::Sql { operation, source },
        crate::SchemaError::Transient {
            operation,
            kind,
            source,
        } => ImportError::Transient {
            operation,
            kind,
            source,
        },
    }
}
