//! Migration-only import into the SQLite Dovecote tables.
//!
//! The legacy schema is intentionally outside this crate. A migration caller
//! extracts and validates its source row, then passes a checked event and one
//! of the portable delivery states below in the caller's write transaction.

use crate::{
    delivery_state::{DeliveryRow, matches_import},
    enqueue::{ExistingEvent, parse_timestamp, same_event, validate_existing_event},
    error::ImportError,
    schema::check_schema_connection,
    transaction_is_write,
};
use dovecote::{ImportOutcome, ImportedDeliveryState, NewEvent, RowId, TenantId};
use sqlx::{FromRow, Sqlite, Transaction, query, query_as, query_scalar};

/// Imports one event and a portable legacy delivery state in the supplied
/// transaction. The caller remains responsible for commit or rollback.
///
/// SQLite supplies database-authoritative millisecond operation time (stored
/// in Dovecote's microsecond representation with a zeroed final three
/// fractional digits) for `enqueued_at` and `available_at`. Claims, claim
/// tokens, retries, and quarantines are not importable.
/// An active legacy claim must finish, expire, or be explicitly fenced before
/// the caller maps that source row to `Pending`.
pub(crate) async fn import_for_scope<'c>(
    transaction: &mut Transaction<'c, Sqlite>,
    tenant_id: &TenantId,
    event: NewEvent,
    state: ImportedDeliveryState,
) -> Result<ImportOutcome, ImportError> {
    state
        .validate()
        .map_err(|source| ImportError::InvalidState { source })?;
    if !transaction_is_write(transaction)
        .await
        .map_err(|source| ImportError::sql("inspect import transaction state", source))?
    {
        return Err(ImportError::WriteTransactionRequired);
    }
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;

    let operation_time =
        query_scalar::<_, String>("SELECT strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')")
            .fetch_one(&mut **transaction)
            .await
            .map_err(|source| ImportError::sql("read import operation time", source))?;
    let operation_time = parse_timestamp(&operation_time).map_err(ImportError::serialization)?;
    let operation_time = crate::enqueue::format_timestamp(operation_time);

    let (data_kind, data) = event.data().map_or((None, None), |data| {
        (
            Some(if data.is_json() { "json" } else { "binary" }),
            Some(data.as_bytes().to_vec()),
        )
    });
    let inserted = query_as::<_, InsertedEvent>(
        "INSERT INTO dovecote_events (tenant_id, stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data, enqueued_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(tenant_id, source, event_id) DO NOTHING RETURNING row_id",
    )
    .bind(tenant_id.as_str())
    .bind(event.stream().as_str())
    .bind(event.specversion())
    .bind(event.id().as_str())
    .bind(event.source().as_str())
    .bind(event.event_type().as_str())
    .bind(event.subject().map(|value| value.as_str()))
    .bind(event.time().map(crate::enqueue::format_timestamp))
    .bind(event.datacontenttype().map(|value| value.as_str()))
    .bind(event.dataschema().map(|value| value.as_str()))
    .bind(event.partitionkey().map(|value| value.as_str()))
    .bind(event.extensions().canonical_json())
    .bind(data_kind)
    .bind(data)
    .bind(&operation_time)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("insert imported event", source))?;

    if let Some(inserted) = inserted {
        let row_id = row_id(inserted.row_id)?;
        insert_delivery(
            transaction,
            tenant_id,
            inserted.row_id,
            &operation_time,
            state,
        )
        .await?;
        return Ok(ImportOutcome::Imported { row_id });
    }

    let existing = query_as::<_, ExistingEvent>(
        "SELECT row_id, stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data, enqueued_at FROM dovecote_events WHERE tenant_id = ? COLLATE BINARY AND source = ? COLLATE BINARY AND event_id = ? COLLATE BINARY",
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
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason, available_at FROM dovecote_deliveries WHERE tenant_id = ? AND event_row_id = ?",
    )
    .bind(tenant_id.as_str()).bind(existing.row_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("find imported delivery", source))?
    .ok_or_else(|| ImportError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if matches_import(&delivery, state, &existing.enqueued_at).map_err(|detail| {
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
    transaction: &mut Transaction<'c, Sqlite>,
    tenant_id: &TenantId,
    event_row_id: i64,
    operation_time: &str,
    state: ImportedDeliveryState,
) -> Result<(), ImportError> {
    let (state_name, delivered_at) = match state {
        ImportedDeliveryState::Pending => ("pending", None),
        ImportedDeliveryState::Delivered { delivered_at } => (
            "delivered",
            Some(crate::enqueue::format_timestamp(delivered_at)),
        ),
        _ => {
            return Err(ImportError::MigrationMismatch {
                detail: "adapter does not support this imported delivery state".to_owned(),
            });
        }
    };
    query(
        "INSERT INTO dovecote_deliveries (tenant_id, event_row_id, state, available_at, attempts, delivered_at) VALUES (?, ?, ?, ?, 0, ?)",
    )
    .bind(tenant_id.as_str()).bind(event_row_id)
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
        crate::SchemaError::BusyExhausted { operation, source } => {
            ImportError::BusyExhausted { operation, source }
        }
        crate::SchemaError::Sql { operation, source } => ImportError::Sql { operation, source },
    }
}
