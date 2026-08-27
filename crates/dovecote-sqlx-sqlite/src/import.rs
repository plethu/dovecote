//! Migration-only import into the SQLite Dovecote tables.
//!
//! The legacy schema is intentionally outside this crate. A migration caller
//! extracts and validates its source row, then passes a checked event and one
//! of the portable delivery states below in the caller's write transaction.

use crate::{
    enqueue::{ExistingEvent, parse_timestamp, same_event, validate_existing_event},
    error::ImportError,
    schema::check_schema_connection,
    transaction_is_write,
};
use dovecote::{ImportOutcome, ImportedDeliveryState, NewEvent, RowId};
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
pub async fn import_for_migration<'c>(
    transaction: &mut Transaction<'c, Sqlite>,
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
        "INSERT INTO dovecote_events (stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data, enqueued_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source, event_id) DO NOTHING RETURNING row_id",
    )
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
        insert_delivery(transaction, inserted.row_id, &operation_time, state).await?;
        return Ok(ImportOutcome::Imported { row_id });
    }

    let existing = query_as::<_, ExistingEvent>(
        "SELECT row_id, stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data, enqueued_at FROM dovecote_events WHERE source = ? COLLATE BINARY AND event_id = ? COLLATE BINARY",
    )
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

    let delivery = query_as::<_, ExistingDelivery>(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason, available_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(existing.row_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("find imported delivery", source))?
    .ok_or_else(|| ImportError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if delivery_matches(&delivery, state, &existing.enqueued_at) {
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
            unreachable!("validated imported delivery state")
        }
    };
    query(
        "INSERT INTO dovecote_deliveries (event_row_id, state, available_at, attempts, delivered_at) VALUES (?, ?, ?, 0, ?)",
    )
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

#[derive(Debug, FromRow)]
struct ExistingDelivery {
    state: String,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<String>,
    claim_expires_at: Option<String>,
    last_failure_code: Option<String>,
    last_failure_detail: Option<String>,
    delivered_at: Option<String>,
    quarantined_at: Option<String>,
    quarantine_reason: Option<String>,
    available_at: String,
}

fn delivery_matches(
    row: &ExistingDelivery,
    state: ImportedDeliveryState,
    enqueued_at: &str,
) -> bool {
    let canonical = row.attempts == 0
        && row.claim_token.is_none()
        && row.claimed_by.is_none()
        && row.claim_expires_at.is_none()
        && row.last_failure_code.is_none()
        && row.last_failure_detail.is_none()
        && row.quarantined_at.is_none()
        && row.quarantine_reason.is_none()
        && row.available_at == enqueued_at;
    match state {
        ImportedDeliveryState::Pending => {
            canonical && row.state == "pending" && row.delivered_at.is_none()
        }
        ImportedDeliveryState::Delivered { delivered_at } => {
            let delivered_at = crate::enqueue::format_timestamp(delivered_at);
            canonical
                && row.state == "delivered"
                && row.delivered_at.as_deref() == Some(delivered_at.as_str())
        }
        _ => false,
    }
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
