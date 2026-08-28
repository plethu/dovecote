//! Migration-only finalisation of a legacy publisher's delivery.
//!
//! This is intentionally not part of the ordinary fenced acknowledgement
//! API. A migration caller supplies the Dovecote delivery row id and the
//! authoritative timestamp recorded by the legacy publisher. Only the
//! untouched pending shape produced by `import_for_migration` can cross this
//! boundary.

use crate::{
    delivery_state::{DeliveryRow, canonical, canonical_pending},
    enqueue::format_timestamp,
    error::FinalizeError,
    schema::check_schema_connection,
    transaction_is_write,
};
use dovecote::{FinalizeOutcome, ImportedDeliveryState, RowId, TenantId};
use sqlx::{FromRow, Sqlite, Transaction, query, query_as};
use time::OffsetDateTime;

/// Finalises one canonical pending migration import in the caller-owned
/// transaction.
///
/// The caller must provide a SQLite write transaction (normally from
/// [`crate::begin_write`]) and remains responsible for commit or rollback.
/// SQLite's single-writer lock serialises the row inspection and update. An
/// exact rerun with the same delivered timestamp returns
/// [`FinalizeOutcome::AlreadyFinalized`]; every other non-canonical, claimed,
/// failed, quarantined, or timestamp-differing state returns a typed conflict.
/// SQLite stores Dovecote instants as canonical RFC3339 text with millisecond
/// precision, so a supplied timestamp must be in the common range and have
/// microsecond precision (the final three digits are zero on this backend).
pub(crate) async fn finalize_for_scope<'c>(
    transaction: &mut Transaction<'c, Sqlite>,
    tenant_id: &TenantId,
    row_id: RowId,
    delivered_at: OffsetDateTime,
) -> Result<FinalizeOutcome, FinalizeError> {
    ImportedDeliveryState::delivered(delivered_at)
        .map_err(|source| FinalizeError::InvalidTimestamp { source })?;
    if !transaction_is_write(transaction)
        .await
        .map_err(|source| FinalizeError::sql("inspect finalization transaction state", source))?
    {
        return Err(FinalizeError::WriteTransactionRequired);
    }
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;
    let delivered_at = format_timestamp(delivered_at);

    let event = query_as::<_, EventRow>(
        "SELECT enqueued_at FROM dovecote_events WHERE tenant_id = ? AND row_id = ?",
    )
    .bind(tenant_id.as_str())
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration event", source))?
    .ok_or(FinalizeError::NotFound)?;

    let delivery = query_as::<_, DeliveryRow>(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason, available_at FROM dovecote_deliveries WHERE tenant_id = ? AND event_row_id = ?",
    )
    .bind(tenant_id.as_str()).bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration delivery", source))?
    .ok_or_else(|| FinalizeError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if delivery.state == "delivered"
        && canonical(&delivery, &event.enqueued_at)
        && delivery.delivered_at.as_deref() == Some(delivered_at.as_str())
    {
        return Ok(FinalizeOutcome::AlreadyFinalized { row_id });
    }

    if !canonical_pending(&delivery, &event.enqueued_at) {
        return Err(FinalizeError::StateConflict { row_id });
    }

    let result = query(
        "UPDATE dovecote_deliveries SET state = 'delivered', delivered_at = ? WHERE tenant_id = ? AND event_row_id = ? AND state = 'pending' AND attempts = 0 AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND last_failure_code IS NULL AND last_failure_detail IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL AND available_at = ?",
    )
    .bind(&delivered_at)
    .bind(tenant_id.as_str()).bind(row_id.get())
    .bind(&event.enqueued_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("finalize migration delivery", source))?;
    if result.rows_affected() != 1 {
        return Err(FinalizeError::StateConflict { row_id });
    }
    Ok(FinalizeOutcome::Finalized { row_id })
}

#[derive(Debug, FromRow)]
struct EventRow {
    enqueued_at: String,
}

fn map_schema_error(error: crate::SchemaError) -> FinalizeError {
    match error {
        crate::SchemaError::MigrationMismatch { detail } => {
            FinalizeError::MigrationMismatch { detail }
        }
        crate::SchemaError::BusyExhausted { operation, source } => {
            FinalizeError::BusyExhausted { operation, source }
        }
        crate::SchemaError::Sql { operation, source } => FinalizeError::Sql { operation, source },
    }
}
