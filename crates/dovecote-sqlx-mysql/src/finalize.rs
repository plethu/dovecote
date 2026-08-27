//! Migration-only finalisation of a legacy publisher's delivery.
//!
//! This is intentionally not part of the ordinary fenced acknowledgement
//! API. A migration caller supplies the Dovecote delivery row id and the
//! authoritative timestamp recorded by the legacy publisher. Only the
//! untouched pending shape produced by `import_for_migration` can cross this
//! boundary.

use crate::{enqueue::database_datetime, error::FinalizeError, schema::check_schema_connection};
use dovecote::{FinalizeOutcome, ImportedDeliveryState, RowId};
use sqlx::{FromRow, MySql, Transaction, query, query_as};
use time::{OffsetDateTime, PrimitiveDateTime};

/// Finalises one canonical pending migration import in the caller-owned
/// transaction.
///
/// The caller remains responsible for commit or rollback. The event row and
/// delivery row are locked before the state is inspected. An exact rerun with
/// the same delivered timestamp returns [`FinalizeOutcome::AlreadyFinalized`];
/// every other non-canonical, claimed, failed, quarantined, or timestamp-
/// differing state returns a typed conflict. MySQL and MariaDB store these
/// instants as UTC `DATETIME(6)` values; Dovecote validation rejects values
/// outside the portable range or below microsecond precision.
pub async fn finalize_pending_delivery_for_migration<'c>(
    transaction: &mut Transaction<'c, MySql>,
    row_id: RowId,
    delivered_at: OffsetDateTime,
) -> Result<FinalizeOutcome, FinalizeError> {
    ImportedDeliveryState::delivered(delivered_at)
        .map_err(|source| FinalizeError::InvalidTimestamp { source })?;
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;
    let delivered_at = database_datetime(delivered_at);

    let event = query_as::<_, EventRow>(
        "SELECT enqueued_at FROM dovecote_events WHERE row_id = ? FOR UPDATE",
    )
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration event", source))?
    .ok_or(FinalizeError::NotFound)?;

    let delivery = query_as::<_, DeliveryRow>(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason, available_at FROM dovecote_deliveries WHERE event_row_id = ? FOR UPDATE",
    )
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration delivery", source))?
    .ok_or_else(|| FinalizeError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if delivery.state == b"delivered"
        && canonical(&delivery, event.enqueued_at)
        && delivery.delivered_at == Some(delivered_at)
    {
        return Ok(FinalizeOutcome::AlreadyFinalized { row_id });
    }

    if !canonical_pending(&delivery, event.enqueued_at) {
        return Err(FinalizeError::StateConflict { row_id });
    }

    let result = query(
        "UPDATE dovecote_deliveries SET state = _binary 'delivered', delivered_at = ? WHERE event_row_id = ? AND state = _binary 'pending' AND attempts = 0 AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND last_failure_code IS NULL AND last_failure_detail IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL AND available_at = ?",
    )
    .bind(delivered_at)
    .bind(row_id.get())
    .bind(event.enqueued_at)
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
    enqueued_at: PrimitiveDateTime,
}

#[derive(Debug, FromRow)]
struct DeliveryRow {
    state: Vec<u8>,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<PrimitiveDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
    delivered_at: Option<PrimitiveDateTime>,
    quarantined_at: Option<PrimitiveDateTime>,
    quarantine_reason: Option<Vec<u8>>,
    available_at: PrimitiveDateTime,
}

fn canonical(row: &DeliveryRow, enqueued_at: PrimitiveDateTime) -> bool {
    row.attempts == 0
        && row.claim_token.is_none()
        && row.claimed_by.is_none()
        && row.claim_expires_at.is_none()
        && row.last_failure_code.is_none()
        && row.last_failure_detail.is_none()
        && row.quarantined_at.is_none()
        && row.quarantine_reason.is_none()
        && row.available_at == enqueued_at
}

fn canonical_pending(row: &DeliveryRow, enqueued_at: PrimitiveDateTime) -> bool {
    canonical(row, enqueued_at) && row.state == b"pending" && row.delivered_at.is_none()
}

fn map_schema_error(error: crate::SchemaError) -> FinalizeError {
    match error {
        crate::SchemaError::MigrationMismatch { detail } => {
            FinalizeError::MigrationMismatch { detail }
        }
        crate::SchemaError::BackendMismatch { detail } => FinalizeError::BackendMismatch { detail },
        crate::SchemaError::Sql { operation, source } => FinalizeError::Sql { operation, source },
        crate::SchemaError::Transient {
            operation,
            kind,
            source,
        } => FinalizeError::Transient {
            operation,
            kind,
            source,
        },
    }
}
