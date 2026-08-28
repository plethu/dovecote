//! Migration-only finalisation of a legacy publisher's delivery.
//!
//! This is intentionally not part of the ordinary fenced acknowledgement
//! API. A migration caller supplies the Dovecote delivery row id and the
//! authoritative timestamp recorded by the legacy publisher. Only the
//! untouched pending shape produced by `import_for_migration` can cross this
//! boundary.

use crate::{
    delivery_state::{DeliveryRow, canonical, canonical_pending},
    error::FinalizeError,
    schema::check_schema_connection,
};
use dovecote::{FinalizeOutcome, ImportedDeliveryState, RowId, TenantId};
use sqlx::{FromRow, Postgres, Transaction, query, query_as};
use time::OffsetDateTime;

/// Finalises one canonical pending migration import in the caller-owned
/// transaction.
///
/// The caller remains responsible for commit or rollback. The event row and
/// delivery row are locked before the state is inspected. An exact rerun with
/// the same delivered timestamp returns [`FinalizeOutcome::AlreadyFinalized`];
/// every other non-canonical, claimed, failed, quarantined, or timestamp-
/// differing state returns a typed conflict. PostgreSQL stores these instants
/// at microsecond precision, which is also enforced by Dovecote validation.
pub(crate) async fn finalize_for_scope<'c>(
    transaction: &mut Transaction<'c, Postgres>,
    tenant_id: &TenantId,
    row_id: RowId,
    delivered_at: OffsetDateTime,
) -> Result<FinalizeOutcome, FinalizeError> {
    ImportedDeliveryState::delivered(delivered_at)
        .map_err(|source| FinalizeError::InvalidTimestamp { source })?;
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;

    let event = query_as::<_, EventRow>(
        "SELECT enqueued_at FROM dovecote_events WHERE tenant_id = $1 AND row_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_str())
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration event", source))?
    .ok_or(FinalizeError::NotFound)?;

    let delivery = query_as::<_, DeliveryRow>(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason, available_at FROM dovecote_deliveries WHERE tenant_id = $1 AND event_row_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_str())
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| FinalizeError::sql("lock migration delivery", source))?
    .ok_or_else(|| FinalizeError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if delivery.state == "delivered"
        && canonical(&delivery, event.enqueued_at)
        && delivery.delivered_at == Some(delivered_at)
    {
        return Ok(FinalizeOutcome::AlreadyFinalized { row_id });
    }

    if !canonical_pending(&delivery, event.enqueued_at) {
        return Err(FinalizeError::StateConflict { row_id });
    }

    let result = query(
        "UPDATE dovecote_deliveries SET state = 'delivered', delivered_at = $1 WHERE tenant_id = $2 AND event_row_id = $3 AND state = 'pending' AND attempts = 0 AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND last_failure_code IS NULL AND last_failure_detail IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL AND available_at = $4",
    )
    .bind(delivered_at)
    .bind(tenant_id.as_str())
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
    enqueued_at: OffsetDateTime,
}

fn map_schema_error(error: crate::SchemaError) -> FinalizeError {
    match error {
        crate::SchemaError::MigrationMismatch { detail } => {
            FinalizeError::MigrationMismatch { detail }
        }
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
