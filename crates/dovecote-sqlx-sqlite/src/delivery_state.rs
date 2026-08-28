//! Canonical delivery-row shapes shared by migration workflows.

use crate::enqueue::format_timestamp;
use dovecote::ImportedDeliveryState;
use sqlx::FromRow;

/// The delivery columns required to compare migration state idempotently.
#[derive(Debug, FromRow)]
pub(crate) struct DeliveryRow {
    pub(crate) state: String,
    pub(crate) attempts: i64,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_expires_at: Option<String>,
    pub(crate) last_failure_code: Option<String>,
    pub(crate) last_failure_detail: Option<String>,
    pub(crate) delivered_at: Option<String>,
    pub(crate) quarantined_at: Option<String>,
    pub(crate) quarantine_reason: Option<String>,
    pub(crate) available_at: String,
}

/// Returns whether all non-state delivery columns have their canonical values.
pub(crate) fn canonical(row: &DeliveryRow, enqueued_at: &str) -> bool {
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

/// Returns whether a row is the untouched pending shape emitted by import.
pub(crate) fn canonical_pending(row: &DeliveryRow, enqueued_at: &str) -> bool {
    canonical(row, enqueued_at) && row.state == "pending" && row.delivered_at.is_none()
}

/// Compares an existing row with one supported imported state.
pub(crate) fn matches_import(
    row: &DeliveryRow,
    state: ImportedDeliveryState,
    enqueued_at: &str,
) -> Result<bool, &'static str> {
    let canonical = canonical(row, enqueued_at);
    match state {
        ImportedDeliveryState::Pending => {
            Ok(canonical && row.state == "pending" && row.delivered_at.is_none())
        }
        ImportedDeliveryState::Delivered { delivered_at } => {
            let delivered_at = format_timestamp(delivered_at);
            Ok(canonical
                && row.state == "delivered"
                && row.delivered_at.as_deref() == Some(delivered_at.as_str()))
        }
        _ => Err("adapter does not support this imported delivery state"),
    }
}
