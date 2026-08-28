//! Canonical delivery-row shapes shared by migration workflows.

use dovecote::ImportedDeliveryState;
use sqlx::FromRow;
use time::OffsetDateTime;

/// The delivery columns needed to compare migration state idempotently.
#[derive(Debug, FromRow)]
pub(crate) struct DeliveryRow {
    pub(crate) state: String,
    pub(crate) attempts: i64,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_expires_at: Option<OffsetDateTime>,
    pub(crate) last_failure_code: Option<String>,
    pub(crate) last_failure_detail: Option<String>,
    pub(crate) delivered_at: Option<OffsetDateTime>,
    pub(crate) quarantined_at: Option<OffsetDateTime>,
    pub(crate) quarantine_reason: Option<String>,
    pub(crate) available_at: OffsetDateTime,
}

/// Returns whether all non-state delivery columns have their canonical values.
pub(crate) fn canonical(row: &DeliveryRow, enqueued_at: OffsetDateTime) -> bool {
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
pub(crate) fn canonical_pending(row: &DeliveryRow, enqueued_at: OffsetDateTime) -> bool {
    canonical(row, enqueued_at) && row.state == "pending" && row.delivered_at.is_none()
}

/// Compares an existing row with one supported imported state.
pub(crate) fn matches_import(
    row: &DeliveryRow,
    state: ImportedDeliveryState,
    enqueued_at: OffsetDateTime,
) -> Result<bool, &'static str> {
    let canonical = canonical(row, enqueued_at);
    match state {
        ImportedDeliveryState::Pending => {
            Ok(canonical && row.state == "pending" && row.delivered_at.is_none())
        }
        ImportedDeliveryState::Delivered { delivered_at } => {
            Ok(canonical && row.state == "delivered" && row.delivered_at == Some(delivered_at))
        }
        _ => Err("adapter does not support this imported delivery state"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_row(enqueued_at: OffsetDateTime) -> DeliveryRow {
        DeliveryRow {
            state: "pending".to_owned(),
            attempts: 0,
            claim_token: None,
            claimed_by: None,
            claim_expires_at: None,
            last_failure_code: None,
            last_failure_detail: None,
            delivered_at: None,
            quarantined_at: None,
            quarantine_reason: None,
            available_at: enqueued_at,
        }
    }

    #[test]
    fn canonical_shapes_match_supported_import_states() {
        let enqueued_at = OffsetDateTime::UNIX_EPOCH;
        let pending = canonical_row(enqueued_at);
        assert!(canonical(&pending, enqueued_at));
        assert!(canonical_pending(&pending, enqueued_at));
        assert!(
            matches_import(&pending, ImportedDeliveryState::Pending, enqueued_at)
                .expect("pending is a supported import state")
        );

        let delivered_at = enqueued_at + time::Duration::seconds(1);
        let delivered = DeliveryRow {
            state: "delivered".to_owned(),
            delivered_at: Some(delivered_at),
            ..pending
        };
        assert!(
            matches_import(
                &delivered,
                ImportedDeliveryState::delivered(delivered_at).expect("valid timestamp"),
                enqueued_at
            )
            .expect("delivered is a supported import state")
        );
    }

    #[test]
    fn noncanonical_shapes_and_timestamp_mismatches_are_rejected() {
        let enqueued_at = OffsetDateTime::UNIX_EPOCH;
        let retried = DeliveryRow {
            attempts: 1,
            ..canonical_row(enqueued_at)
        };
        assert!(!canonical(&retried, enqueued_at));
        assert!(!canonical_pending(&retried, enqueued_at));
        assert!(
            !matches_import(&retried, ImportedDeliveryState::Pending, enqueued_at)
                .expect("pending is a supported import state")
        );

        let delivered_at = enqueued_at + time::Duration::seconds(1);
        let delivered = DeliveryRow {
            state: "delivered".to_owned(),
            delivered_at: Some(delivered_at),
            ..canonical_row(enqueued_at)
        };
        let wrong_timestamp =
            ImportedDeliveryState::delivered(delivered_at + time::Duration::seconds(1))
                .expect("valid timestamp");
        assert!(
            !matches_import(&delivered, wrong_timestamp, enqueued_at)
                .expect("delivered is a supported import state")
        );
    }
}
