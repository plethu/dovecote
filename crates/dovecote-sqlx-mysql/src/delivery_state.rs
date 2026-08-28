//! Shared predicates for migration delivery-state validation.

use time::PrimitiveDateTime;

pub(crate) struct DeliveryShape<'a> {
    pub(crate) attempts: i64,
    pub(crate) claim_token: Option<&'a [u8]>,
    pub(crate) claimed_by: Option<&'a [u8]>,
    pub(crate) claim_expires_at: Option<PrimitiveDateTime>,
    pub(crate) last_failure_code: Option<&'a [u8]>,
    pub(crate) last_failure_detail: Option<&'a [u8]>,
    pub(crate) quarantined_at: Option<PrimitiveDateTime>,
    pub(crate) quarantine_reason: Option<&'a [u8]>,
    pub(crate) available_at: PrimitiveDateTime,
}

pub(crate) fn is_canonical_delivery_shape(
    shape: DeliveryShape<'_>,
    enqueued_at: PrimitiveDateTime,
) -> bool {
    shape.attempts == 0
        && shape.claim_token.is_none()
        && shape.claimed_by.is_none()
        && shape.claim_expires_at.is_none()
        && shape.last_failure_code.is_none()
        && shape.last_failure_detail.is_none()
        && shape.quarantined_at.is_none()
        && shape.quarantine_reason.is_none()
        && shape.available_at == enqueued_at
}
