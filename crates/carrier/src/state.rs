use std::fmt;

use time::OffsetDateTime;

use crate::{
    AttemptCount, Failure, QuarantineReason, RowId, StoredEvent, ValidationError, WorkerId,
    bounds::{CLAIM_TOKEN_BYTES, validate_instant},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryState {
    Pending,
    Claimed,
    Delivered,
    Quarantined,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EnqueueOutcome {
    Enqueued { row_id: RowId },
    AlreadyEnqueued { row_id: RowId },
}

#[derive(Clone, Eq, PartialEq)]
pub struct ClaimToken([u8; CLAIM_TOKEN_BYTES]);

impl ClaimToken {
    pub const fn from_bytes(value: [u8; CLAIM_TOKEN_BYTES]) -> Self {
        Self(value)
    }

    pub const fn as_bytes(&self) -> &[u8; CLAIM_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for ClaimToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClaimToken([redacted])")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliverySnapshot {
    Pending {
        available_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    },
    Claimed {
        available_at: OffsetDateTime,
        worker: WorkerId,
        expires_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    },
    Delivered {
        available_at: OffsetDateTime,
        delivered_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    },
    Quarantined {
        available_at: OffsetDateTime,
        quarantined_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
        reason: QuarantineReason,
    },
}

impl DeliverySnapshot {
    pub fn pending(
        available_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        validate_instant("available_at", available_at)?;
        Ok(Self::Pending {
            available_at,
            attempts,
            last_failure,
        })
    }

    pub fn claimed(
        available_at: OffsetDateTime,
        worker: WorkerId,
        expires_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        validate_instant("available_at", available_at)?;
        validate_instant("claim_expires_at", expires_at)?;
        Ok(Self::Claimed {
            available_at,
            worker,
            expires_at,
            attempts,
            last_failure,
        })
    }

    pub fn delivered(
        available_at: OffsetDateTime,
        delivered_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        validate_instant("available_at", available_at)?;
        validate_instant("delivered_at", delivered_at)?;
        Ok(Self::Delivered {
            available_at,
            delivered_at,
            attempts,
            last_failure,
        })
    }

    pub fn quarantined(
        available_at: OffsetDateTime,
        quarantined_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
        reason: QuarantineReason,
    ) -> Result<Self, ValidationError> {
        validate_instant("available_at", available_at)?;
        validate_instant("quarantined_at", quarantined_at)?;
        Ok(Self::Quarantined {
            available_at,
            quarantined_at,
            attempts,
            last_failure,
            reason,
        })
    }

    pub const fn state(&self) -> DeliveryState {
        match self {
            Self::Pending { .. } => DeliveryState::Pending,
            Self::Claimed { .. } => DeliveryState::Claimed,
            Self::Delivered { .. } => DeliveryState::Delivered,
            Self::Quarantined { .. } => DeliveryState::Quarantined,
        }
    }

    pub const fn attempts(&self) -> AttemptCount {
        match self {
            Self::Pending { attempts, .. }
            | Self::Claimed { attempts, .. }
            | Self::Delivered { attempts, .. }
            | Self::Quarantined { attempts, .. } => *attempts,
        }
    }

    pub fn last_failure(&self) -> Option<&Failure> {
        match self {
            Self::Pending { last_failure, .. }
            | Self::Claimed { last_failure, .. }
            | Self::Delivered { last_failure, .. }
            | Self::Quarantined { last_failure, .. } => last_failure.as_ref(),
        }
    }

    pub const fn available_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Pending { available_at, .. }
            | Self::Claimed { available_at, .. }
            | Self::Delivered { available_at, .. }
            | Self::Quarantined { available_at, .. } => Some(*available_at),
        }
    }

    pub const fn claim_expires_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Claimed { expires_at, .. } => Some(*expires_at),
            _ => None,
        }
    }

    pub fn claimed_by(&self) -> Option<&WorkerId> {
        match self {
            Self::Claimed { worker, .. } => Some(worker),
            _ => None,
        }
    }

    pub const fn delivered_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Delivered { delivered_at, .. } => Some(*delivered_at),
            _ => None,
        }
    }

    pub const fn quarantined_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Quarantined { quarantined_at, .. } => Some(*quarantined_at),
            _ => None,
        }
    }

    pub fn quarantine_reason(&self) -> Option<&QuarantineReason> {
        match self {
            Self::Quarantined { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedEvent {
    row_id: RowId,
    event: StoredEvent,
    attempts: AttemptCount,
    claim_token: ClaimToken,
    claimed_by: WorkerId,
    claim_expires_at: OffsetDateTime,
}

impl ClaimedEvent {
    pub fn new(
        row_id: RowId,
        event: StoredEvent,
        attempts: AttemptCount,
        claim_token: ClaimToken,
        claimed_by: WorkerId,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Self, ValidationError> {
        validate_instant("claim_expires_at", claim_expires_at)?;
        Ok(Self {
            row_id,
            event,
            attempts,
            claim_token,
            claimed_by,
            claim_expires_at,
        })
    }

    pub const fn row_id(&self) -> RowId {
        self.row_id
    }

    pub fn event(&self) -> &StoredEvent {
        &self.event
    }

    pub const fn attempts(&self) -> AttemptCount {
        self.attempts
    }

    pub fn claim_token(&self) -> &ClaimToken {
        &self.claim_token
    }

    pub fn claimed_by(&self) -> &WorkerId {
        &self.claimed_by
    }

    pub const fn claim_expires_at(&self) -> OffsetDateTime {
        self.claim_expires_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PagedEvent {
    row_id: RowId,
    event: StoredEvent,
    enqueued_at: OffsetDateTime,
    delivery: DeliverySnapshot,
}

impl PagedEvent {
    pub fn new(
        row_id: RowId,
        event: StoredEvent,
        enqueued_at: OffsetDateTime,
        delivery: DeliverySnapshot,
    ) -> Result<Self, ValidationError> {
        validate_instant("enqueued_at", enqueued_at)?;
        Ok(Self {
            row_id,
            event,
            enqueued_at,
            delivery,
        })
    }

    pub const fn row_id(&self) -> RowId {
        self.row_id
    }

    pub fn event(&self) -> &StoredEvent {
        &self.event
    }

    pub const fn enqueued_at(&self) -> OffsetDateTime {
        self.enqueued_at
    }

    pub fn delivery(&self) -> &DeliverySnapshot {
        &self.delivery
    }
}
