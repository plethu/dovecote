use std::fmt;

use time::OffsetDateTime;

use crate::{
    AttemptCount, Failure, QuarantineReason, RowId, StoredEvent, TenantId, ValidationError,
    WorkerId,
    bounds::{CLAIM_TOKEN_BYTES, canonicalize_instant, validate_instant},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Durable lifecycle state of a delivery row.
pub enum DeliveryState {
    /// The row is available for claiming.
    Pending,
    /// The row is fenced to an active worker claim.
    Claimed,
    /// The row has been acknowledged successfully.
    Delivered,
    /// The row is terminal and requires quarantine handling.
    Quarantined,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Result of attempting to enqueue an event.
pub enum EnqueueOutcome {
    /// A new row was inserted.
    Enqueued {
        /// Identifier of the inserted row.
        row_id: RowId,
    },
    /// The immutable event identity was already present.
    AlreadyEnqueued {
        /// Identifier of the existing row.
        row_id: RowId,
    },
}

/// The only legacy delivery states that can be imported into Dovecote.
///
/// A legacy claim is never portable across the cutover because its token and
/// clock belong to the old outbox. An active claim must finish, expire, or be
/// explicitly fenced before the migration caller maps its row to `Pending`.
/// `Delivered` retains the source system's authoritative delivery instant;
/// constructors validate it and store it in UTC without changing the instant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ImportedDeliveryState {
    /// The imported row has not yet been delivered.
    Pending,
    /// The source system already delivered the event at this instant.
    Delivered {
        /// Authoritative delivery instant from the source system.
        delivered_at: OffsetDateTime,
    },
}

impl ImportedDeliveryState {
    /// Constructs a pending import.  The adapter supplies database time for
    /// both `enqueued_at` and `available_at` when it performs the import.
    pub const fn pending() -> Self {
        Self::Pending
    }

    /// Constructs a delivered import after checking the common Dovecote
    /// instant range and exact microsecond precision.
    pub fn delivered(delivered_at: OffsetDateTime) -> Result<Self, ValidationError> {
        let delivered_at = canonicalize_instant("delivered_at", delivered_at)?;
        Ok(Self::Delivered { delivered_at })
    }

    /// Validates a state supplied through the public enum constructor.  This
    /// also protects callers that pattern-match and construct the public
    /// `Delivered` variant directly.
    pub fn validate(self) -> Result<(), ValidationError> {
        match self {
            Self::Pending => Ok(()),
            Self::Delivered { delivered_at } => validate_instant("delivered_at", delivered_at),
        }
    }

    /// Returns the delivered instant, if this is a delivered import, in UTC.
    pub const fn delivered_at(self) -> Option<OffsetDateTime> {
        match self {
            Self::Pending => None,
            Self::Delivered { delivered_at } => Some(delivered_at.to_offset(time::UtcOffset::UTC)),
        }
    }
}

/// Result of a migration import.  Replaying the same immutable event and
/// imported delivery state is an acknowledged no-op.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Result of attempting to import a legacy event and delivery state.
pub enum ImportOutcome {
    /// A new imported row was inserted.
    Imported {
        /// Identifier of the inserted row.
        row_id: RowId,
    },
    /// The same immutable import was already recorded.
    AlreadyImported {
        /// Identifier of the existing row.
        row_id: RowId,
    },
}

/// Result of finalising a canonical pending delivery imported from a legacy
/// publisher.
///
/// Finalisation is deliberately separate from ordinary acknowledgement: the
/// migration caller supplies the authoritative legacy delivery timestamp and
/// the adapters only permit the transition from the untouched pending shape
/// created by [`ImportedDeliveryState::Pending`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Result of finalizing a pending legacy import.
pub enum FinalizeOutcome {
    /// The pending row was finalized.
    Finalized {
        /// Identifier of the finalized row.
        row_id: RowId,
    },
    /// The row had already been finalized.
    AlreadyFinalized {
        /// Identifier of the existing row.
        row_id: RowId,
    },
}

#[derive(Clone, Eq, PartialEq)]
/// Opaque fencing token associated with a delivery claim.
pub struct ClaimToken([u8; CLAIM_TOKEN_BYTES]);

impl ClaimToken {
    /// Creates a claim token from its durable fixed-width bytes.
    pub const fn from_bytes(value: [u8; CLAIM_TOKEN_BYTES]) -> Self {
        Self(value)
    }

    /// Returns the token bytes for persistence or comparison.
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
/// Immutable delivery state paired with an event page or lifecycle result.
pub enum DeliverySnapshot {
    /// A row available for claiming.
    Pending {
        /// Time at which the row becomes available.
        available_at: OffsetDateTime,
        /// Number of delivery attempts so far.
        attempts: AttemptCount,
        /// Last retryable failure, if any.
        last_failure: Option<Failure>,
    },
    /// A row currently claimed by a worker.
    Claimed {
        /// Time at which the row becomes available.
        available_at: OffsetDateTime,
        /// Worker holding the claim.
        worker: WorkerId,
        /// Time at which the claim expires.
        expires_at: OffsetDateTime,
        /// Number of delivery attempts so far.
        attempts: AttemptCount,
        /// Last retryable failure, if any.
        last_failure: Option<Failure>,
    },
    /// A row acknowledged as delivered.
    Delivered {
        /// Time at which the row became available.
        available_at: OffsetDateTime,
        /// Authoritative delivery time.
        delivered_at: OffsetDateTime,
        /// Number of delivery attempts so far.
        attempts: AttemptCount,
        /// Last retryable failure, if any.
        last_failure: Option<Failure>,
    },
    /// A row terminally quarantined after delivery failure.
    Quarantined {
        /// Time at which the row became available.
        available_at: OffsetDateTime,
        /// Time at which quarantine became effective.
        quarantined_at: OffsetDateTime,
        /// Number of delivery attempts so far.
        attempts: AttemptCount,
        /// Last retryable failure, if any.
        last_failure: Option<Failure>,
        /// Redacted terminal reason.
        reason: QuarantineReason,
    },
}

impl DeliverySnapshot {
    /// Constructs a pending snapshot, canonicalizing its timestamp to UTC.
    pub fn pending(
        available_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        let available_at = canonicalize_instant("available_at", available_at)?;
        Ok(Self::Pending {
            available_at,
            attempts,
            last_failure,
        })
    }

    /// Constructs a claimed snapshot, canonicalizing its timestamps to UTC.
    pub fn claimed(
        available_at: OffsetDateTime,
        worker: WorkerId,
        expires_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        let available_at = canonicalize_instant("available_at", available_at)?;
        let expires_at = canonicalize_instant("claim_expires_at", expires_at)?;
        Ok(Self::Claimed {
            available_at,
            worker,
            expires_at,
            attempts,
            last_failure,
        })
    }

    /// Constructs a delivered snapshot, canonicalizing its timestamps to UTC.
    pub fn delivered(
        available_at: OffsetDateTime,
        delivered_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
    ) -> Result<Self, ValidationError> {
        let available_at = canonicalize_instant("available_at", available_at)?;
        let delivered_at = canonicalize_instant("delivered_at", delivered_at)?;
        Ok(Self::Delivered {
            available_at,
            delivered_at,
            attempts,
            last_failure,
        })
    }

    /// Constructs a quarantined snapshot, canonicalizing its timestamps to UTC.
    pub fn quarantined(
        available_at: OffsetDateTime,
        quarantined_at: OffsetDateTime,
        attempts: AttemptCount,
        last_failure: Option<Failure>,
        reason: QuarantineReason,
    ) -> Result<Self, ValidationError> {
        let available_at = canonicalize_instant("available_at", available_at)?;
        let quarantined_at = canonicalize_instant("quarantined_at", quarantined_at)?;
        Ok(Self::Quarantined {
            available_at,
            quarantined_at,
            attempts,
            last_failure,
            reason,
        })
    }

    /// Returns the lifecycle state represented by this snapshot.
    pub const fn state(&self) -> DeliveryState {
        match self {
            Self::Pending { .. } => DeliveryState::Pending,
            Self::Claimed { .. } => DeliveryState::Claimed,
            Self::Delivered { .. } => DeliveryState::Delivered,
            Self::Quarantined { .. } => DeliveryState::Quarantined,
        }
    }

    /// Returns the number of delivery attempts.
    pub const fn attempts(&self) -> AttemptCount {
        match self {
            Self::Pending { attempts, .. }
            | Self::Claimed { attempts, .. }
            | Self::Delivered { attempts, .. }
            | Self::Quarantined { attempts, .. } => *attempts,
        }
    }

    /// Returns the most recent retryable failure, if present.
    pub fn last_failure(&self) -> Option<&Failure> {
        match self {
            Self::Pending { last_failure, .. }
            | Self::Claimed { last_failure, .. }
            | Self::Delivered { last_failure, .. }
            | Self::Quarantined { last_failure, .. } => last_failure.as_ref(),
        }
    }

    /// Returns the availability instant in UTC, if represented.
    pub const fn available_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Pending { available_at, .. }
            | Self::Claimed { available_at, .. }
            | Self::Delivered { available_at, .. }
            | Self::Quarantined { available_at, .. } => {
                Some(available_at.to_offset(time::UtcOffset::UTC))
            }
        }
    }

    /// Returns the claim expiry instant in UTC, if claimed.
    pub const fn claim_expires_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Claimed { expires_at, .. } => Some(expires_at.to_offset(time::UtcOffset::UTC)),
            _ => None,
        }
    }

    /// Returns the claiming worker, if claimed.
    pub fn claimed_by(&self) -> Option<&WorkerId> {
        match self {
            Self::Claimed { worker, .. } => Some(worker),
            _ => None,
        }
    }

    /// Returns the delivery instant in UTC, if delivered.
    pub const fn delivered_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Delivered { delivered_at, .. } => {
                Some(delivered_at.to_offset(time::UtcOffset::UTC))
            }
            _ => None,
        }
    }

    /// Returns the quarantine instant in UTC, if quarantined.
    pub const fn quarantined_at(&self) -> Option<OffsetDateTime> {
        match self {
            Self::Quarantined { quarantined_at, .. } => {
                Some(quarantined_at.to_offset(time::UtcOffset::UTC))
            }
            _ => None,
        }
    }

    /// Returns the quarantine reason, if quarantined.
    pub fn quarantine_reason(&self) -> Option<&QuarantineReason> {
        match self {
            Self::Quarantined { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Event and fencing metadata returned for an active claim.
pub struct ClaimedEvent {
    tenant_id: TenantId,
    row_id: RowId,
    event: StoredEvent,
    attempts: AttemptCount,
    claim_token: ClaimToken,
    claimed_by: WorkerId,
    claim_expires_at: OffsetDateTime,
}

impl ClaimedEvent {
    /// Constructs a claimed event, canonicalizing the expiry timestamp to UTC.
    pub fn new(
        tenant_id: TenantId,
        row_id: RowId,
        event: StoredEvent,
        attempts: AttemptCount,
        claim_token: ClaimToken,
        claimed_by: WorkerId,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Self, ValidationError> {
        let claim_expires_at = canonicalize_instant("claim_expires_at", claim_expires_at)?;
        Ok(Self {
            tenant_id,
            row_id,
            event,
            attempts,
            claim_token,
            claimed_by,
            claim_expires_at,
        })
    }

    /// Returns the storage tenant.
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the delivery row identifier.
    pub const fn row_id(&self) -> RowId {
        self.row_id
    }

    /// Returns the immutable stored event.
    pub fn event(&self) -> &StoredEvent {
        &self.event
    }

    /// Returns the number of attempts recorded at claim time.
    pub const fn attempts(&self) -> AttemptCount {
        self.attempts
    }

    /// Returns the opaque fencing token.
    pub fn claim_token(&self) -> &ClaimToken {
        &self.claim_token
    }

    /// Returns the worker that owns this claim.
    pub fn claimed_by(&self) -> &WorkerId {
        &self.claimed_by
    }

    /// Returns the claim expiry instant in UTC.
    pub const fn claim_expires_at(&self) -> OffsetDateTime {
        self.claim_expires_at.to_offset(time::UtcOffset::UTC)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Event and delivery state returned in a cursor page.
pub struct PagedEvent {
    tenant_id: TenantId,
    row_id: RowId,
    event: StoredEvent,
    enqueued_at: OffsetDateTime,
    delivery: DeliverySnapshot,
}

impl PagedEvent {
    /// Constructs a paged event, canonicalizing its enqueue timestamp to UTC.
    pub fn new(
        tenant_id: TenantId,
        row_id: RowId,
        event: StoredEvent,
        enqueued_at: OffsetDateTime,
        delivery: DeliverySnapshot,
    ) -> Result<Self, ValidationError> {
        let enqueued_at = canonicalize_instant("enqueued_at", enqueued_at)?;
        Ok(Self {
            tenant_id,
            row_id,
            event,
            enqueued_at,
            delivery,
        })
    }

    /// Returns the storage tenant.
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the delivery row identifier.
    pub const fn row_id(&self) -> RowId {
        self.row_id
    }

    /// Returns the immutable stored event.
    pub fn event(&self) -> &StoredEvent {
        &self.event
    }

    /// Returns the enqueue instant in UTC.
    pub const fn enqueued_at(&self) -> OffsetDateTime {
        self.enqueued_at.to_offset(time::UtcOffset::UTC)
    }

    /// Returns the immutable delivery snapshot.
    pub fn delivery(&self) -> &DeliverySnapshot {
        &self.delivery
    }
}
