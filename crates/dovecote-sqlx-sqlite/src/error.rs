//! Typed errors at the SQLite adapter boundary.

use dovecote::{DeliveryState, RowId};
use thiserror::Error;

/// Errors that can be retried by the adapter's bounded busy policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransientKind {
    /// SQLite could not acquire its single-writer lock before the configured
    /// busy timeout. The complete operation has been rolled back.
    BusyExhausted,
}

impl std::fmt::Display for TransientKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SQLite busy timeout exhausted")
    }
}

pub(crate) fn is_busy(source: &sqlx::Error) -> bool {
    source
        .as_database_error()
        .and_then(|error| error.code())
        .and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| code == 5 || code == 6 || code & 0xff == 5 || code & 0xff == 6)
}

/// Errors returned while enqueueing an event.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnqueueError {
    /// The transaction was not opened with SQLite's writer lock.
    #[error("enqueue requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
    /// The adapter's bounded busy policy is not representable by SQLite.
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration {
        /// Diagnostic describing the invalid configuration.
        detail: String,
    },
    /// Existing identity has different immutable event content.
    #[error("idempotency conflict for existing row {existing_row_id:?}")]
    IdempotencyConflict {
        /// Existing Dovecote row that conflicted with the event.
        existing_row_id: RowId,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A caller transaction remained blocked after the configured busy wait.
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        /// Operation being performed when the lock wait was exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl EnqueueError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        if is_busy(&source) {
            Self::BusyExhausted { operation, source }
        } else {
            Self::Sql { operation, source }
        }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
}

/// Errors returned by the migration-only importer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ImportError {
    /// The transaction was not opened with SQLite's writer lock.
    #[error("import requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
    /// Existing identity has different immutable event content.
    #[error("immutable event identity conflict for existing row {existing_row_id:?}")]
    IdentityConflict {
        /// Existing Dovecote row that conflicted with the imported event.
        existing_row_id: RowId,
    },
    /// Existing delivery state differs from the requested imported state.
    #[error("imported delivery state conflict for existing row {existing_row_id:?}")]
    ImportConflict {
        /// Existing Dovecote row whose delivery state conflicted.
        existing_row_id: RowId,
    },
    /// The adapter configuration is not valid for SQLite.
    #[error("invalid SQLite configuration: {detail}")]
    Configuration {
        /// Diagnostic describing the invalid configuration.
        detail: String,
    },
    /// Imported state failed Dovecote validation.
    #[error("invalid imported delivery state: {source}")]
    InvalidState {
        #[source]
        /// Original validation error.
        source: dovecote::ValidationError,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A caller transaction remained blocked after the configured busy wait.
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        /// Operation being performed when the lock wait was exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

/// Errors returned by the migration-only delivery finalizer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FinalizeError {
    /// The transaction was not opened with SQLite's writer lock.
    #[error("finalization requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
    /// No event row exists for the requested delivery.
    #[error("event row not found")]
    NotFound,
    /// The row is not a canonical imported pending delivery.
    #[error("delivery row {row_id:?} is not a canonical imported pending delivery")]
    StateConflict {
        /// Dovecote row that was not in the expected state.
        row_id: RowId,
    },
    /// Authoritative delivery time failed Dovecote validation.
    #[error("invalid authoritative delivery timestamp: {source}")]
    InvalidTimestamp {
        #[source]
        /// Original validation error.
        source: dovecote::ValidationError,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A caller transaction remained blocked after the configured busy wait.
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        /// Operation being performed when the lock wait was exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl FinalizeError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        if is_busy(&source) {
            Self::BusyExhausted { operation, source }
        } else {
            Self::Sql { operation, source }
        }
    }
}

impl ImportError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        if is_busy(&source) {
            Self::BusyExhausted { operation, source }
        } else {
            Self::Sql { operation, source }
        }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
}

/// Errors returned while selecting and claiming a batch of events.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClaimError {
    #[cfg(test)]
    /// Test-only failpoint used to verify claim rollback.
    #[error("test claim failpoint triggered after delivery updates")]
    InjectedFailure,
    /// The delivery attempt counter cannot be incremented.
    #[error("attempt counter overflow for row {row_id:?}")]
    CounterOverflow {
        /// Dovecote row whose attempt counter overflowed.
        row_id: RowId,
    },
    /// The operating system could not provide a fresh claim token.
    #[error("operating-system entropy unavailable: {source}")]
    EntropyUnavailable {
        #[source]
        /// Original entropy error.
        source: getrandom::Error,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The adapter's bounded busy policy is not representable by SQLite.
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration {
        /// Diagnostic describing the invalid configuration.
        detail: String,
    },
    /// A claim could not finish after the configured busy retries.
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        /// Operation being performed when retries were exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl ClaimError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        Self::Sql { operation, source }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }

    pub(crate) fn busy_source(&self) -> Option<&sqlx::Error> {
        match self {
            Self::Sql { source, .. } | Self::BusyExhausted { source, .. } if is_busy(source) => {
                Some(source)
            }
            _ => None,
        }
    }

    pub(crate) fn into_busy_exhausted(self) -> Self {
        match self {
            Self::Sql { operation, source } => Self::BusyExhausted { operation, source },
            other => other,
        }
    }
}

/// Errors returned by claim-token-fenced delivery mutations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MutationError {
    /// No event row exists for the requested delivery.
    #[error("event row not found")]
    NotFound,
    /// The requested mutation is invalid for the current delivery state.
    #[error("illegal delivery transition from {state:?}")]
    IllegalTransition {
        /// Current durable delivery state.
        state: DeliveryState,
    },
    /// The supplied claim token no longer owns the delivery.
    #[error("claim was lost")]
    LostClaim,
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The adapter's bounded busy policy is not representable by SQLite.
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration {
        /// Diagnostic describing the invalid configuration.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A mutation could not finish after the configured busy retries.
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        /// Operation being performed when retries were exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl MutationError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        Self::Sql { operation, source }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
    pub(crate) fn into_busy_exhausted(self) -> Self {
        match self {
            Self::Sql { operation, source } => Self::BusyExhausted { operation, source },
            other => other,
        }
    }
    pub(crate) fn is_busy(&self) -> bool {
        matches!(self, Self::Sql { source, .. } if is_busy(source))
    }
}

/// Errors returned by live and snapshot paging.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PageError {
    /// The snapshot pager has already been finished or rolled back.
    #[error("snapshot pager is closed")]
    Closed,
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A page operation could not finish after the configured busy retries.
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        /// Operation being performed when retries were exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl PageError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        if is_busy(&source) {
            Self::BusyExhausted { operation, source }
        } else {
            Self::Sql { operation, source }
        }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
}

/// Errors returned while checking the installed schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// A schema query could not finish after the configured busy wait.
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        /// Operation being performed when the lock wait was exhausted.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
    /// A non-busy SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying SQLite error.
        source: sqlx::Error,
    },
}

impl SchemaError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        if is_busy(&source) {
            Self::BusyExhausted { operation, source }
        } else {
            Self::Sql { operation, source }
        }
    }
}
