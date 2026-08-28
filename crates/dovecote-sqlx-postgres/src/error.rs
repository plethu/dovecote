//! Typed errors at the PostgreSQL adapter boundary.

use dovecote::{DeliveryState, RowId};
use thiserror::Error;

/// PostgreSQL SQLSTATE categories for failures callers may retry as a whole
/// operation.  The original SQLx error remains available as the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransientKind {
    /// Serialization failure (`40001`).
    SerializationFailure,
    /// Deadlock detected (`40P01`).
    DeadlockDetected,
    /// Statement/query cancellation or lock timeout (`57014`/`55P03`).
    StatementOrLockTimeout,
}

impl std::fmt::Display for TransientKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::SerializationFailure => "serialization failure",
            Self::DeadlockDetected => "deadlock detected",
            Self::StatementOrLockTimeout => "statement or lock timeout",
        };
        formatter.write_str(label)
    }
}

impl TransientKind {
    pub(crate) fn from_sqlx(source: &sqlx::Error) -> Option<Self> {
        Self::from_sqlstate(source.as_database_error()?.code()?.as_ref())
    }

    pub(crate) fn from_sqlstate(code: &str) -> Option<Self> {
        match code {
            "40001" => Some(Self::SerializationFailure),
            "40P01" => Some(Self::DeadlockDetected),
            "57014" | "55P03" => Some(Self::StatementOrLockTimeout),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransientKind;

    #[test]
    fn postgres_transient_sqlstates_have_typed_categories() {
        assert_eq!(
            TransientKind::from_sqlstate("40001"),
            Some(TransientKind::SerializationFailure)
        );
        assert_eq!(
            TransientKind::from_sqlstate("40P01"),
            Some(TransientKind::DeadlockDetected)
        );
        for code in ["57014", "55P03"] {
            assert_eq!(
                TransientKind::from_sqlstate(code),
                Some(TransientKind::StatementOrLockTimeout)
            );
        }
        assert_eq!(TransientKind::from_sqlstate("23505"), None);
    }
}

/// Errors returned while enqueueing an event.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnqueueError {
    /// The immutable event identity already exists with different content.
    #[error("idempotency conflict for existing row {existing_row_id:?}")]
    IdempotencyConflict {
        /// The existing event row whose identity conflicted.
        existing_row_id: RowId,
    },
    #[error("migration mismatch: {detail}")]
    /// The installed schema does not satisfy this adapter's migration contract.
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    /// A database value could not be reconstructed as a valid domain value.
    #[error("serialization: {detail}")]
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

impl EnqueueError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
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
    /// The immutable event identity conflicts with an existing row.
    #[error("immutable event identity conflict for existing row {existing_row_id:?}")]
    IdentityConflict {
        /// The existing event row whose identity conflicted.
        existing_row_id: RowId,
    },
    #[error("imported delivery state conflict for existing row {existing_row_id:?}")]
    /// The imported delivery state conflicts with an existing row.
    ImportConflict {
        /// The existing event row whose delivery state conflicted.
        existing_row_id: RowId,
    },
    #[error("invalid imported delivery state: {source}")]
    /// The supplied legacy delivery state is invalid.
    InvalidState {
        /// The validation failure from the domain state.
        #[source]
        source: dovecote::ValidationError,
    },
    #[error("migration mismatch: {detail}")]
    /// The installed schema does not satisfy this adapter's migration contract.
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    #[error("serialization: {detail}")]
    /// A database value could not be reconstructed as a valid domain value.
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

/// Errors returned by the migration-only delivery finalizer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FinalizeError {
    /// The requested event row does not exist.
    #[error("event row not found")]
    NotFound,
    #[error("delivery row {row_id:?} is not a canonical imported pending delivery")]
    /// The delivery row is not in the canonical imported pending state.
    StateConflict {
        /// The event row whose delivery state conflicted.
        row_id: RowId,
    },
    #[error("invalid authoritative delivery timestamp: {source}")]
    /// The supplied authoritative timestamp is invalid.
    InvalidTimestamp {
        /// The timestamp validation failure from the domain type.
        #[source]
        source: dovecote::ValidationError,
    },
    #[error("migration mismatch: {detail}")]
    /// The installed schema does not satisfy this adapter's migration contract.
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    #[error("serialization: {detail}")]
    /// A database value could not be reconstructed as a valid domain value.
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

impl FinalizeError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
        }
    }
}

impl ImportError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
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
    /// The delivery attempt counter cannot be incremented safely.
    #[error("attempt counter overflow for row {row_id:?}")]
    CounterOverflow {
        /// The event row whose attempt count overflowed.
        row_id: RowId,
    },
    #[error("operating-system entropy unavailable: {source}")]
    /// The operating system could not provide claim-token entropy.
    EntropyUnavailable {
        /// The underlying entropy-provider error.
        #[source]
        source: getrandom::Error,
    },
    #[error("serialization: {detail}")]
    /// A database value could not be reconstructed as a valid domain value.
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("migration mismatch: {detail}")]
    /// The installed schema does not satisfy this adapter's migration contract.
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

impl ClaimError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
        }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
}

/// Errors returned by fenced post-claim mutations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MutationError {
    /// The requested event row does not exist.
    #[error("event row not found")]
    NotFound,
    #[error("illegal delivery transition from {state:?}")]
    /// The current delivery state cannot perform this mutation.
    IllegalTransition {
        /// The delivery state that rejected the mutation.
        state: DeliveryState,
    },
    #[error("claim was lost")]
    /// The claim token is stale or the lease has expired.
    LostClaim,
    #[error("migration mismatch: {detail}")]
    /// The installed schema does not satisfy this adapter's migration contract.
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    #[error("serialization: {detail}")]
    /// A database value could not be reconstructed as a valid domain value.
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

/// Errors returned by live and snapshot paging.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PageError {
    /// A database value could not be reconstructed as a valid domain value.
    #[error("serialization: {detail}")]
    Serialization {
        /// Details describing the invalid stored value.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

impl PageError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
        }
    }

    pub(crate) fn serialization(detail: impl Into<String>) -> Self {
        Self::Serialization {
            detail: detail.into(),
        }
    }
}

impl MutationError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
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
    /// The installed schema does not satisfy this adapter's migration contract.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Details identifying the incompatible schema contract.
        detail: String,
    },
    #[error("{operation}: {source}")]
    /// A non-transient SQL operation failed.
    Sql {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    /// A SQL operation failed with a retryable PostgreSQL condition.
    Transient {
        /// The adapter operation that failed.
        operation: &'static str,
        /// The retryable PostgreSQL failure category.
        kind: TransientKind,
        /// The underlying SQLx error.
        #[source]
        source: sqlx::Error,
    },
}

impl SchemaError {
    pub(crate) fn sql(operation: &'static str, source: sqlx::Error) -> Self {
        match TransientKind::from_sqlx(&source) {
            Some(kind) => Self::Transient {
                operation,
                kind,
                source,
            },
            None => Self::Sql { operation, source },
        }
    }
}
