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

#[derive(Debug, Error)]
pub enum EnqueueError {
    #[error("idempotency conflict for existing row {existing_row_id:?}")]
    IdempotencyConflict { existing_row_id: RowId },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
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
pub enum ImportError {
    #[error("immutable event identity conflict for existing row {existing_row_id:?}")]
    IdentityConflict { existing_row_id: RowId },
    #[error("imported delivery state conflict for existing row {existing_row_id:?}")]
    ImportConflict { existing_row_id: RowId },
    #[error("invalid imported delivery state: {source}")]
    InvalidState {
        #[source]
        source: dovecote::ValidationError,
    },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
        #[source]
        source: sqlx::Error,
    },
}

/// Errors returned by the migration-only delivery finalizer.
#[derive(Debug, Error)]
pub enum FinalizeError {
    #[error("event row not found")]
    NotFound,
    #[error("delivery row {row_id:?} is not a canonical imported pending delivery")]
    StateConflict { row_id: RowId },
    #[error("invalid authoritative delivery timestamp: {source}")]
    InvalidTimestamp {
        #[source]
        source: dovecote::ValidationError,
    },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
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
pub enum ClaimError {
    #[error("attempt counter overflow for row {row_id:?}")]
    CounterOverflow { row_id: RowId },
    #[error("operating-system entropy unavailable: {source}")]
    EntropyUnavailable {
        #[source]
        source: getrandom::Error,
    },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
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
pub enum MutationError {
    #[error("event row not found")]
    NotFound,
    #[error("illegal delivery transition from {state:?}")]
    IllegalTransition { state: DeliveryState },
    #[error("claim was lost")]
    LostClaim,
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
        #[source]
        source: sqlx::Error,
    },
}

/// Errors returned by live and snapshot paging.
#[derive(Debug, Error)]
pub enum PageError {
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
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

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {kind}: {source}")]
    Transient {
        operation: &'static str,
        kind: TransientKind,
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
