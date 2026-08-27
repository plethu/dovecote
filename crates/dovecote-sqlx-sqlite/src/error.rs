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

#[derive(Debug, Error)]
pub enum EnqueueError {
    #[error("enqueue requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration { detail: String },
    #[error("idempotency conflict for existing row {existing_row_id:?}")]
    IdempotencyConflict { existing_row_id: RowId },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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
pub enum ImportError {
    #[error("import requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
    #[error("immutable event identity conflict for existing row {existing_row_id:?}")]
    IdentityConflict { existing_row_id: RowId },
    #[error("imported delivery state conflict for existing row {existing_row_id:?}")]
    ImportConflict { existing_row_id: RowId },
    #[error("invalid SQLite configuration: {detail}")]
    Configuration { detail: String },
    #[error("invalid imported delivery state: {source}")]
    InvalidState {
        #[source]
        source: dovecote::ValidationError,
    },
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
}

/// Errors returned by the migration-only delivery finalizer.
#[derive(Debug, Error)]
pub enum FinalizeError {
    #[error("finalization requires a SQLite write transaction (BEGIN IMMEDIATE or a prior write)")]
    WriteTransactionRequired,
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
    #[error("{operation}: busy lock exhausted by the caller transaction: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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

#[derive(Debug, Error)]
pub enum ClaimError {
    #[cfg(test)]
    #[error("test claim failpoint triggered after delivery updates")]
    InjectedFailure,
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
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration { detail: String },
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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
    #[error("invalid SQLite busy configuration: {detail}")]
    Configuration { detail: String },
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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

#[derive(Debug, Error)]
pub enum PageError {
    #[error("snapshot pager is closed")]
    Closed,
    #[error("serialization: {detail}")]
    Serialization { detail: String },
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("migration mismatch: {detail}")]
    MigrationMismatch { detail: String },
    #[error("{operation}: busy lock exhausted after bounded retries: {source}")]
    BusyExhausted {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{operation}: {source}")]
    Sql {
        operation: &'static str,
        #[source]
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
