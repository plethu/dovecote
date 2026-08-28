//! Typed errors at the MySQL/MariaDB adapter boundary.

use dovecote::{DeliveryState, RowId};
use thiserror::Error;

/// MySQL/MariaDB error categories for failures callers may retry as a whole
/// operation.  The original SQLx error remains available as the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransientKind {
    /// Serialization/current-read failure (`40001`, MariaDB `1020`).
    SerializationFailure,
    /// Deadlock detected (server error `1213`).
    DeadlockDetected,
    /// Statement/query cancellation or lock timeout (server error `1205`).
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
        let database_error = source.as_database_error()?;
        let code = database_error.code()?;
        let number = database_error
            .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
            .map(|error| error.number());
        Self::from_code(code.as_ref(), number)
    }

    pub(crate) fn from_code(code: &str, number: Option<u16>) -> Option<Self> {
        match (code, number) {
            (_, Some(1213)) => Some(Self::DeadlockDetected),
            (_, Some(1205)) => Some(Self::StatementOrLockTimeout),
            // MariaDB can report a current-read conflict as HY000/1020 after
            // a competing unique-key insert. It is retryable at the
            // transaction boundary just like SQLSTATE 40001.
            (_, Some(1020)) => Some(Self::SerializationFailure),
            ("40001", _) => Some(Self::SerializationFailure),
            _ => None,
        }
    }
}

const TENANT_SOURCE_EVENT_ID_KEY: &str = "dovecote_events_tenant_source_event_id";

/// Reports whether an insert failed on Dovecote's tenant-scoped identity key.
///
/// SQLx 0.9 exposes MySQL/MariaDB's error number and message, but no duplicate
/// key field.  The server's `1062`/`23000` packet is therefore the structured
/// category available to this driver; only its exact `for key '…'` suffix is
/// parsed.  A primary-key or tenant-row duplicate remains an ordinary `Sql`
/// error instead of being mistaken for an idempotent identity replay.
pub(crate) fn is_tenant_source_event_id_duplicate(source: &sqlx::Error) -> bool {
    let Some(database_error) = source.as_database_error() else {
        return false;
    };
    let Some(mysql_error) = database_error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
    else {
        return false;
    };
    mysql_error.number() == 1062
        && mysql_error.code() == Some("23000")
        && duplicate_key_name(mysql_error.message()) == Some(TENANT_SOURCE_EVENT_ID_KEY)
}

fn duplicate_key_name(message: &str) -> Option<&str> {
    let (_, suffix) = message.rsplit_once(" for key ")?;
    let key = suffix.strip_prefix('\'')?.strip_suffix('\'')?;
    if key.is_empty() || key.contains('\'') {
        return None;
    }
    Some(key.rsplit('.').next().unwrap_or(key))
}

#[cfg(test)]
mod tests {
    use super::{TransientKind, duplicate_key_name, is_tenant_source_event_id_duplicate};

    #[test]
    fn mysql_transient_error_codes_have_typed_categories() {
        assert_eq!(
            TransientKind::from_code("40001", None),
            Some(TransientKind::SerializationFailure)
        );
        assert_eq!(
            TransientKind::from_code("40001", Some(1213)),
            Some(TransientKind::DeadlockDetected)
        );
        assert_eq!(
            TransientKind::from_code("HY000", Some(1205)),
            Some(TransientKind::StatementOrLockTimeout)
        );
        assert_eq!(
            TransientKind::from_code("HY000", Some(1020)),
            Some(TransientKind::SerializationFailure)
        );
        assert_eq!(TransientKind::from_code("23000", Some(1062)), None);
    }

    #[test]
    fn duplicate_key_parser_requires_the_complete_server_suffix() {
        assert_eq!(
            duplicate_key_name(
                "Duplicate entry 'tenant' for key 'dovecote_events_tenant_source_event_id'"
            ),
            Some("dovecote_events_tenant_source_event_id")
        );
        assert_eq!(
            duplicate_key_name(
                "Duplicate entry 'tenant' for key 'fixture.dovecote_events_tenant_source_event_id'"
            ),
            Some("dovecote_events_tenant_source_event_id")
        );
        assert_eq!(
            duplicate_key_name("Duplicate entry 'tenant' for key 'PRIMARY'"),
            Some("PRIMARY")
        );
        assert_eq!(duplicate_key_name("Duplicate entry 'tenant'"), None);
        assert_eq!(
            duplicate_key_name(
                "Duplicate entry 'tenant' for key 'dovecote_events_tenant_source_event_id' extra"
            ),
            None
        );
    }

    #[test]
    fn identity_duplicate_classifier_has_no_key_name_substring_fallback() {
        assert!(!is_tenant_source_event_id_duplicate(
            &sqlx::Error::Protocol("Duplicate entry 'x' for key 'PRIMARY'".to_owned(),)
        ));
        assert!(!is_tenant_source_event_id_duplicate(
            &sqlx::Error::Protocol(
                "Duplicate entry 'x' for key 'dovecote_events_tenant_source_event_id'".to_owned(),
            )
        ));
    }
}

/// Failure while enqueueing an event in a caller-owned transaction.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnqueueError {
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
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
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
    /// Imported state failed Dovecote validation.
    #[error("invalid imported delivery state: {source}")]
    InvalidState {
        #[source]
        /// Original underlying error.
        source: dovecote::ValidationError,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
}

/// Errors returned by the migration-only delivery finalizer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FinalizeError {
    /// No delivery row exists for the requested event.
    #[error("event row not found")]
    NotFound,
    /// The row is not a canonical imported pending delivery.
    #[error("delivery row {row_id:?} is not a canonical imported pending delivery")]
    StateConflict {
        /// Dovecote row that was not in the expected state.
        row_id: RowId,
    },
    /// Authoritative delivery time failed core validation.
    #[error("invalid authoritative delivery timestamp: {source}")]
    InvalidTimestamp {
        #[source]
        /// Original underlying error.
        source: dovecote::ValidationError,
    },
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
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
        /// Original underlying error.
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
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
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
    /// No delivery row exists for the requested event.
    #[error("event row not found")]
    NotFound,
    /// The requested operation is invalid for the current state.
    #[error("illegal delivery transition from {state:?}")]
    IllegalTransition {
        /// Current delivery state.
        state: DeliveryState,
    },
    /// Another worker reclaimed the delivery or the lease expired.
    #[error("claim was lost")]
    LostClaim,
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
}

/// Errors returned by live and snapshot paging.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PageError {
    /// Stored or returned data could not be represented safely.
    #[error("serialization: {detail}")]
    Serialization {
        /// Diagnostic describing the invalid stored data.
        detail: String,
    },
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
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
/// Failure while verifying the backend or installed Dovecote schema.
#[non_exhaustive]
pub enum SchemaError {
    /// Installed durable schema is incompatible with this adapter.
    #[error("migration mismatch: {detail}")]
    MigrationMismatch {
        /// Diagnostic describing the incompatible schema.
        detail: String,
    },
    /// The active server does not provide the required capabilities.
    #[error("backend mismatch: {detail}")]
    BackendMismatch {
        /// Diagnostic describing the unsupported backend.
        detail: String,
    },
    /// A non-transient SQLx operation failed.
    #[error("{operation}: {source}")]
    Sql {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        #[source]
        /// Original underlying error.
        source: sqlx::Error,
    },
    /// A retryable SQLx operation failed.
    #[error("{operation}: {kind}: {source}")]
    Transient {
        /// Operation being performed when SQL failed.
        operation: &'static str,
        /// Classification useful to a caller's retry policy.
        kind: TransientKind,
        #[source]
        /// Original underlying error.
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
