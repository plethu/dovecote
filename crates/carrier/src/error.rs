use std::fmt;

use thiserror::Error;

/// Stable low-level reasons for rejecting caller-controlled values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidationKind {
    Empty,
    Length,
    Characters,
    Syntax,
    MediaType,
    Json,
    Combination,
    Range,
    Precision,
    ReservedName,
    Duplicate,
    TraceContext,
    Size,
}

impl ValidationKind {
    /// Returns the code a presentation layer can map to its own catalogue.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Length => "length",
            Self::Characters => "characters",
            Self::Syntax => "syntax",
            Self::MediaType => "media_type",
            Self::Json => "json",
            Self::Combination => "combination",
            Self::Range => "range",
            Self::Precision => "precision",
            Self::ReservedName => "reserved_name",
            Self::Duplicate => "duplicate",
            Self::TraceContext => "trace_context",
            Self::Size => "size",
        }
    }
}

impl fmt::Display for ValidationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Empty => "must not be empty",
            Self::Length => "has an invalid byte length",
            Self::Characters => "contains forbidden characters",
            Self::Syntax => "has invalid syntax",
            Self::MediaType => "must be a valid media type",
            Self::Json => "must contain one valid JSON value encoded as UTF-8",
            Self::Combination => "is invalid in combination with another value",
            Self::Range => "is outside the supported range",
            Self::Precision => "has unsupported precision",
            Self::ReservedName => "uses a reserved name",
            Self::Duplicate => "is already present",
            Self::TraceContext => "does not satisfy the trace-context rules",
            Self::Size => "exceeds the configured event-size limit",
        };
        formatter.write_str(message)
    }
}

/// The operation boundary that produced a validation failure.
///
/// Callers can branch on this value without parsing field names or English
/// diagnostics; the low-level [`ValidationKind`] remains available alongside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidationOperation {
    Event,
    Limit,
    Duration,
    OperationalField,
    State,
}

impl ValidationOperation {
    /// Returns the stable category code for presentation or telemetry.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Event => "invalid_event",
            Self::Limit => "invalid_limit",
            Self::Duration => "invalid_duration",
            Self::OperationalField => "invalid_operational_field",
            Self::State => "invalid_state",
        }
    }
}

/// A caller-controlled value did not satisfy Carrier's portable contract.
///
/// The fields and codes are stable inputs to application error handling. The
/// English projection is kept separate so applications can replace it with a
/// locale or interface appropriate to their users.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("{field}: {kind}")]
pub struct ValidationError {
    field: &'static str,
    operation: ValidationOperation,
    kind: ValidationKind,
}

impl ValidationError {
    pub(crate) const fn new(field: &'static str, kind: ValidationKind) -> Self {
        Self {
            field,
            operation: ValidationOperation::Event,
            kind,
        }
    }

    pub(crate) const fn with_operation(
        field: &'static str,
        operation: ValidationOperation,
        kind: ValidationKind,
    ) -> Self {
        Self {
            field,
            operation,
            kind,
        }
    }

    /// Names the value that failed validation.
    pub const fn field(&self) -> &'static str {
        self.field
    }

    /// Returns the low-level reason for programmatic handling.
    pub const fn kind(&self) -> ValidationKind {
        self.kind
    }

    /// Returns the operation category without requiring string inspection.
    pub const fn operation(&self) -> ValidationOperation {
        self.operation
    }

    /// Returns the low-level reason code.
    pub const fn code(&self) -> &'static str {
        self.kind.code()
    }

    /// Returns the operation-level code for logs, metrics, or a presentation layer.
    pub const fn category_code(&self) -> &'static str {
        self.operation.code()
    }

    /// Returns a locale-neutral diagnostic for command-line and local logs.
    pub fn to_english(&self) -> String {
        self.to_string()
    }
}
