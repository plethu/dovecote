use std::time::Duration;

use time::OffsetDateTime;

use crate::{
    error::{ValidationError, ValidationKind, ValidationOperation},
    validation::validate_string,
};

pub const MAX_PORTABLE_EVENT_BYTES: usize = 65_536;
pub const MAX_CLAIM_OR_PAGE_LIMIT: u32 = 1_000;
pub const MAX_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
pub const MAX_BACKOFF_OR_DELAY: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Byte limits shared by the validated values and durable schemas.
pub const MAX_STREAM_BYTES: usize = 255;
pub const MAX_EVENT_ID_BYTES: usize = 1_024;
pub const MAX_EVENT_TYPE_BYTES: usize = 1_024;
pub const MAX_SUBJECT_BYTES: usize = 2_048;
pub const MAX_SOURCE_BYTES: usize = 2_048;
pub const MAX_SCHEMA_URI_BYTES: usize = 2_048;
pub const MAX_CONTENT_TYPE_BYTES: usize = 255;
pub const MAX_PARTITION_KEY_BYTES: usize = 255;
pub const MAX_IDENTITY_BYTES: usize = 2_048;
pub const MAX_EXTENSION_NAME_BYTES: usize = 20;
pub const MAX_WORKER_ID_BYTES: usize = 255;
pub const MAX_FAILURE_CODE_BYTES: usize = 128;
pub const MAX_FAILURE_DETAIL_BYTES: usize = 2_048;
pub const MAX_QUARANTINE_REASON_BYTES: usize = 2_048;

/// Fixed widths and limits from the W3C Trace Context representation.
pub const TRACEPARENT_VERSION_CHARS: usize = 2;
pub const TRACEPARENT_FIELDS: usize = 4;
pub const TRACEPARENT_TRACE_ID_CHARS: usize = 32;
pub const TRACEPARENT_PARENT_ID_CHARS: usize = 16;
pub const TRACEPARENT_FLAGS_CHARS: usize = 2;
pub const MAX_TRACESTATE_BYTES: usize = 512;
pub const MAX_TRACESTATE_MEMBERS: usize = 32;
pub const MAX_TRACESTATE_KEY_BYTES: usize = 256;
pub const MAX_TRACESTATE_VALUE_BYTES: usize = 256;
pub const MAX_TRACESTATE_TENANT_ID_BYTES: usize = 241;
pub const MAX_TRACESTATE_SYSTEM_ID_BYTES: usize = 14;

/// The durable claim-token width. It is part of the fencing contract.
pub const CLAIM_TOKEN_BYTES: usize = 16;

/// The two keys in each tagged durable extension object: `type` and `value`.
pub const TAGGED_EXTENSION_FIELDS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct RowId(i64);

impl RowId {
    pub const fn new(value: i64) -> Result<Self, ValidationError> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(ValidationError::with_operation(
                "row_id",
                ValidationOperation::State,
                ValidationKind::Range,
            ))
        }
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct AttemptCount(i64);

impl AttemptCount {
    pub const fn new(value: i64) -> Result<Self, ValidationError> {
        if value >= 0 {
            Ok(Self(value))
        } else {
            Err(ValidationError::with_operation(
                "attempts",
                ValidationOperation::State,
                ValidationKind::Range,
            ))
        }
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Limit(u32);

impl Limit {
    pub const fn new(value: u32) -> Result<Self, ValidationError> {
        if value >= 1 && value <= MAX_CLAIM_OR_PAGE_LIMIT {
            Ok(Self(value))
        } else {
            Err(ValidationError::with_operation(
                "limit",
                ValidationOperation::Limit,
                ValidationKind::Range,
            ))
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Lease(Duration);

impl Lease {
    pub fn new(value: Duration) -> Result<Self, ValidationError> {
        validate_duration("lease", value, false, MAX_LEASE)?;
        Ok(Self(value))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Delay(Duration);

impl Delay {
    pub fn new(value: Duration) -> Result<Self, ValidationError> {
        validate_duration("delay or backoff", value, true, MAX_BACKOFF_OR_DELAY)?;
        Ok(Self(value))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

fn validate_duration(
    field: &'static str,
    value: Duration,
    allow_zero: bool,
    maximum: Duration,
) -> Result<(), ValidationError> {
    if (!allow_zero && value.is_zero()) || value > maximum {
        return Err(ValidationError::with_operation(
            field,
            ValidationOperation::Duration,
            ValidationKind::Range,
        ));
    }

    if !value.is_zero() && !value.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(ValidationError::with_operation(
            field,
            ValidationOperation::Duration,
            ValidationKind::Precision,
        ));
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct EventSizeLimit(usize);

impl EventSizeLimit {
    pub const fn new(value: usize) -> Result<Self, ValidationError> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(ValidationError::with_operation(
                "event size limit",
                ValidationOperation::Event,
                ValidationKind::Range,
            ))
        }
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for EventSizeLimit {
    fn default() -> Self {
        Self(MAX_PORTABLE_EVENT_BYTES)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerId(String);

impl WorkerId {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("worker_id", &value, Some(MAX_WORKER_ID_BYTES), false).map_err(
            |error| {
                ValidationError::with_operation(
                    error.field(),
                    ValidationOperation::OperationalField,
                    error.kind(),
                )
            },
        )?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    code: String,
    detail: String,
}

impl Failure {
    pub fn new(
        code: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let code = code.into();
        let detail = detail.into();
        validate_string("failure code", &code, Some(MAX_FAILURE_CODE_BYTES), false).map_err(
            |error| {
                ValidationError::with_operation(
                    error.field(),
                    ValidationOperation::OperationalField,
                    error.kind(),
                )
            },
        )?;
        validate_string(
            "failure detail",
            &detail,
            Some(MAX_FAILURE_DETAIL_BYTES),
            false,
        )
        .map_err(|error| {
            ValidationError::with_operation(
                error.field(),
                ValidationOperation::OperationalField,
                error.kind(),
            )
        })?;
        Ok(Self { code, detail })
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineReason(String);

impl QuarantineReason {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string(
            "quarantine reason",
            &value,
            Some(MAX_QUARANTINE_REASON_BYTES),
            false,
        )
        .map_err(|error| {
            ValidationError::with_operation(
                error.field(),
                ValidationOperation::OperationalField,
                error.kind(),
            )
        })?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub(crate) fn validate_instant(
    field: &'static str,
    value: OffsetDateTime,
) -> Result<(), ValidationError> {
    let minimum = OffsetDateTime::UNIX_EPOCH;
    let maximum = OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31)
            .expect("the documented upper date is valid"),
        time::Time::from_hms_micro(23, 59, 59, 999_999)
            .expect("the documented upper time is valid"),
        time::UtcOffset::UTC,
    );

    if !(minimum..=maximum).contains(&value) {
        return Err(ValidationError::with_operation(
            field,
            ValidationOperation::State,
            ValidationKind::Range,
        ));
    }

    if !value.nanosecond().is_multiple_of(1_000) {
        return Err(ValidationError::with_operation(
            field,
            ValidationOperation::State,
            ValidationKind::Precision,
        ));
    }

    Ok(())
}

pub(crate) fn validate_optional_instant(
    field: &'static str,
    value: Option<OffsetDateTime>,
) -> Result<(), ValidationError> {
    if let Some(value) = value {
        validate_instant(field, value)?;
    }

    Ok(())
}
