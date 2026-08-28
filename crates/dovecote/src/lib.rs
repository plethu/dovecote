//! Synchronous, runtime-free values for Dovecote's transactional-outbox contract.
//!
//! This crate deliberately does not know about SQLx, an async runtime, a
//! database clock, or a transport. The adapter crates own those effects and
//! use these validated values at their boundaries.
#![warn(missing_docs)]

mod bounds;
mod error;
mod event;
mod extension;
mod projection;
mod serialization;
mod state;
mod validation;
mod value;

pub use bounds::{
    AttemptCount, CLAIM_TOKEN_BYTES, Delay, EventSizeLimit, Failure, Lease, Limit,
    MAX_BACKOFF_OR_DELAY, MAX_CLAIM_OR_PAGE_LIMIT, MAX_CONTENT_TYPE_BYTES, MAX_EVENT_ID_BYTES,
    MAX_EVENT_TYPE_BYTES, MAX_EXTENSION_NAME_BYTES, MAX_FAILURE_CODE_BYTES,
    MAX_FAILURE_DETAIL_BYTES, MAX_IDENTITY_BYTES, MAX_LEASE, MAX_PARTITION_KEY_BYTES,
    MAX_PORTABLE_EVENT_BYTES, MAX_QUARANTINE_REASON_BYTES, MAX_SCHEMA_URI_BYTES, MAX_SOURCE_BYTES,
    MAX_STREAM_BYTES, MAX_SUBJECT_BYTES, MAX_TENANT_ID_BYTES, MAX_TRACESTATE_BYTES,
    MAX_TRACESTATE_KEY_BYTES, MAX_TRACESTATE_MEMBERS, MAX_TRACESTATE_SYSTEM_ID_BYTES,
    MAX_TRACESTATE_TENANT_ID_BYTES, MAX_TRACESTATE_VALUE_BYTES, MAX_WORKER_ID_BYTES,
    QuarantineReason, RowId, TAGGED_EXTENSION_FIELDS, TRACEPARENT_FIELDS, TRACEPARENT_FLAGS_CHARS,
    TRACEPARENT_PARENT_ID_CHARS, TRACEPARENT_TRACE_ID_CHARS, TRACEPARENT_VERSION_CHARS, WorkerId,
};
pub use error::{ValidationError, ValidationKind, ValidationOperation};
pub use event::{NewEvent, NewEventBuilder, StoredEvent};
pub use extension::{
    ExtensionDecodeError, ExtensionName, ExtensionString, ExtensionValue, Extensions, Timestamp,
};
pub use projection::{BinaryProjection, StructuredJsonProjection};
pub use state::{
    ClaimToken, ClaimedEvent, DeliverySnapshot, DeliveryState, EnqueueOutcome, FinalizeOutcome,
    ImportOutcome, ImportedDeliveryState, PagedEvent,
};
pub use value::{
    AbsoluteUri, ContentType, EventData, EventId, EventSource, EventSubject, EventType, JsonData,
    PartitionKey, SchemaUri, StreamName, TenantId, UriReference,
};

/// The CloudEvents version persisted in the durable row and emitted projections.
pub const SPEC_VERSION: &str = "1.0";
