use time::OffsetDateTime;

use crate::{
    SPEC_VERSION,
    bounds::{EventSizeLimit, MAX_IDENTITY_BYTES, validate_optional_instant},
    error::{ValidationError, ValidationKind},
    extension::Extensions,
    serialization::{binary_material_bytes, structured_json_bytes},
    value::{
        ContentType, EventData, EventId, EventSource, EventSubject, EventType, PartitionKey,
        SchemaUri, StreamName,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EventContent {
    pub(crate) stream: StreamName,
    pub(crate) id: EventId,
    pub(crate) source: EventSource,
    pub(crate) event_type: EventType,
    pub(crate) subject: Option<EventSubject>,
    pub(crate) time: Option<OffsetDateTime>,
    pub(crate) datacontenttype: Option<ContentType>,
    pub(crate) dataschema: Option<SchemaUri>,
    pub(crate) partitionkey: Option<PartitionKey>,
    pub(crate) extensions: Extensions,
    pub(crate) data: Option<EventData>,
}

/// Validated event content ready to cross into an adapter enqueue operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewEvent {
    pub(crate) content: EventContent,
    size_limit: EventSizeLimit,
}

/// Staged event assembly that postpones cross-field checks until build.
#[derive(Clone, Debug)]
pub struct NewEventBuilder {
    content: EventContent,
}

impl NewEvent {
    /// Builds a validated event with the default portable size limit.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn new(
        stream: StreamName,
        id: EventId,
        source: EventSource,
        event_type: EventType,
    ) -> Result<Self, ValidationError> {
        Self::builder(stream, id, source, event_type).build()
    }

    /// Starts building an event from its required `CloudEvents` and routing fields.
    #[must_use]
    pub fn builder(
        stream: StreamName,
        id: EventId,
        source: EventSource,
        event_type: EventType,
    ) -> NewEventBuilder {
        NewEventBuilder::new(stream, id, source, event_type)
    }

    /// Revalidates the event using the limit captured during construction.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.validate_with_limit(self.size_limit)
    }

    /// Revalidates the event against a caller-supplied logical size limit.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn validate_with_limit(&self, limit: EventSizeLimit) -> Result<(), ValidationError> {
        validate_content(&self.content, limit)
    }

    /// Returns the logical size limit captured during construction.
    #[must_use]
    pub const fn size_limit(&self) -> EventSizeLimit {
        self.size_limit
    }

    /// Computes the larger structured-or-binary logical material size.
    ///
    /// # Errors
    /// Returns an error if the event cannot be serialized within the portable size
    /// representation.
    pub fn portable_size(&self) -> Result<usize, ValidationError> {
        portable_size(&self.content)
    }

    /// Converts this validated event into the adapter-facing stored form.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn into_stored(self) -> Result<StoredEvent, ValidationError> {
        StoredEvent::from_new(self)
    }
}

impl NewEventBuilder {
    fn new(stream: StreamName, id: EventId, source: EventSource, event_type: EventType) -> Self {
        Self {
            content: EventContent {
                stream,
                id,
                source,
                event_type,
                subject: None,
                time: None,
                datacontenttype: None,
                dataschema: None,
                partitionkey: None,
                extensions: Extensions::new(),
                data: None,
            },
        }
    }

    /// Adds a `CloudEvents` subject.
    #[must_use]
    pub fn subject(mut self, value: EventSubject) -> Self {
        self.content.subject = Some(value);
        self
    }

    /// Adds an occurrence timestamp, stored canonically in UTC.
    #[must_use]
    pub const fn time(mut self, value: OffsetDateTime) -> Self {
        self.content.time = Some(value.to_offset(time::UtcOffset::UTC));
        self
    }

    /// Adds the media type for the event data.
    #[must_use]
    pub fn datacontenttype(mut self, value: ContentType) -> Self {
        self.content.datacontenttype = Some(value);
        self
    }

    /// Adds an absolute `CloudEvents` schema URI.
    #[must_use]
    pub fn dataschema(mut self, value: SchemaUri) -> Self {
        self.content.dataschema = Some(value);
        self
    }

    /// Adds the `CloudEvents` partition-key extension and routing value.
    #[must_use]
    pub fn partitionkey(mut self, value: PartitionKey) -> Self {
        self.content.partitionkey = Some(value);
        self
    }

    /// Replaces the complete validated extension set.
    #[must_use]
    pub fn extensions(mut self, value: Extensions) -> Self {
        self.content.extensions = value;
        self
    }

    /// Adds event data, preserving absent, empty, JSON, and binary distinctions.
    #[must_use]
    pub fn data(mut self, value: EventData) -> Self {
        self.content.data = Some(value);
        self
    }

    /// Finalizes the builder with the default portable size limit.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn build(self) -> Result<NewEvent, ValidationError> {
        self.build_with_limit(EventSizeLimit::default())
    }

    /// Finalizes the builder with an explicit logical size limit.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn build_with_limit(self, limit: EventSizeLimit) -> Result<NewEvent, ValidationError> {
        validate_content(&self.content, limit)?;
        Ok(NewEvent {
            content: self.content,
            size_limit: limit,
        })
    }
}

macro_rules! event_accessors {
    ($type:ty) => {
        impl $type {
            /// Returns the application routing stream.
            pub const fn stream(&self) -> &StreamName {
                &self.content.stream
            }

            /// Returns the `CloudEvents` event ID.
            pub const fn id(&self) -> &EventId {
                &self.content.id
            }

            /// Returns the `CloudEvents` source URI-reference.
            pub const fn source(&self) -> &EventSource {
                &self.content.source
            }

            /// Returns the `CloudEvents` event type.
            pub const fn event_type(&self) -> &EventType {
                &self.content.event_type
            }

            /// Returns the optional `CloudEvents` subject.
            pub const fn subject(&self) -> Option<&EventSubject> {
                self.content.subject.as_ref()
            }

            /// Returns the optional occurrence time in UTC.
            pub const fn time(&self) -> Option<OffsetDateTime> {
                self.content.time
            }

            /// Returns the optional event data media type.
            pub const fn datacontenttype(&self) -> Option<&ContentType> {
                self.content.datacontenttype.as_ref()
            }

            /// Returns the optional absolute schema URI.
            pub const fn dataschema(&self) -> Option<&SchemaUri> {
                self.content.dataschema.as_ref()
            }

            /// Returns the optional partition key.
            pub const fn partitionkey(&self) -> Option<&PartitionKey> {
                self.content.partitionkey.as_ref()
            }

            /// Returns the canonical extension set.
            pub const fn extensions(&self) -> &Extensions {
                &self.content.extensions
            }

            /// Returns the optional event data.
            pub const fn data(&self) -> Option<&EventData> {
                self.content.data.as_ref()
            }

            /// Returns the fixed `CloudEvents` specification version.
            pub const fn specversion(&self) -> &'static str {
                SPEC_VERSION
            }
        }
    };
}

event_accessors!(NewEvent);

/// Validated immutable event content ready for adapter persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEvent {
    pub(crate) content: EventContent,
}

impl StoredEvent {
    /// Converts a validated new event into stored immutable content.
    ///
    /// # Errors
    /// Returns an error for excessive identity or event size, invalid time or trace
    /// context, or incompatible data and content-type attributes.
    pub fn from_new(event: NewEvent) -> Result<Self, ValidationError> {
        event.validate_with_limit(event.size_limit)?;
        Ok(Self {
            content: event.content,
        })
    }
}

event_accessors!(StoredEvent);

fn validate_content(content: &EventContent, limit: EventSizeLimit) -> Result<(), ValidationError> {
    if content
        .source
        .as_str()
        .len()
        .checked_add(content.id.as_str().len())
        .is_none_or(|length| length > MAX_IDENTITY_BYTES)
    {
        return Err(ValidationError::new(
            "source and event id",
            ValidationKind::Length,
        ));
    }
    validate_optional_instant("time", content.time)?;
    content.extensions.validate_trace_context()?;

    if let Some(data) = &content.data {
        if !data.as_bytes().is_empty() && content.datacontenttype.is_none() {
            return Err(ValidationError::new(
                "datacontenttype",
                ValidationKind::Combination,
            ));
        }

        if data.is_json()
            && !content
                .datacontenttype
                .as_ref()
                .is_some_and(ContentType::is_json)
        {
            return Err(ValidationError::new(
                "datacontenttype",
                ValidationKind::Combination,
            ));
        }
    }

    if portable_size(content)? > limit.get() {
        return Err(ValidationError::new("event", ValidationKind::Size));
    }
    Ok(())
}

fn portable_size(content: &EventContent) -> Result<usize, ValidationError> {
    const CONTENT_TYPE_BYTES: usize = "Content-Type: ".len()
        + crate::projection::StructuredJsonProjection::CONTENT_TYPE.len()
        + "\r\n".len();
    let structured = structured_json_bytes(content)?
        .len()
        .checked_add(CONTENT_TYPE_BYTES)
        .ok_or_else(|| ValidationError::new("event", ValidationKind::Size))?;
    let binary = binary_material_bytes(content)?;
    Ok(structured.max(binary))
}

#[cfg(test)]
#[path = "event_tests.rs"]
mod tests;
