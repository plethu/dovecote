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
    pub fn new(
        stream: StreamName,
        id: EventId,
        source: EventSource,
        event_type: EventType,
    ) -> Result<Self, ValidationError> {
        Self::builder(stream, id, source, event_type).build()
    }

    pub fn builder(
        stream: StreamName,
        id: EventId,
        source: EventSource,
        event_type: EventType,
    ) -> NewEventBuilder {
        NewEventBuilder::new(stream, id, source, event_type)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        self.validate_with_limit(self.size_limit)
    }

    pub fn validate_with_limit(&self, limit: EventSizeLimit) -> Result<(), ValidationError> {
        validate_content(&self.content, limit)
    }

    pub const fn size_limit(&self) -> EventSizeLimit {
        self.size_limit
    }

    pub fn portable_size(&self) -> Result<usize, ValidationError> {
        portable_size(&self.content)
    }

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

    pub fn subject(mut self, value: EventSubject) -> Self {
        self.content.subject = Some(value);
        self
    }

    pub fn time(mut self, value: OffsetDateTime) -> Self {
        self.content.time = Some(value);
        self
    }

    pub fn datacontenttype(mut self, value: ContentType) -> Self {
        self.content.datacontenttype = Some(value);
        self
    }

    pub fn dataschema(mut self, value: SchemaUri) -> Self {
        self.content.dataschema = Some(value);
        self
    }

    pub fn partitionkey(mut self, value: PartitionKey) -> Self {
        self.content.partitionkey = Some(value);
        self
    }

    pub fn extensions(mut self, value: Extensions) -> Self {
        self.content.extensions = value;
        self
    }

    pub fn data(mut self, value: EventData) -> Self {
        self.content.data = Some(value);
        self
    }

    pub fn build(self) -> Result<NewEvent, ValidationError> {
        self.build_with_limit(EventSizeLimit::default())
    }

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
            pub fn stream(&self) -> &StreamName {
                &self.content.stream
            }

            pub fn id(&self) -> &EventId {
                &self.content.id
            }

            pub fn source(&self) -> &EventSource {
                &self.content.source
            }

            pub fn event_type(&self) -> &EventType {
                &self.content.event_type
            }

            pub fn subject(&self) -> Option<&EventSubject> {
                self.content.subject.as_ref()
            }

            pub fn time(&self) -> Option<OffsetDateTime> {
                self.content.time
            }

            pub fn datacontenttype(&self) -> Option<&ContentType> {
                self.content.datacontenttype.as_ref()
            }

            pub fn dataschema(&self) -> Option<&SchemaUri> {
                self.content.dataschema.as_ref()
            }

            pub fn partitionkey(&self) -> Option<&PartitionKey> {
                self.content.partitionkey.as_ref()
            }

            pub fn extensions(&self) -> &Extensions {
                &self.content.extensions
            }

            pub fn data(&self) -> Option<&EventData> {
                self.content.data.as_ref()
            }

            pub const fn specversion(&self) -> &'static str {
                SPEC_VERSION
            }
        }
    };
}

event_accessors!(NewEvent);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEvent {
    pub(crate) content: EventContent,
}

impl StoredEvent {
    pub fn from_new(event: NewEvent) -> Result<Self, ValidationError> {
        event.validate_with_limit(event.size_limit)?;
        Ok(Self {
            content: event.content,
        })
    }
}

event_accessors!(StoredEvent);

fn validate_content(content: &EventContent, limit: EventSizeLimit) -> Result<(), ValidationError> {
    if content.source.as_str().len() + content.id.as_str().len() > MAX_IDENTITY_BYTES {
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
    let structured = structured_json_bytes(content)?.len()
        + "Content-Type: ".len()
        + crate::projection::StructuredJsonProjection::CONTENT_TYPE.len()
        + "\r\n".len();
    let binary = binary_material_bytes(content)?;
    Ok(structured.max(binary))
}

#[cfg(test)]
#[path = "event_tests.rs"]
mod tests;
