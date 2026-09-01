use std::str::FromStr;

use serde_json::Value;

use crate::{
    bounds::{
        MAX_CONTENT_TYPE_BYTES, MAX_EVENT_ID_BYTES, MAX_EVENT_TYPE_BYTES, MAX_PARTITION_KEY_BYTES,
        MAX_SCHEMA_URI_BYTES, MAX_SOURCE_BYTES, MAX_STREAM_BYTES, MAX_SUBJECT_BYTES,
        MAX_TENANT_ID_BYTES,
    },
    error::{ValidationError, ValidationKind},
    validation::{validate_string, validate_uri_reference},
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated application routing stream name.
pub struct StreamName(String);

impl StreamName {
    /// Creates a stream name using Dovecote's portable routing grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("stream", &value, Some(MAX_STREAM_BYTES), false)?;
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(ValidationError::new("stream", ValidationKind::Characters));
        }

        if !value.as_bytes()[0].is_ascii_alphanumeric() {
            return Err(ValidationError::new("stream", ValidationKind::Characters));
        }
        Ok(Self(value))
    }

    /// Returns the stream name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! bounded_string {
    ($name:ident, $field:literal, $maximum:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        #[doc = concat!("Validated bounded ", $field, ".")]
        pub struct $name(String);

        impl $name {
            /// Creates the validated value.
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                let value = value.into();
                validate_string($field, &value, Some($maximum), false)?;
                Ok(Self(value))
            }

            /// Returns the value as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

bounded_string!(EventId, "event id", MAX_EVENT_ID_BYTES);
bounded_string!(EventType, "event type", MAX_EVENT_TYPE_BYTES);
bounded_string!(EventSubject, "subject", MAX_SUBJECT_BYTES);
bounded_string!(PartitionKey, "partition key", MAX_PARTITION_KEY_BYTES);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated bounded tenant identity.
pub struct TenantId(String);

impl TenantId {
    /// Creates a tenant identity, rejecting empty and whitespace-only values.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("tenant id", &value, Some(MAX_TENANT_ID_BYTES), false)?;
        if value.trim().is_empty() {
            return Err(ValidationError::new("tenant id", ValidationKind::Empty));
        }
        Ok(Self(value))
    }

    /// Returns the value as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated CloudEvents source URI-reference.
pub struct EventSource(String);

impl EventSource {
    /// Creates a source URI-reference.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_uri_reference("source", &value, Some(MAX_SOURCE_BYTES), false)?;
        Ok(Self(value))
    }

    /// Returns the source as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated absolute URI used by schema and extension values.
pub struct AbsoluteUri(String);

impl AbsoluteUri {
    /// Parses and validates an absolute URI.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("URI", &value, None, false)?;
        fluent_uri::Uri::parse(value.as_str())
            .map_err(|_| ValidationError::new("URI", ValidationKind::Syntax))?;
        Ok(Self(value))
    }

    /// Returns the URI as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated RFC 3986 URI-reference.
pub struct UriReference(String);

impl UriReference {
    /// Parses and validates a URI-reference.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_uri_reference("URI-reference", &value, None, false)?;
        Ok(Self(value))
    }

    /// Returns the URI-reference as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated absolute CloudEvents schema URI.
pub struct SchemaUri(AbsoluteUri);

impl SchemaUri {
    /// Parses and validates a bounded absolute schema URI.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("schema URI", &value, Some(MAX_SCHEMA_URI_BYTES), false)?;
        Ok(Self(AbsoluteUri::new(value)?))
    }

    /// Returns the schema URI as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// Validated media type for event data.
pub struct ContentType(String);

impl ContentType {
    /// Parses a media type within the public byte bound.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string(
            "datacontenttype",
            &value,
            Some(MAX_CONTENT_TYPE_BYTES),
            false,
        )?;
        mime::Mime::from_str(&value)
            .map_err(|_| ValidationError::new("datacontenttype", ValidationKind::MediaType))?;
        Ok(Self(value))
    }

    /// Returns the media type as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this media type is JSON or has a `+json` suffix.
    pub fn is_json(&self) -> bool {
        let media_type =
            mime::Mime::from_str(&self.0).expect("ContentType validates on construction");
        media_type.subtype() == mime::JSON || media_type.suffix() == Some(mime::JSON)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Validated UTF-8 JSON data bytes.
pub struct JsonData(Vec<u8>);

impl JsonData {
    /// Parses exactly one JSON value encoded as UTF-8.
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, ValidationError> {
        let value = value.into();
        std::str::from_utf8(&value)
            .map_err(|_| ValidationError::new("JSON data", ValidationKind::Json))?;
        serde_json::from_slice::<Value>(&value)
            .map_err(|_| ValidationError::new("JSON data", ValidationKind::Json))?;
        Ok(Self(value))
    }

    /// Returns the original validated JSON bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn as_value(&self) -> Value {
        serde_json::from_slice(&self.0).expect("JsonData validates on construction")
    }
}

/// Optional event material, preserving JSON/binary and absent/empty distinctions.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EventData {
    /// UTF-8 JSON data that projects as a JSON value.
    Json(JsonData),
    /// Opaque data bytes that project as base64 in structured mode.
    Binary(Vec<u8>),
}

impl EventData {
    /// Validates and wraps JSON data bytes.
    pub fn json(value: impl Into<Vec<u8>>) -> Result<Self, ValidationError> {
        Ok(Self::Json(JsonData::new(value)?))
    }

    /// Wraps opaque data bytes without interpreting them.
    pub fn binary(value: impl Into<Vec<u8>>) -> Self {
        Self::Binary(value.into())
    }

    /// Returns the exact data bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Json(value) => value.as_bytes(),
            Self::Binary(value) => value,
        }
    }

    /// Returns whether this value contains validated JSON data.
    pub const fn is_json(&self) -> bool {
        matches!(self, Self::Json(_))
    }
}
