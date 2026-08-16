use std::str::FromStr;

use serde_json::Value;

use crate::{
    bounds::{
        MAX_CONTENT_TYPE_BYTES, MAX_EVENT_ID_BYTES, MAX_EVENT_TYPE_BYTES, MAX_PARTITION_KEY_BYTES,
        MAX_SCHEMA_URI_BYTES, MAX_SOURCE_BYTES, MAX_STREAM_BYTES, MAX_SUBJECT_BYTES,
    },
    error::{ValidationError, ValidationKind},
    validation::{validate_string, validate_uri_reference},
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StreamName(String);

impl StreamName {
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

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! bounded_string {
    ($name:ident, $field:literal, $maximum:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                let value = value.into();
                validate_string($field, &value, Some($maximum), false)?;
                Ok(Self(value))
            }

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
pub struct EventSource(String);

impl EventSource {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_uri_reference("source", &value, Some(MAX_SOURCE_BYTES), false)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AbsoluteUri(String);

impl AbsoluteUri {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("URI", &value, None, false)?;
        url::Url::parse(&value).map_err(|_| ValidationError::new("URI", ValidationKind::Syntax))?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UriReference(String);

impl UriReference {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_uri_reference("URI-reference", &value, None, false)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SchemaUri(AbsoluteUri);

impl SchemaUri {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("schema URI", &value, Some(MAX_SCHEMA_URI_BYTES), false)?;
        Ok(Self(AbsoluteUri::new(value)?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ContentType(String);

impl ContentType {
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

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_json(&self) -> bool {
        let media_type =
            mime::Mime::from_str(&self.0).expect("ContentType validates on construction");
        media_type.subtype() == mime::JSON || media_type.suffix() == Some(mime::JSON)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonData(Vec<u8>);

impl JsonData {
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, ValidationError> {
        let value = value.into();
        std::str::from_utf8(&value)
            .map_err(|_| ValidationError::new("JSON data", ValidationKind::Json))?;
        serde_json::from_slice::<Value>(&value)
            .map_err(|_| ValidationError::new("JSON data", ValidationKind::Json))?;
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn as_value(&self) -> Value {
        serde_json::from_slice(&self.0).expect("JsonData validates on construction")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EventData {
    Json(JsonData),
    Binary(Vec<u8>),
}

impl EventData {
    pub fn json(value: impl Into<Vec<u8>>) -> Result<Self, ValidationError> {
        Ok(Self::Json(JsonData::new(value)?))
    }

    pub fn binary(value: impl Into<Vec<u8>>) -> Self {
        Self::Binary(value.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Json(value) => value.as_bytes(),
            Self::Binary(value) => value,
        }
    }

    pub const fn is_json(&self) -> bool {
        matches!(self, Self::Json(_))
    }
}
