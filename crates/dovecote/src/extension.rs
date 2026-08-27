use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Map, Value};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    bounds::{MAX_EXTENSION_NAME_BYTES, TAGGED_EXTENSION_FIELDS},
    error::{ValidationError, ValidationKind},
    validation::{format_timestamp, validate_string, validate_traceparent, validate_tracestate},
    value::{AbsoluteUri, UriReference},
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ExtensionName(String);

impl ExtensionName {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_EXTENSION_NAME_BYTES {
            return Err(ValidationError::new(
                "extension name",
                ValidationKind::Length,
            ));
        }

        if !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(ValidationError::new(
                "extension name",
                ValidationKind::Characters,
            ));
        }

        if matches!(
            value.as_str(),
            "id" | "source"
                | "type"
                | "specversion"
                | "subject"
                | "time"
                | "datacontenttype"
                | "dataschema"
                | "data"
                | "data_base64"
                | "partitionkey"
        ) {
            return Err(ValidationError::new(
                "extension name",
                ValidationKind::ReservedName,
            ));
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionString(String);

impl ExtensionString {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        validate_string("extension string", &value, None, true)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Timestamp(OffsetDateTime);

impl Timestamp {
    pub fn new(value: OffsetDateTime) -> Result<Self, ValidationError> {
        crate::bounds::validate_instant("extension timestamp", value)?;
        Ok(Self(value))
    }

    pub const fn get(&self) -> OffsetDateTime {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExtensionValue {
    Boolean(bool),
    Integer(i32),
    String(ExtensionString),
    Binary(Vec<u8>),
    Uri(AbsoluteUri),
    UriReference(UriReference),
    Timestamp(Timestamp),
}

impl ExtensionValue {
    pub fn string(value: impl Into<String>) -> Result<Self, ValidationError> {
        Ok(Self::String(ExtensionString::new(value)?))
    }

    pub fn uri(value: impl Into<String>) -> Result<Self, ValidationError> {
        Ok(Self::Uri(AbsoluteUri::new(value)?))
    }

    pub fn uri_reference(value: impl Into<String>) -> Result<Self, ValidationError> {
        Ok(Self::UriReference(UriReference::new(value)?))
    }

    pub fn timestamp(value: OffsetDateTime) -> Result<Self, ValidationError> {
        Ok(Self::Timestamp(Timestamp::new(value)?))
    }

    fn tagged_json(&self) -> Value {
        let (type_name, value) = match self {
            Self::Boolean(value) => ("boolean", Value::Bool(*value)),
            Self::Integer(value) => ("integer", Value::Number((*value).into())),
            Self::String(value) => ("string", Value::String(value.as_str().to_owned())),
            Self::Binary(value) => ("binary", Value::String(BASE64.encode(value))),
            Self::Uri(value) => ("uri", Value::String(value.as_str().to_owned())),
            Self::UriReference(value) => {
                ("uri-reference", Value::String(value.as_str().to_owned()))
            }
            Self::Timestamp(value) => ("timestamp", Value::String(format_timestamp(value.get()))),
        };
        let mut tagged = Map::new();
        tagged.insert("type".to_owned(), Value::String(type_name.to_owned()));
        tagged.insert("value".to_owned(), value);
        Value::Object(tagged)
    }

    pub(crate) fn structured_json_value(&self) -> Value {
        match self {
            Self::Boolean(value) => Value::Bool(*value),
            Self::Integer(value) => Value::Number((*value).into()),
            Self::String(value) => Value::String(value.as_str().to_owned()),
            Self::Binary(value) => Value::String(BASE64.encode(value)),
            Self::Uri(value) => Value::String(value.as_str().to_owned()),
            Self::UriReference(value) => Value::String(value.as_str().to_owned()),
            Self::Timestamp(value) => Value::String(format_timestamp(value.get())),
        }
    }

    fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Extensions(BTreeMap<ExtensionName, ExtensionValue>);

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        name: ExtensionName,
        value: ExtensionValue,
    ) -> Result<(), ValidationError> {
        if self.0.contains_key(&name) {
            return Err(ValidationError::new(
                "extension name",
                ValidationKind::Duplicate,
            ));
        }
        self.0.insert(name, value);
        Ok(())
    }

    pub fn get(&self, name: &ExtensionName) -> Option<&ExtensionValue> {
        self.0.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ExtensionName, &ExtensionValue)> {
        self.0.iter()
    }

    pub fn canonical_json(&self) -> String {
        let mut object = Map::new();
        for (name, value) in &self.0 {
            object.insert(name.0.clone(), value.tagged_json());
        }
        serde_json::to_string(&object).expect("tagged extension values are JSON values")
    }

    /// Decodes and checks the exact durable representation emitted by the
    /// canonical JSON method. Whitespace, duplicate members, and alternate
    /// spellings are rejected so a read cannot silently rewrite stored data.
    pub fn from_canonical_json(value: &str) -> Result<Self, ExtensionDecodeError> {
        let encoded = value;
        let value: Value =
            serde_json::from_str(value).map_err(|source| ExtensionDecodeError::Json {
                message: source.to_string(),
            })?;
        let Value::Object(entries) = value else {
            return Err(ExtensionDecodeError::InvalidShape {
                name: "<root>".to_owned(),
            });
        };

        let mut extensions = Self::new();
        for (name, tagged) in entries {
            let extension_name = ExtensionName::new(name.clone())
                .map_err(|source| ExtensionDecodeError::InvalidName { source })?;
            let Value::Object(mut tagged) = tagged else {
                return Err(ExtensionDecodeError::InvalidShape { name });
            };

            if tagged.len() != TAGGED_EXTENSION_FIELDS {
                return Err(ExtensionDecodeError::InvalidShape { name });
            }

            let Some(Value::String(type_name)) = tagged.remove("type") else {
                return Err(ExtensionDecodeError::InvalidShape { name });
            };

            let Some(value) = tagged.remove("value") else {
                return Err(ExtensionDecodeError::InvalidShape { name });
            };

            let extension_value = match type_name.as_str() {
                "boolean" => value.as_bool().map(Self::value_boolean).ok_or_else(|| {
                    ExtensionDecodeError::InvalidValue {
                        name: name.clone(),
                        type_name: type_name.clone(),
                        source: None,
                    }
                })?,
                "integer" => value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .map(Self::value_integer)
                    .ok_or_else(|| ExtensionDecodeError::InvalidValue {
                        name: name.clone(),
                        type_name: type_name.clone(),
                        source: None,
                    })?,
                "string" => decode_validated_string(&name, &type_name, &value, |value| {
                    ExtensionValue::string(value.to_owned())
                })?,
                "binary" => decode_binary(&name, &value)?,
                "uri" => decode_validated_string(&name, &type_name, &value, |value| {
                    ExtensionValue::uri(value.to_owned())
                })?,
                "uri-reference" => decode_validated_string(&name, &type_name, &value, |value| {
                    ExtensionValue::uri_reference(value.to_owned())
                })?,
                "timestamp" => decode_timestamp(&name, &type_name, &value)?,
                _ => return Err(ExtensionDecodeError::UnsupportedType { name, type_name }),
            };

            extensions.0.insert(extension_name, extension_value);
        }

        extensions
            .validate_trace_context()
            .map_err(|source| ExtensionDecodeError::InvalidExtensions { source })?;
        if extensions.canonical_json() != encoded {
            return Err(ExtensionDecodeError::NonCanonical);
        }
        Ok(extensions)
    }

    pub(crate) fn validate_trace_context(&self) -> Result<(), ValidationError> {
        let traceparent = self.0.get(&ExtensionName("traceparent".to_owned()));
        let tracestate = self.0.get(&ExtensionName("tracestate".to_owned()));
        if tracestate.is_some() && traceparent.is_none() {
            return Err(ValidationError::new(
                "tracestate",
                ValidationKind::TraceContext,
            ));
        }

        self.validate_trace_extension("traceparent", validate_traceparent)?;
        self.validate_trace_extension("tracestate", validate_tracestate)?;

        Ok(())
    }

    fn value_boolean(value: bool) -> ExtensionValue {
        ExtensionValue::Boolean(value)
    }

    fn value_integer(value: i32) -> ExtensionValue {
        ExtensionValue::Integer(value)
    }

    fn validate_trace_extension(
        &self,
        name: &'static str,
        validator: fn(&str) -> Result<(), ValidationError>,
    ) -> Result<(), ValidationError> {
        let extension = self.0.get(&ExtensionName(name.to_owned()));
        if let Some(value) = extension.and_then(ExtensionValue::as_string) {
            validator(value)
        } else if extension.is_some() {
            Err(ValidationError::new(name, ValidationKind::TraceContext))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExtensionDecodeError {
    #[error("extensions JSON is invalid: {message}")]
    Json { message: String },
    #[error("extension name is invalid: {source}")]
    InvalidName { source: ValidationError },
    #[error("extension {name} has invalid shape")]
    InvalidShape { name: String },
    #[error("extension {name} has invalid {type_name} value")]
    InvalidValue {
        name: String,
        type_name: String,
        source: Option<ValidationError>,
    },
    #[error("extension {name} has invalid binary data: {message}")]
    InvalidBinary { name: String, message: String },
    #[error("extension {name} has invalid timestamp: {message}")]
    InvalidTimestamp { name: String, message: String },
    #[error("extension set is invalid: {source}")]
    InvalidExtensions { source: ValidationError },
    #[error("extensions JSON is not the canonical durable representation")]
    NonCanonical,
    #[error("extension {name} uses unsupported type {type_name}")]
    UnsupportedType { name: String, type_name: String },
}

fn decode_validated_string(
    name: &str,
    type_name: &str,
    value: &Value,
    constructor: impl FnOnce(&str) -> Result<ExtensionValue, ValidationError>,
) -> Result<ExtensionValue, ExtensionDecodeError> {
    let value = value
        .as_str()
        .ok_or_else(|| ExtensionDecodeError::InvalidValue {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            source: None,
        })?;
    constructor(value).map_err(|source| ExtensionDecodeError::InvalidValue {
        name: name.to_owned(),
        type_name: type_name.to_owned(),
        source: Some(source),
    })
}

fn decode_binary(name: &str, value: &Value) -> Result<ExtensionValue, ExtensionDecodeError> {
    let value = value
        .as_str()
        .ok_or_else(|| invalid_extension_value(name, "binary"))?;
    BASE64
        .decode(value)
        .map(ExtensionValue::Binary)
        .map_err(|source| ExtensionDecodeError::InvalidBinary {
            name: name.to_owned(),
            message: source.to_string(),
        })
}

fn decode_timestamp(
    name: &str,
    type_name: &str,
    value: &Value,
) -> Result<ExtensionValue, ExtensionDecodeError> {
    let value = value
        .as_str()
        .ok_or_else(|| invalid_extension_value(name, type_name))?;
    let timestamp = OffsetDateTime::parse(value, &Rfc3339).map_err(|source| {
        ExtensionDecodeError::InvalidTimestamp {
            name: name.to_owned(),
            message: source.to_string(),
        }
    })?;
    if format_timestamp(timestamp) != value {
        return Err(invalid_extension_value(name, type_name));
    }
    ExtensionValue::timestamp(timestamp)
        .map_err(|source| invalid_extension_value_with_source(name, type_name, source))
}

fn invalid_extension_value(name: &str, type_name: &str) -> ExtensionDecodeError {
    ExtensionDecodeError::InvalidValue {
        name: name.to_owned(),
        type_name: type_name.to_owned(),
        source: None,
    }
}

fn invalid_extension_value_with_source(
    name: &str,
    type_name: &str,
    source: ValidationError,
) -> ExtensionDecodeError {
    ExtensionDecodeError::InvalidValue {
        name: name.to_owned(),
        type_name: type_name.to_owned(),
        source: Some(source),
    }
}
