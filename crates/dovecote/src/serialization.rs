use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Map, Value};

use crate::{
    SPEC_VERSION,
    error::{ValidationError, ValidationKind},
    event::EventContent,
    extension::ExtensionValue,
    validation::format_timestamp,
    value::EventData,
};

pub(crate) fn structured_json_bytes(content: &EventContent) -> Result<Vec<u8>, ValidationError> {
    let mut object = Map::new();
    object.insert(
        "specversion".to_owned(),
        Value::String(SPEC_VERSION.to_owned()),
    );
    object.insert(
        "id".to_owned(),
        Value::String(content.id.as_str().to_owned()),
    );
    object.insert(
        "source".to_owned(),
        Value::String(content.source.as_str().to_owned()),
    );
    object.insert(
        "type".to_owned(),
        Value::String(content.event_type.as_str().to_owned()),
    );
    // CloudEvents member order is part of the canonical byte projection, so keep
    // these calls aligned with the format's defined order.
    insert_optional_json_member(
        &mut object,
        "subject",
        content
            .subject
            .as_ref()
            .map(|value| Value::String(value.as_str().to_owned())),
    );
    insert_optional_json_member(
        &mut object,
        "time",
        content
            .time
            .map(|value| Value::String(format_timestamp(value))),
    );
    insert_optional_json_member(
        &mut object,
        "datacontenttype",
        content
            .datacontenttype
            .as_ref()
            .map(|value| Value::String(value.as_str().to_owned())),
    );
    insert_optional_json_member(
        &mut object,
        "dataschema",
        content
            .dataschema
            .as_ref()
            .map(|value| Value::String(value.as_str().to_owned())),
    );
    insert_optional_json_member(
        &mut object,
        "partitionkey",
        content
            .partitionkey
            .as_ref()
            .map(|value| Value::String(value.as_str().to_owned())),
    );

    for (name, value) in content.extensions.iter() {
        object.insert(name.as_str().to_owned(), value.structured_json_value());
    }

    if let Some(data) = &content.data {
        match data {
            EventData::Json(value) => {
                object.insert("data".to_owned(), canonicalize_json(value.as_value()));
            }
            EventData::Binary(value) => {
                object.insert(
                    "data_base64".to_owned(),
                    Value::String(BASE64.encode(value)),
                );
            }
        }
    }

    serde_json::to_vec(&object).map_err(|_| ValidationError::new("event", ValidationKind::Json))
}

fn insert_optional_json_member(object: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        object.insert(name.to_owned(), value);
    }
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut sorted = BTreeMap::new();
            for (key, value) in object {
                sorted.insert(key, canonicalize_json(value));
            }

            let mut object = Map::new();
            for (key, value) in sorted {
                object.insert(key, value);
            }

            Value::Object(object)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        value => value,
    }
}

pub(crate) fn binary_material_bytes(content: &EventContent) -> Result<usize, ValidationError> {
    let mut size = content
        .data
        .as_ref()
        .map_or(0, |value| value.as_bytes().len());
    add_binary_context(&mut size, "ce-specversion", SPEC_VERSION);
    add_binary_context(&mut size, "ce-id", content.id.as_str());
    add_binary_context(&mut size, "ce-source", content.source.as_str());
    add_binary_context(&mut size, "ce-type", content.event_type.as_str());
    add_optional_binary_context(
        &mut size,
        "ce-subject",
        content.subject.as_ref().map(|value| value.as_str()),
    );
    add_optional_binary_context(&mut size, "ce-time", content.time.map(format_timestamp));
    add_optional_binary_context(
        &mut size,
        "ce-dataschema",
        content.dataschema.as_ref().map(|value| value.as_str()),
    );
    add_optional_binary_context(
        &mut size,
        "ce-partitionkey",
        content.partitionkey.as_ref().map(|value| value.as_str()),
    );

    for (name, value) in content.extensions.iter() {
        add_binary_context(
            &mut size,
            &format!("ce-{}", name.as_str()),
            &extension_binary_string(value),
        );
    }

    if let Some(value) = content.datacontenttype.as_ref() {
        let header_name = if "ce-datacontenttype".len() > "Content-Type".len() {
            "ce-datacontenttype"
        } else {
            "Content-Type"
        };
        add_binary_context(&mut size, header_name, value.as_str());
    }
    Ok(size)
}

fn add_binary_context(size: &mut usize, name: &str, value: &str) {
    *size = size
        .saturating_add(name.len() + 4)
        .saturating_add(value.len().saturating_mul(3));
}

fn add_optional_binary_context<T: AsRef<str>>(size: &mut usize, name: &str, value: Option<T>) {
    if let Some(value) = value {
        add_binary_context(size, name, value.as_ref());
    }
}

pub(crate) fn extension_binary_string(value: &ExtensionValue) -> String {
    match value {
        ExtensionValue::Boolean(value) => value.to_string(),
        ExtensionValue::Integer(value) => value.to_string(),
        ExtensionValue::String(value) => value.as_str().to_owned(),
        ExtensionValue::Binary(value) => BASE64.encode(value),
        ExtensionValue::Uri(value) => value.as_str().to_owned(),
        ExtensionValue::UriReference(value) => value.as_str().to_owned(),
        ExtensionValue::Timestamp(value) => format_timestamp(value.get()),
    }
}
