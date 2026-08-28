use crate::{
    ContentType, StoredEvent, ValidationError, serialization::extension_binary_string,
    validation::format_timestamp,
};

/// Bytes for a structured CloudEvents message, with member order and timestamp
/// spelling fixed by Dovecote so durable rows produce repeatable output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructuredJsonProjection {
    bytes: Vec<u8>,
}

impl StructuredJsonProjection {
    /// The CloudEvents structured JSON content type.
    pub const CONTENT_TYPE: &'static str = "application/cloudevents+json";

    /// Returns the deterministic UTF-8 structured projection bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// The body, content type, and ordered attributes needed by a binary-mode
/// transport, without committing the core crate to an HTTP or broker client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryProjection {
    body: Option<Vec<u8>>,
    datacontenttype: Option<ContentType>,
    attributes: Vec<(String, String)>,
}

impl BinaryProjection {
    /// Returns the exact event body, preserving absent versus empty data.
    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }

    /// Returns the optional event data content type.
    pub fn datacontenttype(&self) -> Option<&ContentType> {
        self.datacontenttype.as_ref()
    }

    /// Iterates over ordered, transport-neutral CloudEvents context attributes.
    pub fn attributes(&self) -> impl Iterator<Item = (&str, &str)> {
        self.attributes
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

impl StoredEvent {
    /// Produces the repeatable structured message sent by a structured-mode transport.
    pub fn structured_json(&self) -> Result<StructuredJsonProjection, ValidationError> {
        Ok(StructuredJsonProjection {
            bytes: crate::serialization::structured_json_bytes(&self.content)?,
        })
    }

    /// Produces transport-neutral binary fields for a binary-mode transport.
    pub fn binary(&self) -> BinaryProjection {
        BinaryProjection {
            body: self.data().map(|data| data.as_bytes().to_vec()),
            datacontenttype: self.datacontenttype().cloned(),
            attributes: binary_attributes(self),
        }
    }
}

fn binary_attributes(event: &StoredEvent) -> Vec<(String, String)> {
    let mut attributes = vec![
        ("specversion".to_owned(), event.specversion().to_owned()),
        ("id".to_owned(), event.id().as_str().to_owned()),
        ("source".to_owned(), event.source().as_str().to_owned()),
        ("type".to_owned(), event.event_type().as_str().to_owned()),
    ];
    // These optional members follow the canonical CloudEvents field order; keep
    // the order visible while treating absence as a value-level concern.
    for (name, value) in [
        (
            "subject",
            event.subject().map(|value| value.as_str().to_owned()),
        ),
        ("time", event.time().map(format_timestamp)),
        (
            "dataschema",
            event.dataschema().map(|value| value.as_str().to_owned()),
        ),
        (
            "partitionkey",
            event.partitionkey().map(|value| value.as_str().to_owned()),
        ),
    ] {
        if let Some(value) = value {
            attributes.push((name.to_owned(), value));
        }
    }

    for (name, value) in event.extensions().iter() {
        attributes.push((name.as_str().to_owned(), extension_binary_string(value)));
    }

    attributes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventData, EventId, EventSource, EventType, ExtensionValue, NewEvent, StreamName};

    fn base() -> NewEvent {
        NewEvent::new(
            StreamName::new("audit").unwrap(),
            EventId::new("event-1").unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.audit").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn structured_projection_has_stable_order_and_timestamp_precision() {
        let time = time::OffsetDateTime::parse(
            "2026-01-01T00:00:00.120000Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let stored = NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new("event-1").unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.audit").unwrap(),
        )
        .time(time)
        .data(EventData::json(br#"{"b":1,"a":{"d":4,"c":3}}"#.to_vec()).unwrap())
        .datacontenttype(ContentType::new("application/json").unwrap())
        .build()
        .unwrap()
        .into_stored()
        .unwrap();
        let json =
            String::from_utf8(stored.structured_json().unwrap().as_bytes().to_vec()).unwrap();
        assert!(json.contains(r#""time":"2026-01-01T00:00:00.12Z""#));
        assert!(json.contains(r#""data":{"a":{"c":3,"d":4},"b":1}"#));
        assert!(json.find("specversion").unwrap() < json.find("id").unwrap());
        assert!(json.find("id").unwrap() < json.find("source").unwrap());
        assert!(json.find("source").unwrap() < json.find("type").unwrap());
    }

    #[test]
    fn absent_and_empty_data_have_distinct_projection_shapes() {
        let absent = base().into_stored().unwrap().structured_json().unwrap();
        let empty = NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new("event-2").unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.audit").unwrap(),
        )
        .data(EventData::binary(Vec::new()))
        .build()
        .unwrap()
        .into_stored()
        .unwrap()
        .structured_json()
        .unwrap();
        let absent = String::from_utf8(absent.as_bytes().to_vec()).unwrap();
        let empty = String::from_utf8(empty.as_bytes().to_vec()).unwrap();
        assert_eq!(
            absent,
            r#"{"specversion":"1.0","id":"event-1","source":"https://example.test/source","type":"com.example.audit"}"#
        );
        assert_eq!(
            empty,
            r#"{"specversion":"1.0","id":"event-2","source":"https://example.test/source","type":"com.example.audit","data_base64":""}"#
        );
    }

    #[test]
    fn optional_attributes_and_trace_context_have_a_golden_projection() {
        let time = time::OffsetDateTime::parse(
            "2026-01-01T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let mut extensions = crate::Extensions::new();
        extensions
            .insert(
                crate::ExtensionName::new("traceparent").unwrap(),
                ExtensionValue::string("00-4bf92f3577b34da6a3ce929d0e0e4736-0123456789abcdef-01")
                    .unwrap(),
            )
            .unwrap();
        extensions
            .insert(
                crate::ExtensionName::new("tracestate").unwrap(),
                ExtensionValue::string("vendor=value").unwrap(),
            )
            .unwrap();
        let stored = NewEvent::builder(
            crate::StreamName::new("audit").unwrap(),
            crate::EventId::new("event-optional").unwrap(),
            crate::EventSource::new("https://example.test/source").unwrap(),
            crate::EventType::new("com.example.audit").unwrap(),
        )
        .subject(crate::EventSubject::new("subject").unwrap())
        .time(time)
        .datacontenttype(ContentType::new("application/json; charset=utf-8").unwrap())
        .dataschema(crate::SchemaUri::new("https://example.test/schema").unwrap())
        .partitionkey(crate::PartitionKey::new("partition").unwrap())
        .extensions(extensions)
        .data(EventData::json(br#"{"ok":true}"#.to_vec()).unwrap())
        .build()
        .unwrap()
        .into_stored()
        .unwrap();
        let json =
            String::from_utf8(stored.structured_json().unwrap().as_bytes().to_vec()).unwrap();
        assert_eq!(
            json,
            r#"{"specversion":"1.0","id":"event-optional","source":"https://example.test/source","type":"com.example.audit","subject":"subject","time":"2026-01-01T00:00:00Z","datacontenttype":"application/json; charset=utf-8","dataschema":"https://example.test/schema","partitionkey":"partition","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-0123456789abcdef-01","tracestate":"vendor=value","data":{"ok":true}}"#
        );
    }

    #[test]
    fn all_extension_types_have_stable_projection_mappings() {
        let timestamp = time::OffsetDateTime::parse(
            "2026-01-01T00:00:00.120Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let mut extensions = crate::Extensions::new();
        for (name, value) in [
            ("boolean", ExtensionValue::Boolean(true)),
            ("integer", ExtensionValue::Integer(7)),
            (
                "string",
                ExtensionValue::string("café").expect("valid extension string"),
            ),
            ("binary", ExtensionValue::Binary(vec![1, 2, 3])),
            (
                "uri",
                ExtensionValue::uri("https://example.test/schema").unwrap(),
            ),
            (
                "reference",
                ExtensionValue::uri_reference("/schema").unwrap(),
            ),
            ("timestamp", ExtensionValue::timestamp(timestamp).unwrap()),
        ] {
            extensions
                .insert(crate::ExtensionName::new(name).unwrap(), value)
                .unwrap();
        }

        let stored = NewEvent::builder(
            crate::StreamName::new("audit").unwrap(),
            crate::EventId::new("event-extensions").unwrap(),
            crate::EventSource::new("https://example.test/source").unwrap(),
            crate::EventType::new("com.example.audit").unwrap(),
        )
        .extensions(extensions)
        .build()
        .unwrap()
        .into_stored()
        .unwrap();
        let json =
            String::from_utf8(stored.structured_json().unwrap().as_bytes().to_vec()).unwrap();
        assert_eq!(
            json,
            r#"{"specversion":"1.0","id":"event-extensions","source":"https://example.test/source","type":"com.example.audit","binary":"AQID","boolean":true,"integer":7,"reference":"/schema","string":"café","timestamp":"2026-01-01T00:00:00.12Z","uri":"https://example.test/schema"}"#
        );
        let binary = stored.binary();
        assert_eq!(
            binary.attributes().collect::<Vec<_>>(),
            vec![
                ("specversion", "1.0"),
                ("id", "event-extensions"),
                ("source", "https://example.test/source"),
                ("type", "com.example.audit"),
                ("binary", "AQID"),
                ("boolean", "true"),
                ("integer", "7"),
                ("reference", "/schema"),
                ("string", "café"),
                ("timestamp", "2026-01-01T00:00:00.12Z"),
                ("uri", "https://example.test/schema"),
            ]
        );
    }
}
