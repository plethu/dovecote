//! Checked-in projection fixture constructors and local transport mappings.

pub(crate) use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
pub(crate) use cloudevents::{
    AttributesReader, Data, Event as ExternalCloudEvent, event::SpecVersion,
};
pub(crate) use dovecote::{
    ContentType, EventData, EventId, EventSource, EventSubject, EventType, ExtensionName,
    ExtensionValue, Extensions, NewEvent, PartitionKey, SchemaUri, StreamName,
};
use serde::Deserialize;
pub(crate) use serde_json::Value;
pub(crate) use sha2::{Digest, Sha256};
pub(crate) use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub(crate) const CLOUDEVENTS_SCHEMA_SHA256: &str =
    "e28a6d252d7b7238d176618f6bbf6cde570b26a867bc5241563aed34c9dd1d83";

pub(crate) fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("fixture timestamp is valid")
}

pub(crate) fn full_event() -> NewEvent {
    let mut extensions = Extensions::new();
    for (name, value) in [
        ("binary", ExtensionValue::Binary(vec![0, 255, 16])),
        ("boolean", ExtensionValue::Boolean(true)),
        ("integer", ExtensionValue::Integer(-7)),
        (
            "reference",
            ExtensionValue::uri_reference("/schema?x=space%20value")
                .expect("fixture URI reference is valid"),
        ),
        (
            "string",
            ExtensionValue::string("space \" % café").expect("fixture string is valid"),
        ),
        (
            "timestamp",
            ExtensionValue::timestamp(timestamp("2026-01-01T00:00:00.120Z"))
                .expect("fixture timestamp is valid"),
        ),
        (
            "traceparent",
            ExtensionValue::string("00-4bf92f3577b34da6a3ce929d0e0e4736-0123456789abcdef-01")
                .expect("fixture traceparent is valid"),
        ),
        (
            "tracestate",
            ExtensionValue::string("vendor=value").expect("fixture tracestate is valid"),
        ),
        (
            "uri",
            ExtensionValue::uri("https://example.test/a%20b").expect("fixture URI is valid"),
        ),
        (
            "xextra",
            ExtensionValue::string("opaque").expect("fixture extension is valid"),
        ),
    ] {
        extensions
            .insert(
                ExtensionName::new(name).expect("fixture extension name is valid"),
                value,
            )
            .expect("fixture extension names are unique");
    }

    NewEvent::builder(
        StreamName::new("audit.v1").expect("fixture stream is valid"),
        EventId::new("evt-full").expect("fixture ID is valid"),
        EventSource::new("https://example.test/source").expect("fixture source is valid"),
        EventType::new("com.example.audit").expect("fixture type is valid"),
    )
    .subject(EventSubject::new("subject \" % café").expect("fixture subject is valid"))
    .time(timestamp("2026-01-01T00:00:00.123456Z"))
    .datacontenttype(
        ContentType::new("application/problem+json; charset=utf-8")
            .expect("fixture media type is valid"),
    )
    .dataschema(SchemaUri::new("https://example.test/schema").expect("fixture schema is valid"))
    .partitionkey(
        PartitionKey::new("partition key/% café").expect("fixture partition key is valid"),
    )
    .extensions(extensions)
    .data(
        EventData::json(r#"{"z":"raw café","a":[true,1]}"#.as_bytes().to_vec())
            .expect("fixture JSON is valid"),
    )
    .build()
    .expect("fixture event is valid")
}

pub(crate) fn binary_event() -> NewEvent {
    NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-binary").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.binary").unwrap(),
    )
    .datacontenttype(ContentType::new("application/octet-stream").unwrap())
    .data(EventData::binary(vec![0, 1, 2, 127, 128, 255]))
    .build()
    .unwrap()
}

pub(crate) fn absent_event() -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-absent").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.empty").unwrap(),
    )
    .unwrap()
}

pub(crate) fn empty_event() -> NewEvent {
    NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-empty").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.empty").unwrap(),
    )
    .data(EventData::binary(Vec::new()))
    .build()
    .unwrap()
}

pub(crate) fn scalar_event() -> NewEvent {
    NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-scalar").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.scalar").unwrap(),
    )
    .datacontenttype(ContentType::new("application/vnd.example+json").unwrap())
    .data(EventData::json(br#"42"#.to_vec()).unwrap())
    .build()
    .unwrap()
}

pub(crate) fn text_event() -> NewEvent {
    NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-text").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.text").unwrap(),
    )
    .datacontenttype(ContentType::new("text/plain; charset=utf-8").unwrap())
    .data(EventData::binary("héllo, café".as_bytes().to_vec()))
    .build()
    .unwrap()
}

pub(crate) fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if (0x21..=0x7e).contains(byte) && !matches!(byte, b' ' | b'"' | b'%') {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
            encoded.push(char::from(b"0123456789ABCDEF"[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

pub(crate) fn http_binary(event: &dovecote::StoredEvent) -> (Vec<u8>, Vec<(String, String)>) {
    let projection = event.binary();
    let mut headers = Vec::new();
    if let Some(content_type) = projection.datacontenttype() {
        headers.push(("Content-Type".to_owned(), content_type.as_str().to_owned()));
    }
    headers.extend(
        projection
            .attributes()
            .map(|(name, value)| (format!("ce-{name}"), percent_encode(value))),
    );
    (projection.body().unwrap_or_default().to_vec(), headers)
}

pub(crate) struct KafkaBinaryBinding {
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) headers: Vec<(String, String)>,
}

pub(crate) fn kafka_binary(event: &dovecote::StoredEvent) -> KafkaBinaryBinding {
    let projection = event.binary();
    let key = event
        .partitionkey()
        .map(|value| value.as_str().as_bytes().to_vec());
    let mut headers = Vec::new();
    if let Some(content_type) = projection.datacontenttype() {
        headers.push(("content-type".to_owned(), content_type.as_str().to_owned()));
    }
    headers.extend(
        projection
            .attributes()
            .map(|(name, value)| (format!("ce_{name}"), value.to_owned())),
    );
    KafkaBinaryBinding {
        key,
        body: projection.body().map(ToOwned::to_owned),
        headers,
    }
}

pub(crate) fn nats_binary(
    event: &dovecote::StoredEvent,
) -> (Vec<u8>, Vec<(String, String)>, String) {
    let projection = event.binary();
    let mut headers = projection
        .attributes()
        .map(|(name, value)| (format!("ce-{name}"), percent_encode(value)))
        .collect::<Vec<_>>();
    if let Some(content_type) = projection.datacontenttype() {
        headers.push((
            "ce-datacontenttype".to_owned(),
            percent_encode(content_type.as_str()),
        ));
    }

    let mut digest_input = Vec::new();
    for value in [
        event.source().as_str().as_bytes(),
        event.id().as_str().as_bytes(),
    ] {
        digest_input.extend_from_slice(&(value.len() as u64).to_be_bytes());
        digest_input.extend_from_slice(value);
    }

    let digest = Sha256::digest(digest_input);
    let msg_id = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    (
        projection.body().unwrap_or_default().to_vec(),
        headers,
        msg_id,
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProjectionFixture {
    pub(crate) name: String,
    pub(crate) durable_extensions: String,
    pub(crate) structured_json: String,
    pub(crate) binary: BinaryFixture,
    pub(crate) http: HttpFixture,
    pub(crate) kafka: KafkaFixture,
    pub(crate) nats: NatsFixture,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BinaryFixture {
    pub(crate) body: Option<String>,
    pub(crate) datacontenttype: Option<String>,
    pub(crate) attributes: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HttpFixture {
    pub(crate) body: String,
    pub(crate) headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct KafkaFixture {
    pub(crate) key: Option<String>,
    pub(crate) body: Option<String>,
    pub(crate) headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct NatsFixture {
    pub(crate) body: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) msg_id: String,
}

pub(crate) fn fixtures() -> Vec<ProjectionFixture> {
    serde_json::from_str(include_str!("../fixtures/projections.json"))
        .expect("checked-in projection fixtures are valid JSON")
}

pub(crate) fn expected_bytes(value: &Option<String>) -> Option<Vec<u8>> {
    value.as_deref().map(|encoded| {
        BASE64
            .decode(encoded)
            .expect("fixture bytes are valid base64")
    })
}

pub(crate) fn event_for(name: &str) -> NewEvent {
    match name {
        "full-json" => full_event(),
        "binary" => binary_event(),
        "absent" => absent_event(),
        "empty" => empty_event(),
        "scalar" => scalar_event(),
        "text" => text_event(),
        unknown => panic!("unknown projection fixture: {unknown}"),
    }
}
