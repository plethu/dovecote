//! Runtime-free projection vectors and local binding reference mappings.
//!
//! These tests deliberately exercise the public, transport-neutral projection
//! API. HTTP, Kafka, and NATS here are small local reference mappings over
//! `BinaryProjection`; they are not transport clients or evidence of broker and
//! HTTP-server conformance. The structured JSON vectors are separately checked
//! against the official CloudEvents schema and an external SDK parser below.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use cloudevents::{AttributesReader, Data, Event as ExternalCloudEvent, event::SpecVersion};
use dovecote::{
    ContentType, EventData, EventId, EventSource, EventSubject, EventType, ExtensionName,
    ExtensionValue, Extensions, NewEvent, PartitionKey, SchemaUri, StreamName,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const CLOUDEVENTS_SCHEMA_SHA256: &str =
    "e28a6d252d7b7238d176618f6bbf6cde570b26a867bc5241563aed34c9dd1d83";

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("fixture timestamp is valid")
}

fn full_event() -> NewEvent {
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

fn binary_event() -> NewEvent {
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

fn absent_event() -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("evt-absent").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.empty").unwrap(),
    )
    .unwrap()
}

fn empty_event() -> NewEvent {
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

fn scalar_event() -> NewEvent {
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

fn text_event() -> NewEvent {
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

fn percent_encode(value: &str) -> String {
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

fn http_binary(event: &dovecote::StoredEvent) -> (Vec<u8>, Vec<(String, String)>) {
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

struct KafkaBinaryBinding {
    key: Option<Vec<u8>>,
    body: Option<Vec<u8>>,
    headers: Vec<(String, String)>,
}

fn kafka_binary(event: &dovecote::StoredEvent) -> KafkaBinaryBinding {
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

fn nats_binary(event: &dovecote::StoredEvent) -> (Vec<u8>, Vec<(String, String)>, String) {
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
struct ProjectionFixture {
    name: String,
    durable_extensions: String,
    structured_json: String,
    binary: BinaryFixture,
    http: HttpFixture,
    kafka: KafkaFixture,
    nats: NatsFixture,
}

#[derive(Debug, Deserialize)]
struct BinaryFixture {
    body: Option<String>,
    datacontenttype: Option<String>,
    attributes: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct HttpFixture {
    body: String,
    headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct KafkaFixture {
    key: Option<String>,
    body: Option<String>,
    headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct NatsFixture {
    body: String,
    headers: Vec<(String, String)>,
    msg_id: String,
}

fn fixtures() -> Vec<ProjectionFixture> {
    serde_json::from_str(include_str!("fixtures/projections.json"))
        .expect("checked-in projection fixtures are valid JSON")
}

fn expected_bytes(value: &Option<String>) -> Option<Vec<u8>> {
    value.as_deref().map(|encoded| {
        BASE64
            .decode(encoded)
            .expect("fixture bytes are valid base64")
    })
}

fn event_for(name: &str) -> NewEvent {
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

#[test]
fn checked_in_vectors_match_projection_and_local_binding_reference_mappings() {
    for fixture in fixtures() {
        let event = event_for(&fixture.name);
        assert_eq!(
            event.extensions().canonical_json(),
            fixture.durable_extensions
        );
        let portable_size = event.portable_size().expect("fixture size is valid");
        let stored = event.into_stored().expect("fixture event is valid");
        let structured = stored
            .structured_json()
            .expect("structured projection is valid");
        assert_eq!(structured.as_bytes(), fixture.structured_json.as_bytes());

        let structured_value: Value = serde_json::from_slice(structured.as_bytes())
            .expect("structured projection is valid JSON");
        let expected_value: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        assert_eq!(structured_value, expected_value);

        let binary = stored.binary();
        assert_eq!(
            binary.body().map(ToOwned::to_owned),
            expected_bytes(&fixture.binary.body)
        );
        assert_eq!(
            binary
                .datacontenttype()
                .map(|value| value.as_str().to_owned()),
            fixture.binary.datacontenttype
        );
        assert_eq!(
            binary
                .attributes()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect::<Vec<_>>(),
            fixture.binary.attributes
        );

        let (http_body, http_headers) = http_binary(&stored);
        assert_eq!(BASE64.encode(http_body), fixture.http.body);
        assert_eq!(http_headers, fixture.http.headers);
        assert!(
            !http_headers
                .iter()
                .any(|(name, _)| name == "ce-datacontenttype")
        );

        let kafka = kafka_binary(&stored);
        assert_eq!(
            kafka.key.map(|value| BASE64.encode(value)),
            fixture.kafka.key
        );
        assert_eq!(
            kafka.body.map(|value| BASE64.encode(value)),
            fixture.kafka.body
        );
        assert_eq!(kafka.headers, fixture.kafka.headers);

        let (nats_body, nats_headers, nats_msg_id) = nats_binary(&stored);
        assert_eq!(BASE64.encode(nats_body), fixture.nats.body);
        assert_eq!(nats_headers, fixture.nats.headers);
        assert_eq!(nats_msg_id, fixture.nats.msg_id);

        // The fixture's event remains valid at its exact logical-size boundary;
        // one byte below it is rejected before any adapter can insert it.
        assert!(portable_size > 0);
        let exact = event_for(&fixture.name)
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size).unwrap());
        assert!(exact.is_ok());
        let below = event_for(&fixture.name)
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size - 1).unwrap());
        assert!(below.is_err());
    }
}

#[test]
fn structured_projection_vectors_are_parsed_by_external_cloudevents_sdk() {
    for fixture in fixtures() {
        let raw: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        let mut sdk_input = raw.clone();

        // cloudevents-sdk 0.9.0 currently decides whether `data` is JSON by
        // checking whether the raw content type ends in `+json`. The schema
        // validation below checks the exact projection, including media-type
        // parameters; normalize only this SDK compatibility probe so that the
        // independent parser can still cover the event and its data.
        if let Some(Value::String(content_type)) = sdk_input.get_mut("datacontenttype") {
            *content_type = content_type.split_once(';').map_or_else(
                || content_type.clone(),
                |(media_type, _)| media_type.to_owned(),
            );
        }

        let event: ExternalCloudEvent = serde_json::from_value(sdk_input).unwrap_or_else(|error| {
            panic!(
                "{} is not accepted by cloudevents-sdk: {error}",
                fixture.name
            )
        });

        assert!(
            matches!(event.specversion(), SpecVersion::V10),
            "{}",
            fixture.name
        );
        assert!(!event.id().is_empty(), "{}", fixture.name);
        assert!(!event.source().as_str().is_empty(), "{}", fixture.name);
        assert!(!event.ty().is_empty(), "{}", fixture.name);

        match event.data() {
            None => assert_eq!(fixture.name, "absent"),
            Some(Data::Binary(data)) => {
                assert_eq!(
                    Some(BASE64.encode(data)),
                    fixture.binary.body,
                    "{}",
                    fixture.name
                )
            }
            Some(Data::Json(data)) => assert_eq!(Some(data), raw.get("data"), "{}", fixture.name),
            Some(Data::String(_)) => panic!(
                "{} was decoded as a string rather than JSON or binary data",
                fixture.name
            ),
        }
    }
}

#[test]
fn structured_projection_validates_against_official_cloudevents_schema() {
    let schema_bytes = include_bytes!("fixtures/cloudevents-v1.0.2.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(schema_bytes)),
        CLOUDEVENTS_SCHEMA_SHA256,
        "the checked-in schema must remain the official v1.0.2 artifact"
    );
    let schema: Value =
        serde_json::from_slice(schema_bytes).expect("checked-in CloudEvents schema is valid JSON");
    jsonschema::draft7::meta::validate(&schema).expect("CloudEvents schema is Draft 7 JSON");
    let validator = jsonschema::draft7::options()
        .should_validate_formats(true)
        .build(&schema)
        .expect("CloudEvents schema compiles");

    for fixture in fixtures() {
        let value: Value = serde_json::from_str(&fixture.structured_json)
            .expect("fixture structured projection is valid JSON");
        validator.validate(&value).unwrap_or_else(|error| {
            panic!(
                "{} is not valid under the CloudEvents v1.0.2 JSON Schema: {error}",
                fixture.name
            )
        });

        let has_data = value.get("data").is_some();
        let has_binary_data = value.get("data_base64").is_some();
        assert!(
            !(has_data && has_binary_data),
            "{} has both data forms",
            fixture.name
        );
        match fixture.name.as_str() {
            "absent" => assert!(!has_data && !has_binary_data),
            "binary" | "empty" | "text" => assert!(!has_data && has_binary_data),
            "full-json" | "scalar" => assert!(has_data && !has_binary_data),
            unknown => panic!("unknown projection fixture: {unknown}"),
        }
    }
}

fn kafka_binary_value(
    event: &dovecote::StoredEvent,
    compacted: bool,
    allow_compaction_tombstone: bool,
) -> Result<Option<Vec<u8>>, &'static str> {
    let value = event.binary().body().map(ToOwned::to_owned);
    if compacted && value.is_none() && !allow_compaction_tombstone {
        return Err("absent binary data would be a Kafka compaction tombstone");
    }
    Ok(value)
}

#[test]
fn kafka_reference_mapping_keeps_absent_and_empty_values_distinct() {
    let absent = absent_event().into_stored().unwrap();
    assert_eq!(
        kafka_binary_value(&absent, true, false),
        Err("absent binary data would be a Kafka compaction tombstone")
    );
    assert_eq!(kafka_binary_value(&absent, true, true), Ok(None));

    let empty = empty_event().into_stored().unwrap();
    assert_eq!(
        kafka_binary_value(&empty, true, false),
        Ok(Some(Vec::new()))
    );
}

#[test]
fn public_boundary_vectors_cover_base64_percent_encoding_and_both_size_formulas() {
    let binary = binary_event().into_stored().unwrap();
    let structured = binary.structured_json().unwrap();
    assert!(structured.as_bytes().len() > binary.binary().body().unwrap().len());

    let full = full_event();
    let portable_size = full.portable_size().unwrap();
    let stored = full.into_stored().unwrap();
    let structured = stored.structured_json().unwrap();
    let structured_material = structured.as_bytes().len()
        + "Content-Type: ".len()
        + dovecote::StructuredJsonProjection::CONTENT_TYPE.len()
        + "\r\n".len();
    let projection = stored.binary();
    let mut binary_material = projection.body().map_or(0, <[u8]>::len);
    for (name, value) in projection.attributes() {
        binary_material += format!("ce-{name}").len() + 4 + value.len() * 3;
    }

    if let Some(content_type) = projection.datacontenttype() {
        binary_material += "ce-datacontenttype".len() + 4 + content_type.as_str().len() * 3;
    }
    assert_eq!(portable_size, structured_material.max(binary_material));

    assert_eq!(percent_encode("é \"%"), "%C3%A9%20%22%25");
    let (_, headers) = http_binary(&stored);
    assert!(headers.iter().any(|(name, value)| {
        name == "ce-subject" && value == "subject%20%22%20%25%20caf%C3%A9"
    }));

    // The public event-size API can enforce a destination-specific finite
    // logical limit exactly, but lower transport framing is intentionally not
    // represented by Dovecote and remains an integration-owned check.
    assert!(
        full_event()
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size).unwrap())
            .is_ok()
    );
    assert!(
        full_event()
            .validate_with_limit(dovecote::EventSizeLimit::new(portable_size - 1).unwrap())
            .is_err()
    );
}
