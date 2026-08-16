use crate::validation::{validate_traceparent, validate_tracestate};
use crate::{
    ContentType, EventData, EventId, EventSizeLimit, EventSource, EventType, ExtensionName,
    ExtensionValue, Extensions, NewEvent, StreamName, Timestamp, UriReference,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .unwrap()
}

#[test]
fn canonical_extensions_are_sorted_and_tagged() {
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("zeta").unwrap(),
            ExtensionValue::Integer(3),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("alpha").unwrap(),
            ExtensionValue::Boolean(true),
        )
        .unwrap();

    assert_eq!(
        extensions.canonical_json(),
        r#"{"alpha":{"type":"boolean","value":true},"zeta":{"type":"integer","value":3}}"#
    );
    assert_eq!(
        Extensions::from_canonical_json(&extensions.canonical_json()).unwrap(),
        extensions
    );
}

#[test]
fn json_data_requires_a_json_media_type() {
    let value = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("event-1").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .datacontenttype(ContentType::new("text/plain").unwrap())
    .data(EventData::json(br#"{"ok":true}"#.to_vec()).unwrap())
    .build();

    assert!(value.is_err());
}

#[test]
fn absent_and_empty_data_remain_distinct() {
    let absent = event("event-1");
    let empty = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("event-2").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .data(EventData::binary(Vec::new()))
    .build()
    .unwrap();

    assert_ne!(absent, empty);
    assert!(absent.validate().is_ok());
    assert!(empty.validate().is_ok());
}

#[test]
fn reserved_and_duplicate_extensions_are_rejected() {
    assert!(ExtensionName::new("source").is_err());
    let mut extensions = Extensions::new();
    let name = ExtensionName::new("traceparent").unwrap();
    extensions
        .insert(name.clone(), ExtensionValue::string("00-11-22-33").unwrap())
        .unwrap();
    assert!(
        extensions
            .insert(name, ExtensionValue::Boolean(true))
            .is_err()
    );
}

#[test]
fn trace_state_requires_a_valid_trace_parent() {
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("tracestate").unwrap(),
            ExtensionValue::string("vendor=value").unwrap(),
        )
        .unwrap();
    assert!(
        NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new("event-3").unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.audit").unwrap(),
        )
        .extensions(extensions)
        .build()
        .is_err()
    );
}

#[test]
fn malformed_uri_references_are_rejected() {
    assert!(EventSource::new("https://example.test/%ZZ").is_err());
    assert!(EventSource::new("1abc:foo").is_err());
    assert!(EventSource::new(":foo").is_err());
    assert!(UriReference::new("foo:bar").is_ok());
    assert!(UriReference::new("/valid/%20/path").is_ok());
}

#[test]
fn trace_context_uses_strict_w3c_grammar() {
    assert!(
        validate_traceparent("00-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA-0123456789abcdef-01").is_err()
    );
    assert!(validate_tracestate("vendor.foo=value").is_err());
    assert!(validate_tracestate("vendor@abcdefghijklmnop=value").is_err());
    assert!(validate_tracestate("vendor@tenant=value").is_ok());
    assert!(validate_tracestate("1tenant@vendor=hello world").is_ok());
    assert!(validate_tracestate("vendor@1tenant=hello world").is_err());
    assert!(validate_tracestate("1vendor=value").is_err());
    assert!(validate_tracestate("_vendor=value").is_err());
    assert!(validate_tracestate("-vendor=value").is_err());
    assert!(validate_tracestate("vendor=").is_ok());
    assert!(validate_tracestate("").is_ok());
    assert!(validate_tracestate(" , \t").is_ok());
}

#[test]
fn decoded_timestamps_must_use_the_canonical_utc_form() {
    let timestamp =
        Timestamp::new(OffsetDateTime::parse("2026-01-01T00:00:00.120Z", &Rfc3339).unwrap())
            .unwrap();
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("received").unwrap(),
            ExtensionValue::Timestamp(timestamp),
        )
        .unwrap();
    let encoded = extensions.canonical_json();
    assert_eq!(
        Extensions::from_canonical_json(&encoded).unwrap(),
        extensions
    );
    assert!(
        Extensions::from_canonical_json(
            r#"{"received":{"type":"timestamp","value":"2026-01-01T01:00:00+01:00"}}"#
        )
        .is_err()
    );
    assert!(
        Extensions::from_canonical_json(
            r#"{"tracestate":{"type":"string","value":"vendor=value"}}"#
        )
        .is_err()
    );
    assert!(
        Extensions::from_canonical_json(
            r#" { "received": { "type": "timestamp", "value": "2026-01-01T00:00:00.12Z" } } "#
        )
        .is_err()
    );
    assert!(Extensions::from_canonical_json(
        r#"{"received":{"type":"timestamp","type":"timestamp","value":"2026-01-01T00:00:00.12Z"}}"#
    )
    .is_err());
}

#[test]
fn configured_event_size_limit_is_enforced_at_finalization() {
    let result = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("event-4").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .datacontenttype(ContentType::new("application/octet-stream").unwrap())
    .data(EventData::binary(vec![b'x'; 128]))
    .build_with_limit(EventSizeLimit::new(64).unwrap());

    assert!(result.is_err());
}

#[test]
fn configured_size_limit_survives_stored_event_finalization() {
    let event = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("event-large").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .datacontenttype(ContentType::new("application/octet-stream").unwrap())
    .data(EventData::binary(vec![b'x'; 70_000]))
    .build_with_limit(EventSizeLimit::new(100_000).unwrap())
    .unwrap();

    assert!(event.portable_size().unwrap() > crate::MAX_PORTABLE_EVENT_BYTES);
    assert!(event.into_stored().is_ok());
}

#[test]
fn portable_size_accepts_exact_boundary_only() {
    let event = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("event-size").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .datacontenttype(ContentType::new("application/octet-stream").unwrap())
    .data(EventData::binary(vec![b'x'; 128]))
    .build()
    .unwrap();
    let size = event.portable_size().unwrap();
    assert!(
        event
            .validate_with_limit(EventSizeLimit::new(size).unwrap())
            .is_ok()
    );
    assert!(
        event
            .validate_with_limit(EventSizeLimit::new(size - 1).unwrap())
            .is_err()
    );
}
