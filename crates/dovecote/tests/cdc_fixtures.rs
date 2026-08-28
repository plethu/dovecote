//! Reference-transform conformance fixtures for the documented Debezium path.
//!
//! This is deliberately not a Kafka Connect or Debezium runner. It models the
//! row selected by `table.include.list=dovecote_events`, the stable fields
//! emitted by the Outbox Event Router, and the caller-owned downstream
//! CloudEvents transform. The fixtures are useful evidence for mapping and
//! byte preservation; they do not advertise live connector coverage.

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use dovecote::{
    ContentType, EventData, EventId, EventSource, EventSubject, EventType, Extensions, NewEvent,
    PartitionKey, SchemaUri, StreamName,
};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const EVENTS_TABLE: &str = "dovecote_events";
const CONVERTER: &str = "dovecote-json-envelope-base64-v1";

#[derive(Clone, Debug, Deserialize)]
struct CdcFixture {
    name: String,
    input: CdcInput,
    expected: ExpectedCdc,
}

#[derive(Clone, Debug, Deserialize)]
struct CdcInput {
    table: String,
    operation: String,
    row: Option<CdcRow>,
}

#[derive(Clone, Debug, Deserialize)]
struct CdcRow {
    row_id: i64,
    tenant_id: String,
    event_id: String,
    event_type: String,
    stream: String,
    source: String,
    subject: Option<String>,
    occurred_at: Option<String>,
    datacontenttype: Option<String>,
    dataschema: Option<String>,
    partitionkey: Option<String>,
    extensions: String,
    data_kind: Option<String>,
    data_base64: Option<String>,
    enqueued_at: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ExpectedCdc {
    action: String,
    raw: Option<RawSmt>,
    downstream: Option<DownstreamOutput>,
    error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct RawSmt {
    converter: String,
    topic: String,
    key_base64: Option<String>,
    timestamp_ms: i64,
    headers: BTreeMap<String, Option<String>>,
    envelope: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct DownstreamOutput {
    structured_json: String,
    binary_body_base64: Option<String>,
    binary_datacontenttype: Option<String>,
    binary_attributes: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TransformResult {
    Ignored,
    Emitted {
        raw: Box<RawSmt>,
        downstream: Box<DownstreamOutput>,
    },
}

fn fixtures() -> Vec<CdcFixture> {
    serde_json::from_str(include_str!("fixtures/cdc.json"))
        .expect("checked-in CDC fixtures are valid JSON")
}

fn transform(input: &CdcInput) -> Result<TransformResult, String> {
    if input.table != EVENTS_TABLE {
        return Ok(TransformResult::Ignored);
    }

    if input.operation != "c" {
        return Err("watched dovecote_events changed without an insert".to_owned());
    }

    let row = input
        .row
        .as_ref()
        .ok_or_else(|| "watched insert has no row".to_owned())?;

    let enqueued_at = OffsetDateTime::parse(&row.enqueued_at, &Rfc3339)
        .map_err(|error| format!("invalid enqueue timestamp: {error}"))?;
    let timestamp_ms = enqueued_at
        .unix_timestamp_nanos()
        .checked_div(1_000_000)
        .ok_or_else(|| "enqueue timestamp cannot be represented as milliseconds".to_owned())?;
    let timestamp_ms = i64::try_from(timestamp_ms)
        .map_err(|_| "enqueue timestamp milliseconds overflowed i64".to_owned())?;

    let data = row
        .data_base64
        .as_deref()
        .map(|value| BASE64.decode(value).map_err(|error| error.to_string()))
        .transpose()
        .map_err(|error| format!("invalid payload base64: {error}"))?;

    let mut headers = BTreeMap::new();
    headers.insert("id".to_owned(), Some(row.event_id.clone()));
    headers.insert("type".to_owned(), Some(row.event_type.clone()));
    headers.insert("ce_specversion".to_owned(), Some("1.0".to_owned()));
    headers.insert("ce_source".to_owned(), Some(row.source.clone()));
    headers.insert("ce_subject".to_owned(), row.subject.clone());
    headers.insert("ce_time".to_owned(), row.occurred_at.clone());
    headers.insert("content-type".to_owned(), row.datacontenttype.clone());
    headers.insert("ce_dataschema".to_owned(), row.dataschema.clone());
    headers.insert("ce_partitionkey".to_owned(), row.partitionkey.clone());

    // The declared converter puts the payload and Dovecote's tenant/routing
    // and additional envelope fields in one value envelope. `payload` is a
    // base64 string under this fixture's JSON converter assumption.
    let mut envelope = BTreeMap::new();
    envelope.insert(
        "payload".to_owned(),
        data.as_deref().map(|value| BASE64.encode(value)),
    );
    envelope.insert("dovecote_tenant_id".to_owned(), Some(row.tenant_id.clone()));
    envelope.insert(
        "dovecote_extensions".to_owned(),
        Some(row.extensions.clone()),
    );
    envelope.insert("dovecote_data_kind".to_owned(), row.data_kind.clone());
    envelope.insert("dovecote_row_id".to_owned(), Some(row.row_id.to_string()));
    // Keep the source logical timestamp rather than reformatting the value
    // through the millisecond Kafka record timestamp.
    envelope.insert(
        "dovecote_enqueued_at".to_owned(),
        Some(row.enqueued_at.clone()),
    );

    let raw = RawSmt {
        converter: CONVERTER.to_owned(),
        topic: format!("outbox.event.{}", row.stream),
        key_base64: row
            .partitionkey
            .as_ref()
            .map(|value| BASE64.encode(value.as_bytes())),
        timestamp_ms,
        headers,
        envelope,
    };
    let downstream = downstream_event(row, data)?;
    Ok(TransformResult::Emitted {
        raw: Box::new(raw),
        downstream: Box::new(downstream),
    })
}

fn downstream_event(row: &CdcRow, data: Option<Vec<u8>>) -> Result<DownstreamOutput, String> {
    let stream = StreamName::new(row.stream.clone()).map_err(|error| error.to_string())?;
    let id = EventId::new(row.event_id.clone()).map_err(|error| error.to_string())?;
    let source = EventSource::new(row.source.clone()).map_err(|error| error.to_string())?;
    let event_type = EventType::new(row.event_type.clone()).map_err(|error| error.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);

    // These optional CloudEvents attributes are independent fields; matching
    // each one explicitly keeps source-column order from becoming policy.
    builder = match &row.subject {
        Some(value) => {
            builder.subject(EventSubject::new(value.clone()).map_err(|error| error.to_string())?)
        }
        None => builder,
    };

    builder = match &row.occurred_at {
        Some(value) => {
            builder.time(OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())?)
        }
        None => builder,
    };

    builder = match &row.datacontenttype {
        Some(value) => builder
            .datacontenttype(ContentType::new(value.clone()).map_err(|error| error.to_string())?),
        None => builder,
    };

    builder = match &row.dataschema {
        Some(value) => {
            builder.dataschema(SchemaUri::new(value.clone()).map_err(|error| error.to_string())?)
        }
        None => builder,
    };

    builder = match &row.partitionkey {
        Some(value) => builder
            .partitionkey(PartitionKey::new(value.clone()).map_err(|error| error.to_string())?),
        None => builder,
    };
    builder = builder.extensions(
        Extensions::from_canonical_json(&row.extensions).map_err(|error| error.to_string())?,
    );
    if let Some(data) = data {
        builder = match row.data_kind.as_deref() {
            Some("json") => builder.data(EventData::json(data).map_err(|error| error.to_string())?),
            Some("binary") | None => builder.data(EventData::binary(data)),
            Some(other) => return Err(format!("unsupported Dovecote data kind: {other}")),
        };
    }

    let stored = builder
        .build()
        .map_err(|error| error.to_string())?
        .into_stored()
        .map_err(|error| error.to_string())?;
    let structured = stored
        .structured_json()
        .map_err(|error| error.to_string())?;
    let binary = stored.binary();
    Ok(DownstreamOutput {
        structured_json: String::from_utf8(structured.as_bytes().to_vec())
            .map_err(|error| error.to_string())?,
        binary_body_base64: binary.body().map(|value| BASE64.encode(value)),
        binary_datacontenttype: binary
            .datacontenttype()
            .map(|value| value.as_str().to_owned()),
        binary_attributes: binary
            .attributes()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
    })
}

#[test]
fn reference_transform_fixtures_cover_selection_bytes_nulls_and_time_precision() {
    for fixture in fixtures() {
        let actual = transform(&fixture.input);
        match fixture.expected.action.as_str() {
            "ignored" => assert_eq!(actual, Ok(TransformResult::Ignored), "{}", fixture.name),
            "rejected" => {
                let error = actual.expect_err(&fixture.name);
                assert_eq!(Some(error), fixture.expected.error, "{}", fixture.name);
            }
            "emitted" => {
                let Ok(TransformResult::Emitted { raw, downstream }) = actual else {
                    panic!("{} did not emit", fixture.name);
                };
                assert_eq!(Some(*raw.clone()), fixture.expected.raw, "{}", fixture.name);
                assert_eq!(
                    Some(*downstream),
                    fixture.expected.downstream,
                    "{}",
                    fixture.name
                );
                assert_eq!(raw.envelope.len(), 6, "{}", fixture.name);
                assert!(raw.envelope.contains_key("payload"), "{}", fixture.name);
                assert!(
                    raw.headers.contains_key("ce_specversion"),
                    "{}",
                    fixture.name
                );
                assert!(raw.headers.contains_key("ce_source"), "{}", fixture.name);
                assert!(raw.headers.contains_key("content-type"), "{}", fixture.name);
            }
            unknown => panic!("unknown CDC fixture action: {unknown}"),
        }
    }
}

#[test]
fn reference_transform_does_not_treat_a_delivery_insert_as_watched_cdc() {
    let fixture = fixtures()
        .into_iter()
        .find(|fixture| fixture.name == "delivery-insert")
        .expect("delivery fixture exists");
    assert_eq!(transform(&fixture.input), Ok(TransformResult::Ignored));
}

#[test]
fn every_delivery_lifecycle_table_operation_is_outside_the_watched_set() {
    for operation in ["c", "u", "d"] {
        let input = CdcInput {
            table: "dovecote_deliveries".to_owned(),
            operation: operation.to_owned(),
            row: None,
        };
        assert_eq!(
            transform(&input),
            Ok(TransformResult::Ignored),
            "{operation}"
        );
    }
}
