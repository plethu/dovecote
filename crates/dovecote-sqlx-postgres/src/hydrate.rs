//! Validation and reconstruction of events read from PostgreSQL.

use dovecote::{EventData, EventSizeLimit, NewEvent, StoredEvent};
use time::OffsetDateTime;

/// Event columns shared by claim and page queries.
#[derive(Debug)]
pub(crate) struct EventRow {
    pub(crate) stream: String,
    pub(crate) specversion: String,
    pub(crate) event_id: String,
    pub(crate) source: String,
    pub(crate) event_type: String,
    pub(crate) subject: Option<String>,
    pub(crate) occurred_at: Option<OffsetDateTime>,
    pub(crate) datacontenttype: Option<String>,
    pub(crate) dataschema: Option<String>,
    pub(crate) partitionkey: Option<String>,
    pub(crate) extensions: String,
    pub(crate) data_kind: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
}

/// Reconstructs and validates one stored event from its database columns.
pub(crate) fn hydrate_event(row: &EventRow) -> Result<StoredEvent, String> {
    if row.specversion != dovecote::SPEC_VERSION {
        return Err("stored event has an unsupported specversion".to_owned());
    }

    let stream =
        dovecote::StreamName::new(row.stream.clone()).map_err(|error| error.to_string())?;
    let id = dovecote::EventId::new(row.event_id.clone()).map_err(|error| error.to_string())?;
    let source =
        dovecote::EventSource::new(row.source.clone()).map_err(|error| error.to_string())?;
    let event_type =
        dovecote::EventType::new(row.event_type.clone()).map_err(|error| error.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);
    builder = match &row.subject {
        Some(value) => builder.subject(
            dovecote::EventSubject::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.occurred_at {
        Some(value) => builder.time(value),
        None => builder,
    };
    builder = match &row.datacontenttype {
        Some(value) => builder.datacontenttype(
            dovecote::ContentType::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match &row.dataschema {
        Some(value) => builder.dataschema(
            dovecote::SchemaUri::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match &row.partitionkey {
        Some(value) => builder.partitionkey(
            dovecote::PartitionKey::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };

    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(&row.extensions)
            .map_err(|error| error.to_string())?,
    );
    match (&row.data_kind, &row.data) {
        (None, None) => {}
        (Some(kind), Some(bytes)) if kind == "json" => {
            builder =
                builder.data(EventData::json(bytes.clone()).map_err(|error| error.to_string())?);
        }
        (Some(kind), Some(bytes)) if kind == "binary" => {
            builder = builder.data(EventData::binary(bytes.clone()));
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }

    builder
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("maximum size is non-zero"))
        .map_err(|error| error.to_string())?
        .into_stored()
        .map_err(|error| error.to_string())
}
