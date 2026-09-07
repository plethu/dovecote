//! Validation and reconstruction of events read from `PostgreSQL`.

use dovecote::{EventData, EventSizeLimit, NewEvent, StoredEvent};
use time::OffsetDateTime;

/// Event columns shared by claim and page queries.
#[derive(Debug)]
pub(crate) struct EventRow<'a> {
    pub(crate) stream: &'a str,
    pub(crate) specversion: &'a str,
    pub(crate) event_id: &'a str,
    pub(crate) source: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) subject: Option<&'a str>,
    pub(crate) occurred_at: Option<OffsetDateTime>,
    pub(crate) datacontenttype: Option<&'a str>,
    pub(crate) dataschema: Option<&'a str>,
    pub(crate) partitionkey: Option<&'a str>,
    pub(crate) extensions: &'a str,
    pub(crate) data_kind: Option<&'a str>,
    pub(crate) data: Option<&'a [u8]>,
}

/// Reconstructs and validates one stored event from its database columns.
pub(crate) fn hydrate_event(row: &EventRow<'_>) -> Result<StoredEvent, String> {
    if row.specversion != dovecote::SPEC_VERSION {
        return Err("stored event has an unsupported specversion".to_owned());
    }

    let stream =
        dovecote::StreamName::new(row.stream.to_owned()).map_err(|error| error.to_string())?;
    let id = dovecote::EventId::new(row.event_id.to_owned()).map_err(|error| error.to_string())?;
    let source =
        dovecote::EventSource::new(row.source.to_owned()).map_err(|error| error.to_string())?;
    let event_type =
        dovecote::EventType::new(row.event_type.to_owned()).map_err(|error| error.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);
    builder = match row.subject {
        Some(value) => builder.subject(
            dovecote::EventSubject::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.occurred_at {
        Some(value) => builder.time(value),
        None => builder,
    };
    builder = match row.datacontenttype {
        Some(value) => builder.datacontenttype(
            dovecote::ContentType::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.dataschema {
        Some(value) => builder.dataschema(
            dovecote::SchemaUri::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.partitionkey {
        Some(value) => builder.partitionkey(
            dovecote::PartitionKey::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };

    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(row.extensions)
            .map_err(|error| error.to_string())?,
    );
    match (row.data_kind, row.data) {
        (None, None) => {}
        (Some("json"), Some(bytes)) => {
            builder =
                builder.data(EventData::json(bytes.to_owned()).map_err(|error| error.to_string())?);
        }
        (Some("binary"), Some(bytes)) => {
            builder = builder.data(EventData::binary(bytes.to_owned()));
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }

    builder
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("maximum size is non-zero"))
        .map_err(|error| error.to_string())?
        .into_stored()
        .map_err(|error| error.to_string())
}
