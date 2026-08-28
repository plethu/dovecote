//! Conversion of validated MySQL rows into Dovecote events.

use dovecote::{EventData, EventSizeLimit, NewEvent, StoredEvent};

/// Event columns shared by claim and paging queries.
pub(crate) struct EventColumns<'a> {
    pub(crate) stream: &'a [u8],
    pub(crate) specversion: &'a [u8],
    pub(crate) event_id: &'a [u8],
    pub(crate) source: &'a [u8],
    pub(crate) event_type: &'a [u8],
    pub(crate) subject: Option<&'a [u8]>,
    pub(crate) occurred_at: Option<time::OffsetDateTime>,
    pub(crate) datacontenttype: Option<&'a [u8]>,
    pub(crate) dataschema: Option<&'a [u8]>,
    pub(crate) partitionkey: Option<&'a [u8]>,
    pub(crate) extensions: &'a [u8],
    pub(crate) data_kind: Option<&'a [u8]>,
    pub(crate) data: Option<&'a [u8]>,
}

/// Hydrates an event after the adapter has loaded its complete event row.
pub(crate) fn hydrate_event(row: &EventColumns<'_>) -> Result<StoredEvent, String> {
    if row.specversion != dovecote::SPEC_VERSION.as_bytes() {
        return Err("stored event has unsupported specversion".to_owned());
    }

    let strv = |value: &[u8], field: &str| {
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| format!("stored {field} is not UTF-8"))
    };
    let stream =
        dovecote::StreamName::new(strv(row.stream, "stream")?).map_err(|e| e.to_string())?;
    let id = dovecote::EventId::new(strv(row.event_id, "event id")?).map_err(|e| e.to_string())?;
    let source =
        dovecote::EventSource::new(strv(row.source, "source")?).map_err(|e| e.to_string())?;
    let event_type =
        dovecote::EventType::new(strv(row.event_type, "event type")?).map_err(|e| e.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);

    builder = match row.subject {
        Some(value) => builder.subject(
            dovecote::EventSubject::new(strv(value, "subject")?).map_err(|e| e.to_string())?,
        ),
        None => builder,
    };
    builder = match row.occurred_at {
        Some(value) => builder.time(value),
        None => builder,
    };
    builder = match row.datacontenttype {
        Some(value) => builder.datacontenttype(
            dovecote::ContentType::new(strv(value, "content type")?).map_err(|e| e.to_string())?,
        ),
        None => builder,
    };
    builder = match row.dataschema {
        Some(value) => builder.dataschema(
            dovecote::SchemaUri::new(strv(value, "schema URI")?).map_err(|e| e.to_string())?,
        ),
        None => builder,
    };
    builder = match row.partitionkey {
        Some(value) => builder.partitionkey(
            dovecote::PartitionKey::new(strv(value, "partition key")?)
                .map_err(|e| e.to_string())?,
        ),
        None => builder,
    };
    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(&strv(row.extensions, "extensions")?)
            .map_err(|e| e.to_string())?,
    );
    match (row.data_kind, row.data) {
        (None, None) => {}
        (Some(kind), Some(value)) if kind == b"json" => {
            builder = builder.data(EventData::json(value.to_owned()).map_err(|e| e.to_string())?);
        }
        (Some(kind), Some(value)) if kind == b"binary" => {
            builder = builder.data(EventData::binary(value.to_owned()));
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }

    builder
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("nonzero"))
        .map_err(|e| e.to_string())?
        .into_stored()
        .map_err(|e| e.to_string())
}
