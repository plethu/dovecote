//! Caller-transaction enqueue and durable-event hydration for MySQL/MariaDB.

use crate::{backend, error::EnqueueError, migration::current_migration};
use dovecote::{EnqueueOutcome, EventData, EventSizeLimit, NewEvent, RowId};
use sqlx::{FromRow, MySql, Transaction, query, query_as, query_scalar};
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

/// Inserts an event and its pending delivery in the supplied transaction.
/// The caller owns commit and rollback; no pool transaction is created here.
pub async fn enqueue<'c>(
    transaction: &mut Transaction<'c, MySql>,
    event: NewEvent,
) -> Result<EnqueueOutcome, EnqueueError> {
    validate_enqueue_schema(transaction).await?;
    let operation_time = query_scalar::<_, OffsetDateTime>("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| EnqueueError::sql("read enqueue operation time", source))?;
    let extensions = event.extensions().canonical_json();
    // MySQL/MariaDB DATETIME is a timezone-free UTC value.  Bind occurrence
    // times as SQLx's DATETIME-native `PrimitiveDateTime`, rather than its
    // TIMESTAMP-typed `OffsetDateTime` encoding.  This preserves the full
    // common range, including MariaDB's 9999-12-31 endpoint.
    let occurred_at = event.time().map(database_datetime);
    let (data_kind, data): (Option<&str>, Option<Vec<u8>>) =
        event.data().map_or((None, None), |data| {
            (
                Some(if data.is_json() { "json" } else { "binary" }),
                Some(data.as_bytes().to_vec()),
            )
        });

    // The identity index serializes concurrent producers. Duplicate identity
    // is handled as an expected branch only after SQLx reports a unique
    // violation; every other database failure remains an actionable error.
    let inserted = match query(
        r#"
        INSERT INTO dovecote_events
            (stream, specversion, event_id, source, event_type, subject,
             occurred_at, datacontenttype, dataschema, partitionkey, extensions,
             data_kind, data, enqueued_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#,
    )
    .bind(event.stream().as_str().as_bytes())
    .bind(event.specversion().as_bytes())
    .bind(event.id().as_str().as_bytes())
    .bind(event.source().as_str().as_bytes())
    .bind(event.event_type().as_str().as_bytes())
    .bind(event.subject().map(|value| value.as_str().as_bytes()))
    .bind(occurred_at)
    .bind(
        event
            .datacontenttype()
            .map(|value| value.as_str().as_bytes()),
    )
    .bind(event.dataschema().map(|value| value.as_str().as_bytes()))
    .bind(event.partitionkey().map(|value| value.as_str().as_bytes()))
    .bind(extensions.as_bytes())
    .bind(data_kind.map(str::as_bytes))
    .bind(data)
    .bind(operation_time)
    .execute(&mut **transaction)
    .await
    {
        Ok(_) => true,
        Err(source)
            if source
                .as_database_error()
                .is_some_and(|error| error.is_unique_violation()) =>
        {
            false
        }
        Err(source) => return Err(EnqueueError::sql("insert event", source)),
    };

    let existing = query_as::<_, ExistingEvent>(
        r#"
        SELECT row_id, stream, specversion, event_id, source, event_type,
               subject, occurred_at, datacontenttype, dataschema,
               partitionkey, extensions, data_kind, data, enqueued_at
        FROM dovecote_events
        WHERE source = ? AND event_id = ?
    "#,
    )
    .bind(event.source().as_str().as_bytes())
    .bind(event.id().as_str().as_bytes())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("find inserted event", source))?
    .ok_or_else(|| {
        EnqueueError::sql(
            "resolve inserted event",
            sqlx::Error::Protocol("identity insert returned no row".to_owned()),
        )
    })?;
    let existing_id = RowId::new(existing.row_id)
        .map_err(|error| EnqueueError::serialization(error.to_string()))?;
    validate_existing_event(&existing).map_err(EnqueueError::serialization)?;
    if !same_event(&event, &existing)? {
        return Err(EnqueueError::IdempotencyConflict {
            existing_row_id: existing_id,
        });
    }

    let delivery_exists: Option<i64> =
        query_scalar("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(existing.row_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|source| EnqueueError::sql("check duplicate delivery", source))?;
    if delivery_exists.is_some() {
        return Ok(EnqueueOutcome::AlreadyEnqueued {
            row_id: existing_id,
        });
    }

    // A new event is the only valid path without a delivery row.  Existing
    // events with a missing companion row indicate schema/data corruption.
    if !inserted {
        return Err(EnqueueError::MigrationMismatch {
            detail: "an existing event has no delivery row".to_owned(),
        });
    }
    query("INSERT INTO dovecote_deliveries (event_row_id, state, available_at) VALUES (?, ?, ?)")
        .bind(existing.row_id)
        .bind(b"pending".as_slice())
        .bind(operation_time)
        .execute(&mut **transaction)
        .await
        .map_err(|source| EnqueueError::sql("insert delivery", source))?;
    Ok(EnqueueOutcome::Enqueued {
        row_id: existing_id,
    })
}

#[derive(Debug, FromRow)]
pub(crate) struct ExistingEvent {
    pub(crate) row_id: i64,
    pub(crate) stream: Vec<u8>,
    pub(crate) specversion: Vec<u8>,
    pub(crate) event_id: Vec<u8>,
    pub(crate) source: Vec<u8>,
    pub(crate) event_type: Vec<u8>,
    pub(crate) subject: Option<Vec<u8>>,
    pub(crate) occurred_at: Option<PrimitiveDateTime>,
    pub(crate) datacontenttype: Option<Vec<u8>>,
    pub(crate) dataschema: Option<Vec<u8>>,
    pub(crate) partitionkey: Option<Vec<u8>>,
    pub(crate) extensions: Vec<u8>,
    pub(crate) data_kind: Option<Vec<u8>>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) enqueued_at: PrimitiveDateTime,
}

fn text<'a>(value: &'a [u8], field: &str) -> Result<&'a str, String> {
    std::str::from_utf8(value).map_err(|_| format!("stored {field} is not UTF-8"))
}
fn optional_text<'a>(value: &'a Option<Vec<u8>>, field: &str) -> Result<Option<&'a str>, String> {
    value.as_deref().map(|value| text(value, field)).transpose()
}

pub(crate) fn same_event(event: &NewEvent, row: &ExistingEvent) -> Result<bool, EnqueueError> {
    let equal = event.stream().as_str()
        == text(&row.stream, "stream").map_err(EnqueueError::serialization)?
        && event.specversion()
            == text(&row.specversion, "specversion").map_err(EnqueueError::serialization)?
        && event.id().as_str()
            == text(&row.event_id, "event id").map_err(EnqueueError::serialization)?
        && event.source().as_str()
            == text(&row.source, "source").map_err(EnqueueError::serialization)?
        && event.event_type().as_str()
            == text(&row.event_type, "event type").map_err(EnqueueError::serialization)?
        && event.subject().map(|value| value.as_str())
            == optional_text(&row.subject, "subject").map_err(EnqueueError::serialization)?
        && event.time().map(database_datetime) == row.occurred_at
        && event.datacontenttype().map(|value| value.as_str())
            == optional_text(&row.datacontenttype, "content type")
                .map_err(EnqueueError::serialization)?
        && event.dataschema().map(|value| value.as_str())
            == optional_text(&row.dataschema, "schema URI").map_err(EnqueueError::serialization)?
        && event.partitionkey().map(|value| value.as_str())
            == optional_text(&row.partitionkey, "partition key")
                .map_err(EnqueueError::serialization)?
        && event.extensions().canonical_json().as_bytes() == row.extensions.as_slice()
        && event.data().map(|data| {
            if data.is_json() {
                b"json".as_slice()
            } else {
                b"binary".as_slice()
            }
        }) == row.data_kind.as_deref()
        && event.data().map(|data| data.as_bytes()) == row.data.as_deref();
    Ok(equal)
}

#[allow(clippy::single_match)]
pub(crate) fn validate_existing_event(row: &ExistingEvent) -> Result<(), String> {
    if text(&row.specversion, "specversion")? != dovecote::SPEC_VERSION {
        return Err("stored event has unsupported specversion".to_owned());
    }

    let stream = dovecote::StreamName::new(text(&row.stream, "stream")?.to_owned())
        .map_err(|e| e.to_string())?;
    let id = dovecote::EventId::new(text(&row.event_id, "event id")?.to_owned())
        .map_err(|e| e.to_string())?;
    let source = dovecote::EventSource::new(text(&row.source, "source")?.to_owned())
        .map_err(|e| e.to_string())?;
    let event_type = dovecote::EventType::new(text(&row.event_type, "event type")?.to_owned())
        .map_err(|e| e.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);
    // These optional CloudEvents attributes are independent, not priority
    // policy. Their source-column order stays explicit for deterministic
    // hydration.
    match optional_text(&row.subject, "subject")? {
        Some(value) => {
            builder = builder
                .subject(dovecote::EventSubject::new(value.to_owned()).map_err(|e| e.to_string())?);
        }
        None => {}
    }

    match row.occurred_at {
        Some(value) => {
            builder = builder.time(value.assume_utc());
        }
        None => {}
    }

    match optional_text(&row.datacontenttype, "content type")? {
        Some(value) => {
            builder = builder.datacontenttype(
                dovecote::ContentType::new(value.to_owned()).map_err(|e| e.to_string())?,
            );
        }
        None => {}
    }

    match optional_text(&row.dataschema, "schema URI")? {
        Some(value) => {
            builder = builder
                .dataschema(dovecote::SchemaUri::new(value.to_owned()).map_err(|e| e.to_string())?);
        }
        None => {}
    }

    match optional_text(&row.partitionkey, "partition key")? {
        Some(value) => {
            builder = builder.partitionkey(
                dovecote::PartitionKey::new(value.to_owned()).map_err(|e| e.to_string())?,
            );
        }
        None => {}
    }
    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(text(&row.extensions, "extensions")?)
            .map_err(|e| e.to_string())?,
    );
    match (&row.data_kind, &row.data) {
        (None, None) => {}
        (Some(kind), Some(bytes)) if kind.as_slice() == b"json" => {
            builder = builder.data(EventData::json(bytes.clone()).map_err(|e| e.to_string())?);
        }
        (Some(kind), Some(bytes)) if kind.as_slice() == b"binary" => {
            builder = builder.data(EventData::binary(bytes.clone()));
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }
    builder
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("nonzero"))
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn database_datetime(value: OffsetDateTime) -> PrimitiveDateTime {
    let value = value.to_offset(UtcOffset::UTC);
    PrimitiveDateTime::new(value.date(), value.time())
}

#[derive(Debug, FromRow)]
struct SchemaSelection {
    events_table: i64,
    deliveries_table: i64,
}

pub(crate) async fn validate_enqueue_schema<'c>(
    transaction: &mut Transaction<'c, MySql>,
) -> Result<(), EnqueueError> {
    let info = backend::detect_on_connection(transaction)
        .await
        .map_err(|error| match error {
            crate::SchemaError::BackendMismatch { detail } => {
                EnqueueError::BackendMismatch { detail }
            }
            crate::SchemaError::Sql { operation, source } => EnqueueError::sql(operation, source),
            crate::SchemaError::Transient {
                operation,
                kind,
                source,
            } => EnqueueError::Transient {
                operation,
                kind,
                source,
            },
            crate::SchemaError::MigrationMismatch { detail } => {
                EnqueueError::MigrationMismatch { detail }
            }
        })?;
    if !info.capabilities.enforced_checks {
        return Err(EnqueueError::BackendMismatch {
            detail: "CHECK constraints are not enforced".to_owned(),
        });
    }

    let selection = query_as::<_, SchemaSelection>(r#"SELECT
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'dovecote_events') AS events_table,
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries') AS deliveries_table"#)
        .fetch_one(&mut **transaction).await.map_err(|source| EnqueueError::sql("validate enqueue schema selection", source))?;
    if selection.events_table == 0 || selection.deliveries_table == 0 {
        return Err(EnqueueError::MigrationMismatch {
            detail: "current database does not contain the Dovecote tables".to_owned(),
        });
    }
    current_migration().map_err(|detail| EnqueueError::MigrationMismatch { detail })?;
    Ok(())
}
