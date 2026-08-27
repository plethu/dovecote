//! Caller-transaction-bound SQLite enqueue and idempotency.

use crate::{
    error::EnqueueError,
    migration::{current_migration, migration_is_usable},
    transaction_is_write,
};
use dovecote::{EnqueueOutcome, EventData, EventSizeLimit, NewEvent, RowId};
use sqlx::{FromRow, Sqlite, Transaction, query, query_as, query_scalar};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

/// Inserts an event and its delivery in the caller's transaction.
///
/// The caller remains responsible for commit or rollback. No migration or
/// implicit transaction is started here, so application state and the outbox
/// rows retain one atomic commit boundary.
pub async fn enqueue<'c>(
    transaction: &mut Transaction<'c, Sqlite>,
    event: NewEvent,
) -> Result<EnqueueOutcome, EnqueueError> {
    // A deferred transaction can read the identity before another connection
    // claims the writer slot, then later fail with SQLITE_BUSY (or, worse,
    // commit an application write without the outbox row). Require the caller
    // to own SQLite's write transaction before touching adapter state.
    if !transaction_is_write(transaction)
        .await
        .map_err(|source| EnqueueError::sql("inspect enqueue transaction state", source))?
    {
        return Err(EnqueueError::WriteTransactionRequired);
    }

    validate_enqueue_schema(transaction).await?;
    let extensions = event.extensions().canonical_json();
    let (data_kind, data) = event.data().map_or((None, None), |data| {
        (
            Some(if data.is_json() { "json" } else { "binary" }),
            Some(data.as_bytes().to_vec()),
        )
    });
    let occurred_at = event.time().map(format_timestamp);
    let inserted = query_as::<_, InsertedEvent>(
        "INSERT INTO dovecote_events (stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source, event_id) DO NOTHING RETURNING row_id, enqueued_at",
    )
    .bind(event.stream().as_str())
    .bind(event.specversion())
    .bind(event.id().as_str())
    .bind(event.source().as_str())
    .bind(event.event_type().as_str())
    .bind(event.subject().map(|value| value.as_str()))
    .bind(occurred_at)
    .bind(event.datacontenttype().map(|value| value.as_str()))
    .bind(event.dataschema().map(|value| value.as_str()))
    .bind(event.partitionkey().map(|value| value.as_str()))
    .bind(extensions)
    .bind(data_kind)
    .bind(data)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("insert event", source))?;

    let Some(inserted) = inserted else {
        let existing = query_as::<_, ExistingEvent>(
            "SELECT row_id, stream, specversion, event_id, source, event_type, subject, occurred_at, datacontenttype, dataschema, partitionkey, extensions, data_kind, data, enqueued_at FROM dovecote_events WHERE source = ? COLLATE BINARY AND event_id = ? COLLATE BINARY",
        )
        .bind(event.source().as_str())
        .bind(event.id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| EnqueueError::sql("find duplicate event", source))?
        .ok_or_else(|| EnqueueError::sql("resolve duplicate event", sqlx::Error::Protocol("identity disappeared after conflict".to_owned())))?;

        let existing_id = RowId::new(existing.row_id)
            .map_err(|error| EnqueueError::serialization(error.to_string()))?;
        validate_existing_event(&existing).map_err(EnqueueError::serialization)?;
        if !same_event(&event, &existing) {
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
        if delivery_exists.is_none() {
            return Err(EnqueueError::MigrationMismatch {
                detail: "an existing event has no delivery row".to_owned(),
            });
        }

        return Ok(EnqueueOutcome::AlreadyEnqueued {
            row_id: existing_id,
        });
    };

    let row_id = RowId::new(inserted.row_id)
        .map_err(|error| EnqueueError::serialization(error.to_string()))?;
    query("INSERT INTO dovecote_deliveries (event_row_id, state, available_at) VALUES (?, 'pending', ?)")
        .bind(inserted.row_id).bind(inserted.enqueued_at)
        .execute(&mut **transaction).await
        .map_err(|source| EnqueueError::sql("insert delivery", source))?;
    Ok(EnqueueOutcome::Enqueued { row_id })
}

pub(crate) async fn validate_enqueue_schema<'c>(
    transaction: &mut Transaction<'c, Sqlite>,
) -> Result<(), EnqueueError> {
    let foreign_keys: i64 = query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| EnqueueError::sql("check enqueue foreign-key enforcement", source))?;
    if foreign_keys != 1 {
        return Err(EnqueueError::MigrationMismatch {
            detail: "foreign-key enforcement is disabled".to_owned(),
        });
    }

    for table in ["dovecote_events", "dovecote_deliveries"] {
        let present: Option<String> =
            query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|source| EnqueueError::sql("validate enqueue domain tables", source))?;
        if present.is_none() {
            return Err(EnqueueError::MigrationMismatch {
                detail: format!("{table} is missing"),
            });
        }
    }

    let migration =
        current_migration().map_err(|detail| EnqueueError::MigrationMismatch { detail })?;
    migration_is_usable(migration).map_err(|detail| EnqueueError::MigrationMismatch { detail })
}

#[derive(Debug, FromRow)]
struct InsertedEvent {
    row_id: i64,
    enqueued_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ExistingEvent {
    pub(crate) row_id: i64,
    pub(crate) stream: String,
    pub(crate) specversion: String,
    pub(crate) event_id: String,
    pub(crate) source: String,
    pub(crate) event_type: String,
    pub(crate) subject: Option<String>,
    pub(crate) occurred_at: Option<String>,
    pub(crate) datacontenttype: Option<String>,
    pub(crate) dataschema: Option<String>,
    pub(crate) partitionkey: Option<String>,
    pub(crate) extensions: String,
    pub(crate) data_kind: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) enqueued_at: String,
}

pub(crate) fn same_event(event: &NewEvent, existing: &ExistingEvent) -> bool {
    event.stream().as_str() == existing.stream
        && event.specversion() == existing.specversion
        && event.id().as_str() == existing.event_id
        && event.source().as_str() == existing.source
        && event.event_type().as_str() == existing.event_type
        && event.subject().map(|value| value.as_str()) == existing.subject.as_deref()
        && event.time().map(format_timestamp).as_deref() == existing.occurred_at.as_deref()
        && event.datacontenttype().map(|value| value.as_str())
            == existing.datacontenttype.as_deref()
        && event.dataschema().map(|value| value.as_str()) == existing.dataschema.as_deref()
        && event.partitionkey().map(|value| value.as_str()) == existing.partitionkey.as_deref()
        && event.extensions().canonical_json() == existing.extensions
        && event
            .data()
            .map(|data| if data.is_json() { "json" } else { "binary" })
            == existing.data_kind.as_deref()
        && event.data().map(|data| data.as_bytes()) == existing.data.as_deref()
}

pub(crate) fn validate_existing_event(existing: &ExistingEvent) -> Result<(), String> {
    if existing.specversion != dovecote::SPEC_VERSION {
        return Err("stored event has an unsupported specversion".to_owned());
    }

    let stream =
        dovecote::StreamName::new(existing.stream.clone()).map_err(|error| error.to_string())?;
    let id =
        dovecote::EventId::new(existing.event_id.clone()).map_err(|error| error.to_string())?;
    let source =
        dovecote::EventSource::new(existing.source.clone()).map_err(|error| error.to_string())?;
    let event_type =
        dovecote::EventType::new(existing.event_type.clone()).map_err(|error| error.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);
    builder = match &existing.subject {
        Some(value) => builder.subject(
            dovecote::EventSubject::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    if let Some(value) = &existing.occurred_at {
        builder = builder.time(parse_timestamp(value)?);
    }
    builder = match &existing.datacontenttype {
        Some(value) => builder.datacontenttype(
            dovecote::ContentType::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match &existing.dataschema {
        Some(value) => builder.dataschema(
            dovecote::SchemaUri::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match &existing.partitionkey {
        Some(value) => builder.partitionkey(
            dovecote::PartitionKey::new(value.clone()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(&existing.extensions)
            .map_err(|error| error.to_string())?,
    );
    match (&existing.data_kind, &existing.data) {
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
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("non-zero limit"))
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    value
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("validated timestamp is RFC3339 representable")
}

pub(crate) fn parse_timestamp(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|value| value.to_offset(UtcOffset::UTC))
        .map_err(|error| format!("invalid stored timestamp: {error}"))
}
