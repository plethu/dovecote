//! Caller-transaction enqueue and durable-event hydration.

use crate::{
    error::EnqueueError,
    hydrate::{EventRow, hydrate_event},
    migration::{SchemaMarker, current_migration, marker_matches_migration},
};
use dovecote::{EnqueueOutcome, NewEvent, RowId, TenantId};
use sqlx::{FromRow, Postgres, Transaction, query, query_as, query_scalar};
use time::OffsetDateTime;

/// Inserts an event and its pending delivery in the supplied caller transaction.
///
/// The transaction's `search_path` must resolve to a namespace containing an
/// accepted Dovecote schema. Any database and namespace with the shipped
/// migration is valid; this operation does not bind itself to a prior
/// [`crate::check_schema`] call. The marker and domain tables are validated in
/// the caller transaction before any write, then unqualified table names are
/// used for the remainder of the operation.
pub(crate) async fn enqueue_for_scope<'c>(
    transaction: &mut Transaction<'c, Postgres>,
    tenant_id: &TenantId,
    event: NewEvent,
) -> Result<EnqueueOutcome, EnqueueError> {
    validate_enqueue_schema(transaction).await?;
    let extensions = event.extensions().canonical_json();
    let (data_kind, data) = event.data().map_or((None, None), |data| {
        let kind = if data.is_json() { "json" } else { "binary" };
        (Some(kind), Some(data.as_bytes().to_vec()))
    });

    let inserted = query_as::<_, InsertedEvent>(
        r"
        INSERT INTO dovecote_events
            (tenant_id, stream, specversion, event_id, source, event_type, subject,
             occurred_at, datacontenttype, dataschema, partitionkey, extensions,
             data_kind, data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (tenant_id, source, event_id) DO NOTHING
        RETURNING row_id, enqueued_at
        ",
    )
    .bind(tenant_id.as_str())
    .bind(event.stream().as_str())
    .bind(event.specversion())
    .bind(event.id().as_str())
    .bind(event.source().as_str())
    .bind(event.event_type().as_str())
    .bind(event.subject().map(dovecote::EventSubject::as_str))
    .bind(event.time())
    .bind(event.datacontenttype().map(dovecote::ContentType::as_str))
    .bind(event.dataschema().map(dovecote::SchemaUri::as_str))
    .bind(event.partitionkey().map(dovecote::PartitionKey::as_str))
    .bind(extensions)
    .bind(data_kind)
    .bind(data)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("insert event", source))?;

    let Some(inserted) = inserted else {
        let existing = query_as::<_, ExistingEvent>(
            r#"
            SELECT row_id, stream, specversion, event_id, source, event_type,
                   subject, occurred_at, datacontenttype, dataschema,
                   partitionkey, extensions, data_kind, data, enqueued_at
            FROM dovecote_events
            WHERE tenant_id = $1 COLLATE "C"
              AND source = $2 COLLATE "C" AND event_id = $3 COLLATE "C"
            "#,
        )
        .bind(tenant_id.as_str())
        .bind(event.source().as_str())
        .bind(event.id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| EnqueueError::sql("find duplicate event", source))?
        .ok_or_else(|| EnqueueError::sql("resolve duplicate event", missing_row_error()))?;

        let existing_id = row_id(existing.row_id).map_err(EnqueueError::serialization)?;
        validate_existing_event(&existing).map_err(EnqueueError::serialization)?;
        if !same_event(&event, &existing) {
            return Err(EnqueueError::IdempotencyConflict {
                existing_row_id: existing_id,
            });
        }

        let delivery_exists: Option<i64> =
            query_scalar("SELECT event_row_id FROM dovecote_deliveries WHERE tenant_id = $1 AND event_row_id = $2")
                .bind(tenant_id.as_str())
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

    let row_id = row_id(inserted.row_id).map_err(EnqueueError::serialization)?;
    query(
        "INSERT INTO dovecote_deliveries (tenant_id, event_row_id, state, available_at) VALUES ($1, $2, 'pending', $3)",
    )
    .bind(tenant_id.as_str())
    .bind(inserted.row_id)
    .bind(inserted.enqueued_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("insert delivery", source))?;

    Ok(EnqueueOutcome::Enqueued { row_id })
}

#[derive(Debug, FromRow)]
struct EnqueueSchemaSelection {
    schema_name: String,
    marker_table: bool,
    events_table: bool,
    deliveries_table: bool,
}

pub(crate) async fn validate_enqueue_schema<'c>(
    transaction: &mut Transaction<'c, Postgres>,
) -> Result<(), EnqueueError> {
    let selection = query_as::<_, EnqueueSchemaSelection>(
        r"
        SELECT namespace.nspname AS schema_name,
               EXISTS (
                   SELECT 1 FROM pg_class table_class
                   WHERE table_class.relnamespace = namespace.oid
                     AND table_class.relname = 'dovecote_schema'
                     AND table_class.relkind IN ('r', 'p')
               ) AS marker_table,
               EXISTS (
                   SELECT 1 FROM pg_class table_class
                   WHERE table_class.relnamespace = namespace.oid
                     AND table_class.relname = 'dovecote_events'
                     AND table_class.relkind IN ('r', 'p')
               ) AS events_table,
               EXISTS (
                   SELECT 1 FROM pg_class table_class
                   WHERE table_class.relnamespace = namespace.oid
                     AND table_class.relname = 'dovecote_deliveries'
                     AND table_class.relkind IN ('r', 'p')
               ) AS deliveries_table
        FROM pg_namespace namespace
        WHERE namespace.nspname = current_schema()
        ",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("validate enqueue schema selection", source))?
    .ok_or_else(|| EnqueueError::MigrationMismatch {
        detail: "the caller transaction has no current schema".to_owned(),
    })?;
    if !selection.marker_table || !selection.events_table || !selection.deliveries_table {
        return Err(EnqueueError::MigrationMismatch {
            detail: format!(
                "current schema {} does not contain the Dovecote tables",
                selection.schema_name
            ),
        });
    }

    let marker = query_as::<_, SchemaMarker>(
        r"
        SELECT schema_version, minimum_crate_major, minimum_crate_minor,
               minimum_crate_patch, rolling_compatible
        FROM dovecote_schema
        ORDER BY schema_version DESC
        LIMIT 1
        ",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| EnqueueError::sql("check enqueue schema marker", source))?
    .ok_or_else(|| EnqueueError::MigrationMismatch {
        detail: "schema marker is missing".to_owned(),
    })?;
    let migration =
        current_migration().map_err(|detail| EnqueueError::MigrationMismatch { detail })?;
    if let Err(detail) = marker_matches_migration(&marker, migration) {
        return Err(EnqueueError::MigrationMismatch { detail });
    }

    Ok(())
}

#[derive(Debug, FromRow)]
struct InsertedEvent {
    row_id: i64,
    enqueued_at: OffsetDateTime,
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
    pub(crate) occurred_at: Option<OffsetDateTime>,
    pub(crate) datacontenttype: Option<String>,
    pub(crate) dataschema: Option<String>,
    pub(crate) partitionkey: Option<String>,
    pub(crate) extensions: String,
    pub(crate) data_kind: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) enqueued_at: OffsetDateTime,
}

pub(crate) fn same_event(event: &NewEvent, existing: &ExistingEvent) -> bool {
    event.stream().as_str() == existing.stream
        && event.specversion() == existing.specversion
        && event.id().as_str() == existing.event_id
        && event.source().as_str() == existing.source
        && event.event_type().as_str() == existing.event_type
        && event.subject().map(dovecote::EventSubject::as_str) == existing.subject.as_deref()
        && event.time() == existing.occurred_at
        && event.datacontenttype().map(dovecote::ContentType::as_str)
            == existing.datacontenttype.as_deref()
        && event.dataschema().map(dovecote::SchemaUri::as_str) == existing.dataschema.as_deref()
        && event.partitionkey().map(dovecote::PartitionKey::as_str)
            == existing.partitionkey.as_deref()
        && event.extensions().canonical_json() == existing.extensions
        && event
            .data()
            .map(|data| if data.is_json() { "json" } else { "binary" })
            == existing.data_kind.as_deref()
        && event.data().map(dovecote::EventData::as_bytes) == existing.data.as_deref()
}

pub(crate) fn validate_existing_event(existing: &ExistingEvent) -> Result<(), String> {
    hydrate_event(&EventRow {
        stream: &existing.stream,
        specversion: &existing.specversion,
        event_id: &existing.event_id,
        source: &existing.source,
        event_type: &existing.event_type,
        subject: existing.subject.as_deref(),
        occurred_at: existing.occurred_at,
        datacontenttype: existing.datacontenttype.as_deref(),
        dataschema: existing.dataschema.as_deref(),
        partitionkey: existing.partitionkey.as_deref(),
        extensions: &existing.extensions,
        data_kind: existing.data_kind.as_deref(),
        data: existing.data.as_deref(),
    })
    .map(|_| ())
}

fn row_id(value: i64) -> Result<RowId, String> {
    RowId::new(value).map_err(|error| error.to_string())
}

fn missing_row_error() -> sqlx::Error {
    sqlx::Error::Protocol("expected a row after a successful identity lookup".to_owned())
}
