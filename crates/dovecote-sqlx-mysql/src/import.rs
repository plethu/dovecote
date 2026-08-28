//! Migration-only import into the MySQL/MariaDB Dovecote tables.
//!
//! The legacy schema is intentionally outside this crate. A migration caller
//! extracts and validates its source row, then passes a checked event and one
//! of the portable delivery states below in the caller's transaction.

use crate::{
    enqueue::{ExistingEvent, database_datetime, same_event, validate_existing_event},
    error::{EnqueueError, ImportError},
    schema::check_schema_connection,
};
use dovecote::{ImportOutcome, ImportedDeliveryState, NewEvent, RowId};
use sqlx::{FromRow, MySql, Transaction, query, query_as, query_scalar};
use time::{OffsetDateTime, PrimitiveDateTime};

/// Imports one event and a portable legacy delivery state in the supplied
/// transaction. The caller remains responsible for commit or rollback.
///
/// MySQL and MariaDB supply UTC database operation time for `enqueued_at` and
/// `available_at`, while DATETIME(6) preserves the supplied delivered instant
/// at exact microsecond precision. Claims, claim tokens, retries, and
/// quarantines are not importable.
/// An active legacy claim must finish, expire, or be explicitly fenced before
/// the caller maps that source row to `Pending`.
pub async fn import_for_migration<'c>(
    transaction: &mut Transaction<'c, MySql>,
    event: NewEvent,
    state: ImportedDeliveryState,
) -> Result<ImportOutcome, ImportError> {
    state
        .validate()
        .map_err(|source| ImportError::InvalidState { source })?;
    check_schema_connection(transaction)
        .await
        .map_err(map_schema_error)?;

    let operation_time = query_scalar::<_, OffsetDateTime>("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| ImportError::sql("read import operation time", source))?;
    ImportedDeliveryState::delivered(operation_time)
        .map_err(|source| ImportError::InvalidState { source })?;

    let (data_kind, data): (Option<&str>, Option<Vec<u8>>) =
        event.data().map_or((None, None), |data| {
            (
                Some(if data.is_json() { "json" } else { "binary" }),
                Some(data.as_bytes().to_vec()),
            )
        });
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
    .bind(event.time().map(database_datetime))
    .bind(
        event
            .datacontenttype()
            .map(|value| value.as_str().as_bytes()),
    )
    .bind(event.dataschema().map(|value| value.as_str().as_bytes()))
    .bind(event.partitionkey().map(|value| value.as_str().as_bytes()))
    .bind(event.extensions().canonical_json().as_bytes())
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
        Err(source) => return Err(ImportError::sql("insert imported event", source)),
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
    .map_err(|source| ImportError::sql("find imported event", source))?
    .ok_or_else(|| {
        ImportError::sql(
            "resolve imported event",
            sqlx::Error::Protocol("identity insert returned no row".to_owned()),
        )
    })?;
    let existing_id = row_id(existing.row_id)?;

    if inserted {
        insert_delivery(transaction, existing.row_id, operation_time, state).await?;
        return Ok(ImportOutcome::Imported {
            row_id: existing_id,
        });
    }

    validate_existing_event(&existing).map_err(ImportError::serialization)?;
    if !same_event(&event, &existing).map_err(map_enqueue_error)? {
        return Err(ImportError::IdentityConflict {
            existing_row_id: existing_id,
        });
    }

    let delivery = query_as::<_, ExistingDelivery>(
        r#"
        SELECT state, attempts, claim_token, claimed_by, claim_expires_at,
               last_failure_code, last_failure_detail, delivered_at,
               quarantined_at, quarantine_reason, available_at
        FROM dovecote_deliveries
        WHERE event_row_id = ?
        "#,
    )
    .bind(existing.row_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("find imported delivery", source))?
    .ok_or_else(|| ImportError::MigrationMismatch {
        detail: "an existing event has no delivery row".to_owned(),
    })?;

    if delivery_matches(&delivery, state, existing.enqueued_at)? {
        Ok(ImportOutcome::AlreadyImported {
            row_id: existing_id,
        })
    } else {
        Err(ImportError::ImportConflict {
            existing_row_id: existing_id,
        })
    }
}

async fn insert_delivery<'c>(
    transaction: &mut Transaction<'c, MySql>,
    event_row_id: i64,
    operation_time: OffsetDateTime,
    state: ImportedDeliveryState,
) -> Result<(), ImportError> {
    let (state_name, delivered_at) = match state {
        ImportedDeliveryState::Pending => (b"pending".as_slice(), None),
        ImportedDeliveryState::Delivered { delivered_at } => (
            b"delivered".as_slice(),
            Some(database_datetime(delivered_at)),
        ),
        _ => {
            return Err(ImportError::MigrationMismatch {
                detail: "adapter does not support this imported delivery state".to_owned(),
            });
        }
    };
    query(
        "INSERT INTO dovecote_deliveries (event_row_id, state, available_at, attempts, delivered_at) VALUES (?, ?, ?, 0, ?)",
    )
    .bind(event_row_id)
    .bind(state_name)
    .bind(operation_time)
    .bind(delivered_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| ImportError::sql("insert imported delivery", source))?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct ExistingDelivery {
    state: Vec<u8>,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<PrimitiveDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
    delivered_at: Option<PrimitiveDateTime>,
    quarantined_at: Option<PrimitiveDateTime>,
    quarantine_reason: Option<Vec<u8>>,
    available_at: PrimitiveDateTime,
}

fn delivery_matches(
    row: &ExistingDelivery,
    state: ImportedDeliveryState,
    enqueued_at: PrimitiveDateTime,
) -> Result<bool, ImportError> {
    let canonical = row.attempts == 0
        && row.claim_token.is_none()
        && row.claimed_by.is_none()
        && row.claim_expires_at.is_none()
        && row.last_failure_code.is_none()
        && row.last_failure_detail.is_none()
        && row.quarantined_at.is_none()
        && row.quarantine_reason.is_none()
        && row.available_at == enqueued_at;
    Ok(match state {
        ImportedDeliveryState::Pending => {
            canonical && row.state == b"pending" && row.delivered_at.is_none()
        }
        ImportedDeliveryState::Delivered { delivered_at } => {
            canonical
                && row.state == b"delivered"
                && row.delivered_at == Some(database_datetime(delivered_at))
        }
        _ => {
            return Err(ImportError::MigrationMismatch {
                detail: "adapter does not support this imported delivery state".to_owned(),
            });
        }
    })
}

fn row_id(value: i64) -> Result<RowId, ImportError> {
    RowId::new(value).map_err(|error| ImportError::serialization(error.to_string()))
}

fn map_enqueue_error(error: EnqueueError) -> ImportError {
    match error {
        EnqueueError::IdempotencyConflict { existing_row_id } => {
            ImportError::IdentityConflict { existing_row_id }
        }
        EnqueueError::MigrationMismatch { detail } => ImportError::MigrationMismatch { detail },
        EnqueueError::BackendMismatch { detail } => ImportError::BackendMismatch { detail },
        EnqueueError::Serialization { detail } => ImportError::Serialization { detail },
        EnqueueError::Sql { operation, source } => ImportError::Sql { operation, source },
        EnqueueError::Transient {
            operation,
            kind,
            source,
        } => ImportError::Transient {
            operation,
            kind,
            source,
        },
    }
}

fn map_schema_error(error: crate::SchemaError) -> ImportError {
    match error {
        crate::SchemaError::MigrationMismatch { detail } => {
            ImportError::MigrationMismatch { detail }
        }
        crate::SchemaError::BackendMismatch { detail } => ImportError::BackendMismatch { detail },
        crate::SchemaError::Sql { operation, source } => ImportError::Sql { operation, source },
        crate::SchemaError::Transient {
            operation,
            kind,
            source,
        } => ImportError::Transient {
            operation,
            kind,
            source,
        },
    }
}
