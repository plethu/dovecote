//! MySQL/MariaDB live and finite consistent-snapshot paging.

use crate::{backend, error::PageError};
use dovecote::{
    AttemptCount, DeliverySnapshot, EventData, EventSizeLimit, Failure, Limit, NewEvent,
    PagedEvent, QuarantineReason, RowId, StoredEvent, WorkerId,
};
use sqlx::{FromRow, MySql, MySqlConnection, MySqlPool, Transaction, query_as, query_scalar};
use std::marker::PhantomData;
use time::OffsetDateTime;

pub async fn page(
    pool: &MySqlPool,
    after_row_id: Option<RowId>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| PageError::sql("acquire live page connection", source))?;
    backend::detect_on_connection(&mut connection)
        .await
        .map_err(schema_to_page)?;
    query_page(
        &mut connection,
        after_row_id.map_or(0, RowId::get),
        None,
        limit,
    )
    .await
}

pub async fn begin_snapshot(pool: &MySqlPool) -> Result<SnapshotPager, PageError> {
    let mut transaction = pool
        .begin_with(sqlx::AssertSqlSafe(
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        ))
        .await
        .map_err(|source| PageError::sql("begin snapshot transaction", source))?;
    let info = match backend::detect_on_connection(&mut transaction).await {
        Ok(info) => info,
        Err(error) => {
            let _ = transaction.rollback().await;
            return Err(schema_to_page(error));
        }
    };

    if !info.capabilities.repeatable_read_snapshot {
        let _ = transaction.rollback().await;
        return Err(PageError::BackendMismatch {
            detail: "backend lacks consistent InnoDB snapshots".to_owned(),
        });
    }

    if !info
        .transaction_isolation
        .eq_ignore_ascii_case("REPEATABLE-READ")
    {
        let _ = transaction.rollback().await;
        return Err(PageError::BackendMismatch {
            detail: format!(
                "snapshot requires REPEATABLE-READ, got {:?}",
                info.transaction_isolation
            ),
        });
    }

    let upper_bound =
        match query_scalar::<_, Option<i64>>("SELECT MAX(row_id) FROM dovecote_events")
            .fetch_one(&mut *transaction)
            .await
        {
            Ok(value) => match value
                .map(|value| {
                    RowId::new(value).map_err(|error| PageError::serialization(error.to_string()))
                })
                .transpose()
            {
                Ok(value) => value,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            },
            Err(source) => {
                let _ = transaction.rollback().await;
                return Err(PageError::sql("read snapshot upper row ID", source));
            }
        };
    Ok(SnapshotPager {
        transaction,
        upper_bound,
        cursor: None,
        exhausted: upper_bound.is_none(),
        _not_send: PhantomData,
    })
}

/// A finite, connection-bound InnoDB consistent snapshot. It is deliberately
/// not `Send`, so callers cannot move the snapshot between executors.
///
/// ```compile_fail
/// use dovecote_sqlx_mysql::SnapshotPager;
/// fn requires_send<T: Send>() {}
/// fn main() { requires_send::<SnapshotPager>(); }
/// ```
pub struct SnapshotPager {
    transaction: Transaction<'static, MySql>,
    upper_bound: Option<RowId>,
    cursor: Option<RowId>,
    exhausted: bool,
    _not_send: PhantomData<*mut ()>,
}

impl SnapshotPager {
    pub const fn cursor(&self) -> Option<RowId> {
        self.cursor
    }
    pub const fn upper_bound(&self) -> Option<RowId> {
        self.upper_bound
    }
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }
    pub async fn next_page(&mut self, limit: Limit) -> Result<Vec<PagedEvent>, PageError> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let upper = self
            .upper_bound
            .expect("non-exhausted snapshot has upper bound");
        let rows = query_page(
            &mut self.transaction,
            self.cursor.map_or(0, RowId::get),
            Some(upper.get()),
            limit,
        )
        .await?;
        if let Some(last) = rows.last() {
            self.cursor = Some(last.row_id());
            if rows.len() < limit.get() as usize || self.cursor == self.upper_bound {
                self.exhausted = true;
            }
        } else {
            self.exhausted = true;
        }
        Ok(rows)
    }
    pub async fn finish(self) -> Result<(), PageError> {
        self.transaction
            .commit()
            .await
            .map_err(|source| PageError::sql("finish snapshot transaction", source))
    }
    pub async fn rollback(self) -> Result<(), PageError> {
        self.transaction
            .rollback()
            .await
            .map_err(|source| PageError::sql("rollback snapshot transaction", source))
    }
    pub async fn close(self) -> Result<(), PageError> {
        self.rollback().await
    }
}

async fn query_page(
    connection: &mut MySqlConnection,
    after: i64,
    upper: Option<i64>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let rows = match upper {
        Some(upper) => {
            query_as::<_, PageRow>(SNAPSHOT_SQL)
                .bind(after)
                .bind(upper)
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *connection)
                .await
        }
        None => {
            query_as::<_, PageRow>(PAGE_SQL)
                .bind(after)
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *connection)
                .await
        }
    }
    .map_err(|source| PageError::sql("read event page", source))?;
    rows.into_iter()
        .map(hydrate_page)
        .collect::<Result<Vec<_>, _>>()
        .map_err(PageError::serialization)
}

const PAGE_SQL: &str = "SELECT e.row_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e JOIN dovecote_deliveries d ON d.event_row_id=e.row_id WHERE e.row_id > ? ORDER BY e.row_id ASC LIMIT ?";
const SNAPSHOT_SQL: &str = "SELECT e.row_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e JOIN dovecote_deliveries d ON d.event_row_id=e.row_id WHERE e.row_id > ? AND e.row_id <= ? ORDER BY e.row_id ASC LIMIT ?";

#[derive(Debug, FromRow)]
struct PageRow {
    row_id: i64,
    stream: Vec<u8>,
    specversion: Vec<u8>,
    event_id: Vec<u8>,
    source: Vec<u8>,
    event_type: Vec<u8>,
    subject: Option<Vec<u8>>,
    occurred_at: Option<OffsetDateTime>,
    enqueued_at: OffsetDateTime,
    datacontenttype: Option<Vec<u8>>,
    dataschema: Option<Vec<u8>>,
    partitionkey: Option<Vec<u8>>,
    extensions: Vec<u8>,
    data_kind: Option<Vec<u8>>,
    data: Option<Vec<u8>>,
    state: Vec<u8>,
    available_at: OffsetDateTime,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<OffsetDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
    delivered_at: Option<OffsetDateTime>,
    quarantined_at: Option<OffsetDateTime>,
    quarantine_reason: Option<Vec<u8>>,
}

fn strv(value: &[u8], field: &str) -> Result<String, String> {
    String::from_utf8(value.to_owned()).map_err(|_| format!("stored {field} is not UTF-8"))
}
fn absent<T>(field: &str, value: Option<&T>) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{field} must be NULL for its delivery state"))
    } else {
        Ok(())
    }
}
fn token_width(value: Option<&[u8]>) -> Result<(), String> {
    match value {
        Some(v) if v.len() == dovecote::CLAIM_TOKEN_BYTES => Ok(()),
        Some(v) => Err(format!("invalid claim token width {}", v.len())),
        None => Err("claimed delivery has no claim token".to_owned()),
    }
}
fn parse_failure(
    code: Option<Vec<u8>>,
    detail: Option<Vec<u8>>,
) -> Result<Option<Failure>, String> {
    match (code, detail) {
        (None, None) => Ok(None),
        (Some(c), Some(d)) => Failure::new(strv(&c, "failure code")?, strv(&d, "failure detail")?)
            .map(Some)
            .map_err(|e| e.to_string()),
        _ => Err("failure code and detail must be paired".to_owned()),
    }
}

fn hydrate_page(row: PageRow) -> Result<PagedEvent, String> {
    let row_id = RowId::new(row.row_id).map_err(|e| e.to_string())?;
    let event = hydrate_event(&row)?;
    let attempts = AttemptCount::new(row.attempts).map_err(|e| e.to_string())?;
    let failure = parse_failure(row.last_failure_code, row.last_failure_detail)?;
    let delivery = match row.state.as_slice() {
        b"pending" => {
            absent("pending claim token", row.claim_token.as_ref())?;
            absent("pending worker", row.claimed_by.as_ref())?;
            absent("pending expiry", row.claim_expires_at.as_ref())?;
            absent("pending delivered", row.delivered_at.as_ref())?;
            absent("pending quarantined", row.quarantined_at.as_ref())?;
            absent("pending reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::pending(row.available_at, attempts, failure)
        }
        b"claimed" => {
            token_width(row.claim_token.as_deref())?;
            let worker = WorkerId::new(strv(
                &row.claimed_by
                    .ok_or_else(|| "claimed delivery has no worker".to_owned())?,
                "worker",
            )?)
            .map_err(|e| e.to_string())?;
            let expires = row
                .claim_expires_at
                .ok_or_else(|| "claimed delivery has no expiry".to_owned())?;
            absent("claimed delivered", row.delivered_at.as_ref())?;
            absent("claimed quarantined", row.quarantined_at.as_ref())?;
            absent("claimed reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::claimed(row.available_at, worker, expires, attempts, failure)
        }
        b"delivered" => {
            absent("delivered token", row.claim_token.as_ref())?;
            absent("delivered worker", row.claimed_by.as_ref())?;
            absent("delivered expiry", row.claim_expires_at.as_ref())?;
            let delivered = row
                .delivered_at
                .ok_or_else(|| "delivered delivery has no timestamp".to_owned())?;
            absent("delivered quarantined", row.quarantined_at.as_ref())?;
            absent("delivered reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::delivered(row.available_at, delivered, attempts, failure)
        }
        b"quarantined" => {
            absent("quarantined token", row.claim_token.as_ref())?;
            absent("quarantined worker", row.claimed_by.as_ref())?;
            absent("quarantined expiry", row.claim_expires_at.as_ref())?;
            absent("quarantined delivered", row.delivered_at.as_ref())?;
            let at = row
                .quarantined_at
                .ok_or_else(|| "quarantined delivery has no timestamp".to_owned())?;
            let reason = QuarantineReason::new(strv(
                &row.quarantine_reason
                    .ok_or_else(|| "quarantined delivery has no reason".to_owned())?,
                "quarantine reason",
            )?)
            .map_err(|e| e.to_string())?;
            DeliverySnapshot::quarantined(row.available_at, at, attempts, failure, reason)
        }
        _ => return Err("unknown delivery state".to_owned()),
    }
    .map_err(|e| e.to_string())?;
    PagedEvent::new(row_id, event, row.enqueued_at, delivery).map_err(|e| e.to_string())
}

#[allow(clippy::single_match)]
fn hydrate_event(row: &PageRow) -> Result<StoredEvent, String> {
    if row.specversion.as_slice() != dovecote::SPEC_VERSION.as_bytes() {
        return Err("stored event has unsupported specversion".to_owned());
    }

    let stream =
        dovecote::StreamName::new(strv(&row.stream, "stream")?).map_err(|e| e.to_string())?;
    let id = dovecote::EventId::new(strv(&row.event_id, "event id")?).map_err(|e| e.to_string())?;
    let source =
        dovecote::EventSource::new(strv(&row.source, "source")?).map_err(|e| e.to_string())?;
    let event_type = dovecote::EventType::new(strv(&row.event_type, "event type")?)
        .map_err(|e| e.to_string())?;
    let mut b = NewEvent::builder(stream, id, source, event_type);
    // These optional CloudEvents attributes are independent, not priority
    // policy. Their source-column order stays explicit for deterministic
    // hydration.
    // Each optional CloudEvents attribute is hydrated independently; order is
    // column order, not a policy cascade.
    let _ = ();
    if let Some(v) = &row.subject {
        b = b.subject(dovecote::EventSubject::new(strv(v, "subject")?).map_err(|e| e.to_string())?);
    }

    let _ = ();
    if let Some(v) = row.occurred_at {
        b = b.time(v);
    }

    let _ = ();
    if let Some(v) = &row.datacontenttype {
        b = b.datacontenttype(
            dovecote::ContentType::new(strv(v, "content type")?).map_err(|e| e.to_string())?,
        );
    }

    let _ = ();
    if let Some(v) = &row.dataschema {
        b = b.dataschema(
            dovecote::SchemaUri::new(strv(v, "schema URI")?).map_err(|e| e.to_string())?,
        );
    }

    let _ = ();
    if let Some(v) = &row.partitionkey {
        b = b.partitionkey(
            dovecote::PartitionKey::new(strv(v, "partition key")?).map_err(|e| e.to_string())?,
        );
    }
    b = b.extensions(
        dovecote::Extensions::from_canonical_json(&strv(&row.extensions, "extensions")?)
            .map_err(|e| e.to_string())?,
    );
    match (&row.data_kind, &row.data) {
        (None, None) => {}
        (Some(k), Some(v)) if k.as_slice() == b"json" => {
            b = b.data(EventData::json(v.clone()).map_err(|e| e.to_string())?)
        }
        (Some(k), Some(v)) if k.as_slice() == b"binary" => b = b.data(EventData::binary(v.clone())),
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }
    b.build_with_limit(EventSizeLimit::new(usize::MAX).expect("nonzero"))
        .map_err(|e| e.to_string())?
        .into_stored()
        .map_err(|e| e.to_string())
}
fn schema_to_page(error: crate::SchemaError) -> PageError {
    match error {
        crate::SchemaError::BackendMismatch { detail } => PageError::BackendMismatch { detail },
        crate::SchemaError::MigrationMismatch { detail } => PageError::Serialization { detail },
        crate::SchemaError::Sql { operation, source } => PageError::sql(operation, source),
        crate::SchemaError::Transient {
            operation,
            kind,
            source,
        } => PageError::Transient {
            operation,
            kind,
            source,
        },
    }
}
