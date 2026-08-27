//! PostgreSQL live and finite snapshot paging.
//!
//! Live pages are independent reads: they are ordered by the immutable event
//! row ID, but callers must reconcile later if concurrent commits can invert
//! row-ID allocation and commit order.  [`SnapshotPager`] keeps one read-only
//! repeatable-read transaction for a finite export instead.

use crate::error::PageError;
use dovecote::{
    AttemptCount, DeliverySnapshot, EventData, EventSizeLimit, Failure, Limit, NewEvent,
    PagedEvent, QuarantineReason, RowId, StoredEvent, WorkerId,
};
use sqlx::{FromRow, PgConnection, PgPool, Postgres, Transaction, query_as, query_scalar};
use std::marker::PhantomData;
use time::OffsetDateTime;

/// Reads a bounded live page after `after_row_id`.
///
/// This operation does not lock or mutate delivery rows.  `None` starts before
/// the first event.  Separate calls do not share a snapshot, so a caller that
/// requires finite completeness should use [`begin_snapshot`] instead.
pub async fn page(
    pool: &PgPool,
    after_row_id: Option<RowId>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| PageError::sql("acquire live page connection", source))?;
    query_page_on_connection(
        &mut connection,
        after_row_id.map_or(0, RowId::get),
        None,
        limit,
    )
    .await
}

/// Starts a finite PostgreSQL snapshot pager.
///
/// The transaction is acquired from `pool`, explicitly started as
/// `REPEATABLE READ READ ONLY`, and retained by the returned pager until
/// [`SnapshotPager::finish`], [`SnapshotPager::rollback`], or drop.
pub async fn begin_snapshot(pool: &PgPool) -> Result<SnapshotPager, PageError> {
    let mut transaction = pool
        .begin_with(sqlx::AssertSqlSafe(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        ))
        .await
        .map_err(|source| PageError::sql("begin snapshot transaction", source))?;

    let upper_bound = query_scalar::<_, Option<i64>>("SELECT MAX(row_id) FROM dovecote_events")
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| PageError::sql("read snapshot upper row ID", source))?
        .map(|value| RowId::new(value).map_err(|error| PageError::serialization(error.to_string())))
        .transpose()?;

    Ok(SnapshotPager {
        transaction,
        upper_bound,
        cursor: None,
        exhausted: upper_bound.is_none(),
        _not_send: PhantomData,
    })
}

/// A bounded, finite read over one PostgreSQL repeatable-read snapshot.
///
/// The pager owns the connection-bound transaction.  It does not accept an
/// arbitrary executor and never releases the transaction between pages.  A
/// pager is intentionally not a stream: callers choose explicit page bounds
/// and must finish or roll back the read transaction.
///
/// The pager is deliberately not `Send`: the snapshot and its connection must
/// stay with the executor that created them.
///
/// ```compile_fail
/// use dovecote_sqlx_postgres::SnapshotPager;
///
/// fn requires_send<T: Send>() {}
///
/// fn main() {
///     requires_send::<SnapshotPager>();
/// }
/// ```
pub struct SnapshotPager {
    transaction: Transaction<'static, Postgres>,
    upper_bound: Option<RowId>,
    cursor: Option<RowId>,
    exhausted: bool,
    _not_send: PhantomData<*mut ()>,
}

impl SnapshotPager {
    /// Returns the last row ID returned by a non-empty page.
    pub const fn cursor(&self) -> Option<RowId> {
        self.cursor
    }

    /// Returns the maximum row ID visible to this pager's finite export.
    pub const fn upper_bound(&self) -> Option<RowId> {
        self.upper_bound
    }

    /// Returns whether the pager has returned its final page.
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Reads the next bounded page from the retained snapshot.
    ///
    /// An empty page marks the pager exhausted and does not advance its
    /// cursor.  Once exhausted, subsequent calls return an empty page without
    /// issuing SQL; call [`finish`](Self::finish) or
    /// [`rollback`](Self::rollback) to release the transaction explicitly.
    pub async fn next_page(&mut self, limit: Limit) -> Result<Vec<PagedEvent>, PageError> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let upper_bound = self
            .upper_bound
            .expect("a non-exhausted pager has an upper bound");
        let rows = query_page_on_connection(
            &mut self.transaction,
            self.cursor.map_or(0, RowId::get),
            Some(upper_bound.get()),
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

    /// Commits the read-only transaction and releases its pooled connection.
    pub async fn finish(self) -> Result<(), PageError> {
        self.transaction
            .commit()
            .await
            .map_err(|source| PageError::sql("finish snapshot transaction", source))
    }

    /// Rolls back the read-only transaction and releases its pooled connection.
    pub async fn rollback(self) -> Result<(), PageError> {
        self.transaction
            .rollback()
            .await
            .map_err(|source| PageError::sql("rollback snapshot transaction", source))
    }

    /// Closes the pager by rolling back its read-only transaction.
    pub async fn close(self) -> Result<(), PageError> {
        self.rollback().await
    }
}

/// Executes a page query on the dedicated connection held by the caller.
async fn query_page_on_connection(
    connection: &mut PgConnection,
    after_row_id: i64,
    upper_bound: Option<i64>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let rows = match upper_bound {
        Some(upper_bound) => {
            query_as::<_, PageRow>(SNAPSHOT_PAGE_SQL)
                .bind(after_row_id)
                .bind(i64::from(limit.get()))
                .bind(upper_bound)
                .fetch_all(&mut *connection)
                .await
        }
        None => {
            query_as::<_, PageRow>(PAGE_SQL)
                .bind(after_row_id)
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

// Keep this SQL in one visible shape for both live and snapshot reads.  The
// snapshot variant adds an upper bound while retaining the same strict cursor
// and ordering semantics.
const PAGE_SQL: &str = r#"
    SELECT e.row_id,
           e.stream,
           e.specversion,
           e.event_id,
           e.source,
           e.event_type,
           e.subject,
           e.occurred_at,
           e.enqueued_at,
           e.datacontenttype,
           e.dataschema,
           e.partitionkey,
           e.extensions,
           e.data_kind,
           e.data,
           d.state,
           d.available_at,
           d.attempts,
           d.claim_token,
           d.claimed_by,
           d.claim_expires_at,
           d.last_failure_code,
           d.last_failure_detail,
           d.delivered_at,
           d.quarantined_at,
           d.quarantine_reason
    FROM dovecote_events AS e
    JOIN dovecote_deliveries AS d ON d.event_row_id = e.row_id
    WHERE e.row_id > $1
    ORDER BY e.row_id ASC
    LIMIT $2
"#;

const SNAPSHOT_PAGE_SQL: &str = r#"
    SELECT e.row_id,
           e.stream,
           e.specversion,
           e.event_id,
           e.source,
           e.event_type,
           e.subject,
           e.occurred_at,
           e.enqueued_at,
           e.datacontenttype,
           e.dataschema,
           e.partitionkey,
           e.extensions,
           e.data_kind,
           e.data,
           d.state,
           d.available_at,
           d.attempts,
           d.claim_token,
           d.claimed_by,
           d.claim_expires_at,
           d.last_failure_code,
           d.last_failure_detail,
           d.delivered_at,
           d.quarantined_at,
           d.quarantine_reason
    FROM dovecote_events AS e
    JOIN dovecote_deliveries AS d ON d.event_row_id = e.row_id
    WHERE e.row_id > $1 AND e.row_id <= $3
    ORDER BY e.row_id ASC
    LIMIT $2
"#;

#[derive(Debug, FromRow)]
struct PageRow {
    row_id: i64,
    stream: String,
    specversion: String,
    event_id: String,
    source: String,
    event_type: String,
    subject: Option<String>,
    occurred_at: Option<OffsetDateTime>,
    enqueued_at: OffsetDateTime,
    datacontenttype: Option<String>,
    dataschema: Option<String>,
    partitionkey: Option<String>,
    extensions: String,
    data_kind: Option<String>,
    data: Option<Vec<u8>>,
    state: String,
    available_at: OffsetDateTime,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<String>,
    claim_expires_at: Option<OffsetDateTime>,
    last_failure_code: Option<String>,
    last_failure_detail: Option<String>,
    delivered_at: Option<OffsetDateTime>,
    quarantined_at: Option<OffsetDateTime>,
    quarantine_reason: Option<String>,
}

fn hydrate_page(row: PageRow) -> Result<PagedEvent, String> {
    let row_id = RowId::new(row.row_id).map_err(|error| error.to_string())?;
    let event = hydrate_event(&row)?;
    let attempts = AttemptCount::new(row.attempts).map_err(|error| error.to_string())?;
    let failure = parse_failure(row.last_failure_code, row.last_failure_detail)?;
    let delivery = match row.state.as_str() {
        "pending" => {
            require_absent("pending claim token", row.claim_token.as_ref())?;
            require_absent("pending claimed worker", row.claimed_by.as_ref())?;
            require_absent("pending claim expiry", row.claim_expires_at.as_ref())?;
            require_absent("pending delivered time", row.delivered_at.as_ref())?;
            require_absent("pending quarantine time", row.quarantined_at.as_ref())?;
            require_absent("pending quarantine reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::pending(row.available_at, attempts, failure)
        }
        "claimed" => {
            require_token_width(row.claim_token.as_deref())?;
            let worker = row
                .claimed_by
                .ok_or_else(|| "claimed delivery has no worker".to_owned())?;
            let expires_at = row
                .claim_expires_at
                .ok_or_else(|| "claimed delivery has no claim expiry".to_owned())?;
            require_absent("claimed delivered time", row.delivered_at.as_ref())?;
            require_absent("claimed quarantine time", row.quarantined_at.as_ref())?;
            require_absent("claimed quarantine reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::claimed(
                row.available_at,
                WorkerId::new(worker).map_err(|error| error.to_string())?,
                expires_at,
                attempts,
                failure,
            )
        }
        "delivered" => {
            require_absent("delivered claim token", row.claim_token.as_ref())?;
            require_absent("delivered claimed worker", row.claimed_by.as_ref())?;
            require_absent("delivered claim expiry", row.claim_expires_at.as_ref())?;
            let delivered_at = row
                .delivered_at
                .ok_or_else(|| "delivered delivery has no delivered time".to_owned())?;
            require_absent("delivered quarantine time", row.quarantined_at.as_ref())?;
            require_absent(
                "delivered quarantine reason",
                row.quarantine_reason.as_ref(),
            )?;
            DeliverySnapshot::delivered(row.available_at, delivered_at, attempts, failure)
        }
        "quarantined" => {
            require_absent("quarantined claim token", row.claim_token.as_ref())?;
            require_absent("quarantined claimed worker", row.claimed_by.as_ref())?;
            require_absent("quarantined claim expiry", row.claim_expires_at.as_ref())?;
            require_absent("quarantined delivered time", row.delivered_at.as_ref())?;
            let quarantined_at = row
                .quarantined_at
                .ok_or_else(|| "quarantined delivery has no quarantine time".to_owned())?;
            let reason = row
                .quarantine_reason
                .ok_or_else(|| "quarantined delivery has no quarantine reason".to_owned())?;
            DeliverySnapshot::quarantined(
                row.available_at,
                quarantined_at,
                attempts,
                failure,
                QuarantineReason::new(reason).map_err(|error| error.to_string())?,
            )
        }
        state => return Err(format!("unknown delivery state {state:?}")),
    }
    .map_err(|error| error.to_string())?;

    PagedEvent::new(row_id, event, row.enqueued_at, delivery).map_err(|error| error.to_string())
}

fn require_absent<T>(field: &str, value: Option<&T>) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{field} must be NULL for its delivery state"))
    } else {
        Ok(())
    }
}

fn require_token_width(value: Option<&[u8]>) -> Result<(), String> {
    match value {
        Some(value) if value.len() == dovecote::CLAIM_TOKEN_BYTES => Ok(()),
        Some(value) => Err(format!(
            "claimed delivery has an invalid claim token width: {}",
            value.len()
        )),
        None => Err("claimed delivery has no claim token".to_owned()),
    }
}

fn parse_failure(code: Option<String>, detail: Option<String>) -> Result<Option<Failure>, String> {
    match (code, detail) {
        (None, None) => Ok(None),
        (Some(code), Some(detail)) => Failure::new(code, detail)
            .map(Some)
            .map_err(|error| error.to_string()),
        _ => Err("delivery failure code and detail must be both NULL or non-NULL".to_owned()),
    }
}

fn hydrate_event(row: &PageRow) -> Result<StoredEvent, String> {
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
