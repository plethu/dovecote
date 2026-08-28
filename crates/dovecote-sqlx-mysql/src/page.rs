//! MySQL/MariaDB live and finite consistent-snapshot paging.

use crate::{backend, error::PageError, hydrate};
use dovecote::{
    AttemptCount, DeliverySnapshot, Failure, Limit, PagedEvent, QuarantineReason, RowId, TenantId,
    WorkerId,
};
use sqlx::{FromRow, MySql, MySqlConnection, MySqlPool, Transaction, query_as, query_scalar};
use std::marker::PhantomData;
use time::OffsetDateTime;

pub(crate) async fn page_for_scope(
    pool: &MySqlPool,
    tenant_id: Option<&TenantId>,
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
    query_page_scoped(
        &mut connection,
        tenant_id,
        after_row_id.map_or(0, RowId::get),
        None,
        limit,
    )
    .await
}

pub(crate) async fn begin_snapshot_for_scope(
    pool: &MySqlPool,
    tenant_id: Option<&TenantId>,
) -> Result<SnapshotPager, PageError> {
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

    let upper_bound = match query_scalar::<_, Option<i64>>(
        "SELECT MAX(row_id) FROM dovecote_events WHERE (? IS NULL OR tenant_id = ?)",
    )
    .bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec()))
    .bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec()))
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
        tenant_id: tenant_id.cloned(),
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
    tenant_id: Option<TenantId>,
}

impl SnapshotPager {
    /// Returns the last row ID returned by this pager.
    pub const fn cursor(&self) -> Option<RowId> {
        self.cursor
    }
    /// Returns the snapshot's fixed upper row-ID bound.
    pub const fn upper_bound(&self) -> Option<RowId> {
        self.upper_bound
    }
    /// Reports whether all rows in the snapshot have been returned.
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }
    /// Reads the next bounded page from this snapshot.
    pub async fn next_page(&mut self, limit: Limit) -> Result<Vec<PagedEvent>, PageError> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let upper = self
            .upper_bound
            .expect("non-exhausted snapshot has upper bound");
        let rows = query_page_scoped(
            &mut self.transaction,
            self.tenant_id.as_ref(),
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
    /// Commits and closes the snapshot transaction.
    pub async fn finish(self) -> Result<(), PageError> {
        self.transaction
            .commit()
            .await
            .map_err(|source| PageError::sql("finish snapshot transaction", source))
    }
    /// Rolls back and closes the snapshot transaction.
    pub async fn rollback(self) -> Result<(), PageError> {
        self.transaction
            .rollback()
            .await
            .map_err(|source| PageError::sql("rollback snapshot transaction", source))
    }
    /// Rolls back and closes the snapshot transaction.
    pub async fn close(self) -> Result<(), PageError> {
        self.rollback().await
    }
}

async fn query_page_scoped(
    connection: &mut MySqlConnection,
    tenant_id: Option<&TenantId>,
    after: i64,
    upper: Option<i64>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let sql = match (tenant_id.is_some(), upper.is_some()) {
        (false, false) => PAGE_SQL,
        (false, true) => SNAPSHOT_SQL,
        (true, false) => SCOPED_PAGE_SQL,
        (true, true) => SCOPED_SNAPSHOT_SQL,
    };
    let mut request = query_as::<_, PageRow>(sql).bind(after);
    if let Some(upper) = upper {
        request = request.bind(upper);
    }

    if let Some(tenant_id) = tenant_id {
        request = request.bind(tenant_id.as_str().as_bytes());
    }

    request = request.bind(i64::from(limit.get()));
    let rows = request
        .fetch_all(&mut *connection)
        .await
        .map_err(|source| PageError::sql("read event page", source))?;
    rows.into_iter()
        .map(hydrate_page)
        .collect::<Result<Vec<_>, _>>()
        .map_err(PageError::serialization)
}

const PAGE_SQL: &str = "SELECT e.row_id,e.tenant_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e LEFT JOIN dovecote_deliveries d ON d.tenant_id=e.tenant_id AND d.event_row_id=e.row_id WHERE e.row_id > ? ORDER BY e.row_id ASC LIMIT ?";
const SNAPSHOT_SQL: &str = "SELECT e.row_id,e.tenant_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e LEFT JOIN dovecote_deliveries d ON d.tenant_id=e.tenant_id AND d.event_row_id=e.row_id WHERE e.row_id > ? AND e.row_id <= ? ORDER BY e.row_id ASC LIMIT ?";
const SCOPED_PAGE_SQL: &str = "SELECT e.row_id,e.tenant_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e LEFT JOIN dovecote_deliveries d ON d.tenant_id=e.tenant_id AND d.event_row_id=e.row_id WHERE e.row_id > ? AND e.tenant_id = ? ORDER BY e.row_id ASC LIMIT ?";
const SCOPED_SNAPSHOT_SQL: &str = "SELECT e.row_id,e.tenant_id,e.stream,e.specversion,e.event_id,e.source,e.event_type,e.subject,e.occurred_at,e.enqueued_at,e.datacontenttype,e.dataschema,e.partitionkey,e.extensions,e.data_kind,e.data,d.state,d.available_at,d.attempts,d.claim_token,d.claimed_by,d.claim_expires_at,d.last_failure_code,d.last_failure_detail,d.delivered_at,d.quarantined_at,d.quarantine_reason FROM dovecote_events e LEFT JOIN dovecote_deliveries d ON d.tenant_id=e.tenant_id AND d.event_row_id=e.row_id WHERE e.row_id > ? AND e.row_id <= ? AND e.tenant_id = ? ORDER BY e.row_id ASC LIMIT ?";

#[derive(Debug, FromRow)]
struct PageRow {
    row_id: i64,
    tenant_id: Vec<u8>,
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
    state: Option<Vec<u8>>,
    available_at: Option<OffsetDateTime>,
    attempts: Option<i64>,
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
    let tenant_id = TenantId::new(strv(&row.tenant_id, "tenant id")?).map_err(|e| e.to_string())?;
    let row_id = RowId::new(row.row_id).map_err(|e| e.to_string())?;
    let event = hydrate::hydrate_event(&hydrate::EventColumns {
        stream: &row.stream,
        specversion: &row.specversion,
        event_id: &row.event_id,
        source: &row.source,
        event_type: &row.event_type,
        subject: row.subject.as_deref(),
        occurred_at: row.occurred_at,
        datacontenttype: row.datacontenttype.as_deref(),
        dataschema: row.dataschema.as_deref(),
        partitionkey: row.partitionkey.as_deref(),
        extensions: &row.extensions,
        data_kind: row.data_kind.as_deref(),
        data: row.data.as_deref(),
    })?;
    let state = row
        .state
        .ok_or_else(|| format!("event row {} has no required delivery row", row.row_id))?;
    let available_at = row
        .available_at
        .ok_or_else(|| "delivery row has no available_at".to_owned())?;
    let attempts = AttemptCount::new(
        row.attempts
            .ok_or_else(|| "delivery row has no attempts".to_owned())?,
    )
    .map_err(|e| e.to_string())?;
    let failure = parse_failure(row.last_failure_code, row.last_failure_detail)?;
    let delivery = match state.as_slice() {
        b"pending" => {
            absent("pending claim token", row.claim_token.as_ref())?;
            absent("pending worker", row.claimed_by.as_ref())?;
            absent("pending expiry", row.claim_expires_at.as_ref())?;
            absent("pending delivered", row.delivered_at.as_ref())?;
            absent("pending quarantined", row.quarantined_at.as_ref())?;
            absent("pending reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::pending(available_at, attempts, failure)
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
            DeliverySnapshot::claimed(available_at, worker, expires, attempts, failure)
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
            DeliverySnapshot::delivered(available_at, delivered, attempts, failure)
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
            DeliverySnapshot::quarantined(available_at, at, attempts, failure, reason)
        }
        _ => return Err("unknown delivery state".to_owned()),
    }
    .map_err(|e| e.to_string())?;
    PagedEvent::new(tenant_id, row_id, event, row.enqueued_at, delivery).map_err(|e| e.to_string())
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
