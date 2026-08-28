//! Shared setup and fixtures for MySQL/MariaDB conformance tests.

pub(crate) use dovecote::{
    ClaimToken, Delay, DeliveryState, EnqueueOutcome, EventId, EventSource, EventType, Failure,
    FinalizeOutcome, ImportOutcome, ImportedDeliveryState, Lease, Limit, NewEvent,
    QuarantineReason, RowId, StreamName, TenantId, WorkerId,
};
pub(crate) use dovecote_sqlx_mysql::{
    ClaimError, EnqueueError, MIGRATIONS, MutationError, MySqlDovecote, PageError, TransientKind,
};
pub(crate) use sqlx::{MySqlPool, mysql::MySqlPoolOptions, query, query_as, query_scalar};
pub(crate) use std::error::Error;

/// Compatibility vocabulary for the pre-tenant fixtures. It lives only in
/// tests; production code must construct an explicit tenant handle.
pub(crate) trait TestTenantOps {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_mysql::EnqueueError>;
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_mysql::ImportError>;
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        row_id: RowId,
        at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_mysql::FinalizeError>;
    async fn page(
        &self,
        after: Option<RowId>,
        limit: Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, dovecote_sqlx_mysql::PageError>;
    async fn begin_snapshot(
        &self,
    ) -> Result<dovecote_sqlx_mysql::SnapshotPager, dovecote_sqlx_mysql::PageError>;
    async fn claim(
        &self,
        worker: WorkerId,
        lease: Lease,
        limit: Limit,
    ) -> Result<Vec<dovecote::ClaimedEvent>, dovecote_sqlx_mysql::ClaimError>;
    async fn renew(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        lease: Lease,
    ) -> Result<(), MutationError>;
    async fn ack(&self, row_id: RowId, token: &ClaimToken) -> Result<(), MutationError>;
    async fn retry(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        failure: &Failure,
        delay: Delay,
    ) -> Result<(), MutationError>;
    async fn release(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        delay: Delay,
    ) -> Result<(), MutationError>;
    async fn quarantine(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        reason: &QuarantineReason,
    ) -> Result<(), MutationError>;
}

impl TestTenantOps for MySqlDovecote {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_mysql::EnqueueError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .enqueue(tx, event)
            .await
    }
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_mysql::ImportError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .import_for_migration(tx, event, state)
            .await
    }
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::MySql>,
        row_id: RowId,
        at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_mysql::FinalizeError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .finalize_pending_delivery_for_migration(tx, row_id, at)
            .await
    }
    async fn page(
        &self,
        after: Option<RowId>,
        limit: Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, dovecote_sqlx_mysql::PageError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .page(after, limit)
            .await
    }
    async fn begin_snapshot(
        &self,
    ) -> Result<dovecote_sqlx_mysql::SnapshotPager, dovecote_sqlx_mysql::PageError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .begin_snapshot()
            .await
    }
    async fn claim(
        &self,
        worker: WorkerId,
        lease: Lease,
        limit: Limit,
    ) -> Result<Vec<dovecote::ClaimedEvent>, dovecote_sqlx_mysql::ClaimError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .claim(worker, lease, limit)
            .await
    }
    async fn renew(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        lease: Lease,
    ) -> Result<(), MutationError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .renew(row_id, token, lease)
            .await
    }
    async fn ack(&self, row_id: RowId, token: &ClaimToken) -> Result<(), MutationError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .ack(row_id, token)
            .await
    }
    async fn retry(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        failure: &Failure,
        delay: Delay,
    ) -> Result<(), MutationError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .retry(row_id, token, failure, delay)
            .await
    }
    async fn release(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        delay: Delay,
    ) -> Result<(), MutationError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .release(row_id, token, delay)
            .await
    }
    async fn quarantine(
        &self,
        row_id: RowId,
        token: &ClaimToken,
        reason: &QuarantineReason,
    ) -> Result<(), MutationError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .quarantine(row_id, token, reason)
            .await
    }
}

use std::sync::OnceLock;

pub(crate) static CONFORMANCE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
pub(crate) static INSTALL_DONE: OnceLock<()> = OnceLock::new();

pub(crate) async fn serialize_live_tests() -> tokio::sync::MutexGuard<'static, ()> {
    CONFORMANCE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

pub(crate) fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref().is_some_and(is_truthy)
}

pub(crate) fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "yes")
}

pub(crate) fn required() -> bool {
    env_flag("DOVECOTE_MYSQL_REQUIRED")
        || env_flag("DOVECOTE_RELEASE_MODE")
        || (env_flag("CI") && !env_flag("DOVECOTE_MYSQL_OPTIONAL"))
}

pub(crate) async fn live_pool() -> Result<Option<MySqlPool>, Box<dyn std::error::Error>> {
    let Some(url) = std::env::var_os("DOVECOTE_MYSQL_URL") else {
        if required() {
            return Err("DOVECOTE_MYSQL_URL is required for MySQL/MariaDB conformance".into());
        }
        eprintln!("skipping MySQL/MariaDB conformance: DOVECOTE_MYSQL_URL is unset");
        return Ok(None);
    };
    Ok(Some(
        MySqlPoolOptions::new()
            .max_connections(4)
            .connect(url.to_str().ok_or("database URL is not UTF-8")?)
            .await?,
    ))
}

pub(crate) async fn install(pool: &MySqlPool) -> Result<(), Box<dyn std::error::Error>> {
    if INSTALL_DONE.get().is_some() {
        return Ok(());
    }
    // MySQL DDL and trigger bodies must use the raw/unprepared protocol.  The
    // server understands semicolons inside a trigger body when the whole
    // artifact is sent as one COM_QUERY; splitting on semicolons here would
    // also corrupt semicolons in SQL comments.
    sqlx::raw_sql(MIGRATIONS[0].sql()).execute(pool).await?;

    let _ = INSTALL_DONE.set(());
    Ok(())
}

pub(crate) async fn clear_conformance_rows(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE d FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.stream = ?")
        .bind(b"mysql-conformance".as_slice()).execute(pool).await?;
    sqlx::query("DELETE FROM dovecote_events WHERE stream = ?")
        .bind(b"mysql-conformance".as_slice())
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("mysql-conformance").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://dovecote.test/mysql").unwrap(),
        EventType::new("conformance.event").unwrap(),
    )
    .unwrap()
}

pub(crate) fn event_with_type(id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("mysql-conformance").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://dovecote.test/mysql").unwrap(),
        EventType::new(event_type).unwrap(),
    )
    .unwrap()
}

pub(crate) fn maximum_timestamp() -> time::OffsetDateTime {
    time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    )
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct RetryRow {
    pub(crate) state: Vec<u8>,
    pub(crate) available_at: time::OffsetDateTime,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<Vec<u8>>,
    pub(crate) claim_expires_at: Option<time::OffsetDateTime>,
    pub(crate) last_failure_code: Option<Vec<u8>>,
    pub(crate) last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct ReleaseRow {
    pub(crate) state: Vec<u8>,
    pub(crate) available_at: time::OffsetDateTime,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<Vec<u8>>,
    pub(crate) claim_expires_at: Option<time::OffsetDateTime>,
    pub(crate) last_failure_code: Option<Vec<u8>>,
    pub(crate) last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct QuarantineRow {
    pub(crate) state: Vec<u8>,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<Vec<u8>>,
    pub(crate) claim_expires_at: Option<time::OffsetDateTime>,
    pub(crate) quarantined_at: Option<time::OffsetDateTime>,
    pub(crate) quarantine_reason: Option<Vec<u8>>,
    pub(crate) last_failure_code: Option<Vec<u8>>,
    pub(crate) last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct AckRow {
    pub(crate) state: Vec<u8>,
    pub(crate) delivered_at: Option<time::OffsetDateTime>,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<Vec<u8>>,
    pub(crate) claim_expires_at: Option<time::OffsetDateTime>,
    pub(crate) quarantined_at: Option<time::OffsetDateTime>,
    pub(crate) quarantine_reason: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub(crate) struct DeliveryStateRow {
    pub(crate) state: Vec<u8>,
    pub(crate) attempts: i64,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<Vec<u8>>,
}

pub(crate) async fn enqueue_committed(
    pool: &MySqlPool,
    event: NewEvent,
) -> Result<dovecote::RowId, Box<dyn Error>> {
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter.enqueue(&mut transaction, event).await?;
    transaction.commit().await?;
    match outcome {
        EnqueueOutcome::Enqueued { row_id } | EnqueueOutcome::AlreadyEnqueued { row_id } => {
            Ok(row_id)
        }
        _ => Err("unknown enqueue outcome".into()),
    }
}
