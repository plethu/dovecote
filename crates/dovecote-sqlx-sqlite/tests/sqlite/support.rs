//! Helpers shared by the SQLite integration-test concerns.

pub(crate) use super::support::database;
pub(crate) use dovecote::{
    ClaimToken, ContentType, Delay, EnqueueOutcome, EventData, EventId, EventSource, EventSubject,
    EventType, ExtensionName, ExtensionValue, Extensions, Failure, FinalizeOutcome, ImportOutcome,
    ImportedDeliveryState, Lease, Limit, NewEvent, PartitionKey, QuarantineReason, RowId,
    SchemaUri, StreamName, TenantId, WorkerId,
};
pub(crate) use dovecote_sqlx_sqlite::{
    LEGACY_MIGRATION, MIGRATIONS, MutationError, SqliteDovecote, V1_TENANT_ACTIVATE_SQL,
    V1_TENANT_PREPARE_SQL, check_schema,
};
pub(crate) use sqlx::{AssertSqlSafe, SqlitePool, raw_sql, sqlite::SqlitePoolOptions};
pub(crate) use std::path::PathBuf;
pub(crate) use std::sync::Arc;
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) use std::time::Duration;
pub(crate) use tokio::sync::Barrier;

/// Test-only bridge for legacy fixture call sites; production callers use
/// `SqliteDovecote::for_tenant` directly.
#[allow(dead_code)]
pub(crate) trait TestTenantOps {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_sqlite::EnqueueError>;
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_sqlite::ImportError>;
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        row_id: RowId,
        at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_sqlite::FinalizeError>;
    async fn page(
        &self,
        after: Option<RowId>,
        limit: Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, dovecote_sqlx_sqlite::PageError>;
    async fn begin_snapshot(
        &self,
    ) -> Result<dovecote_sqlx_sqlite::SnapshotPager, dovecote_sqlx_sqlite::PageError>;
    async fn claim(
        &self,
        worker: WorkerId,
        lease: Lease,
        limit: Limit,
    ) -> Result<Vec<dovecote::ClaimedEvent>, dovecote_sqlx_sqlite::ClaimError>;
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

impl TestTenantOps for SqliteDovecote {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_sqlite::EnqueueError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .enqueue(tx, event)
            .await
    }
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_sqlite::ImportError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .import_for_migration(tx, event, state)
            .await
    }
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        row_id: RowId,
        at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_sqlite::FinalizeError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .finalize_pending_delivery_for_migration(tx, row_id, at)
            .await
    }
    async fn page(
        &self,
        after: Option<RowId>,
        limit: Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, dovecote_sqlx_sqlite::PageError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .page(after, limit)
            .await
    }
    async fn begin_snapshot(
        &self,
    ) -> Result<dovecote_sqlx_sqlite::SnapshotPager, dovecote_sqlx_sqlite::PageError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .begin_snapshot()
            .await
    }
    async fn claim(
        &self,
        worker: WorkerId,
        lease: Lease,
        limit: Limit,
    ) -> Result<Vec<dovecote::ClaimedEvent>, dovecote_sqlx_sqlite::ClaimError> {
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

pub(crate) async fn file_database(busy_timeout: Duration) -> (SqlitePool, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "dovecote-sqlite-{}-{}.db",
        std::process::id(),
        unique_suffix()
    ));
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .busy_timeout(busy_timeout);
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("SQLite file pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("migration");
    check_schema(&pool).await.expect("schema");
    (pool, path)
}

pub(crate) fn unique_suffix() -> u128 {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .wrapping_add(u128::from(SEQUENCE.fetch_add(1, Ordering::Relaxed)))
}

pub(crate) fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MutationExpectation {
    NotFound,
    LostClaim,
    IllegalTransition(dovecote::DeliveryState),
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
pub(crate) struct DurableDeliveryState {
    pub(crate) state: String,
    pub(crate) attempts: i64,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_expires_at: Option<String>,
    pub(crate) available_at: String,
    pub(crate) last_failure_code: Option<String>,
    pub(crate) last_failure_detail: Option<String>,
    pub(crate) delivered_at: Option<String>,
    pub(crate) quarantined_at: Option<String>,
    pub(crate) quarantine_reason: Option<String>,
}

pub(crate) async fn durable_delivery_state(
    pool: &SqlitePool,
    row_id: dovecote::RowId,
) -> Option<DurableDeliveryState> {
    sqlx::query_as(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, available_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_optional(pool)
    .await
    .unwrap()
}

#[derive(Default)]
pub(crate) struct FakeTransport {
    pub(crate) accepted: Vec<(String, String)>,
}

impl FakeTransport {
    pub(crate) fn accept(&mut self, event: &dovecote::ClaimedEvent) {
        self.accepted.push((
            event.event().source().as_str().to_owned(),
            event.event().id().as_str().to_owned(),
        ));
    }
}

pub(crate) fn assert_mutation_classification(
    operation: &str,
    result: Result<(), dovecote_sqlx_sqlite::MutationError>,
    expected: MutationExpectation,
) {
    match (result, expected) {
        (Err(dovecote_sqlx_sqlite::MutationError::NotFound), MutationExpectation::NotFound)
        | (Err(dovecote_sqlx_sqlite::MutationError::LostClaim), MutationExpectation::LostClaim) => {
        }
        (
            Err(dovecote_sqlx_sqlite::MutationError::IllegalTransition { state }),
            MutationExpectation::IllegalTransition(expected),
        ) => assert_eq!(state, expected, "{operation} returned the wrong state"),
        (result, expected) => panic!("{operation} returned {result:?}, expected {expected:?}"),
    }
}

pub(crate) async fn assert_all_mutation_classifications(
    adapter: &SqliteDovecote,
    pool: &SqlitePool,
    row_id: dovecote::RowId,
    token: &dovecote::ClaimToken,
    expected: MutationExpectation,
) {
    let before = if matches!(expected, MutationExpectation::NotFound) {
        None
    } else {
        Some(
            durable_delivery_state(pool, row_id)
                .await
                .expect("delivery row"),
        )
    };
    let failure = Failure::new("classification", "classification detail").unwrap();
    let reason = QuarantineReason::new("classification reason").unwrap();
    let lease = Lease::new(Duration::from_secs(5)).unwrap();
    let delay = Delay::new(Duration::ZERO).unwrap();
    assert_mutation_classification("renew", adapter.renew(row_id, token, lease).await, expected);
    assert_mutation_classification("ack", adapter.ack(row_id, token).await, expected);
    assert_mutation_classification(
        "retry",
        adapter.retry(row_id, token, &failure, delay).await,
        expected,
    );
    assert_mutation_classification(
        "release",
        adapter.release(row_id, token, delay).await,
        expected,
    );
    assert_mutation_classification(
        "quarantine",
        adapter.quarantine(row_id, token, &reason).await,
        expected,
    );
    if let Some(before) = before {
        assert_eq!(
            durable_delivery_state(pool, row_id).await,
            Some(before),
            "failed mutation group changed durable delivery state",
        );
    }
}
