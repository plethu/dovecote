//! SQLite schema and SQLx boundary for Dovecote.
//!
//! SQLite's single-writer model is a distinct support contract. Write and
//! claim transactions therefore use explicit `BEGIN IMMEDIATE`; busy errors
//! are retried only by the bounded policy configured on [`SqliteDovecote`].

mod enqueue;
mod error;
mod finalize;
mod hydrate;
mod import;
mod lifecycle;
mod migration;
mod page;
mod schema;

pub use enqueue::enqueue;
pub use error::{
    ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError, SchemaError,
    TransientKind,
};
pub use finalize::finalize_pending_delivery_for_migration;
pub use import::import_for_migration;
pub use lifecycle::{ack, claim, quarantine, release, renew, retry};
pub use migration::{
    CrateVersion, MIGRATIONS, Migration, MigrationCompatibility, MigrationCompatibilityError,
    SCHEMA_VERSION,
};
pub use page::{SnapshotPager, begin_snapshot, page};
pub use schema::check_schema;

use dovecote::{EnqueueOutcome, FinalizeOutcome, ImportOutcome, ImportedDeliveryState, NewEvent};
use sqlx::{AssertSqlSafe, SqlSafeStr, Sqlite, SqlitePool, Transaction, query, query_scalar};
use std::time::Duration;

/// Default number of whole-operation retries after each configured busy timeout.
pub const DEFAULT_BUSY_RETRIES: u32 = 3;
/// Default per-connection wait before returning `SQLITE_BUSY`.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Begins a caller write transaction using the default busy policy. The
/// returned transaction is safe to pass to [`enqueue`].
pub async fn begin_write(pool: &SqlitePool) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
    begin_write_with_config(pool, BusyConfig::default()).await
}

/// Alias for [`begin_write`] for callers about to enqueue an event.
pub async fn begin_enqueue(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
    begin_write(pool).await
}

/// Bounded busy handling for SQLite's single-writer lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BusyConfig {
    timeout: Duration,
    retries: u32,
}

impl BusyConfig {
    /// Creates a policy with a finite per-lock wait and at most `retries`
    /// immediate whole-operation retries. The total lock-wait budget is at
    /// most `(retries + 1) * timeout` per operation.
    pub const fn new(timeout: Duration, retries: u32) -> Self {
        Self { timeout, retries }
    }

    /// Creates a policy using the default per-lock timeout.
    pub const fn with_retries(retries: u32) -> Self {
        Self::new(DEFAULT_BUSY_TIMEOUT, retries)
    }

    /// Maximum wait for one SQLite writer-lock acquisition.
    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    /// Number of complete operation retries after the first lock wait.
    pub const fn retries(self) -> u32 {
        self.retries
    }
}

impl Default for BusyConfig {
    fn default() -> Self {
        Self::new(DEFAULT_BUSY_TIMEOUT, DEFAULT_BUSY_RETRIES)
    }
}

/// SQLite adapter for Dovecote's durable event and delivery schema.
#[derive(Clone)]
pub struct SqliteDovecote {
    pool: SqlitePool,
    busy: BusyConfig,
}

impl SqliteDovecote {
    /// Creates an adapter with the documented bounded busy policy.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            busy: BusyConfig::default(),
        }
    }

    /// Creates an adapter with an explicit busy retry policy.
    pub const fn with_busy_config(pool: SqlitePool, busy: BusyConfig) -> Self {
        Self { pool, busy }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub const fn busy_config(&self) -> BusyConfig {
        self.busy
    }

    /// Begins the caller transaction used for enqueue and application state.
    /// It acquires SQLite's single writer slot before any adapter reads.
    pub async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        begin_write_with_config(&self.pool, self.busy).await
    }

    /// Alias emphasizing that the returned transaction is suitable for
    /// [`Self::enqueue`].
    pub async fn begin_enqueue(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        self.begin_write().await
    }

    pub async fn enqueue<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue(transaction, event).await
    }

    /// Imports one already-validated event and its legacy delivery state in
    /// the caller-owned transaction. This is migration infrastructure, not a
    /// replacement for [`Self::enqueue`].
    pub async fn import_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import_for_migration(transaction, event, state).await
    }

    /// Records the legacy publisher's authoritative delivery time for a
    /// canonical pending migration import. This operation is migration
    /// infrastructure, not an ordinary acknowledgement shortcut.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        row_id: dovecote::RowId,
        delivered_at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize_pending_delivery_for_migration(transaction, row_id, delivered_at).await
    }

    pub async fn check_schema(&self) -> Result<(), SchemaError> {
        check_schema(&self.pool).await
    }

    pub async fn page(
        &self,
        after_row_id: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page(&self.pool, after_row_id, limit).await
    }

    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        begin_snapshot(&self.pool).await
    }

    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease_for: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::ClaimedEvent>, ClaimError> {
        lifecycle::claim_with_config(&self.pool, worker, lease_for, limit, self.busy).await
    }

    pub async fn renew(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        lease_for: dovecote::Lease,
    ) -> Result<(), MutationError> {
        lifecycle::renew_with_config(&self.pool, row_id, claim_token, lease_for, self.busy).await
    }

    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        lifecycle::ack_with_config(&self.pool, row_id, claim_token, self.busy).await
    }

    pub async fn retry(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        backoff: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::retry_with_config(&self.pool, row_id, claim_token, failure, backoff, self.busy)
            .await
    }

    pub async fn release(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::release_with_config(&self.pool, row_id, claim_token, delay, self.busy).await
    }

    pub async fn quarantine(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        lifecycle::quarantine_with_config(&self.pool, row_id, claim_token, reason, self.busy).await
    }
}

/// Kept private so adapter operations cannot accidentally use a worker clock.
pub(crate) fn checked_milliseconds(value: Duration) -> Result<i64, String> {
    if !value.is_zero() && !value.subsec_nanos().is_multiple_of(1_000_000) {
        return Err("duration must be an exact whole number of milliseconds".to_owned());
    }
    i64::try_from(value.as_millis()).map_err(|_| "duration exceeds SQLite integer range".to_owned())
}

pub(crate) fn checked_busy_timeout(value: Duration) -> Result<i64, String> {
    let milliseconds = checked_milliseconds(value)?;
    if milliseconds > i64::from(i32::MAX) {
        return Err("busy timeout exceeds SQLite's signed 32-bit millisecond range".to_owned());
    }
    Ok(milliseconds)
}

/// Acquires a pool connection, installs the busy timeout on that actual
/// connection, verifies it, and only then starts `BEGIN IMMEDIATE`.
pub(crate) async fn begin_immediate(
    pool: &SqlitePool,
    busy: BusyConfig,
    _operation: &'static str,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    install_foreign_keys(&mut connection).await?;
    install_busy_timeout(&mut connection, busy).await?;
    Transaction::begin(
        sqlx::pool::MaybePoolConnection::PoolConnection(connection),
        Some(AssertSqlSafe("BEGIN IMMEDIATE").into_sql_str()),
    )
    .await
}

async fn begin_write_with_config(
    pool: &SqlitePool,
    busy: BusyConfig,
) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
    validate_busy_config(busy).map_err(|detail| EnqueueError::Configuration { detail })?;
    let mut tries = 0;
    loop {
        match begin_immediate(pool, busy, "begin write transaction").await {
            Ok(transaction) => return Ok(transaction),
            Err(source) if error::is_busy(&source) && tries < busy.retries() => {
                tries += 1;
            }
            Err(source) => return Err(EnqueueError::sql("begin write transaction", source)),
        }
    }
}

pub(crate) fn validate_busy_config(busy: BusyConfig) -> Result<(), String> {
    checked_busy_timeout(busy.timeout()).map(|_| ())
}

pub(crate) async fn begin_read(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    install_foreign_keys(&mut connection).await?;
    Transaction::begin(
        sqlx::pool::MaybePoolConnection::PoolConnection(connection),
        Some(AssertSqlSafe("BEGIN").into_sql_str()),
    )
    .await
}

/// Completes a transaction while retaining the transaction value on commit
/// failure long enough to await its rollback. SQLx's consuming
/// `Transaction::commit` can only schedule a best-effort rollback when the
/// commit fails; this path closes the owned transaction synchronously from the
/// adapter's async operation instead.
pub(crate) async fn commit_transaction(
    mut transaction: Transaction<'static, Sqlite>,
) -> Result<(), sqlx::Error> {
    use sqlx_core::transaction::TransactionManager;

    let result =
        <sqlx::sqlite::SqliteTransactionManager as TransactionManager>::commit(&mut *transaction)
            .await;
    if result.is_err() {
        let _ = <sqlx::sqlite::SqliteTransactionManager as TransactionManager>::rollback(
            &mut *transaction,
        )
        .await;
    }
    result
}

pub(crate) async fn install_foreign_keys(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
) -> Result<(), sqlx::Error> {
    query("PRAGMA foreign_keys = ON")
        .execute(&mut **connection)
        .await?;
    let enabled: i64 = query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut **connection)
        .await?;
    if enabled != 1 {
        return Err(sqlx::Error::Protocol(
            "SQLite foreign-key enforcement could not be enabled".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn install_busy_timeout(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
    busy: BusyConfig,
) -> Result<(), sqlx::Error> {
    let milliseconds = checked_busy_timeout(busy.timeout())
        .map_err(|detail| sqlx::Error::Protocol(format!("invalid busy configuration: {detail}")))?;
    let statement = AssertSqlSafe(format!("PRAGMA busy_timeout = {milliseconds}"));
    query(statement).execute(&mut **connection).await?;
    let installed: i64 = query_scalar("PRAGMA busy_timeout")
        .fetch_one(&mut **connection)
        .await?;
    if installed != milliseconds {
        return Err(sqlx::Error::Protocol(format!(
            "SQLite busy timeout installation mismatch: requested {milliseconds}, installed {installed}"
        )));
    }
    Ok(())
}

/// SQLite exposes transaction state through its C API, not SQL. This is a
/// read-only inspection and does not alter the caller transaction.
pub(crate) async fn transaction_is_write(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<bool, sqlx::Error> {
    let mut handle = transaction.lock_handle().await?;
    let state = unsafe {
        libsqlite3_sys::sqlite3_txn_state(handle.as_raw_handle().as_ptr(), std::ptr::null())
    };
    Ok(state == libsqlite3_sys::SQLITE_TXN_WRITE)
}
