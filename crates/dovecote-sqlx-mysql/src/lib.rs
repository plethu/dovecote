//! MySQL and MariaDB schema and SQLx boundary for Dovecote.
//!
//! The adapter deliberately detects the server family and release before
//! using dialect-sensitive locking and catalog operations. MySQL success is
//! not treated as evidence for MariaDB, or the reverse.

mod backend;
mod enqueue;
mod error;
mod finalize;
mod import;
mod lifecycle;
mod migration;
mod page;
mod schema;

pub use backend::detect as detect_backend;
pub use backend::{BackendInfo, BackendKind, Capabilities, ServerVersion};
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
use sqlx::{MySql, MySqlPool, Transaction};

/// MySQL/MariaDB adapter for Dovecote's durable event and delivery schema.
#[derive(Clone)]
pub struct MySqlDovecote {
    pool: MySqlPool,
}

impl MySqlDovecote {
    /// Creates an adapter using the supplied SQLx pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    /// Borrows the pool used by this adapter.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Detects and verifies the configured backend and schema.
    pub async fn check_schema(&self) -> Result<(), SchemaError> {
        check_schema(&self.pool).await
    }
    /// Detects the backend family, release and capabilities.
    pub async fn backend_info(&self) -> Result<BackendInfo, SchemaError> {
        backend::detect(&self.pool).await
    }
    /// Enqueues an event in the caller-owned transaction.
    pub async fn enqueue<'c>(
        &self,
        transaction: &mut Transaction<'c, MySql>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue(transaction, event).await
    }

    /// Imports one already-validated event and its legacy delivery state in
    /// the caller-owned transaction. This is migration infrastructure, not a
    /// replacement for [`Self::enqueue`].
    pub async fn import_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, MySql>,
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
        transaction: &mut Transaction<'c, MySql>,
        row_id: dovecote::RowId,
        delivered_at: time::OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize_pending_delivery_for_migration(transaction, row_id, delivered_at).await
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
        claim(&self.pool, worker, lease_for, limit).await
    }
    pub async fn renew(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        lease_for: dovecote::Lease,
    ) -> Result<(), MutationError> {
        renew(&self.pool, row_id, claim_token, lease_for).await
    }
    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        ack(&self.pool, row_id, claim_token).await
    }
    pub async fn retry(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        backoff: dovecote::Delay,
    ) -> Result<(), MutationError> {
        retry(&self.pool, row_id, claim_token, failure, backoff).await
    }
    pub async fn release(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        release(&self.pool, row_id, claim_token, delay).await
    }
    pub async fn quarantine(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        quarantine(&self.pool, row_id, claim_token, reason).await
    }
}
