//! Explicit tenant and administrative SQLite operation handles.

use crate::{
    BusyConfig, ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError,
    SnapshotPager, enqueue, finalize, import, lifecycle, lifecycle_mutation, page,
};
use dovecote::{
    ClaimedEvent, EnqueueOutcome, FinalizeOutcome, ImportOutcome, ImportedDeliveryState, NewEvent,
    TenantId,
};
use sqlx::{Sqlite, SqlitePool, Transaction};
use time::OffsetDateTime;

/// Ordinary SQLite operations restricted to one validated tenant.
#[derive(Clone)]
pub struct TenantDovecote {
    pool: SqlitePool,
    tenant_id: TenantId,
    busy: BusyConfig,
}
impl TenantDovecote {
    pub(crate) const fn new(pool: SqlitePool, tenant_id: TenantId, busy: BusyConfig) -> Self {
        Self {
            pool,
            tenant_id,
            busy,
        }
    }
    /// Returns this handle's tenant identifier.
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    /// Borrows the underlying pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    /// Returns this handle's busy policy.
    pub const fn busy_config(&self) -> BusyConfig {
        self.busy
    }
    /// Verifies the installed schema.
    pub async fn check_schema(&self) -> Result<(), crate::SchemaError> {
        crate::check_schema(&self.pool).await
    }
    /// Begins a caller-owned writer transaction.
    pub async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        crate::begin_write_with_config(&self.pool, self.busy).await
    }
    /// Alias for [`Self::begin_write`].
    pub async fn begin_enqueue(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        self.begin_write().await
    }
    /// Enqueues in a caller-owned transaction.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &self.tenant_id, event).await
    }
    /// Imports one event and delivery state in a caller-owned transaction.
    pub async fn import_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(tx, &self.tenant_id, event, state).await
    }
    /// Finalizes one migration delivery in a caller-owned transaction.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(tx, &self.tenant_id, row_id, delivered_at).await
    }
    /// Reads one tenant page.
    pub async fn page(
        &self,
        after: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, Some(&self.tenant_id), after, limit).await
    }
    /// Begins one tenant snapshot.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, Some(&self.tenant_id)).await
    }
    /// Claims one tenant's pending deliveries.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            worker,
            lease,
            limit,
            self.busy,
        )
        .await
    }
    /// Renews one tenant claim.
    pub async fn renew(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        lease: dovecote::Lease,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::renew_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            lease,
            self.busy,
        )
        .await
    }
    /// Acknowledges one tenant claim.
    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::ack_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            self.busy,
        )
        .await
    }
    /// Retries one tenant claim.
    pub async fn retry(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::retry_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            failure,
            delay,
            self.busy,
        )
        .await
    }
    /// Releases one tenant claim.
    pub async fn release(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::release_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            delay,
            self.busy,
        )
        .await
    }
    /// Quarantines one tenant claim.
    pub async fn quarantine(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::quarantine_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            reason,
            self.busy,
        )
        .await
    }
}

/// Explicit administrative SQLite handle.
#[derive(Clone)]
pub struct AdminDovecote {
    pool: SqlitePool,
    busy: BusyConfig,
}
impl AdminDovecote {
    pub(crate) const fn new(pool: SqlitePool, busy: BusyConfig) -> Self {
        Self { pool, busy }
    }
    /// Borrows the underlying pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    /// Reads all tenants and returns tenant metadata on each row.
    pub async fn page(
        &self,
        after: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, None, after, limit).await
    }
    /// Begins an all-tenant snapshot.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, None).await
    }
    /// Claims across tenants.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(&self.pool, None, worker, lease, limit, self.busy).await
    }
    /// Enqueues for an explicitly named tenant.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        tenant: TenantId,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &tenant, event).await
    }
    /// Imports for an explicitly named tenant.
    pub async fn import_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        tenant: TenantId,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(tx, &tenant, event, state).await
    }
    /// Finalizes for an explicitly named tenant.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
        tenant: TenantId,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(tx, &tenant, row_id, delivered_at).await
    }

    /// Renews a claim for an explicitly named tenant.
    pub async fn renew(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        lease: dovecote::Lease,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::renew_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            lease,
            self.busy,
        )
        .await
    }
    /// Acknowledges a claim for an explicitly named tenant.
    pub async fn ack(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::ack_for_scope(&self.pool, Some(&tenant), row_id, token, self.busy).await
    }
    /// Retries a claim for an explicitly named tenant.
    pub async fn retry(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::retry_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            failure,
            delay,
            self.busy,
        )
        .await
    }
    /// Releases a claim for an explicitly named tenant.
    pub async fn release(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::release_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            delay,
            self.busy,
        )
        .await
    }
    /// Quarantines a claim for an explicitly named tenant.
    pub async fn quarantine(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        lifecycle_mutation::quarantine_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            reason,
            self.busy,
        )
        .await
    }
}
