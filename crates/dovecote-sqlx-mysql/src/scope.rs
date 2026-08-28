//! Explicit tenant and administrative MySQL/MariaDB operation handles.

use dovecote::{
    ClaimedEvent, EnqueueOutcome, FinalizeOutcome, ImportOutcome, ImportedDeliveryState, NewEvent,
    TenantId,
};
use sqlx::{MySql, MySqlPool, Transaction};
use time::OffsetDateTime;

use crate::{
    ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError, SnapshotPager,
    enqueue, finalize, import, lifecycle, page,
};

/// Ordinary operations restricted to one validated tenant.
#[derive(Clone)]
pub struct TenantDovecote {
    pool: MySqlPool,
    tenant_id: TenantId,
}

impl TenantDovecote {
    pub(crate) fn new(pool: MySqlPool, tenant_id: TenantId) -> Self {
        Self { pool, tenant_id }
    }
    /// Returns this handle's tenant identifier.
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    /// Borrows the underlying pool.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Enqueues in a caller-owned transaction.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &self.tenant_id, event).await
    }
    /// Imports one event and delivery state in a caller-owned transaction.
    pub async fn import_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(tx, &self.tenant_id, event, state).await
    }
    /// Finalizes one migration delivery in a caller-owned transaction.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
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
        lifecycle::claim_for_scope(&self.pool, Some(&self.tenant_id), worker, lease, limit).await
    }
    /// Renews one tenant claim.
    pub async fn renew(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        lease: dovecote::Lease,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::renew_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            lease,
        )
        .await
    }
    /// Acknowledges one tenant claim.
    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::ack_for_scope(&self.pool, Some(&self.tenant_id), row_id, token)
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
        crate::lifecycle::mutation::retry_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            failure,
            delay,
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
        crate::lifecycle::mutation::release_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            delay,
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
        crate::lifecycle::mutation::quarantine_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            token,
            reason,
        )
        .await
    }
}

/// Explicit administrative handle. It must receive a tenant for every write or mutation.
#[derive(Clone)]
pub struct AdminDovecote {
    pool: MySqlPool,
}

impl AdminDovecote {
    pub(crate) fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    /// Borrows the underlying pool.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Enqueues for an explicitly named tenant.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        tenant: TenantId,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &tenant, event).await
    }
    /// Imports for an explicitly named tenant.
    pub async fn import_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        tenant: TenantId,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(tx, &tenant, event, state).await
    }
    /// Finalizes for an explicitly named tenant.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        tenant: TenantId,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(tx, &tenant, row_id, delivered_at).await
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
        lifecycle::claim_for_scope(&self.pool, None, worker, lease, limit).await
    }

    /// Renews a claim for an explicitly named tenant.
    pub async fn renew(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        lease: dovecote::Lease,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::renew_for_scope(&self.pool, Some(&tenant), row_id, token, lease)
            .await
    }
    /// Acknowledges a claim for an explicitly named tenant.
    pub async fn ack(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::ack_for_scope(&self.pool, Some(&tenant), row_id, token).await
    }
    /// Retries a claim for an explicitly named tenant.
    pub async fn retry(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        backoff: dovecote::Delay,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::retry_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            failure,
            backoff,
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
        crate::lifecycle::mutation::release_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            delay,
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
        crate::lifecycle::mutation::quarantine_for_scope(
            &self.pool,
            Some(&tenant),
            row_id,
            token,
            reason,
        )
        .await
    }
}
