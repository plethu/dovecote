//! Explicit tenant and administrative `MySQL`/`MariaDB` operation handles.

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
    pub(crate) const fn new(pool: MySqlPool, tenant_id: TenantId) -> Self {
        Self { pool, tenant_id }
    }
    /// Returns this handle's tenant identifier.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    /// Borrows the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Enqueues in a caller-owned transaction.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &self.tenant_id, event).await
    }
    /// Imports one event and delivery state in a caller-owned transaction.
    ///
    /// # Errors
    /// Returns a conflict if existing immutable content or delivery history differs,
    /// or an error for unsupported history, incompatible schema, invalid stored data,
    /// or database failure. Roll back the caller-owned transaction on failure.
    pub async fn import_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(tx, &self.tenant_id, event, state).await
    }
    /// Finalizes one migration delivery in a caller-owned transaction.
    ///
    /// # Errors
    /// Returns an error for invalid occurrence time, conflicting or non-pending
    /// delivery history, incompatible schema, or database failure. The caller owns
    /// rollback and commit; a failed operation must not be committed.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(tx, &self.tenant_id, row_id, delivered_at).await
    }
    /// Reads one tenant page.
    ///
    /// # Errors
    /// Returns an error for incompatible schema, invalid stored event or delivery
    /// state, or a database failure. No delivery state is changed.
    pub async fn page(
        &self,
        after: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, Some(&self.tenant_id), after, limit).await
    }
    /// Begins one tenant snapshot.
    ///
    /// # Errors
    /// Returns an error if the backend cannot establish the required snapshot,
    /// the schema is incompatible, or a database operation fails.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, Some(&self.tenant_id)).await
    }
    /// Claims one tenant's pending deliveries.
    ///
    /// # Errors
    /// Returns an error for incompatible backend or schema, invalid stored state,
    /// attempt-counter overflow, unavailable entropy, or database failure. The owned
    /// transaction is rolled back on pre-commit failure; an unknown commit requires
    /// recovery from durable state rather than assuming no claim occurred.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(&self.pool, Some(&self.tenant_id), worker, lease, limit).await
    }
    /// Renews one tenant claim.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, duration overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure. A lost commit response
    /// requires durable-state recovery; delivery remains at least once.
    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::ack_for_scope(&self.pool, Some(&self.tenant_id), row_id, token)
            .await
    }
    /// Retries one tenant claim.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure.
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
    pub(crate) const fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    /// Borrows the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Enqueues for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, MySql>,
        tenant: TenantId,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(tx, &tenant, event).await
    }
    /// Imports for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns a conflict if existing immutable content or delivery history differs,
    /// or an error for unsupported history, incompatible schema, invalid stored data,
    /// or database failure. Roll back the caller-owned transaction on failure.
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
    ///
    /// # Errors
    /// Returns an error for invalid occurrence time, conflicting or non-pending
    /// delivery history, incompatible schema, or database failure. The caller owns
    /// rollback and commit; a failed operation must not be committed.
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
    ///
    /// # Errors
    /// Returns an error for incompatible schema, invalid stored event or delivery
    /// state, or a database failure. No delivery state is changed.
    pub async fn page(
        &self,
        after: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, None, after, limit).await
    }
    /// Begins an all-tenant snapshot.
    ///
    /// # Errors
    /// Returns an error if the backend cannot establish the required snapshot,
    /// the schema is incompatible, or a database operation fails.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, None).await
    }
    /// Claims across tenants.
    ///
    /// # Errors
    /// Returns an error for incompatible backend or schema, invalid stored state,
    /// attempt-counter overflow, unavailable entropy, or database failure. The owned
    /// transaction is rolled back on pre-commit failure; an unknown commit requires
    /// recovery from durable state rather than assuming no claim occurred.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(&self.pool, None, worker, lease, limit).await
    }

    /// Renews a claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, duration overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure. A lost commit response
    /// requires durable-state recovery; delivery remains at least once.
    pub async fn ack(
        &self,
        tenant: TenantId,
        row_id: dovecote::RowId,
        token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        crate::lifecycle::mutation::ack_for_scope(&self.pool, Some(&tenant), row_id, token).await
    }
    /// Retries a claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
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
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure.
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
