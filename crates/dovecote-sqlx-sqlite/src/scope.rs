//! Explicit tenant and administrative `SQLite` operation handles.

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

/// Ordinary `SQLite` operations restricted to one validated tenant.
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
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    /// Borrows the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    /// Returns this handle's busy policy.
    #[must_use]
    pub const fn busy_config(&self) -> BusyConfig {
        self.busy
    }
    /// Verifies the installed schema.
    ///
    /// # Errors
    /// Returns an error for an unsupported backend, missing or incompatible
    /// migration markers, tables, constraints or indexes, or failed catalog reads.
    pub async fn check_schema(&self) -> Result<(), crate::SchemaError> {
        crate::check_schema(&self.pool).await
    }
    /// Begins a caller-owned writer transaction.
    ///
    /// # Errors
    /// Returns an error for invalid busy configuration or if the `SQLite` writer
    /// reservation cannot be acquired within the configured busy budget.
    pub async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        crate::begin_write_with_config(&self.pool, self.busy).await
    }
    /// Alias for [`Self::begin_write`].
    ///
    /// # Errors
    /// Returns an error for invalid busy configuration or if the `SQLite` writer
    /// reservation cannot be acquired within the configured busy budget.
    pub async fn begin_enqueue(&self) -> Result<Transaction<'static, Sqlite>, EnqueueError> {
        self.begin_write().await
    }
    /// Enqueues in a caller-owned transaction.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
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
        tx: &mut Transaction<'c, Sqlite>,
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
        tx: &mut Transaction<'c, Sqlite>,
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

/// Explicit administrative `SQLite` handle.
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
    #[must_use]
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
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
        lifecycle::claim_for_scope(&self.pool, None, worker, lease, limit, self.busy).await
    }
    /// Enqueues for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        tx: &mut Transaction<'c, Sqlite>,
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
        tx: &mut Transaction<'c, Sqlite>,
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
        tx: &mut Transaction<'c, Sqlite>,
        tenant: TenantId,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(tx, &tenant, row_id, delivered_at).await
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
        lifecycle_mutation::ack_for_scope(&self.pool, Some(&tenant), row_id, token, self.busy).await
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
