//! Explicit tenant-scoped and administrative `PostgreSQL` handles.

use dovecote::{
    ClaimedEvent, EnqueueOutcome, FinalizeOutcome, ImportOutcome, ImportedDeliveryState, NewEvent,
    TenantId,
};
use sqlx::{Postgres, Transaction};
use time::OffsetDateTime;

use crate::{
    ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError, SnapshotPager,
    enqueue, finalize, import, lifecycle, page, rls,
};

/// `PostgreSQL` Dovecote operations restricted to one validated tenant.
#[derive(Clone)]
pub struct TenantDovecote {
    pool: sqlx::PgPool,
    tenant_id: TenantId,
}

impl TenantDovecote {
    pub(crate) const fn new(pool: sqlx::PgPool, tenant_id: TenantId) -> Self {
        Self { pool, tenant_id }
    }

    /// Returns this handle's validated tenant identifier.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Borrows the pool used by this handle.
    #[must_use]
    pub const fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Binds this tenant to a transaction for the optional `PostgreSQL` RLS
    /// profile. Adapter predicates remain active even without RLS.
    ///
    /// # Errors
    /// Returns the database error if binding transaction-local tenant context fails.
    /// The caller must roll back the transaction before retrying.
    pub async fn bind_tenant<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
    ) -> Result<(), sqlx::Error> {
        rls::bind_tenant(transaction, &self.tenant_id).await
    }

    /// Enqueues an event for this tenant in the caller-owned transaction.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        self.bind_tenant(transaction)
            .await
            .map_err(|source| EnqueueError::sql("bind tenant", source))?;
        enqueue::enqueue_for_scope(transaction, &self.tenant_id, event).await
    }

    /// Imports one event and legacy state for this tenant.
    ///
    /// # Errors
    /// Returns a conflict if existing immutable content or delivery history differs,
    /// or an error for unsupported history, incompatible schema, invalid stored data,
    /// or database failure. Roll back the caller-owned transaction on failure.
    pub async fn import_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        self.bind_tenant(transaction)
            .await
            .map_err(|source| ImportError::sql("bind tenant", source))?;
        import::import_for_scope(transaction, &self.tenant_id, event, state).await
    }

    /// Finalizes one canonical pending migration row for this tenant.
    ///
    /// # Errors
    /// Returns an error for invalid occurrence time, conflicting or non-pending
    /// delivery history, incompatible schema, or database failure. The caller owns
    /// rollback and commit; a failed operation must not be committed.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        self.bind_tenant(transaction)
            .await
            .map_err(|source| FinalizeError::sql("bind tenant", source))?;
        finalize::finalize_for_scope(transaction, &self.tenant_id, row_id, delivered_at).await
    }

    /// Reads a live page restricted to this tenant.
    ///
    /// # Errors
    /// Returns an error for incompatible schema, invalid stored event or delivery
    /// state, or a database failure. No delivery state is changed.
    pub async fn page(
        &self,
        after_row_id: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, Some(&self.tenant_id), after_row_id, limit).await
    }

    /// Begins a finite snapshot pager restricted to this tenant.
    ///
    /// # Errors
    /// Returns an error if the backend cannot establish the required snapshot,
    /// the schema is incompatible, or a database operation fails.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, Some(&self.tenant_id)).await
    }

    /// Claims pending and expired deliveries for this tenant.
    ///
    /// # Errors
    /// Returns an error for incompatible backend or schema, invalid stored state,
    /// attempt-counter overflow, unavailable entropy, or database failure. The owned
    /// transaction is rolled back on pre-commit failure; an unknown commit requires
    /// recovery from durable state rather than assuming no claim occurred.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease_for: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(&self.pool, Some(&self.tenant_id), worker, lease_for, limit)
            .await
    }

    /// Renews one current claim for this tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, duration overflow, or database failure.
    pub async fn renew(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        lease_for: dovecote::Lease,
    ) -> Result<(), MutationError> {
        lifecycle::renew_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            claim_token,
            lease_for,
        )
        .await
    }

    /// Acknowledges one current claim for this tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure. A lost commit response
    /// requires durable-state recovery; delivery remains at least once.
    pub async fn ack(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        lifecycle::ack_for_scope(&self.pool, Some(&self.tenant_id), row_id, claim_token).await
    }

    /// Returns one current claim to pending for this tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
    pub async fn retry(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        backoff: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::retry_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            claim_token,
            failure,
            backoff,
        )
        .await
    }

    /// Releases one current claim for this tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
    pub async fn release(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::release_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            claim_token,
            delay,
        )
        .await
    }

    /// Quarantines one current claim for this tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure.
    pub async fn quarantine(
        &self,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        lifecycle::quarantine_for_scope(
            &self.pool,
            Some(&self.tenant_id),
            row_id,
            claim_token,
            reason,
        )
        .await
    }
}

/// Explicit all-tenant `PostgreSQL` Dovecote operations.
#[derive(Clone)]
pub struct AdminDovecote {
    pool: sqlx::PgPool,
}

impl AdminDovecote {
    pub(crate) const fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Borrows the pool used by this handle.
    #[must_use]
    pub const fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Enqueues an event for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns an identity conflict for different immutable content, a schema or
    /// backend incompatibility, invalid stored data, or a database error. The caller
    /// must roll back its transaction on failure; this method never commits it.
    pub async fn enqueue<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        tenant_id: TenantId,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, EnqueueError> {
        enqueue::enqueue_for_scope(transaction, &tenant_id, event).await
    }

    /// Imports one event and legacy state for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns a conflict if existing immutable content or delivery history differs,
    /// or an error for unsupported history, incompatible schema, invalid stored data,
    /// or database failure. Roll back the caller-owned transaction on failure.
    pub async fn import_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        tenant_id: TenantId,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, ImportError> {
        import::import_for_scope(transaction, &tenant_id, event, state).await
    }

    /// Finalizes one migration row for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns an error for invalid occurrence time, conflicting or non-pending
    /// delivery history, incompatible schema, or database failure. The caller owns
    /// rollback and commit; a failed operation must not be committed.
    pub async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        transaction: &mut Transaction<'c, Postgres>,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        delivered_at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, FinalizeError> {
        finalize::finalize_for_scope(transaction, &tenant_id, row_id, delivered_at).await
    }

    /// Reads a live page across all tenants.
    ///
    /// # Errors
    /// Returns an error for incompatible schema, invalid stored event or delivery
    /// state, or a database failure. No delivery state is changed.
    pub async fn page(
        &self,
        after_row_id: Option<dovecote::RowId>,
        limit: dovecote::Limit,
    ) -> Result<Vec<dovecote::PagedEvent>, PageError> {
        page::page_for_scope(&self.pool, None, after_row_id, limit).await
    }

    /// Begins a finite snapshot pager across all tenants.
    ///
    /// # Errors
    /// Returns an error if the backend cannot establish the required snapshot,
    /// the schema is incompatible, or a database operation fails.
    pub async fn begin_snapshot(&self) -> Result<SnapshotPager, PageError> {
        page::begin_snapshot_for_scope(&self.pool, None).await
    }

    /// Claims pending and expired deliveries across all tenants.
    ///
    /// # Errors
    /// Returns an error for incompatible backend or schema, invalid stored state,
    /// attempt-counter overflow, unavailable entropy, or database failure. The owned
    /// transaction is rolled back on pre-commit failure; an unknown commit requires
    /// recovery from durable state rather than assuming no claim occurred.
    pub async fn claim(
        &self,
        worker: dovecote::WorkerId,
        lease_for: dovecote::Lease,
        limit: dovecote::Limit,
    ) -> Result<Vec<ClaimedEvent>, ClaimError> {
        lifecycle::claim_for_scope(&self.pool, None, worker, lease_for, limit).await
    }

    /// Renews one claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, duration overflow, or database failure.
    pub async fn renew(
        &self,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        lease_for: dovecote::Lease,
    ) -> Result<(), MutationError> {
        lifecycle::renew_for_scope(&self.pool, Some(&tenant_id), row_id, claim_token, lease_for)
            .await
    }

    /// Acknowledges one claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure. A lost commit response
    /// requires durable-state recovery; delivery remains at least once.
    pub async fn ack(
        &self,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
    ) -> Result<(), MutationError> {
        lifecycle::ack_for_scope(&self.pool, Some(&tenant_id), row_id, claim_token).await
    }

    /// Retries one claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
    pub async fn retry(
        &self,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        failure: &dovecote::Failure,
        backoff: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::retry_for_scope(
            &self.pool,
            Some(&tenant_id),
            row_id,
            claim_token,
            failure,
            backoff,
        )
        .await
    }

    /// Releases one claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state, delay overflow, or database failure.
    pub async fn release(
        &self,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        delay: dovecote::Delay,
    ) -> Result<(), MutationError> {
        lifecycle::release_for_scope(&self.pool, Some(&tenant_id), row_id, claim_token, delay).await
    }

    /// Quarantines one claim for an explicitly named tenant.
    ///
    /// # Errors
    /// Returns `LostClaim` if the token no longer owns an unexpired claim, or an
    /// error for invalid stored state or database failure.
    pub async fn quarantine(
        &self,
        tenant_id: TenantId,
        row_id: dovecote::RowId,
        claim_token: &dovecote::ClaimToken,
        reason: &dovecote::QuarantineReason,
    ) -> Result<(), MutationError> {
        lifecycle::quarantine_for_scope(&self.pool, Some(&tenant_id), row_id, claim_token, reason)
            .await
    }
}
