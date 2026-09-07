//! `MySQL`/`MariaDB` claims and claim-token-fenced delivery mutations.

use crate::{SchemaError, backend, error::ClaimError};
use dovecote::{ClaimToken, ClaimedEvent, Lease, Limit, TenantId, WorkerId};
use sqlx::{MySqlConnection, MySqlPool, query, query_scalar};

mod candidates;
use candidates::lock_candidates;
mod preparation;
use preparation::{EntropySource, OsEntropy, PreparedClaim, prepare_batch};
use time::OffsetDateTime;

/// Claims an ordered batch of pending or expired deliveries.
pub(crate) async fn claim_for_scope(
    pool: &MySqlPool,
    tenant_id: Option<&TenantId>,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut entropy = OsEntropy;
    claim_with_entropy_scoped(pool, tenant_id, worker, lease_for, limit, &mut entropy).await
}

#[cfg(test)]
async fn claim_with_entropy<E: EntropySource>(
    pool: &MySqlPool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    entropy: &mut E,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    claim_with_entropy_scoped(pool, None, worker, lease_for, limit, entropy).await
}

async fn claim_with_entropy_scoped<E: EntropySource>(
    pool: &MySqlPool,
    tenant_id: Option<&TenantId>,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    entropy: &mut E,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|source| ClaimError::sql("begin claim transaction", source))?;

    let result: Result<Vec<ClaimedEvent>, ClaimError> = async {
        let info = backend::detect_on_connection(&mut transaction)
            .await
            .map_err(schema_to_claim)?;
        if !info.capabilities.skip_locked {
            return Err(ClaimError::BackendMismatch {
                detail: "server does not support SKIP LOCKED".to_owned(),
            });
        }

        let candidates = lock_candidates(&mut transaction, tenant_id, limit).await?;

        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        // The selected delivery rows are locked at this point. Read one
        // microsecond-capable database instant only after that lock acquisition and
        // reuse it for every update and expiry in this claim transaction.
        let operation_time = super::database_time(&mut transaction)
            .await
            .map_err(|source| ClaimError::sql("read claim operation time", source))?;

        let prepared = prepare_batch(candidates, operation_time, entropy)?;
        persist_batch(
            &mut transaction,
            prepared,
            &worker,
            lease_for,
            operation_time,
        )
        .await
    }
    .await;
    match result {
        Ok(claimed) => transaction
            .commit()
            .await
            .map(|()| claimed)
            .map_err(|source| ClaimError::sql("commit claim transaction", source)),
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn persist_batch(
    connection: &mut MySqlConnection,
    prepared: Vec<PreparedClaim>,
    worker: &WorkerId,
    lease_for: Lease,
    operation_time: OffsetDateTime,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut claimed = Vec::with_capacity(prepared.len());
    let lease_micros =
        super::duration_micros(lease_for.get()).map_err(ClaimError::serialization)?;
    for PreparedClaim {
        row_id,
        tenant_id,
        event,
        attempts,
        token,
    } in prepared
    {
        query(
        r"UPDATE dovecote_deliveries
    SET state = _binary 'claimed', attempts = ?, claim_token = ?,
        claimed_by = ?, claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, ?)
    WHERE tenant_id = ? AND event_row_id = ? AND (state = _binary 'pending' OR state = _binary 'claimed')",
    )
    .bind(attempts.get())
    .bind(token.as_slice())
    .bind(worker.as_str().as_bytes())
    .bind(lease_micros)
    .bind(operation_time)
    .bind(tenant_id.as_str().as_bytes())
    .bind(row_id.get())
    .execute(&mut *connection)
    .await
    .map_err(|source| ClaimError::sql("update claimed delivery", source))?;
        let expiry = query_scalar::<_, OffsetDateTime>(
        "SELECT claim_expires_at FROM dovecote_deliveries WHERE tenant_id = ? AND event_row_id = ?",
    )
    .bind(tenant_id.as_str().as_bytes())
    .bind(row_id.get())
    .fetch_one(&mut *connection)
    .await
    .map_err(|source| ClaimError::sql("read claimed expiry", source))?;
        claimed.push(
            ClaimedEvent::new(
                tenant_id,
                row_id,
                event,
                attempts,
                ClaimToken::from_bytes(token),
                worker.clone(),
                expiry,
            )
            .map_err(|error| ClaimError::serialization(error.to_string()))?,
        );
    }
    Ok(claimed)
}

fn schema_to_claim(error: SchemaError) -> ClaimError {
    match error {
        SchemaError::BackendMismatch { detail } => ClaimError::BackendMismatch { detail },
        SchemaError::MigrationMismatch { detail } => ClaimError::MigrationMismatch { detail },
        SchemaError::Sql { operation, source } => ClaimError::sql(operation, source),
        SchemaError::Transient {
            operation,
            source,
            kind,
        } => ClaimError::Transient {
            operation,
            source,
            kind,
        },
    }
}
#[cfg(test)]
mod tests;
