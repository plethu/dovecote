//! MySQL/MariaDB claim-token-fenced delivery mutations.

use crate::{backend, error::MutationError};
use dovecote::{
    ClaimToken, Delay, DeliveryState, Failure, Lease, QuarantineReason, RowId, TenantId,
};
use sqlx::{FromRow, MySql, MySqlPool, Transaction, query, query_as};
use time::OffsetDateTime;

async fn mutate(
    pool: &MySqlPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    mutation: Mutation<'_>,
) -> Result<(), MutationError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|source| MutationError::sql("begin mutation transaction", source))?;
    let result: Result<(), MutationError> = async {
    backend::detect_on_connection(&mut transaction)
        .await
        .map_err(schema_to_mutation)?;
    // The common path is a single conditional UPDATE.  InnoDB takes the
    // target record lock before evaluating UTC_TIMESTAMP(6), so a waiter does
    // not use a clock value captured before its lock wait.
    let affected = execute_mutation_fast(&mut transaction, tenant_id, row_id, token, mutation).await?;
    if affected == 1 {
        return Ok(());
    }
    // A zero-row update is the only path that needs classification.  Obtain
    // the row lock first, then read and reuse one database instant.
    let delivery = query_as::<_, DeliveryForMutation>("SELECT state, claim_token, claim_expires_at FROM dovecote_deliveries WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? FOR UPDATE")
        .bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec()))
        .bind(row_id.get()).fetch_optional(&mut *transaction).await.map_err(|source| MutationError::sql("lock delivery for mutation", source))?
        .ok_or(MutationError::NotFound)?;
    let operation_time = super::database_time(&mut transaction)
        .await
        .map_err(|source| MutationError::sql("read mutation operation time", source))?;
    classify_delivery(&delivery, token, operation_time)?;
    let affected =
        execute_mutation_fast(&mut transaction, tenant_id, row_id, token, mutation).await?;
    if affected != 1 {
        return Err(MutationError::sql(
            "conditional delivery mutation",
            sqlx::Error::Protocol("locked claimed delivery did not satisfy mutation".to_owned()),
        ));
    }
    Ok(())
    }.await;
    match result {
        Ok(()) => transaction
            .commit()
            .await
            .map_err(|source| MutationError::sql("commit mutation transaction", source)),
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn renew_for_scope(
    pool: &MySqlPool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    lease: Lease,
) -> Result<(), MutationError> {
    mutate(pool, tenant, row_id, token, Mutation::Renew { lease }).await
}
pub(crate) async fn ack_for_scope(
    pool: &MySqlPool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
) -> Result<(), MutationError> {
    mutate(pool, tenant, row_id, token, Mutation::Ack).await
}
pub(crate) async fn retry_for_scope(
    pool: &MySqlPool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    failure: &Failure,
    backoff: Delay,
) -> Result<(), MutationError> {
    mutate(
        pool,
        tenant,
        row_id,
        token,
        Mutation::Retry { failure, backoff },
    )
    .await
}
pub(crate) async fn release_for_scope(
    pool: &MySqlPool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    delay: Delay,
) -> Result<(), MutationError> {
    mutate(pool, tenant, row_id, token, Mutation::Release { delay }).await
}
pub(crate) async fn quarantine_for_scope(
    pool: &MySqlPool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    reason: &QuarantineReason,
) -> Result<(), MutationError> {
    mutate(pool, tenant, row_id, token, Mutation::Quarantine { reason }).await
}

async fn execute_mutation_fast(
    transaction: &mut Transaction<'_, MySql>,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    mutation: Mutation<'_>,
) -> Result<u64, MutationError> {
    let token = token.as_bytes().as_slice();
    let result = match mutation {
        Mutation::Renew { lease } => query("UPDATE dovecote_deliveries SET claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)) WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(super::duration_micros(lease.get()).map_err(MutationError::serialization)?).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Ack => query("UPDATE dovecote_deliveries SET state = _binary 'delivered', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, delivered_at = UTC_TIMESTAMP(6) WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Retry { failure, backoff } => query("UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, last_failure_code = ?, last_failure_detail = ? WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(super::duration_micros(backoff.get()).map_err(MutationError::serialization)?).bind(failure.code().as_bytes()).bind(failure.detail().as_bytes()).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Release { delay } => query("UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(super::duration_micros(delay.get()).map_err(MutationError::serialization)?).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Quarantine { reason } => query("UPDATE dovecote_deliveries SET state = _binary 'quarantined', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, quarantined_at = UTC_TIMESTAMP(6), quarantine_reason = ? WHERE (? IS NULL OR tenant_id = ?) AND event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(reason.as_str().as_bytes()).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(tenant_id.map(|value| value.as_str().as_bytes().to_vec())).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
    };
    result
        .map(|value| value.rows_affected())
        .map_err(|source| MutationError::sql("execute fast conditional delivery mutation", source))
}

fn classify_delivery(
    delivery: &DeliveryForMutation,
    token: &ClaimToken,
    now: OffsetDateTime,
) -> Result<(), MutationError> {
    let state = parse_state(&delivery.state)?;
    if state != DeliveryState::Claimed {
        return Err(MutationError::IllegalTransition { state });
    }

    let stored = delivery
        .claim_token
        .as_deref()
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim token"))?;
    if stored.len() != dovecote::CLAIM_TOKEN_BYTES {
        return Err(MutationError::serialization(
            "claimed delivery has invalid claim token width",
        ));
    }

    let expiry = delivery
        .claim_expires_at
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim expiry"))?;
    if stored != token.as_bytes() || expiry <= now {
        return Err(MutationError::LostClaim);
    }
    Ok(())
}

fn parse_state(value: &[u8]) -> Result<DeliveryState, MutationError> {
    match value {
        b"pending" => Ok(DeliveryState::Pending),
        b"claimed" => Ok(DeliveryState::Claimed),
        b"delivered" => Ok(DeliveryState::Delivered),
        b"quarantined" => Ok(DeliveryState::Quarantined),
        _ => Err(MutationError::serialization("unknown delivery state")),
    }
}
fn schema_to_mutation(error: crate::SchemaError) -> MutationError {
    match error {
        crate::SchemaError::BackendMismatch { detail } => MutationError::BackendMismatch { detail },
        crate::SchemaError::MigrationMismatch { detail } => {
            MutationError::MigrationMismatch { detail }
        }
        crate::SchemaError::Sql { operation, source } => MutationError::sql(operation, source),
        crate::SchemaError::Transient {
            operation,
            source,
            kind,
        } => MutationError::Transient {
            operation,
            source,
            kind,
        },
    }
}

#[derive(Debug, FromRow)]
struct DeliveryForMutation {
    state: Vec<u8>,
    claim_token: Option<Vec<u8>>,
    claim_expires_at: Option<OffsetDateTime>,
}
#[derive(Clone, Copy)]
enum Mutation<'a> {
    Renew {
        lease: Lease,
    },
    Ack,
    Retry {
        failure: &'a Failure,
        backoff: Delay,
    },
    Release {
        delay: Delay,
    },
    Quarantine {
        reason: &'a QuarantineReason,
    },
}
