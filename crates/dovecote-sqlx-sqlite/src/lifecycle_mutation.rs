//! Claim-token-fenced SQLite delivery mutations.

use crate::{
    BusyConfig, begin_immediate, checked_milliseconds, commit_transaction,
    enqueue::parse_timestamp, error::MutationError, validate_busy_config,
};
use dovecote::{
    ClaimToken, Delay, DeliveryState, Failure, Lease, QuarantineReason, RowId, TenantId,
};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction, query, query_as};

/// A post-claim transition with its caller-owned fencing inputs.
#[derive(Clone, Copy)]
pub(crate) enum Mutation<'a> {
    Renew {
        lease_for: Lease,
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

enum PreparedMutation<'a> {
    Renew {
        duration_ms: i64,
    },
    Ack,
    Retry {
        failure: &'a Failure,
        duration_ms: i64,
    },
    Release {
        duration_ms: i64,
    },
    Quarantine {
        reason: &'a QuarantineReason,
    },
}

pub(crate) async fn renew_for_scope(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    lease: Lease,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        tenant,
        row_id,
        token,
        Mutation::Renew { lease_for: lease },
        busy,
    )
    .await
}
pub(crate) async fn ack_for_scope(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(pool, tenant, row_id, token, Mutation::Ack, busy).await
}
pub(crate) async fn retry_for_scope(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    failure: &Failure,
    delay: Delay,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        tenant,
        row_id,
        token,
        Mutation::Retry {
            failure,
            backoff: delay,
        },
        busy,
    )
    .await
}
pub(crate) async fn release_for_scope(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    delay: Delay,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        tenant,
        row_id,
        token,
        Mutation::Release { delay },
        busy,
    )
    .await
}
pub(crate) async fn quarantine_for_scope(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    token: &ClaimToken,
    reason: &QuarantineReason,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        tenant,
        row_id,
        token,
        Mutation::Quarantine { reason },
        busy,
    )
    .await
}

async fn mutate_with_config(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    validate_busy_config(busy).map_err(|detail| MutationError::Configuration { detail })?;
    let mut tries = 0;
    loop {
        match mutate_once(pool, tenant, row_id, claim_token, mutation, busy).await {
            Err(error) if error.is_busy() && tries < busy.retries() => {
                tries += 1;
                continue;
            }
            Err(error) if error.is_busy() => return Err(error.into_busy_exhausted()),
            result => return result,
        }
    }
}

async fn mutate_once(
    pool: &SqlitePool,
    tenant: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    let mutation = match mutation {
        Mutation::Renew { lease_for } => PreparedMutation::Renew {
            duration_ms: checked_milliseconds(lease_for.get())
                .map_err(MutationError::serialization)?,
        },
        Mutation::Ack => PreparedMutation::Ack,
        Mutation::Retry { failure, backoff } => PreparedMutation::Retry {
            failure,
            duration_ms: checked_milliseconds(backoff.get())
                .map_err(MutationError::serialization)?,
        },
        Mutation::Release { delay } => PreparedMutation::Release {
            duration_ms: checked_milliseconds(delay.get()).map_err(MutationError::serialization)?,
        },
        Mutation::Quarantine { reason } => PreparedMutation::Quarantine { reason },
    };
    let mut transaction = begin_immediate(pool, busy, "mutation")
        .await
        .map_err(|source| MutationError::sql("begin immediate mutation transaction", source))?;
    let operation_time = match crate::lifecycle::database_time(&mut transaction).await {
        Ok(value) => value,
        Err(source) => {
            return rollback_mutation(
                transaction,
                MutationError::sql("read mutation operation time", source),
            )
            .await;
        }
    };

    let changed = match mutation {
        PreparedMutation::Renew { duration_ms } => {
            query("UPDATE dovecote_deliveries SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)) WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?) AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
                .bind(&operation_time)
                .bind(duration_ms)
                .bind(duration_ms)
                .bind(row_id.get())
                .bind(tenant.map(TenantId::as_str))
                .bind(tenant.map(TenantId::as_str))
                .bind(claim_token.as_bytes().as_slice())
                .bind(&operation_time)
                .execute(&mut *transaction)
                .await
        }
        PreparedMutation::Ack => {
            query("UPDATE dovecote_deliveries SET state = 'delivered', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, delivered_at = ? WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?) AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
                .bind(&operation_time)
                .bind(row_id.get())
                .bind(tenant.map(TenantId::as_str))
                .bind(tenant.map(TenantId::as_str))
                .bind(claim_token.as_bytes().as_slice())
                .bind(&operation_time)
                .execute(&mut *transaction)
                .await
        }
        PreparedMutation::Retry {
            failure,
            duration_ms,
        } => {
            query("UPDATE dovecote_deliveries SET state = 'pending', available_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, last_failure_code = ?, last_failure_detail = ? WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?) AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
                .bind(&operation_time)
                .bind(duration_ms)
                .bind(duration_ms)
                .bind(failure.code())
                .bind(failure.detail())
                .bind(row_id.get())
                .bind(tenant.map(TenantId::as_str))
                .bind(tenant.map(TenantId::as_str))
                .bind(claim_token.as_bytes().as_slice())
                .bind(&operation_time)
                .execute(&mut *transaction)
                .await
        }
        PreparedMutation::Release { duration_ms } => {
            query("UPDATE dovecote_deliveries SET state = 'pending', available_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?) AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
                .bind(&operation_time)
                .bind(duration_ms)
                .bind(duration_ms)
                .bind(row_id.get())
                .bind(tenant.map(TenantId::as_str))
                .bind(tenant.map(TenantId::as_str))
                .bind(claim_token.as_bytes().as_slice())
                .bind(&operation_time)
                .execute(&mut *transaction)
                .await
        }
        PreparedMutation::Quarantine { reason } => {
            query("UPDATE dovecote_deliveries SET state = 'quarantined', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, quarantined_at = ?, quarantine_reason = ? WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?) AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
                .bind(&operation_time)
                .bind(reason.as_str())
                .bind(row_id.get())
                .bind(tenant.map(TenantId::as_str))
                .bind(tenant.map(TenantId::as_str))
                .bind(claim_token.as_bytes().as_slice())
                .bind(&operation_time)
                .execute(&mut *transaction)
                .await
        }
    };
    let changed = match changed {
        Ok(value) => value,
        Err(source) => {
            return rollback_mutation(
                transaction,
                MutationError::sql("execute conditional delivery mutation", source),
            )
            .await;
        }
    };

    if changed.rows_affected() != 1 {
        let delivery = match query_as::<_, DeliveryForMutation>(
            "SELECT state, claim_token, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ? AND (? IS NULL OR tenant_id = ?)",
        )
        .bind(row_id.get())
        .bind(tenant.map(TenantId::as_str))
        .bind(tenant.map(TenantId::as_str))
        .fetch_optional(&mut *transaction)
        .await
        {
            Ok(Some(value)) => value,
            Ok(None) => return rollback_mutation(transaction, MutationError::NotFound).await,
            Err(source) => {
                return rollback_mutation(
                    transaction,
                    MutationError::sql("classify delivery mutation", source),
                )
                .await;
            }
        };
        if let Err(error) = classify_delivery(&delivery, claim_token, &operation_time) {
            return rollback_mutation(transaction, error).await;
        }

        return rollback_mutation(
            transaction,
            MutationError::sql(
                "conditional delivery mutation",
                sqlx::Error::Protocol("claimed delivery did not satisfy mutation".to_owned()),
            ),
        )
        .await;
    }

    commit_transaction(transaction)
        .await
        .map_err(|source| MutationError::sql("commit mutation transaction", source))
}

async fn rollback_mutation<T>(
    transaction: Transaction<'static, Sqlite>,
    error: MutationError,
) -> Result<T, MutationError> {
    let _ = transaction.rollback().await;
    Err(error)
}

#[derive(Debug, FromRow)]
struct DeliveryForMutation {
    state: String,
    claim_token: Option<Vec<u8>>,
    claim_expires_at: Option<String>,
}

fn classify_delivery(
    delivery: &DeliveryForMutation,
    token: &ClaimToken,
    operation_time: &str,
) -> Result<(), MutationError> {
    let state = parse_state(&delivery.state)?;
    if state != DeliveryState::Claimed {
        return Err(MutationError::IllegalTransition { state });
    }

    let stored = delivery
        .claim_token
        .as_deref()
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim token"))?;
    if stored.len() != 16 {
        return Err(MutationError::serialization(
            "claimed delivery has an invalid claim token width",
        ));
    }

    let expiry = delivery
        .claim_expires_at
        .as_deref()
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim expiry"))?;
    let expiry = parse_timestamp(expiry).map_err(MutationError::serialization)?;
    let operation_time = parse_timestamp(operation_time).map_err(MutationError::serialization)?;
    if stored != token.as_bytes() || expiry <= operation_time {
        return Err(MutationError::LostClaim);
    }

    Ok(())
}

fn parse_state(value: &str) -> Result<DeliveryState, MutationError> {
    match value {
        "pending" => Ok(DeliveryState::Pending),
        "claimed" => Ok(DeliveryState::Claimed),
        "delivered" => Ok(DeliveryState::Delivered),
        "quarantined" => Ok(DeliveryState::Quarantined),
        _ => Err(MutationError::serialization(format!(
            "unknown delivery state {value:?}"
        ))),
    }
}
