//! Claim-token-fenced PostgreSQL delivery mutations.

use crate::{error::MutationError, rls};
use dovecote::{
    ClaimToken, Delay, DeliveryState, Failure, Lease, QuarantineReason, RowId, TenantId,
};
use sqlx::{FromRow, Postgres, Transaction, query, query_as};
use time::OffsetDateTime;

/// A post-claim state transition with its caller-owned fencing inputs.
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

/// Executes one fenced mutation in its own short transaction.
pub(crate) async fn mutate(
    pool: &sqlx::PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
) -> Result<(), MutationError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|source| MutationError::sql("begin mutation transaction", source))?;
    if let Some(tenant_id) = tenant_id {
        rls::bind_tenant(&mut transaction, tenant_id)
            .await
            .map_err(|source| MutationError::sql("bind mutation tenant", source))?;
    }

    let result: Result<(), MutationError> = async {
        // The common case is one conditional update and commit.  The statement
        // itself materializes a target-row lock before evaluating the database
        // clock, so a worker blocked behind a lease that expires is never allowed
        // to mutate using an instant from before the lock wait.  Only a zero-row
        // result needs the classification path below.
        let affected =
            execute_mutation(&mut transaction, tenant_id, row_id, claim_token, mutation).await?;
        if affected == 1 {
            return Ok(());
        }

        let delivery = lock_delivery(&mut transaction, tenant_id, row_id).await?;

        classify_delivery(&delivery, claim_token)?;

        let affected =
            execute_mutation(&mut transaction, tenant_id, row_id, claim_token, mutation).await?;
        if affected != 1 {
            // The row remains locked.  A second lock/clock read distinguishes a
            // lease that expired during the retry from an unexpected database-side
            // no-op; the latter remains an actionable SQL error.
            let latest = lock_delivery(&mut transaction, tenant_id, row_id).await?;
            classify_delivery(&latest, claim_token).map_err(|error| match error {
                MutationError::LostClaim | MutationError::IllegalTransition { .. } => error,
                other => other,
            })?;
            return Err(MutationError::sql(
                "conditional delivery mutation",
                sqlx::Error::Protocol(
                    "locked claimed delivery did not satisfy mutation".to_owned(),
                ),
            ));
        }

        Ok(())
    }
    .await;

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

async fn execute_mutation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
) -> Result<u64, MutationError> {
    let token = claim_token.as_bytes().as_slice();
    let result = match mutation {
        Mutation::Renew { lease_for } => {
            query(
                r#"
                WITH locked AS MATERIALIZED (
                    SELECT event_row_id
                    FROM dovecote_deliveries
                    WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
                    FOR UPDATE
                ), operation AS MATERIALIZED (
                    SELECT locked.event_row_id, clock_timestamp() AS operation_time
                    FROM locked
                )
                UPDATE dovecote_deliveries AS delivery
                SET claim_expires_at = operation.operation_time + $4
                FROM operation
                WHERE ($1::varchar IS NULL OR delivery.tenant_id = $1)
                  AND delivery.event_row_id = operation.event_row_id
                  AND delivery.state = 'claimed'
                  AND delivery.claim_token = $3
                  AND delivery.claim_expires_at > operation.operation_time
                "#,
            )
            .bind(tenant_id.map(TenantId::as_str))
            .bind(row_id.get())
            .bind(token)
            .bind(lease_for.get())
            .execute(&mut **transaction)
            .await
        }
        Mutation::Ack => {
            query(
                r#"
                WITH locked AS MATERIALIZED (
                    SELECT event_row_id
                    FROM dovecote_deliveries
                    WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
                    FOR UPDATE
                ), operation AS MATERIALIZED (
                    SELECT locked.event_row_id, clock_timestamp() AS operation_time
                    FROM locked
                )
                UPDATE dovecote_deliveries AS delivery
                SET state = 'delivered',
                    claim_token = NULL,
                    claimed_by = NULL,
                    claim_expires_at = NULL,
                    delivered_at = operation.operation_time
                FROM operation
                WHERE ($1::varchar IS NULL OR delivery.tenant_id = $1)
                  AND delivery.event_row_id = operation.event_row_id
                  AND delivery.state = 'claimed'
                  AND delivery.claim_token = $3
                  AND delivery.claim_expires_at > operation.operation_time
                "#,
            )
            .bind(tenant_id.map(TenantId::as_str))
            .bind(row_id.get())
            .bind(token)
            .execute(&mut **transaction)
            .await
        }
        Mutation::Retry { failure, backoff } => {
            query(
                r#"
                WITH locked AS MATERIALIZED (
                    SELECT event_row_id
                    FROM dovecote_deliveries
                    WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
                    FOR UPDATE
                ), operation AS MATERIALIZED (
                    SELECT locked.event_row_id, clock_timestamp() AS operation_time
                    FROM locked
                )
                UPDATE dovecote_deliveries AS delivery
                SET state = 'pending',
                    available_at = operation.operation_time + $4,
                    claim_token = NULL,
                    claimed_by = NULL,
                    claim_expires_at = NULL,
                    last_failure_code = $5,
                    last_failure_detail = $6
                FROM operation
                WHERE ($1::varchar IS NULL OR delivery.tenant_id = $1)
                  AND delivery.event_row_id = operation.event_row_id
                  AND delivery.state = 'claimed'
                  AND delivery.claim_token = $3
                  AND delivery.claim_expires_at > operation.operation_time
                "#,
            )
            .bind(tenant_id.map(TenantId::as_str))
            .bind(row_id.get())
            .bind(token)
            .bind(backoff.get())
            .bind(failure.code())
            .bind(failure.detail())
            .execute(&mut **transaction)
            .await
        }
        Mutation::Release { delay } => {
            query(
                r#"
                WITH locked AS MATERIALIZED (
                    SELECT event_row_id
                    FROM dovecote_deliveries
                    WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
                    FOR UPDATE
                ), operation AS MATERIALIZED (
                    SELECT locked.event_row_id, clock_timestamp() AS operation_time
                    FROM locked
                )
                UPDATE dovecote_deliveries AS delivery
                SET state = 'pending',
                    available_at = operation.operation_time + $4,
                    claim_token = NULL,
                    claimed_by = NULL,
                    claim_expires_at = NULL
                FROM operation
                WHERE ($1::varchar IS NULL OR delivery.tenant_id = $1)
                  AND delivery.event_row_id = operation.event_row_id
                  AND delivery.state = 'claimed'
                  AND delivery.claim_token = $3
                  AND delivery.claim_expires_at > operation.operation_time
                "#,
            )
            .bind(tenant_id.map(TenantId::as_str))
            .bind(row_id.get())
            .bind(token)
            .bind(delay.get())
            .execute(&mut **transaction)
            .await
        }
        Mutation::Quarantine { reason } => {
            query(
                r#"
                WITH locked AS MATERIALIZED (
                    SELECT event_row_id
                    FROM dovecote_deliveries
                    WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
                    FOR UPDATE
                ), operation AS MATERIALIZED (
                    SELECT locked.event_row_id, clock_timestamp() AS operation_time
                    FROM locked
                )
                UPDATE dovecote_deliveries AS delivery
                SET state = 'quarantined',
                    claim_token = NULL,
                    claimed_by = NULL,
                    claim_expires_at = NULL,
                    quarantined_at = operation.operation_time,
                    quarantine_reason = $4
                FROM operation
                WHERE ($1::varchar IS NULL OR delivery.tenant_id = $1)
                  AND delivery.event_row_id = operation.event_row_id
                  AND delivery.state = 'claimed'
                  AND delivery.claim_token = $3
                  AND delivery.claim_expires_at > operation.operation_time
                "#,
            )
            .bind(tenant_id.map(TenantId::as_str))
            .bind(row_id.get())
            .bind(token)
            .bind(reason.as_str())
            .execute(&mut **transaction)
            .await
        }
    };
    result
        .map(|result| result.rows_affected())
        .map_err(|source| MutationError::sql("execute conditional delivery mutation", source))
}

#[derive(Debug, FromRow)]
struct DeliveryForMutation {
    state: String,
    claim_token: Option<Vec<u8>>,
    claim_expires_at: Option<OffsetDateTime>,
    operation_time: OffsetDateTime,
}

async fn lock_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
) -> Result<DeliveryForMutation, MutationError> {
    query_as::<_, DeliveryForMutation>(
        r#"
        WITH locked AS MATERIALIZED (
            SELECT state, claim_token, claim_expires_at
            FROM dovecote_deliveries
            WHERE ($1::varchar IS NULL OR tenant_id = $1) AND event_row_id = $2
            FOR UPDATE
        ), operation AS MATERIALIZED (
            SELECT locked.*, clock_timestamp() AS operation_time
            FROM locked
        )
        SELECT state, claim_token, claim_expires_at, operation_time
        FROM operation
        "#,
    )
    .bind(tenant_id.map(TenantId::as_str))
    .bind(row_id.get())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| MutationError::sql("lock delivery for mutation", source))?
    .ok_or(MutationError::NotFound)
}

fn classify_delivery(
    delivery: &DeliveryForMutation,
    claim_token: &ClaimToken,
) -> Result<(), MutationError> {
    let state = parse_state(&delivery.state)?;
    if state != DeliveryState::Claimed {
        return Err(MutationError::IllegalTransition { state });
    }

    let stored_token = delivery
        .claim_token
        .as_deref()
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim token"))?;
    if stored_token.len() != dovecote::CLAIM_TOKEN_BYTES {
        return Err(MutationError::serialization(
            "claimed delivery has an invalid claim token width",
        ));
    }

    let expires_at = delivery
        .claim_expires_at
        .ok_or_else(|| MutationError::serialization("claimed delivery has no claim expiry"))?;
    if stored_token != claim_token.as_bytes() || expires_at <= delivery.operation_time {
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
