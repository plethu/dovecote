//! SQLite claims and token-fenced lifecycle mutations.
//!
//! SQLite has one writer at a time. Every write operation explicitly acquires
//! that writer slot with `BEGIN IMMEDIATE`, performs its short state change,
//! and commits before returning. No transport work occurs while the lock is
//! held.
//!
//! Claims intentionally scan the delivery relation only. Orphan events are
//! surfaced by live and snapshot paging for explicit reconciliation; claim
//! acquisition does not rescan all historical events on every write.

use crate::{
    BusyConfig, begin_immediate, checked_milliseconds, commit_transaction,
    enqueue::parse_timestamp,
    error::{ClaimError, MutationError},
    hydrate::{DurableRow, hydrate_event},
    validate_busy_config,
};
use dovecote::{
    AttemptCount, ClaimToken, ClaimedEvent, Delay, DeliveryState, Failure, Lease, Limit,
    QuarantineReason, RowId, WorkerId,
};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction, query, query_as, query_scalar};
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;

pub async fn claim(
    pool: &SqlitePool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    claim_with_config(pool, worker, lease_for, limit, BusyConfig::default()).await
}

pub(crate) async fn claim_with_config(
    pool: &SqlitePool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    validate_busy_config(busy).map_err(|detail| ClaimError::Configuration { detail })?;
    let mut entropy = OsEntropy;
    claim_with_entropy(pool, worker, lease_for, limit, busy, &mut entropy, None).await
}

async fn claim_with_entropy<E: EntropySource>(
    pool: &SqlitePool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
    entropy: &mut E,
    failpoint: Option<&AtomicBool>,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut tries = 0;
    loop {
        match claim_once(pool, &worker, lease_for, limit, busy, entropy, failpoint).await {
            Err(error) if error.busy_source().is_some() && tries < busy.retries() => {
                tries += 1;
                continue;
            }
            Err(error) if error.busy_source().is_some() => return Err(error.into_busy_exhausted()),
            result => return result,
        }
    }
}

async fn claim_once(
    pool: &SqlitePool,
    worker: &WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
    entropy: &mut impl EntropySource,
    _failpoint: Option<&AtomicBool>,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let lease_ms = checked_milliseconds(lease_for.get()).map_err(ClaimError::serialization)?;
    let mut transaction = begin_immediate(pool, busy, "claim")
        .await
        .map_err(|source| ClaimError::sql("begin immediate claim transaction", source))?;
    let operation_time = match database_time(&mut transaction).await {
        Ok(value) => value,
        Err(source) => {
            return rollback_claim(
                transaction,
                ClaimError::sql("read claim operation time", source),
            )
            .await;
        }
    };
    let candidates = match query_as::<_, DurableRow>(CLAIM_SQL)
        .bind(&operation_time)
        .bind(&operation_time)
        .bind(i64::from(limit.get()))
        .fetch_all(&mut *transaction)
        .await
    {
        Ok(value) => value,
        Err(source) => {
            return rollback_claim(
                transaction,
                ClaimError::sql("select claim candidates", source),
            )
            .await;
        }
    };

    let mut used_tokens = Vec::with_capacity(candidates.len());
    let mut prepared = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let row_id = match RowId::new(candidate.row_id) {
            Ok(value) => value,
            Err(error) => {
                return rollback_claim(transaction, ClaimError::serialization(error.to_string()))
                    .await;
            }
        };
        let attempts = match candidate
            .attempts
            .ok_or_else(|| ClaimError::MigrationMismatch {
                detail: "eligible delivery row has no attempts".to_owned(),
            })?
            .checked_add(1)
        {
            Some(value) => value,
            None => {
                return rollback_claim(transaction, ClaimError::CounterOverflow { row_id }).await;
            }
        };
        let attempts = match AttemptCount::new(attempts) {
            Ok(value) => value,
            Err(error) => {
                return rollback_claim(transaction, ClaimError::serialization(error.to_string()))
                    .await;
            }
        };
        let token = match fresh_token(candidate.claim_token.as_deref(), &used_tokens, entropy) {
            Ok(value) => value,
            Err(source) => {
                return rollback_claim(transaction, ClaimError::EntropyUnavailable { source })
                    .await;
            }
        };
        used_tokens.push(token);
        let event = match hydrate_event(&candidate) {
            Ok(value) => value,
            Err(error) => {
                return rollback_claim(transaction, ClaimError::serialization(error)).await;
            }
        };
        prepared.push((row_id, candidate.row_id, event, attempts, token));
    }

    let mut claimed = Vec::with_capacity(prepared.len());
    for (row_id, event_row_id, event, attempts, token) in prepared {
        let expiry = match query_scalar::<_, String>(
            "UPDATE dovecote_deliveries SET state = 'claimed', attempts = ?, claim_token = ?, claimed_by = ?, claim_expires_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)) WHERE event_row_id = ? AND (state = 'pending' OR state = 'claimed') RETURNING claim_expires_at",
        )
        .bind(attempts.get()).bind(token.as_slice()).bind(worker.as_str())
        .bind(&operation_time).bind(lease_ms).bind(lease_ms).bind(event_row_id)
        .fetch_optional(&mut *transaction).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return rollback_claim(
                    transaction,
                    ClaimError::sql(
                        "update claimed delivery",
                        sqlx::Error::Protocol(
                            "claim candidate disappeared while writer lock was held".to_owned(),
                        ),
                    ),
                )
                .await;
            }
            Err(source) => {
                return rollback_claim(
                    transaction,
                    ClaimError::sql("update claimed delivery", source),
                )
                .await;
            }
        };
        let expiry = match parse_timestamp(&expiry) {
            Ok(value) => value,
            Err(error) => {
                return rollback_claim(transaction, ClaimError::serialization(error)).await;
            }
        };
        let claimed_event = match ClaimedEvent::new(
            row_id,
            event,
            attempts,
            ClaimToken::from_bytes(token),
            worker.clone(),
            expiry,
        ) {
            Ok(value) => value,
            Err(error) => {
                return rollback_claim(transaction, ClaimError::serialization(error.to_string()))
                    .await;
            }
        };
        claimed.push(claimed_event);
    }
    #[cfg(test)]
    if _failpoint.is_some_and(|failpoint| failpoint.swap(false, Ordering::AcqRel)) {
        return rollback_claim(transaction, ClaimError::InjectedFailure).await;
    }
    commit_transaction(transaction)
        .await
        .map_err(|source| ClaimError::sql("commit claim transaction", source))?;
    Ok(claimed)
}

async fn rollback_claim<T>(
    transaction: Transaction<'static, Sqlite>,
    error: ClaimError,
) -> Result<T, ClaimError> {
    let _ = transaction.rollback().await;
    Err(error)
}

pub async fn renew(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    lease_for: Lease,
) -> Result<(), MutationError> {
    renew_with_config(pool, row_id, claim_token, lease_for, BusyConfig::default()).await
}
pub(crate) async fn renew_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    lease_for: Lease,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        row_id,
        claim_token,
        Mutation::Renew { lease_for },
        busy,
    )
    .await
}
pub async fn ack(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
) -> Result<(), MutationError> {
    ack_with_config(pool, row_id, claim_token, BusyConfig::default()).await
}
pub(crate) async fn ack_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(pool, row_id, claim_token, Mutation::Ack, busy).await
}
pub async fn retry(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    failure: &Failure,
    backoff: Delay,
) -> Result<(), MutationError> {
    retry_with_config(
        pool,
        row_id,
        claim_token,
        failure,
        backoff,
        BusyConfig::default(),
    )
    .await
}
pub(crate) async fn retry_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    failure: &Failure,
    backoff: Delay,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        row_id,
        claim_token,
        Mutation::Retry { failure, backoff },
        busy,
    )
    .await
}
pub async fn release(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    delay: Delay,
) -> Result<(), MutationError> {
    release_with_config(pool, row_id, claim_token, delay, BusyConfig::default()).await
}
pub(crate) async fn release_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    delay: Delay,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(pool, row_id, claim_token, Mutation::Release { delay }, busy).await
}
pub async fn quarantine(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    reason: &QuarantineReason,
) -> Result<(), MutationError> {
    quarantine_with_config(pool, row_id, claim_token, reason, BusyConfig::default()).await
}
pub(crate) async fn quarantine_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    reason: &QuarantineReason,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    mutate_with_config(
        pool,
        row_id,
        claim_token,
        Mutation::Quarantine { reason },
        busy,
    )
    .await
}

async fn mutate_with_config(
    pool: &SqlitePool,
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    validate_busy_config(busy).map_err(|detail| MutationError::Configuration { detail })?;
    let mut tries = 0;
    loop {
        match mutate_once(pool, row_id, claim_token, mutation, busy).await {
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
    row_id: RowId,
    claim_token: &ClaimToken,
    mutation: Mutation<'_>,
    busy: BusyConfig,
) -> Result<(), MutationError> {
    let (duration_ms, failure_code, failure_detail, quarantine_reason) = match mutation {
        Mutation::Renew { lease_for } => (
            Some(checked_milliseconds(lease_for.get()).map_err(MutationError::serialization)?),
            None,
            None,
            None,
        ),
        Mutation::Ack => (None, None, None, None),
        Mutation::Retry { failure, backoff } => (
            Some(checked_milliseconds(backoff.get()).map_err(MutationError::serialization)?),
            Some(failure.code()),
            Some(failure.detail()),
            None,
        ),
        Mutation::Release { delay } => (
            Some(checked_milliseconds(delay.get()).map_err(MutationError::serialization)?),
            None,
            None,
            None,
        ),
        Mutation::Quarantine { reason } => (None, None, None, Some(reason.as_str())),
    };

    let mut transaction = begin_immediate(pool, busy, "mutation")
        .await
        .map_err(|source| MutationError::sql("begin immediate mutation transaction", source))?;
    let operation_time = match database_time(&mut transaction).await {
        Ok(value) => value,
        Err(source) => {
            return rollback_mutation(
                transaction,
                MutationError::sql("read mutation operation time", source),
            )
            .await;
        }
    };
    let changed = match match mutation {
        Mutation::Renew { .. } => query("UPDATE dovecote_deliveries SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)) WHERE event_row_id = ? AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
            .bind(&operation_time).bind(duration_ms.expect("renew duration")).bind(duration_ms.expect("renew duration")).bind(row_id.get()).bind(claim_token.as_bytes().as_slice()).bind(&operation_time)
            .execute(&mut *transaction).await,
        Mutation::Ack => query("UPDATE dovecote_deliveries SET state = 'delivered', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, delivered_at = ? WHERE event_row_id = ? AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
            .bind(&operation_time).bind(row_id.get()).bind(claim_token.as_bytes().as_slice()).bind(&operation_time)
            .execute(&mut *transaction).await,
        Mutation::Retry { .. } => query("UPDATE dovecote_deliveries SET state = 'pending', available_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, last_failure_code = ?, last_failure_detail = ? WHERE event_row_id = ? AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
            .bind(&operation_time).bind(duration_ms.expect("retry duration")).bind(duration_ms.expect("retry duration")).bind(failure_code.expect("retry code")).bind(failure_detail.expect("retry detail")).bind(row_id.get()).bind(claim_token.as_bytes().as_slice()).bind(&operation_time)
            .execute(&mut *transaction).await,
        Mutation::Release { .. } => query("UPDATE dovecote_deliveries SET state = 'pending', available_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE event_row_id = ? AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
            .bind(&operation_time).bind(duration_ms.expect("release duration")).bind(duration_ms.expect("release duration")).bind(row_id.get()).bind(claim_token.as_bytes().as_slice()).bind(&operation_time)
            .execute(&mut *transaction).await,
        Mutation::Quarantine { .. } => query("UPDATE dovecote_deliveries SET state = 'quarantined', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, quarantined_at = ?, quarantine_reason = ? WHERE event_row_id = ? AND state = 'claimed' AND claim_token = ? AND claim_expires_at > ?")
            .bind(&operation_time).bind(quarantine_reason.expect("quarantine reason")).bind(row_id.get()).bind(claim_token.as_bytes().as_slice()).bind(&operation_time)
            .execute(&mut *transaction).await,
    } {
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
        let delivery = match query_as::<_, DeliveryForMutation>("SELECT state, claim_token, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get()).fetch_optional(&mut *transaction).await
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

async fn database_time(transaction: &mut Transaction<'_, Sqlite>) -> Result<String, sqlx::Error> {
    query_scalar("SELECT strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')")
        .fetch_one(&mut **transaction)
        .await
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

fn fresh_token(
    previous: Option<&[u8]>,
    used: &[[u8; 16]],
    entropy: &mut impl EntropySource,
) -> Result<[u8; 16], getrandom::Error> {
    loop {
        let mut token = [0_u8; 16];
        entropy.fill(&mut token)?;
        if previous != Some(token.as_slice()) && used.iter().all(|value| value != &token) {
            return Ok(token);
        }
    }
}

trait EntropySource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error>;
}

struct OsEntropy;

impl EntropySource for OsEntropy {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error> {
        getrandom::fill(output)
    }
}

#[derive(Debug, FromRow)]
struct DeliveryForMutation {
    state: String,
    claim_token: Option<Vec<u8>>,
    claim_expires_at: Option<String>,
}

#[derive(Clone, Copy)]
enum Mutation<'a> {
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

const CLAIM_SQL: &str = "SELECT e.row_id, e.stream, e.specversion, e.event_id, e.source, e.event_type, e.subject, e.occurred_at, e.enqueued_at, e.datacontenttype, e.dataschema, e.partitionkey, e.extensions, e.data_kind, e.data, d.state, d.available_at, d.attempts, d.claim_token, d.claimed_by, d.claim_expires_at, d.last_failure_code, d.last_failure_detail, d.delivered_at, d.quarantined_at, d.quarantine_reason FROM dovecote_deliveries AS d JOIN dovecote_events AS e ON e.row_id = d.event_row_id WHERE (d.state = 'pending' AND d.available_at <= ?) OR (d.state = 'claimed' AND d.claim_expires_at <= ?) ORDER BY d.event_row_id ASC LIMIT ?";

#[cfg(test)]
mod tests {
    use super::{EntropySource, claim_with_config, claim_with_entropy, fresh_token};
    use crate::{MIGRATIONS, SqliteDovecote, check_schema, enqueue};
    use dovecote::{EventId, EventSource, EventType, Limit, NewEvent, StreamName, WorkerId};
    use sqlx::{SqlitePool, raw_sql, sqlite::SqlitePoolOptions};
    use std::error::Error;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    struct FailsEntropy;

    impl EntropySource for FailsEntropy {
        fn fill(&mut self, _output: &mut [u8]) -> Result<(), getrandom::Error> {
            Err(getrandom::Error::new_custom(1))
        }
    }

    fn test_event(id: &str) -> NewEvent {
        NewEvent::new(
            StreamName::new("audit").expect("valid stream"),
            EventId::new(id).expect("valid ID"),
            EventSource::new("https://example.test/source").expect("valid source"),
            EventType::new("com.example.entropy").expect("valid type"),
        )
        .expect("valid event")
    }

    async fn test_pool() -> Result<SqlitePool, Box<dyn Error>> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        raw_sql(MIGRATIONS[0].sql()).execute(&pool).await?;
        check_schema(&pool).await?;
        Ok(pool)
    }

    #[test]
    fn generated_tokens_are_distinct_from_previous_and_batch_values() {
        let mut entropy = super::OsEntropy;
        let first = fresh_token(None, &[], &mut entropy).expect("OS entropy available");
        let second =
            fresh_token(Some(&first), &[first], &mut entropy).expect("OS entropy available");
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn injected_entropy_failure_rolls_back_the_whole_claim_batch()
    -> Result<(), Box<dyn Error>> {
        let pool = test_pool().await?;
        let adapter = SqliteDovecote::new(pool.clone());
        let mut transaction = adapter.begin_write().await?;
        enqueue(&mut transaction, test_event("entropy-first")).await?;
        enqueue(&mut transaction, test_event("entropy-second")).await?;
        transaction.commit().await?;

        let mut entropy = FailsEntropy;
        let claim = claim_with_entropy(
            &pool,
            WorkerId::new("entropy-worker")?,
            dovecote::Lease::new(Duration::from_secs(5))?,
            Limit::new(2)?,
            crate::BusyConfig::default(),
            &mut entropy,
            None,
        )
        .await;
        assert!(matches!(
            claim,
            Err(crate::ClaimError::EntropyUnavailable { .. })
        ));
        let states = sqlx::query_as::<_, (String, i64, Option<Vec<u8>>, Option<String>)>(
            "SELECT state, attempts, claim_token, claim_expires_at FROM dovecote_deliveries ORDER BY event_row_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(states.len(), 2);
        assert!(states.iter().all(|(state, attempts, token, expiry)| {
            state == "pending" && *attempts == 0 && token.is_none() && expiry.is_none()
        }));
        Ok(())
    }

    #[tokio::test]
    async fn injected_failure_after_claim_updates_rolls_back_the_whole_claim_batch()
    -> Result<(), Box<dyn Error>> {
        let pool = test_pool().await?;
        let adapter = SqliteDovecote::new(pool.clone());
        let mut transaction = adapter.begin_write().await?;
        enqueue(&mut transaction, test_event("failpoint-first")).await?;
        enqueue(&mut transaction, test_event("failpoint-second")).await?;
        transaction.commit().await?;

        let before = sqlx::query_as::<_, (String, i64, Option<Vec<u8>>, Option<String>, Option<String>)>(
            "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries ORDER BY event_row_id",
        )
        .fetch_all(&pool)
        .await?;
        let failpoint = AtomicBool::new(true);
        let mut entropy = super::OsEntropy;
        let claim = claim_with_entropy(
            &pool,
            WorkerId::new("failpoint-worker")?,
            dovecote::Lease::new(Duration::from_secs(5))?,
            Limit::new(2)?,
            crate::BusyConfig::default(),
            &mut entropy,
            Some(&failpoint),
        )
        .await;
        assert!(matches!(claim, Err(crate::ClaimError::InjectedFailure)));
        let after = sqlx::query_as::<_, (String, i64, Option<Vec<u8>>, Option<String>, Option<String>)>(
            "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries ORDER BY event_row_id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(after, before);

        let recovered = claim_with_config(
            &pool,
            WorkerId::new("after-failpoint")?,
            dovecote::Lease::new(Duration::from_secs(5))?,
            Limit::new(2)?,
            crate::BusyConfig::default(),
        )
        .await?;
        assert_eq!(recovered.len(), 2);
        assert!(recovered.iter().all(|event| event.attempts().get() == 1));
        Ok(())
    }
}
