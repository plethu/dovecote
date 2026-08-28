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
    error::ClaimError,
    hydrate::{DurableRow, hydrate_event},
    validate_busy_config,
};
use dovecote::{AttemptCount, ClaimToken, ClaimedEvent, Lease, Limit, RowId, TenantId, WorkerId};
use sqlx::{Sqlite, SqlitePool, Transaction, query_as, query_scalar};
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;

#[cfg(test)]
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

pub(crate) async fn claim_for_scope(
    pool: &SqlitePool,
    tenant_id: Option<&TenantId>,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    validate_busy_config(busy).map_err(|detail| ClaimError::Configuration { detail })?;
    let mut entropy = OsEntropy;
    claim_with_entropy_scoped(
        pool,
        tenant_id,
        worker,
        lease_for,
        limit,
        busy,
        &mut entropy,
        None,
    )
    .await
}

#[cfg(test)]
async fn claim_with_entropy<E: EntropySource>(
    pool: &SqlitePool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
    entropy: &mut E,
    failpoint: Option<&AtomicBool>,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    claim_with_entropy_scoped(
        pool, None, worker, lease_for, limit, busy, entropy, failpoint,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn claim_with_entropy_scoped<E: EntropySource>(
    pool: &SqlitePool,
    tenant_id: Option<&TenantId>,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
    busy: BusyConfig,
    entropy: &mut E,
    failpoint: Option<&AtomicBool>,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut tries = 0;
    loop {
        match claim_once(
            pool, tenant_id, &worker, lease_for, limit, busy, entropy, failpoint,
        )
        .await
        {
            Err(error) if error.busy_source().is_some() && tries < busy.retries() => {
                tries += 1;
                continue;
            }
            Err(error) if error.busy_source().is_some() => return Err(error.into_busy_exhausted()),
            result => return result,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn claim_once(
    pool: &SqlitePool,
    tenant_id: Option<&TenantId>,
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
    let candidates = match tenant_id {
        Some(tenant_id) => {
            query_as::<_, DurableRow>(SCOPED_CLAIM_SQL)
                .bind(tenant_id.as_str())
                .bind(&operation_time)
                .bind(&operation_time)
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *transaction)
                .await
        }
        None => {
            query_as::<_, DurableRow>(CLAIM_SQL)
                .bind(&operation_time)
                .bind(&operation_time)
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *transaction)
                .await
        }
    };
    let candidates = match candidates {
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
        let tenant_id = TenantId::new(candidate.tenant_id.clone())
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
        prepared.push((row_id, candidate.row_id, tenant_id, event, attempts, token));
    }

    let mut claimed = Vec::with_capacity(prepared.len());
    for (row_id, event_row_id, tenant_id, event, attempts, token) in prepared {
        let expiry = match query_scalar::<_, String>(
            "UPDATE dovecote_deliveries SET state = 'claimed', attempts = ?, claim_token = ?, claimed_by = ?, claim_expires_at = strftime('%Y-%m-%dT%H:%M:%f000Z', ?, printf('+%lld.%03lld seconds', ? / 1000, ? % 1000)) WHERE tenant_id = ? AND event_row_id = ? AND (state = 'pending' OR state = 'claimed') RETURNING claim_expires_at",
        )
        .bind(attempts.get()).bind(token.as_slice()).bind(worker.as_str())
        .bind(&operation_time).bind(lease_ms).bind(lease_ms).bind(tenant_id.as_str()).bind(event_row_id)
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
            tenant_id,
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

pub(crate) async fn database_time(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<String, sqlx::Error> {
    query_scalar("SELECT strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')")
        .fetch_one(&mut **transaction)
        .await
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

const CLAIM_SQL: &str = "SELECT e.row_id, e.tenant_id, e.stream, e.specversion, e.event_id, e.source, e.event_type, e.subject, e.occurred_at, e.enqueued_at, e.datacontenttype, e.dataschema, e.partitionkey, e.extensions, e.data_kind, e.data, d.state, d.available_at, d.attempts, d.claim_token, d.claimed_by, d.claim_expires_at, d.last_failure_code, d.last_failure_detail, d.delivered_at, d.quarantined_at, d.quarantine_reason FROM dovecote_deliveries AS d JOIN dovecote_events AS e ON e.tenant_id = d.tenant_id AND e.row_id = d.event_row_id WHERE (d.state = 'pending' AND d.available_at <= ?) OR (d.state = 'claimed' AND d.claim_expires_at <= ?) ORDER BY d.event_row_id ASC LIMIT ?";
const SCOPED_CLAIM_SQL: &str = "SELECT e.row_id, e.tenant_id, e.stream, e.specversion, e.event_id, e.source, e.event_type, e.subject, e.occurred_at, e.enqueued_at, e.datacontenttype, e.dataschema, e.partitionkey, e.extensions, e.data_kind, e.data, d.state, d.available_at, d.attempts, d.claim_token, d.claimed_by, d.claim_expires_at, d.last_failure_code, d.last_failure_detail, d.delivered_at, d.quarantined_at, d.quarantine_reason FROM dovecote_deliveries AS d JOIN dovecote_events AS e ON e.tenant_id = d.tenant_id AND e.row_id = d.event_row_id WHERE e.tenant_id = ? AND ((d.state = 'pending' AND d.available_at <= ?) OR (d.state = 'claimed' AND d.claim_expires_at <= ?)) ORDER BY d.event_row_id ASC LIMIT ?";

#[cfg(test)]
mod tests {
    use super::{EntropySource, claim_with_config, claim_with_entropy, fresh_token};
    use crate::{MIGRATIONS, SqliteDovecote, check_schema, enqueue::enqueue_for_scope};
    use dovecote::{
        EventId, EventSource, EventType, Limit, NewEvent, StreamName, TenantId, WorkerId,
    };
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
        let tenant = TenantId::new("test")?;
        enqueue_for_scope(&mut transaction, &tenant, test_event("entropy-first")).await?;
        enqueue_for_scope(&mut transaction, &tenant, test_event("entropy-second")).await?;
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
        let tenant = TenantId::new("test")?;
        enqueue_for_scope(&mut transaction, &tenant, test_event("failpoint-first")).await?;
        enqueue_for_scope(&mut transaction, &tenant, test_event("failpoint-second")).await?;
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
