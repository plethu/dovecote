//! PostgreSQL claims and claim-token-fenced delivery mutations.
//!
//! A claim transaction only selects and updates durable state.  In particular,
//! no transport or caller code is run while its locks are held.  The returned
//! events are assembled before commit but are handed to the caller only after
//! the claim transaction has committed.

use crate::{
    error::{ClaimError, MutationError},
    hydrate::{EventRow, hydrate_event},
    lifecycle_mutation::{Mutation, mutate},
    rls,
};
use dovecote::{
    AttemptCount, ClaimToken, ClaimedEvent, Delay, Failure, Lease, Limit, QuarantineReason, RowId,
    TenantId, WorkerId,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction, query_as, query_scalar};
use time::OffsetDateTime;

pub(crate) async fn claim_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut entropy = OsEntropy;
    claim_with_entropy(pool, tenant_id, worker, lease_for, limit, &mut entropy).await
}

async fn claim_with_entropy<E: EntropySource>(
    pool: &PgPool,
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
    if let Some(tenant_id) = tenant_id {
        rls::bind_tenant(&mut transaction, tenant_id)
            .await
            .map_err(|source| ClaimError::sql("bind claim tenant", source))?;
    }

    let operation_time = database_time(&mut transaction)
        .await
        .map_err(|source| ClaimError::sql("read claim operation time", source))?;

    let candidates = query_as::<_, ClaimCandidate>(
        r#"
        SELECT d.event_row_id,
               d.tenant_id,
               d.state,
               d.attempts,
               d.claim_token,
               e.stream,
               e.specversion,
               e.event_id,
               e.source,
               e.event_type,
               e.subject,
               e.occurred_at,
               e.datacontenttype,
               e.dataschema,
               e.partitionkey,
               e.extensions,
               e.data_kind,
               e.data
        FROM dovecote_deliveries AS d
        JOIN dovecote_events AS e
          ON e.tenant_id = d.tenant_id AND e.row_id = d.event_row_id
        WHERE ($1::varchar IS NULL OR d.tenant_id = $1)
          AND ((d.state = 'pending' AND d.available_at <= $2)
           OR (d.state = 'claimed' AND d.claim_expires_at <= $2))
        ORDER BY d.event_row_id ASC
        LIMIT $3
        FOR UPDATE OF d SKIP LOCKED
        "#,
    )
    .bind(tenant_id.map(TenantId::as_str))
    .bind(operation_time)
    .bind(i64::from(limit.get()))
    .fetch_all(&mut *transaction)
    .await
    .map_err(|source| ClaimError::sql("select claim candidates", source))?;

    // Generate all tokens before touching a delivery row.  Thus an entropy
    // failure rolls back an entirely unchanged batch, including attempts.
    let mut used_tokens = Vec::with_capacity(candidates.len());
    let mut prepared = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let row_id = RowId::new(candidate.event_row_id)
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
        let attempts = candidate
            .attempts
            .checked_add(1)
            .ok_or(ClaimError::CounterOverflow { row_id })?;
        let attempts = AttemptCount::new(attempts)
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
        let token = fresh_token(candidate.claim_token.as_deref(), &used_tokens, entropy)
            .map_err(|source| ClaimError::EntropyUnavailable { source })?;
        used_tokens.push(token);
        if candidate.state != "pending" && candidate.state != "claimed" {
            return Err(ClaimError::serialization(
                "claim candidate has an ineligible state",
            ));
        }

        let event = hydrate_event(&candidate.event_row()).map_err(ClaimError::serialization)?;
        let tenant_id = TenantId::new(candidate.tenant_id.clone())
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
        prepared.push((
            tenant_id,
            row_id,
            candidate.event_row_id,
            event,
            attempts,
            token,
        ));
    }

    let mut claimed = Vec::with_capacity(prepared.len());
    for (tenant_id, row_id, event_row_id, event, attempts, token) in prepared {
        let expiry = query_scalar::<_, OffsetDateTime>(
            r#"
            UPDATE dovecote_deliveries
            SET state = 'claimed',
                attempts = $3,
                claim_token = $4,
                claimed_by = $5,
                claim_expires_at = $6 + $7
            WHERE tenant_id = $1 AND event_row_id = $2
              AND (state = 'pending' OR state = 'claimed')
            RETURNING claim_expires_at
            "#,
        )
        .bind(tenant_id.as_str())
        .bind(event_row_id)
        .bind(attempts.get())
        .bind(token.as_slice())
        .bind(worker.as_str())
        .bind(operation_time)
        .bind(lease_for.get())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| ClaimError::sql("update claimed delivery", source))?
        .ok_or_else(|| {
            ClaimError::sql(
                "update claimed delivery",
                sqlx::Error::Protocol("claim candidate disappeared while locked".to_owned()),
            )
        })?;

        let claimed_event = ClaimedEvent::new(
            tenant_id,
            row_id,
            event,
            attempts,
            ClaimToken::from_bytes(token),
            worker.clone(),
            expiry,
        )
        .map_err(|error| ClaimError::serialization(error.to_string()))?;
        claimed.push(claimed_event);
    }

    transaction
        .commit()
        .await
        .map_err(|source| ClaimError::sql("commit claim transaction", source))?;
    Ok(claimed)
}

pub(crate) async fn renew_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    lease_for: Lease,
) -> Result<(), MutationError> {
    mutate(
        pool,
        tenant_id,
        row_id,
        claim_token,
        Mutation::Renew { lease_for },
    )
    .await
}

pub(crate) async fn ack_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
) -> Result<(), MutationError> {
    mutate(pool, tenant_id, row_id, claim_token, Mutation::Ack).await
}

pub(crate) async fn retry_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    failure: &Failure,
    backoff: Delay,
) -> Result<(), MutationError> {
    mutate(
        pool,
        tenant_id,
        row_id,
        claim_token,
        Mutation::Retry { failure, backoff },
    )
    .await
}

pub(crate) async fn release_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    delay: Delay,
) -> Result<(), MutationError> {
    mutate(
        pool,
        tenant_id,
        row_id,
        claim_token,
        Mutation::Release { delay },
    )
    .await
}

pub(crate) async fn quarantine_for_scope(
    pool: &PgPool,
    tenant_id: Option<&TenantId>,
    row_id: RowId,
    claim_token: &ClaimToken,
    reason: &QuarantineReason,
) -> Result<(), MutationError> {
    mutate(
        pool,
        tenant_id,
        row_id,
        claim_token,
        Mutation::Quarantine { reason },
    )
    .await
}

async fn database_time(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<OffsetDateTime, sqlx::Error> {
    // `CURRENT_TIMESTAMP` is PostgreSQL's transaction-start timestamp.  The
    // lifecycle contract needs the instant at which this operation reaches
    // the database, especially after waiting for a row lock, so use the
    // database clock that advances during a transaction.
    query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await
}

fn fresh_token(
    previous: Option<&[u8]>,
    used_tokens: &[[u8; dovecote::CLAIM_TOKEN_BYTES]],
    entropy: &mut impl EntropySource,
) -> Result<[u8; dovecote::CLAIM_TOKEN_BYTES], getrandom::Error> {
    loop {
        let mut token = [0_u8; dovecote::CLAIM_TOKEN_BYTES];
        entropy.fill(&mut token)?;
        let differs_from_previous = previous != Some(token.as_slice());
        let unique_in_batch = used_tokens.iter().all(|used| used != &token);
        if differs_from_previous && unique_in_batch {
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
struct ClaimCandidate {
    event_row_id: i64,
    tenant_id: String,
    state: String,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    stream: String,
    specversion: String,
    event_id: String,
    source: String,
    event_type: String,
    subject: Option<String>,
    occurred_at: Option<OffsetDateTime>,
    datacontenttype: Option<String>,
    dataschema: Option<String>,
    partitionkey: Option<String>,
    extensions: String,
    data_kind: Option<String>,
    data: Option<Vec<u8>>,
}

impl ClaimCandidate {
    fn event_row(&self) -> EventRow {
        EventRow {
            stream: self.stream.clone(),
            specversion: self.specversion.clone(),
            event_id: self.event_id.clone(),
            source: self.source.clone(),
            event_type: self.event_type.clone(),
            subject: self.subject.clone(),
            occurred_at: self.occurred_at,
            datacontenttype: self.datacontenttype.clone(),
            dataschema: self.dataschema.clone(),
            partitionkey: self.partitionkey.clone(),
            extensions: self.extensions.clone(),
            data_kind: self.data_kind.clone(),
            data: self.data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EntropySource, OsEntropy, claim_with_entropy, fresh_token};
    use crate::{ClaimError, MIGRATIONS, check_schema, enqueue::enqueue_for_scope};
    use dovecote::{
        EventId, EventSource, EventType, Limit, NewEvent, StreamName, TenantId, WorkerId,
    };
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        query, query_as, raw_sql,
    };
    use std::{
        error::Error,
        str::FromStr,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn generated_tokens_are_distinct_from_previous_and_batch_values() {
        let mut entropy = OsEntropy;
        let first = fresh_token(None, &[], &mut entropy).expect("OS entropy available");
        let second =
            fresh_token(Some(&first), &[first], &mut entropy).expect("OS entropy available");
        assert_ne!(first, second);
    }

    struct FailsEntropy;

    impl EntropySource for FailsEntropy {
        fn fill(&mut self, _output: &mut [u8]) -> Result<(), getrandom::Error> {
            Err(getrandom::Error::new_custom(1))
        }
    }

    fn entropy_event(id: &str) -> NewEvent {
        NewEvent::new(
            StreamName::new("audit").expect("valid stream"),
            EventId::new(id).expect("valid id"),
            EventSource::new("https://example.test/source").expect("valid source"),
            EventType::new("com.example.entropy").expect("valid event type"),
        )
        .expect("valid event")
    }

    #[test]
    fn entropy_failure_is_returned_before_a_token_is_accepted() {
        let mut entropy = FailsEntropy;
        let error = fresh_token(None, &[], &mut entropy).expect_err("injected failure");
        assert_eq!(error.raw_os_error(), None);
    }

    #[tokio::test]
    async fn injected_entropy_failure_leaves_the_claim_batch_unchanged_when_configured()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = std::env::var("DOVECOTE_POSTGRES_URL") else {
            return Ok(());
        };

        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let schema = format!("dovecote_entropy_test_{}_{}", std::process::id(), suffix);
        query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await?;
        let result = async {
            let options = PgConnectOptions::from_str(&url)?.options([
                ("search_path", format!("\"{schema}\"")),
            ]);
            let pool = PgPoolOptions::new()
                .max_connections(3)
                .connect_with(options)
                .await?;
            raw_sql(MIGRATIONS[0].sql()).execute(&pool).await?;
            check_schema(&pool).await?;

            let mut transaction = pool.begin().await?;
            let tenant = TenantId::new("entropy-test")?;
            enqueue_for_scope(&mut transaction, &tenant, entropy_event("entropy-first")).await?;
            enqueue_for_scope(&mut transaction, &tenant, entropy_event("entropy-second")).await?;
            transaction.commit().await?;

            let mut entropy = FailsEntropy;
            let claim = claim_with_entropy(
                &pool,
                None,
                WorkerId::new("entropy-worker")?,
                dovecote::Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(2)?,
                &mut entropy,
            )
            .await;
            assert!(matches!(claim, Err(ClaimError::EntropyUnavailable { .. })));
            let snapshots = query_as::<_, (String, i64, Option<Vec<u8>>, Option<time::OffsetDateTime>)>(
                "SELECT state, attempts, claim_token, claim_expires_at FROM dovecote_deliveries ORDER BY event_row_id",
            )
            .fetch_all(&pool)
            .await?;
            assert_eq!(snapshots.len(), 2);
            assert!(snapshots
                .iter()
                .all(|(state, attempts, token, expiry)| state == "pending"
                    && *attempts == 0
                    && token.is_none()
                    && expiry.is_none()));
            pool.close().await;
            Ok::<_, Box<dyn Error>>(())
        }
        .await;
        query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&admin)
        .await?;
        admin.close().await;
        result
    }
}
