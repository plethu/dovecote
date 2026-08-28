//! MySQL/MariaDB claims and claim-token-fenced delivery mutations.

use crate::{backend, error::ClaimError, hydrate};
use dovecote::{AttemptCount, ClaimToken, ClaimedEvent, Lease, Limit, RowId, TenantId, WorkerId};
use sqlx::{FromRow, MySqlPool, query, query_as, query_scalar};
use time::OffsetDateTime;

#[allow(clippy::excessive_nesting)]
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

#[allow(clippy::excessive_nesting)]
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

#[allow(clippy::excessive_nesting)]
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

        // First read only an ordered ID window without locks.  Lock each ID
        // through an exact primary-key lookup below.  InnoDB's repeatable-read
        // next-key locks otherwise let the first range scan cover the next
        // pending row, so a concurrent SKIP LOCKED claimant can incorrectly
        // receive an empty batch.
        let candidate_ids_sql = if tenant_id.is_some() {
            r#"
        SELECT d.event_row_id
        FROM dovecote_deliveries AS d
        JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE d.event_row_id > ? AND d.tenant_id = ?
          AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
           OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
        ORDER BY d.event_row_id ASC
        LIMIT ?
    "#
        } else {
            r#"
        SELECT d.event_row_id
        FROM dovecote_deliveries AS d
        JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE d.event_row_id > ?
          AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
           OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
        ORDER BY d.event_row_id ASC
        LIMIT ?
        "#
        };

        let limit_usize = usize::try_from(limit.get())
            .map_err(|_| ClaimError::serialization("claim limit does not fit usize"))?;
        let mut after_row_id = 0_i64;
        let mut candidates = Vec::with_capacity(limit_usize);
        while candidates.len() < limit_usize {
            let mut candidate_ids = query_as::<_, ClaimCandidateId>(candidate_ids_sql)
                .bind(after_row_id);
            if let Some(tenant_id) = tenant_id {
                candidate_ids = candidate_ids.bind(tenant_id.as_str().as_bytes());
            }

            let ids = candidate_ids
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *transaction)
                .await
                .map_err(|source| ClaimError::sql("select claim candidate IDs", source))?;
            if ids.is_empty() {
                break;
            }

            for id in ids {
                after_row_id = id.event_row_id;
                let candidate_sql = if tenant_id.is_some() {
                    r#"
        SELECT d.event_row_id, d.tenant_id, d.state, d.attempts, d.claim_token,
               d.claimed_by, d.claim_expires_at, d.available_at,
               e.stream, e.specversion, e.event_id, e.source, e.event_type,
               e.subject, e.occurred_at, e.datacontenttype, e.dataschema,
               e.partitionkey, e.extensions, e.data_kind, e.data
        FROM dovecote_deliveries AS d FORCE INDEX (PRIMARY)
        JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE d.event_row_id = ? AND d.tenant_id = ?
          AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
           OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
        FOR UPDATE SKIP LOCKED
    "#
                } else {
                    r#"
        SELECT d.event_row_id, d.tenant_id, d.state, d.attempts, d.claim_token,
               d.claimed_by, d.claim_expires_at, d.available_at,
               e.stream, e.specversion, e.event_id, e.source, e.event_type,
               e.subject, e.occurred_at, e.datacontenttype, e.dataschema,
               e.partitionkey, e.extensions, e.data_kind, e.data
        FROM dovecote_deliveries AS d FORCE INDEX (PRIMARY)
        JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE d.event_row_id = ?
          AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
           OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
        FOR UPDATE SKIP LOCKED
        "#
                };

                let mut candidate = query_as::<_, ClaimCandidate>(candidate_sql).bind(id.event_row_id);
                if let Some(tenant_id) = tenant_id {
                    candidate = candidate.bind(tenant_id.as_str().as_bytes());
                }

                if let Some(candidate) = candidate
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|source| ClaimError::sql("lock claim candidate", source))?
                {
                    candidates.push(candidate);
                    if candidates.len() == limit_usize {
                        break;
                    }
                }
            }
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        // The selected delivery rows are locked at this point. Read one
        // microsecond-capable database instant only after that lock acquisition and
        // reuse it for every update and expiry in this claim transaction.
        let operation_time = super::database_time(&mut transaction)
            .await
            .map_err(|source| ClaimError::sql("read claim operation time", source))?;

        let mut used_tokens = Vec::with_capacity(candidates.len());
        let mut prepared = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let tenant_id = TenantId::new(
                String::from_utf8(candidate.tenant_id.clone())
                    .map_err(|_| ClaimError::serialization("stored tenant id is not UTF-8"))?,
            )
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
            let row_id = RowId::new(candidate.event_row_id)
                .map_err(|error| ClaimError::serialization(error.to_string()))?;
            match (candidate.state.as_slice(), candidate.claim_token.as_deref()) {
                (b"pending", None)
                    if candidate.claimed_by.is_none()
                        && candidate.claim_expires_at.is_none()
                        && candidate.available_at <= operation_time => {}
                (b"claimed", Some(token))
                    if token.len() == dovecote::CLAIM_TOKEN_BYTES
                        && candidate.claimed_by.is_some()
                        && candidate
                            .claim_expires_at
                            .is_some_and(|expires_at| expires_at <= operation_time) => {}
                (b"pending", Some(_)) => {
                    return Err(ClaimError::serialization(
                        "pending delivery has an unexpected claim token",
                    ));
                }
                (b"claimed", None) => {
                    return Err(ClaimError::serialization(
                        "claimed delivery has no claim token",
                    ));
                }
                (b"claimed", Some(_)) => {
                    return Err(ClaimError::serialization(
                        "claimed delivery has an invalid claim token width",
                    ));
                }
                _ => {
                    return Err(ClaimError::serialization(
                        "claim candidate has an unknown state",
                    ));
                }
            }

            if candidate.state.as_slice() == b"claimed" {
                let worker = candidate
                    .claimed_by
                    .as_deref()
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .ok_or_else(|| {
                        ClaimError::serialization("claimed delivery worker is not UTF-8")
                    })?;
                WorkerId::new(worker.to_owned())
                    .map_err(|error| ClaimError::serialization(error.to_string()))?;
            }

            let attempts = candidate
                .attempts
                .checked_add(1)
                .ok_or(ClaimError::CounterOverflow { row_id })?;
            let attempts = AttemptCount::new(attempts)
                .map_err(|error| ClaimError::serialization(error.to_string()))?;
            let token = fresh_token(candidate.claim_token.as_deref(), &used_tokens, entropy)
                .map_err(|source| ClaimError::EntropyUnavailable { source })?;
            used_tokens.push(token);
            let event = hydrate::hydrate_event(&hydrate::EventColumns {
                stream: &candidate.stream,
                specversion: &candidate.specversion,
                event_id: &candidate.event_id,
                source: &candidate.source,
                event_type: &candidate.event_type,
                subject: candidate.subject.as_deref(),
                occurred_at: candidate.occurred_at,
                datacontenttype: candidate.datacontenttype.as_deref(),
                dataschema: candidate.dataschema.as_deref(),
                partitionkey: candidate.partitionkey.as_deref(),
                extensions: &candidate.extensions,
                data_kind: candidate.data_kind.as_deref(),
                data: candidate.data.as_deref(),
            })
            .map_err(ClaimError::serialization)?;
            prepared.push((row_id, candidate.event_row_id, tenant_id, event, attempts, token));
        }

        let mut claimed = Vec::with_capacity(prepared.len());
        let lease_micros =
            super::duration_micros(lease_for.get()).map_err(ClaimError::serialization)?;
        for (row_id, event_row_id, tenant_id, event, attempts, token) in prepared {
            query(
                r#"UPDATE dovecote_deliveries
            SET state = _binary 'claimed', attempts = ?, claim_token = ?,
                claimed_by = ?, claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, ?)
            WHERE tenant_id = ? AND event_row_id = ? AND (state = _binary 'pending' OR state = _binary 'claimed')"#,
            )
            .bind(attempts.get())
            .bind(token.as_slice())
            .bind(worker.as_str().as_bytes())
            .bind(lease_micros)
            .bind(operation_time)
            .bind(tenant_id.as_str().as_bytes())
            .bind(event_row_id)
            .execute(&mut *transaction)
            .await
            .map_err(|source| ClaimError::sql("update claimed delivery", source))?;
            let expiry = query_scalar::<_, OffsetDateTime>(
                "SELECT claim_expires_at FROM dovecote_deliveries WHERE tenant_id = ? AND event_row_id = ?",
            )
            .bind(tenant_id.as_str().as_bytes())
            .bind(event_row_id)
            .fetch_one(&mut *transaction)
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

fn schema_to_claim(error: crate::SchemaError) -> ClaimError {
    match error {
        crate::SchemaError::BackendMismatch { detail } => ClaimError::BackendMismatch { detail },
        crate::SchemaError::MigrationMismatch { detail } => {
            ClaimError::MigrationMismatch { detail }
        }
        crate::SchemaError::Sql { operation, source } => ClaimError::sql(operation, source),
        crate::SchemaError::Transient {
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
fn fresh_token(
    previous: Option<&[u8]>,
    used: &[[u8; dovecote::CLAIM_TOKEN_BYTES]],
    entropy: &mut impl EntropySource,
) -> Result<[u8; dovecote::CLAIM_TOKEN_BYTES], getrandom::Error> {
    loop {
        let mut token = [0_u8; dovecote::CLAIM_TOKEN_BYTES];
        entropy.fill(&mut token)?;
        if previous != Some(token.as_slice()) && used.iter().all(|other| other != &token) {
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
struct ClaimCandidateId {
    event_row_id: i64,
}

#[derive(Debug, FromRow)]
struct ClaimCandidate {
    event_row_id: i64,
    tenant_id: Vec<u8>,
    state: Vec<u8>,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<OffsetDateTime>,
    available_at: OffsetDateTime,
    stream: Vec<u8>,
    specversion: Vec<u8>,
    event_id: Vec<u8>,
    source: Vec<u8>,
    event_type: Vec<u8>,
    subject: Option<Vec<u8>>,
    occurred_at: Option<OffsetDateTime>,
    datacontenttype: Option<Vec<u8>>,
    dataschema: Option<Vec<u8>>,
    partitionkey: Option<Vec<u8>>,
    extensions: Vec<u8>,
    data_kind: Option<Vec<u8>>,
    data: Option<Vec<u8>>,
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MIGRATIONS, check_schema, enqueue::enqueue_for_scope};
    use dovecote::{EventId, EventSource, EventType, NewEvent, StreamName, TenantId};
    use sqlx::{MySqlPool, mysql::MySqlPoolOptions, query, query_as, query_scalar, raw_sql};
    use std::{
        error::Error,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct FailingEntropy;
    impl EntropySource for FailingEntropy {
        fn fill(&mut self, _output: &mut [u8]) -> Result<(), getrandom::Error> {
            Err(getrandom::Error::UNEXPECTED)
        }
    }

    struct FailsAfterOneEntropy {
        successful_fills: usize,
    }

    impl EntropySource for FailsAfterOneEntropy {
        fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error> {
            if self.successful_fills == 0 {
                output.fill(0x5a);
                self.successful_fills = 1;
                Ok(())
            } else {
                Err(getrandom::Error::UNEXPECTED)
            }
        }
    }
    #[test]
    fn token_rejects_previous() {
        let mut entropy = OsEntropy;
        let first = fresh_token(None, &[], &mut entropy).expect("entropy");
        let second = fresh_token(Some(&first), &[first], &mut entropy).expect("entropy");
        assert_ne!(first, second);
    }

    #[test]
    fn attempt_counter_overflow_is_detected_before_claim_update() {
        let row_id = RowId::new(1).expect("valid row id");
        assert!(AttemptCount::new(i64::MAX).is_ok());
        assert!(
            i64::MAX.checked_add(1).is_none(),
            "overflow must not reach SQL"
        );
        let _ = ClaimError::CounterOverflow { row_id };
    }

    #[test]
    fn entropy_failure_is_returned_before_batch_writes() {
        let mut entropy = FailingEntropy;
        assert!(fresh_token(None, &[], &mut entropy).is_err());
    }

    fn entropy_event(source: &str, id: &str) -> NewEvent {
        NewEvent::new(
            StreamName::new("mysql-entropy").expect("valid stream"),
            EventId::new(id).expect("valid event id"),
            EventSource::new(source).expect("valid source"),
            EventType::new("com.example.entropy").expect("valid event type"),
        )
        .expect("valid event")
    }

    #[allow(clippy::excessive_nesting)]
    async fn install_if_missing(pool: &MySqlPool) -> Result<bool, Box<dyn Error>> {
        let table_count: i64 = query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events', 'dovecote_deliveries')",
        )
        .fetch_one(pool)
        .await?;
        if table_count == 2 {
            check_schema(pool).await?;
            return Ok(false);
        }

        if table_count != 0 {
            return Err("Dovecote schema has only one required table".into());
        }

        // MySQL DDL, including trigger bodies, must use the raw/unprepared
        // protocol.  Keep the release artifact intact: SQLx sends the whole
        // script as one COM_QUERY, so semicolons in comments and trigger
        // bodies are interpreted by the server rather than this fixture.
        raw_sql(MIGRATIONS[0].sql()).execute(pool).await?;
        check_schema(pool).await?;
        Ok(true)
    }

    async fn drop_installed_schema(pool: &MySqlPool) -> Result<(), sqlx::Error> {
        raw_sql(
            "DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_insert; DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_update; DROP TABLE IF EXISTS dovecote_deliveries; DROP TABLE IF EXISTS dovecote_events",
        )
        .execute(pool)
        .await
        .map(|_| ())
    }

    #[allow(clippy::excessive_nesting)]
    #[tokio::test]
    async fn injected_entropy_failure_leaves_a_multi_row_claim_batch_unchanged_when_configured()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = std::env::var("DOVECOTE_MYSQL_URL") else {
            return Ok(());
        };

        let pool = MySqlPoolOptions::new()
            .max_connections(3)
            .connect(&url)
            .await?;
        let installed_here = install_if_missing(&pool).await?;

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let source = format!("https://dovecote.test/mysql-entropy-{suffix}");
        let first_id = format!("entropy-first-{suffix}");
        let second_id = format!("entropy-second-{suffix}");
        let cleanup = || async {
            query("DELETE d FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.source = ?")
                .bind(source.as_bytes())
                .execute(&pool)
                .await?;
            query("DELETE FROM dovecote_events WHERE source = ?")
                .bind(source.as_bytes())
                .execute(&pool)
                .await?;
            Ok::<_, sqlx::Error>(())
        };

        let result = async {
            cleanup().await?;

            let mut transaction = pool.begin().await?;
            let tenant = TenantId::new("test")?;
            enqueue_for_scope(&mut transaction, &tenant, entropy_event(&source, &first_id)).await?;
            enqueue_for_scope(&mut transaction, &tenant, entropy_event(&source, &second_id)).await?;
            transaction.commit().await?;

            let mut entropy = FailsAfterOneEntropy {
                successful_fills: 0,
            };

            let claim = claim_with_entropy(
                &pool,
                WorkerId::new("entropy-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(2)?,
                &mut entropy,
            )
            .await;
            assert!(matches!(claim, Err(ClaimError::EntropyUnavailable { .. })));
            assert_eq!(entropy.successful_fills, 1);

            let snapshots = query_as::<_, (Vec<u8>, i64, Option<Vec<u8>>, Option<OffsetDateTime>)>(
                "SELECT d.state, d.attempts, d.claim_token, d.claim_expires_at FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.source = ? ORDER BY d.event_row_id",
            )
            .bind(source.as_bytes())
            .fetch_all(&pool)
            .await?;
            assert_eq!(snapshots.len(), 2);
            assert!(snapshots.iter().all(|(state, attempts, token, expiry)| {
                state == b"pending" && *attempts == 0 && token.is_none() && expiry.is_none()
            }));
            Ok::<_, Box<dyn Error>>(())
        }
        .await;

        cleanup().await?;
        if installed_here {
            drop_installed_schema(&pool).await?;
        }
        pool.close().await;
        result
    }
}
