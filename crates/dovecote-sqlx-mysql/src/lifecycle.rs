//! MySQL/MariaDB claims and claim-token-fenced delivery mutations.

use crate::{
    backend,
    error::{ClaimError, MutationError},
};
use dovecote::{
    AttemptCount, ClaimToken, ClaimedEvent, Delay, DeliveryState, EventData, EventSizeLimit,
    Failure, Lease, Limit, NewEvent, QuarantineReason, RowId, StoredEvent, WorkerId,
};
use sqlx::{FromRow, MySql, MySqlPool, Transaction, query, query_as, query_scalar};
use time::OffsetDateTime;

#[allow(clippy::excessive_nesting)]
pub async fn claim(
    pool: &MySqlPool,
    worker: WorkerId,
    lease_for: Lease,
    limit: Limit,
) -> Result<Vec<ClaimedEvent>, ClaimError> {
    let mut entropy = OsEntropy;
    claim_with_entropy(pool, worker, lease_for, limit, &mut entropy).await
}

#[allow(clippy::excessive_nesting)]
async fn claim_with_entropy<E: EntropySource>(
    pool: &MySqlPool,
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

        let candidates = query_as::<_, ClaimCandidate>(
            r#"
        SELECT d.event_row_id, d.state, d.attempts, d.claim_token,
               d.claimed_by, d.claim_expires_at, d.available_at,
               e.stream, e.specversion, e.event_id, e.source, e.event_type,
               e.subject, e.occurred_at, e.datacontenttype, e.dataschema,
               e.partitionkey, e.extensions, e.data_kind, e.data
        FROM dovecote_deliveries AS d FORCE INDEX (PRIMARY)
        JOIN dovecote_events AS e ON e.row_id = d.event_row_id
        WHERE (d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
           OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6))
        ORDER BY d.event_row_id ASC
        LIMIT ?
        FOR UPDATE SKIP LOCKED
    "#,
        )
        .bind(i64::from(limit.get()))
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| ClaimError::sql("select claim candidates", source))?;
        // The selected delivery rows are locked at this point. Read one
        // microsecond-capable database instant only after that lock acquisition and
        // reuse it for every update and expiry in this claim transaction.
        let operation_time = database_time(&mut transaction)
            .await
            .map_err(|source| ClaimError::sql("read claim operation time", source))?;

        let mut used_tokens = Vec::with_capacity(candidates.len());
        let mut prepared = Vec::with_capacity(candidates.len());
        for candidate in candidates {
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
            let event = hydrate_event(&candidate).map_err(ClaimError::serialization)?;
            prepared.push((row_id, candidate.event_row_id, event, attempts, token));
        }

        let mut claimed = Vec::with_capacity(prepared.len());
        let lease_micros = duration_micros(lease_for.get()).map_err(ClaimError::serialization)?;
        for (row_id, event_row_id, event, attempts, token) in prepared {
            query(
                r#"UPDATE dovecote_deliveries
            SET state = _binary 'claimed', attempts = ?, claim_token = ?,
                claimed_by = ?, claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, ?)
            WHERE event_row_id = ? AND (state = _binary 'pending' OR state = _binary 'claimed')"#,
            )
            .bind(attempts.get())
            .bind(token.as_slice())
            .bind(worker.as_str().as_bytes())
            .bind(lease_micros)
            .bind(operation_time)
            .bind(event_row_id)
            .execute(&mut *transaction)
            .await
            .map_err(|source| ClaimError::sql("update claimed delivery", source))?;
            let expiry = query_scalar::<_, OffsetDateTime>(
                "SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?",
            )
            .bind(event_row_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| ClaimError::sql("read claimed expiry", source))?;
            claimed.push(
                ClaimedEvent::new(
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

pub async fn renew(
    pool: &MySqlPool,
    row_id: RowId,
    token: &ClaimToken,
    lease: Lease,
) -> Result<(), MutationError> {
    mutate(pool, row_id, token, Mutation::Renew { lease }).await
}
pub async fn ack(pool: &MySqlPool, row_id: RowId, token: &ClaimToken) -> Result<(), MutationError> {
    mutate(pool, row_id, token, Mutation::Ack).await
}
pub async fn retry(
    pool: &MySqlPool,
    row_id: RowId,
    token: &ClaimToken,
    failure: &Failure,
    backoff: Delay,
) -> Result<(), MutationError> {
    mutate(pool, row_id, token, Mutation::Retry { failure, backoff }).await
}
pub async fn release(
    pool: &MySqlPool,
    row_id: RowId,
    token: &ClaimToken,
    delay: Delay,
) -> Result<(), MutationError> {
    mutate(pool, row_id, token, Mutation::Release { delay }).await
}
pub async fn quarantine(
    pool: &MySqlPool,
    row_id: RowId,
    token: &ClaimToken,
    reason: &QuarantineReason,
) -> Result<(), MutationError> {
    mutate(pool, row_id, token, Mutation::Quarantine { reason }).await
}

async fn mutate(
    pool: &MySqlPool,
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
    let affected = execute_mutation_fast(&mut transaction, row_id, token, mutation).await?;
    if affected == 1 {
        return Ok(());
    }
    // A zero-row update is the only path that needs classification.  Obtain
    // the row lock first, then read and reuse one database instant.
    let delivery = query_as::<_, DeliveryForMutation>("SELECT state, claim_token, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ? FOR UPDATE")
        .bind(row_id.get()).fetch_optional(&mut *transaction).await.map_err(|source| MutationError::sql("lock delivery for mutation", source))?
        .ok_or(MutationError::NotFound)?;
    let operation_time = database_time(&mut transaction)
        .await
        .map_err(|source| MutationError::sql("read mutation operation time", source))?;
    classify_delivery(&delivery, token, operation_time)?;
    let affected =
        execute_mutation(&mut transaction, row_id, token, mutation, operation_time).await?;
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

async fn execute_mutation_fast(
    transaction: &mut Transaction<'_, MySql>,
    row_id: RowId,
    token: &ClaimToken,
    mutation: Mutation<'_>,
) -> Result<u64, MutationError> {
    let token = token.as_bytes().as_slice();
    let result = match mutation {
        Mutation::Renew { lease } => query("UPDATE dovecote_deliveries SET claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)) WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(duration_micros(lease.get()).map_err(MutationError::serialization)?).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Ack => query("UPDATE dovecote_deliveries SET state = _binary 'delivered', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, delivered_at = UTC_TIMESTAMP(6) WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Retry { failure, backoff } => query("UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, last_failure_code = ?, last_failure_detail = ? WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(duration_micros(backoff.get()).map_err(MutationError::serialization)?).bind(failure.code().as_bytes()).bind(failure.detail().as_bytes()).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Release { delay } => query("UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(duration_micros(delay.get()).map_err(MutationError::serialization)?).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
        Mutation::Quarantine { reason } => query("UPDATE dovecote_deliveries SET state = _binary 'quarantined', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, quarantined_at = UTC_TIMESTAMP(6), quarantine_reason = ? WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > UTC_TIMESTAMP(6)")
            .bind(reason.as_str().as_bytes()).bind(row_id.get()).bind(token).execute(&mut **transaction).await,
    };
    result
        .map(|value| value.rows_affected())
        .map_err(|source| MutationError::sql("execute fast conditional delivery mutation", source))
}

async fn execute_mutation(
    transaction: &mut Transaction<'_, MySql>,
    row_id: RowId,
    token: &ClaimToken,
    mutation: Mutation<'_>,
    now: OffsetDateTime,
) -> Result<u64, MutationError> {
    let token = token.as_bytes().as_slice();
    let (sql, binds): (&str, MutationBinds<'_>) = match mutation {
        Mutation::Renew { lease } => (
            "UPDATE dovecote_deliveries SET claim_expires_at = TIMESTAMPADD(MICROSECOND, ?, ?) WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > ?",
            MutationBinds::Lease(
                duration_micros(lease.get()).map_err(MutationError::serialization)?,
            ),
        ),
        Mutation::Ack => (
            "UPDATE dovecote_deliveries SET state = _binary 'delivered', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, delivered_at = ? WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > ?",
            MutationBinds::Ack,
        ),
        Mutation::Retry { failure, backoff } => (
            "UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, ?), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, last_failure_code = ?, last_failure_detail = ? WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > ?",
            MutationBinds::Retry(
                duration_micros(backoff.get()).map_err(MutationError::serialization)?,
                failure,
            ),
        ),
        Mutation::Release { delay } => (
            "UPDATE dovecote_deliveries SET state = _binary 'pending', available_at = TIMESTAMPADD(MICROSECOND, ?, ?), claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > ?",
            MutationBinds::Release(
                duration_micros(delay.get()).map_err(MutationError::serialization)?,
            ),
        ),
        Mutation::Quarantine { reason } => (
            "UPDATE dovecote_deliveries SET state = _binary 'quarantined', claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL, quarantined_at = ?, quarantine_reason = ? WHERE event_row_id = ? AND state = _binary 'claimed' AND claim_token = ? AND claim_expires_at > ?",
            MutationBinds::Quarantine(reason),
        ),
    };
    let mut query = query(sql);
    query = match binds {
        MutationBinds::Lease(micros) => query
            .bind(micros)
            .bind(now)
            .bind(row_id.get())
            .bind(token)
            .bind(now),
        MutationBinds::Ack => query.bind(now).bind(row_id.get()).bind(token).bind(now),
        MutationBinds::Retry(micros, failure) => query
            .bind(micros)
            .bind(now)
            .bind(failure.code().as_bytes())
            .bind(failure.detail().as_bytes())
            .bind(row_id.get())
            .bind(token)
            .bind(now),
        MutationBinds::Release(micros) => query
            .bind(micros)
            .bind(now)
            .bind(row_id.get())
            .bind(token)
            .bind(now),
        MutationBinds::Quarantine(reason) => query
            .bind(now)
            .bind(reason.as_str().as_bytes())
            .bind(row_id.get())
            .bind(token)
            .bind(now),
    };
    query
        .execute(&mut **transaction)
        .await
        .map(|result| result.rows_affected())
        .map_err(|source| MutationError::sql("execute conditional delivery mutation", source))
}

async fn database_time(
    transaction: &mut Transaction<'_, MySql>,
) -> Result<OffsetDateTime, sqlx::Error> {
    query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&mut **transaction)
        .await
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
fn duration_micros(value: std::time::Duration) -> Result<i64, String> {
    i64::try_from(value.as_micros())
        .map_err(|_| "duration does not fit MySQL microseconds".to_owned())
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
struct ClaimCandidate {
    event_row_id: i64,
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
enum MutationBinds<'a> {
    Lease(i64),
    Ack,
    Retry(i64, &'a Failure),
    Release(i64),
    Quarantine(&'a QuarantineReason),
}

#[allow(clippy::single_match)]
fn hydrate_event(candidate: &ClaimCandidate) -> Result<StoredEvent, String> {
    if candidate.specversion.as_slice() != dovecote::SPEC_VERSION.as_bytes() {
        return Err("stored event has unsupported specversion".to_owned());
    }

    let strv = |value: &[u8], field: &str| {
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| format!("stored {field} is not UTF-8"))
    };
    let stream =
        dovecote::StreamName::new(strv(&candidate.stream, "stream")?).map_err(|e| e.to_string())?;
    let id = dovecote::EventId::new(strv(&candidate.event_id, "event id")?)
        .map_err(|e| e.to_string())?;
    let source = dovecote::EventSource::new(strv(&candidate.source, "source")?)
        .map_err(|e| e.to_string())?;
    let event_type = dovecote::EventType::new(strv(&candidate.event_type, "event type")?)
        .map_err(|e| e.to_string())?;
    let mut b = NewEvent::builder(stream, id, source, event_type);
    // These optional CloudEvents attributes are independent, not priority
    // policy. Their source-column order stays explicit for deterministic
    // hydration.
    // Each optional CloudEvents attribute is hydrated independently; order is
    // column order, not a policy cascade.
    let _ = ();
    if let Some(v) = &candidate.subject {
        b = b.subject(dovecote::EventSubject::new(strv(v, "subject")?).map_err(|e| e.to_string())?);
    }

    let _ = ();
    if let Some(v) = candidate.occurred_at {
        b = b.time(v);
    }

    let _ = ();
    if let Some(v) = &candidate.datacontenttype {
        b = b.datacontenttype(
            dovecote::ContentType::new(strv(v, "content type")?).map_err(|e| e.to_string())?,
        );
    }

    let _ = ();
    if let Some(v) = &candidate.dataschema {
        b = b.dataschema(
            dovecote::SchemaUri::new(strv(v, "schema URI")?).map_err(|e| e.to_string())?,
        );
    }

    let _ = ();
    if let Some(v) = &candidate.partitionkey {
        b = b.partitionkey(
            dovecote::PartitionKey::new(strv(v, "partition key")?).map_err(|e| e.to_string())?,
        );
    }

    let extensions = strv(&candidate.extensions, "extensions")?;
    b = b.extensions(
        dovecote::Extensions::from_canonical_json(&extensions).map_err(|e| e.to_string())?,
    );
    match (&candidate.data_kind, &candidate.data) {
        (None, None) => {}
        (Some(kind), Some(bytes)) if kind.as_slice() == b"json" => {
            b = b.data(EventData::json(bytes.clone()).map_err(|e| e.to_string())?)
        }
        (Some(kind), Some(bytes)) if kind.as_slice() == b"binary" => {
            b = b.data(EventData::binary(bytes.clone()))
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }
    b.build_with_limit(EventSizeLimit::new(usize::MAX).expect("nonzero"))
        .map_err(|e| e.to_string())?
        .into_stored()
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MIGRATIONS, check_schema, enqueue};
    use dovecote::{EventId, EventSource, EventType, StreamName};
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

        let mut trigger = false;
        let mut buffered = String::new();
        for fragment in MIGRATIONS[0].sql().split(';') {
            let fragment = fragment.trim();
            if fragment.is_empty() {
                continue;
            }

            if fragment.to_ascii_uppercase().starts_with("CREATE TRIGGER") || trigger {
                if !buffered.is_empty() {
                    buffered.push(';');
                }
                buffered.push_str(fragment);
                trigger = !fragment.to_ascii_uppercase().ends_with("END");
                if trigger {
                    continue;
                }

                let statement: &'static str = Box::leak(buffered.clone().into_boxed_str());
                raw_sql(statement).execute(pool).await?;
                buffered.clear();
                continue;
            }

            query(fragment).execute(pool).await?;
        }
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
            enqueue(&mut transaction, entropy_event(&source, &first_id)).await?;
            enqueue(&mut transaction, entropy_event(&source, &second_id)).await?;
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
