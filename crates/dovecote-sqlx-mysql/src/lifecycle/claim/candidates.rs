//! Ordered candidate discovery and exact primary-key claim locks.

use crate::error::ClaimError;
use dovecote::{Limit, TenantId};
use sqlx::{FromRow, MySqlConnection, query_as};
use time::OffsetDateTime;

pub(super) async fn lock_candidates(
    connection: &mut MySqlConnection,
    tenant_id: Option<&TenantId>,
    limit: Limit,
) -> Result<Vec<ClaimCandidate>, ClaimError> {
    // First read only an ordered ID window without locks.  Lock each ID
    // through an exact primary-key lookup below.  InnoDB's repeatable-read
    // next-key locks otherwise let the first range scan cover the next
    // pending row, so a concurrent SKIP LOCKED claimant can incorrectly
    // receive an empty batch.
    let candidate_ids_sql = if tenant_id.is_some() {
        r"
SELECT d.event_row_id
FROM dovecote_deliveries AS d
JOIN dovecote_events AS e ON e.row_id = d.event_row_id
WHERE d.event_row_id > ? AND d.tenant_id = ?
  AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
   OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
ORDER BY d.event_row_id ASC
LIMIT ?
    "
    } else {
        r"
SELECT d.event_row_id
FROM dovecote_deliveries AS d
JOIN dovecote_events AS e ON e.row_id = d.event_row_id
WHERE d.event_row_id > ?
  AND ((d.state = _binary 'pending' AND d.available_at <= UTC_TIMESTAMP(6))
   OR (d.state = _binary 'claimed' AND d.claim_expires_at <= UTC_TIMESTAMP(6)))
ORDER BY d.event_row_id ASC
LIMIT ?
"
    };

    let limit_usize = usize::try_from(limit.get())
        .map_err(|_| ClaimError::serialization("claim limit does not fit usize"))?;
    let mut after_row_id = 0_i64;
    let mut candidates = Vec::with_capacity(limit_usize);
    while candidates.len() < limit_usize {
        let mut candidate_ids =
            query_as::<_, ClaimCandidateId>(candidate_ids_sql).bind(after_row_id);
        if let Some(tenant_id) = tenant_id {
            candidate_ids = candidate_ids.bind(tenant_id.as_str().as_bytes());
        }

        let ids = candidate_ids
            .bind(i64::from(limit.get()))
            .fetch_all(&mut *connection)
            .await
            .map_err(|source| ClaimError::sql("select claim candidate IDs", source))?;
        if ids.is_empty() {
            break;
        }

        for id in ids {
            after_row_id = id.event_row_id;
            let Some(candidate) = lock_candidate(connection, tenant_id, id.event_row_id).await?
            else {
                continue;
            };
            candidates.push(candidate);
            if candidates.len() == limit_usize {
                break;
            }
        }
    }
    Ok(candidates)
}

async fn lock_candidate(
    connection: &mut MySqlConnection,
    tenant_id: Option<&TenantId>,
    event_row_id: i64,
) -> Result<Option<ClaimCandidate>, ClaimError> {
    let candidate_sql = if tenant_id.is_some() {
        r"
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
    "
    } else {
        r"
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
"
    };

    let mut candidate = query_as::<_, ClaimCandidate>(candidate_sql).bind(event_row_id);
    if let Some(tenant_id) = tenant_id {
        candidate = candidate.bind(tenant_id.as_str().as_bytes());
    }

    candidate
        .fetch_optional(connection)
        .await
        .map_err(|source| ClaimError::sql("lock claim candidate", source))
}

#[derive(Debug, FromRow)]
struct ClaimCandidateId {
    event_row_id: i64,
}

#[derive(Debug, FromRow)]
pub(super) struct ClaimCandidate {
    pub(super) event_row_id: i64,
    pub(super) tenant_id: Vec<u8>,
    pub(super) state: Vec<u8>,
    pub(super) attempts: i64,
    pub(super) claim_token: Option<Vec<u8>>,
    pub(super) claimed_by: Option<Vec<u8>>,
    pub(super) claim_expires_at: Option<OffsetDateTime>,
    pub(super) available_at: OffsetDateTime,
    pub(super) stream: Vec<u8>,
    pub(super) specversion: Vec<u8>,
    pub(super) event_id: Vec<u8>,
    pub(super) source: Vec<u8>,
    pub(super) event_type: Vec<u8>,
    pub(super) subject: Option<Vec<u8>>,
    pub(super) occurred_at: Option<OffsetDateTime>,
    pub(super) datacontenttype: Option<Vec<u8>>,
    pub(super) dataschema: Option<Vec<u8>>,
    pub(super) partitionkey: Option<Vec<u8>>,
    pub(super) extensions: Vec<u8>,
    pub(super) data_kind: Option<Vec<u8>>,
    pub(super) data: Option<Vec<u8>>,
}
