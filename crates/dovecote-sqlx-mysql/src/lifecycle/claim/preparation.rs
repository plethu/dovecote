//! Validate complete locked deliveries and prepare fresh claim identities before writes.

use super::candidates::ClaimCandidate;
use crate::{error::ClaimError, hydrate};
use dovecote::{AttemptCount, CLAIM_TOKEN_BYTES, RowId, StoredEvent, TenantId, WorkerId};
use time::OffsetDateTime;

pub(super) struct PreparedClaim {
    pub(super) row_id: RowId,
    pub(super) tenant_id: TenantId,
    pub(super) event: StoredEvent,
    pub(super) attempts: AttemptCount,
    pub(super) token: [u8; CLAIM_TOKEN_BYTES],
}

pub(super) fn prepare_batch(
    candidates: Vec<ClaimCandidate>,
    operation_time: OffsetDateTime,
    entropy: &mut impl EntropySource,
) -> Result<Vec<PreparedClaim>, ClaimError> {
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
        validate_delivery(&candidate, operation_time)?;

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
        prepared.push(PreparedClaim {
            row_id,
            tenant_id,
            event,
            attempts,
            token,
        });
    }

    Ok(prepared)
}

fn validate_delivery(
    candidate: &ClaimCandidate,
    operation_time: OffsetDateTime,
) -> Result<(), ClaimError> {
    match (candidate.state.as_slice(), candidate.claim_token.as_deref()) {
        (b"pending", None)
            if candidate.claimed_by.is_none()
                && candidate.claim_expires_at.is_none()
                && candidate.available_at <= operation_time => {}
        (b"claimed", Some(token))
            if token.len() == CLAIM_TOKEN_BYTES
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
            .ok_or_else(|| ClaimError::serialization("claimed delivery worker is not UTF-8"))?;
        WorkerId::new(worker.to_owned())
            .map_err(|error| ClaimError::serialization(error.to_string()))?;
    }

    Ok(())
}

pub(super) fn fresh_token(
    previous: Option<&[u8]>,
    used: &[[u8; CLAIM_TOKEN_BYTES]],
    entropy: &mut impl EntropySource,
) -> Result<[u8; CLAIM_TOKEN_BYTES], getrandom::Error> {
    loop {
        let mut token = [0_u8; CLAIM_TOKEN_BYTES];
        entropy.fill(&mut token)?;
        if previous != Some(token.as_slice()) && used.iter().all(|other| other != &token) {
            return Ok(token);
        }
    }
}
pub(super) trait EntropySource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error>;
}
pub(super) struct OsEntropy;
impl EntropySource for OsEntropy {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error> {
        getrandom::fill(output)
    }
}
