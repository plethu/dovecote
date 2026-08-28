//! MySQL-family legacy source resolution.

use super::{
    Fixture, SourceEvent, SourceHighWaters, gatekeep_source_row_digest, invalid,
    keepsake_source_row_digest_from_fields, reconstructed_fixture_payload, resolve_source,
};
use crate::ledger::SourceCursors;
use std::error::Error;

#[derive(Debug, sqlx::FromRow)]
struct MySqlKeepsakeRow {
    audit_id: i64,
    decision: String,
    occurred_at: String,
    actor_kind: String,
    actor_id: String,
    keepsake_id: String,
    subject_kind: String,
    subject_id: String,
    relation_id: String,
    event_type: String,
    context_attributes: String,
    outbox_id: Option<i64>,
    outbox_event_type: Option<String>,
    outbox_payload: Option<String>,
    claimed_by: Option<String>,
    claimed_until: Option<String>,
    delivered_at: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct MySqlGatekeepRow {
    decision_id: i64,
    entry: String,
    outbox_id: Option<i64>,
    outbox_event_type: Option<String>,
    outbox_payload: Option<String>,
    claimed_by: Option<String>,
    claimed_until: Option<String>,
    delivered_at: Option<String>,
}

pub(crate) async fn resolve_mysql(
    pool: &sqlx::MySqlPool,
    fixture: &Fixture,
    cursors: SourceCursors,
    high_waters: SourceHighWaters,
    batch_size: usize,
) -> Result<Vec<SourceEvent>, Box<dyn Error>> {
    let keepsake = sqlx::query_as::<_, MySqlKeepsakeRow>(
        r#"SELECT a.id AS audit_id,
                  CAST(a.decision AS CHAR) AS decision,
                  DATE_FORMAT(a.occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at,
                  a.actor_kind,
                  a.actor_id,
                  CAST(a.keepsake_id AS CHAR) AS keepsake_id,
                  a.subject_kind,
                  a.subject_id,
                  CAST(a.relation_id AS CHAR) AS relation_id,
                  a.event_type,
                  CAST(COALESCE((SELECT JSON_OBJECTAGG(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}') AS CHAR) AS context_attributes,
                  o.id AS outbox_id,
                  o.event_type AS outbox_event_type,
                  CAST(o.payload AS CHAR) AS outbox_payload,
                  o.claimed_by,
                  DATE_FORMAT(o.claimed_until, '%Y-%m-%dT%H:%i:%s.%fZ') AS claimed_until,
                  DATE_FORMAT(o.delivered_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS delivered_at
             FROM keepsake_audit_events a
             JOIN keepsake_audit_outbox o ON o.audit_event_id = a.id
            WHERE o.id > ? AND o.id <= ?
            ORDER BY o.id
            LIMIT ?"#,
    )
    .bind(i64::try_from(cursors.keepsake.outbox)?)
    .bind(i64::try_from(high_waters.keepsake_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let keepsake_audit_only = sqlx::query_as::<_, MySqlKeepsakeRow>(
        r#"SELECT a.id AS audit_id,
                  CAST(a.decision AS CHAR) AS decision,
                  DATE_FORMAT(a.occurred_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS occurred_at,
                  a.actor_kind,
                  a.actor_id,
                  CAST(a.keepsake_id AS CHAR) AS keepsake_id,
                  a.subject_kind,
                  a.subject_id,
                  CAST(a.relation_id AS CHAR) AS relation_id,
                  a.event_type,
                  CAST(COALESCE((SELECT JSON_OBJECTAGG(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}') AS CHAR) AS context_attributes,
                  NULL AS outbox_id,
                  NULL AS outbox_event_type,
                  NULL AS outbox_payload,
                  NULL AS claimed_by,
                  NULL AS claimed_until,
                  NULL AS delivered_at
             FROM keepsake_audit_events a
            WHERE a.id > ? AND a.id <= ?
              AND NOT EXISTS (SELECT 1 FROM keepsake_audit_outbox o WHERE o.audit_event_id = a.id)
            ORDER BY a.id
            LIMIT ?"#,
    )
    .bind(i64::try_from(cursors.keepsake.audit)?)
    .bind(i64::try_from(high_waters.keepsake_audit)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep = sqlx::query_as::<_, MySqlGatekeepRow>(
        r#"SELECT a.id AS decision_id,
                  CAST(a.entry AS CHAR) AS entry,
                  o.id AS outbox_id,
                  o.event_type AS outbox_event_type,
                  CAST(o.payload AS CHAR) AS outbox_payload,
                  o.claimed_by,
                  DATE_FORMAT(o.claimed_until, '%Y-%m-%dT%H:%i:%s.%fZ') AS claimed_until,
                  DATE_FORMAT(o.delivered_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS delivered_at
             FROM gatekeep_audit_decisions a
             JOIN gatekeep_audit_outbox o ON o.decision_id = a.id
            WHERE o.id > ? AND o.id <= ?
            ORDER BY o.id
            LIMIT ?"#,
    )
    .bind(i64::try_from(cursors.gatekeep.outbox)?)
    .bind(i64::try_from(high_waters.gatekeep_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep_audit_only = sqlx::query_as::<_, MySqlGatekeepRow>(
        r#"SELECT a.id AS decision_id,
                  CAST(a.entry AS CHAR) AS entry,
                  NULL AS outbox_id,
                  NULL AS outbox_event_type,
                  NULL AS outbox_payload,
                  NULL AS claimed_by,
                  NULL AS claimed_until,
                  NULL AS delivered_at
             FROM gatekeep_audit_decisions a
            WHERE a.id > ? AND a.id <= ?
              AND NOT EXISTS (SELECT 1 FROM gatekeep_audit_outbox o WHERE o.decision_id = a.id)
            ORDER BY a.id
            LIMIT ?"#,
    )
    .bind(i64::try_from(cursors.gatekeep.audit)?)
    .bind(i64::try_from(high_waters.gatekeep_audit)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let mut events = Vec::with_capacity(
        keepsake.len() + keepsake_audit_only.len() + gatekeep.len() + gatekeep_audit_only.len(),
    );
    let mut keepsake_rows = keepsake;
    keepsake_rows.extend(keepsake_audit_only);
    for row in keepsake_rows {
        let source_id = u64::try_from(row.outbox_id.unwrap_or(row.audit_id))?;
        if row.claimed_by.is_some()
            && row
                .claimed_until
                .as_deref()
                .is_some_and(|until| until > "2026-01-01T00:00:00.000Z")
        {
            return Err(invalid(format!(
                "active Keepsake claim crossed fence for {source_id}"
            )));
        }
        events.push(resolve_source(
            fixture,
            "keepsake",
            source_id,
            row.outbox_id,
            row.outbox_event_type,
            Some(row.decision.clone()),
            row.outbox_payload.clone(),
            if row.outbox_id.is_none() {
                Some(reconstructed_fixture_payload(
                    fixture,
                    "keepsake",
                    source_id,
                    &keepsake_source_row_digest_from_fields(
                        &row.event_type,
                        &row.occurred_at,
                        &row.actor_kind,
                        &row.actor_id,
                        &row.keepsake_id,
                        &row.subject_kind,
                        &row.subject_id,
                        &row.relation_id,
                        &row.decision,
                        &row.context_attributes,
                    )?,
                )?)
            } else {
                None
            },
            Some(row.occurred_at),
            row.delivered_at,
            if row.outbox_id.is_some() {
                "mysql-json-canonical-v1"
            } else {
                "keepsake.audit.json.v1"
            },
            // MySQL-family JSON has already discarded the original spelling;
            // resolve_source must use the named canonical export codec.
            false,
            u64::try_from(row.audit_id)?,
        )?);
    }

    let mut gatekeep_rows = gatekeep;
    gatekeep_rows.extend(gatekeep_audit_only);
    for row in gatekeep_rows {
        let source_id = u64::try_from(row.outbox_id.unwrap_or(row.decision_id))?;
        if row.claimed_by.is_some()
            && row
                .claimed_until
                .as_deref()
                .is_some_and(|until| until > "2026-01-01T00:00:00.000Z")
        {
            return Err(invalid(format!(
                "active Gatekeep claim crossed fence for {source_id}"
            )));
        }
        events.push(resolve_source(
            fixture,
            "gatekeep",
            source_id,
            row.outbox_id,
            row.outbox_event_type,
            Some(row.entry.clone()),
            row.outbox_payload.clone(),
            if row.outbox_id.is_none() {
                Some(reconstructed_fixture_payload(
                    fixture,
                    "gatekeep",
                    source_id,
                    &gatekeep_source_row_digest(&row.entry)?,
                )?)
            } else {
                None
            },
            None,
            row.delivered_at,
            if row.outbox_id.is_some() {
                "mysql-json-canonical-v1"
            } else {
                "gatekeep-audit-json-v1"
            },
            false,
            u64::try_from(row.decision_id)?,
        )?);
    }
    events.sort_by_key(|event| event.item.legacy_outbox_id);
    events.truncate(batch_size);
    Ok(events)
}
