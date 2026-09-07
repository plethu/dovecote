//! PostgreSQL legacy source resolution.

use super::{
    Fixture, KeepsakeSourceRow, SourceEvent, SourceHighWaters, SourceRecord,
    gatekeep_source_row_digest, invalid, reconstructed_fixture_payload, resolve_source,
};
use crate::ledger::SourceCursors;
use std::error::Error;

#[derive(Debug, sqlx::FromRow)]
struct PostgresKeepsakeRow {
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
struct PostgresGatekeepRow {
    decision_id: i64,
    entry: String,
    outbox_id: Option<i64>,
    outbox_event_type: Option<String>,
    outbox_payload: Option<String>,
    claimed_by: Option<String>,
    claimed_until: Option<String>,
    delivered_at: Option<String>,
}

pub(crate) async fn resolve_postgres(
    pool: &sqlx::PgPool,
    fixture: &Fixture,
    cursors: SourceCursors,
    high_waters: SourceHighWaters,
    batch_size: usize,
) -> Result<Vec<SourceEvent>, Box<dyn Error>> {
    let keepsake = sqlx::query_as::<_, PostgresKeepsakeRow>(
        r#"SELECT a.id AS audit_id,
                  a.decision::text AS decision,
                  to_char(a.occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS occurred_at,
                  a.actor_kind,
                  a.actor_id,
                  a.keepsake_id::text AS keepsake_id,
                  a.subject_kind,
                  a.subject_id,
                  a.relation_id::text AS relation_id,
                  a.event_type,
                  COALESCE((SELECT jsonb_object_agg(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}'::jsonb)::text AS context_attributes,
                  o.id AS outbox_id,
                  o.event_type AS outbox_event_type,
                  o.payload::text AS outbox_payload,
                  o.claimed_by,
                  CASE WHEN o.claimed_until IS NULL THEN NULL
                       ELSE to_char(o.claimed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END AS claimed_until,
                  CASE WHEN o.delivered_at IS NULL THEN NULL
                       ELSE to_char(o.delivered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END AS delivered_at
             FROM keepsake_audit_events a
             JOIN keepsake_audit_outbox o ON o.audit_event_id = a.id
            WHERE o.id > $1 AND o.id <= $2
            ORDER BY o.id
            LIMIT $3"#,
    )
    .bind(i64::try_from(cursors.keepsake.outbox)?)
    .bind(i64::try_from(high_waters.keepsake_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let keepsake_audit_only = sqlx::query_as::<_, PostgresKeepsakeRow>(
        r#"SELECT a.id AS audit_id,
                  a.decision::text AS decision,
                  to_char(a.occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS occurred_at,
                  a.actor_kind,
                  a.actor_id,
                  a.keepsake_id::text AS keepsake_id,
                  a.subject_kind,
                  a.subject_id,
                  a.relation_id::text AS relation_id,
                  a.event_type,
                  COALESCE((SELECT jsonb_object_agg(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}'::jsonb)::text AS context_attributes,
                  NULL::bigint AS outbox_id,
                  NULL::text AS outbox_event_type,
                  NULL::text AS outbox_payload,
                  NULL::text AS claimed_by,
                  NULL::text AS claimed_until,
                  NULL::text AS delivered_at
             FROM keepsake_audit_events a
            WHERE a.id > $1 AND a.id <= $2
              AND NOT EXISTS (SELECT 1 FROM keepsake_audit_outbox o WHERE o.audit_event_id = a.id)
            ORDER BY a.id
            LIMIT $3"#,
    )
    .bind(i64::try_from(cursors.keepsake.audit)?)
    .bind(i64::try_from(high_waters.keepsake_audit)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep = sqlx::query_as::<_, PostgresGatekeepRow>(
        r#"SELECT a.id AS decision_id,
                  a.entry::text AS entry,
                  o.id AS outbox_id,
                  o.event_type AS outbox_event_type,
                  o.payload::text AS outbox_payload,
                  o.claimed_by,
                  CASE WHEN o.claimed_until IS NULL THEN NULL
                       ELSE to_char(o.claimed_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END AS claimed_until,
                  CASE WHEN o.delivered_at IS NULL THEN NULL
                       ELSE to_char(o.delivered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END AS delivered_at
             FROM gatekeep_audit_decisions a
             JOIN gatekeep_audit_outbox o ON o.decision_id = a.id
            WHERE o.id > $1 AND o.id <= $2
            ORDER BY o.id
            LIMIT $3"#,
    )
    .bind(i64::try_from(cursors.gatekeep.outbox)?)
    .bind(i64::try_from(high_waters.gatekeep_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep_audit_only = sqlx::query_as::<_, PostgresGatekeepRow>(
        r#"SELECT a.id AS decision_id,
                  a.entry::text AS entry,
                  NULL::bigint AS outbox_id,
                  NULL::text AS outbox_event_type,
                  NULL::text AS outbox_payload,
                  NULL::text AS claimed_by,
                  NULL::text AS claimed_until,
                  NULL::text AS delivered_at
             FROM gatekeep_audit_decisions a
            WHERE a.id > $1 AND a.id <= $2
              AND NOT EXISTS (SELECT 1 FROM gatekeep_audit_outbox o WHERE o.decision_id = a.id)
            ORDER BY a.id
            LIMIT $3"#,
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
            SourceRecord {
                project: "keepsake",
                source_id,
                outbox_id: row.outbox_id,
                event_type: row.outbox_event_type,
                normalized_payload: Some(row.decision.clone()),
                payload: row.outbox_payload.clone(),
                reconstructed_payload: if row.outbox_id.is_none() {
                    Some(reconstructed_fixture_payload(
                        fixture,
                        "keepsake",
                        source_id,
                        &KeepsakeSourceRow {
                            event_type: &row.event_type,
                            occurred_at: &row.occurred_at,
                            actor_kind: &row.actor_kind,
                            actor_id: &row.actor_id,
                            keepsake_id: &row.keepsake_id,
                            subject_kind: &row.subject_kind,
                            subject_id: &row.subject_id,
                            relation_id: &row.relation_id,
                            decision: &row.decision,
                            context: &row.context_attributes,
                        }
                        .digest()?,
                    )?)
                } else {
                    None
                },
                occurred_at: Some(row.occurred_at),
                delivered_at: row.delivered_at,
                source_export_format: if row.outbox_id.is_some() {
                    "postgres-jsonb-canonical-v1"
                } else {
                    "keepsake.audit.json.v1"
                },
                // PostgreSQL JSONB has already discarded the original spelling;
                // resolve_source must use the named canonical export codec.
                exact_source_bytes: false,
                audit_id: u64::try_from(row.audit_id)?,
            },
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
            SourceRecord {
                project: "gatekeep",
                source_id,
                outbox_id: row.outbox_id,
                event_type: row.outbox_event_type,
                normalized_payload: Some(row.entry.clone()),
                payload: row.outbox_payload.clone(),
                reconstructed_payload: if row.outbox_id.is_none() {
                    Some(reconstructed_fixture_payload(
                        fixture,
                        "gatekeep",
                        source_id,
                        &gatekeep_source_row_digest(&row.entry)?,
                    )?)
                } else {
                    None
                },
                occurred_at: None,
                delivered_at: row.delivered_at,
                source_export_format: if row.outbox_id.is_some() {
                    "postgres-jsonb-canonical-v1"
                } else {
                    "gatekeep-audit-json-v1"
                },
                exact_source_bytes: false,
                audit_id: u64::try_from(row.decision_id)?,
            },
        )?);
    }
    events.sort_by_key(|event| event.item.legacy_outbox_id);
    events.truncate(batch_size);
    Ok(events)
}
