//! Cross-project migration fixture runner.
//!
//! This is intentionally a test-only package. It reads the checked-in fixture
//! description, calls the public Dovecote migration importer, and checks the
//! public paging and claim boundaries. Legacy schemas are installed by the
//! shell harness from the real sibling migration files; this runner never
//! duplicates a backend schema or a Dovecote insert statement.

#![allow(
    clippy::excessive_nesting,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

use chrono::{DateTime, Utc};
use dovecote::{
    ContentType, DeliverySnapshot, EventData, EventId, EventSource, EventType,
    ImportedDeliveryState, Limit, NewEvent, StreamName,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::Write,
    fs,
    io::{self, ErrorKind},
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Debug, Deserialize, Clone)]
struct Fixture {
    streams: BTreeMap<String, String>,
    source_policy: BTreeMap<String, String>,
    codec_versions: BTreeMap<String, String>,
    high_water_marks: Vec<SourceHighWaters>,
    at_least_once_publications: Vec<Publication>,
    events: Vec<FixtureEvent>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
struct SourceHighWaters {
    keepsake_audit: u64,
    keepsake_outbox: u64,
    gatekeep_audit: u64,
    gatekeep_outbox: u64,
}

#[derive(Debug, Deserialize, Clone)]
struct Publication {
    source: String,
    id: String,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureEvent {
    project: String,
    legacy_outbox_id: u64,
    /// The owning audit/decision row ID.  Most published rows use the same
    /// number for both tables; the fixture keeps them separate so a late
    /// outbox row cannot hide an audit row in an independent sequence.
    #[serde(default)]
    legacy_audit_id: Option<u64>,
    #[serde(default = "default_has_outbox")]
    has_outbox: bool,
    state: String,
    source_format: String,
    #[serde(default)]
    codec_version: Option<String>,
    event_type: String,
    #[serde(default)]
    occurred_at: Option<String>,
    #[serde(default)]
    delivered_at: Option<String>,
    payload: String,
}

const fn default_has_outbox() -> bool {
    true
}

#[derive(Debug, Clone, Copy)]
enum Backend {
    Sqlite,
    Postgres,
    MySql,
}

impl Backend {
    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "sqlite" => Ok(Self::Sqlite),
            "postgres" => Ok(Self::Postgres),
            "mysql" | "mysql-innovation" | "mariadb" => Ok(Self::MySql),
            other => Err(invalid(format!(
                "backend must be sqlite, postgres, mysql, mysql-innovation, or mariadb, got {other:?}"
            ))),
        }
    }
}

fn invalid(message: String) -> Box<dyn Error> {
    Box::new(io::Error::new(ErrorKind::InvalidData, message))
}

fn parse_args() -> Result<
    (
        Backend,
        String,
        String,
        SourceHighWaters,
        Option<usize>,
        bool,
        bool,
        bool,
    ),
    Box<dyn Error>,
> {
    let mut args = env::args().skip(1);
    let backend = Backend::parse(
        &args
            .next()
            .ok_or_else(|| invalid("missing backend".into()))?,
    )?;
    let url = args
        .next()
        .ok_or_else(|| invalid("missing database URL".into()))?;
    let fixture = args
        .next()
        .ok_or_else(|| invalid("missing fixture path".into()))?;
    let keepsake_audit = args
        .next()
        .ok_or_else(|| invalid("missing Keepsake audit high-water mark".into()))?
        .parse::<u64>()?;
    let keepsake_outbox = args
        .next()
        .ok_or_else(|| invalid("missing Keepsake outbox high-water mark".into()))?
        .parse::<u64>()?;
    let gatekeep_audit = args
        .next()
        .ok_or_else(|| invalid("missing Gatekeep audit high-water mark".into()))?
        .parse::<u64>()?;
    let gatekeep_outbox = args
        .next()
        .ok_or_else(|| invalid("missing Gatekeep outbox high-water mark".into()))?
        .parse::<u64>()?;
    let high_waters = SourceHighWaters {
        keepsake_audit,
        keepsake_outbox,
        gatekeep_audit,
        gatekeep_outbox,
    };
    let optional = args.next();
    let (stop_after, verify, rollback, crash) = match optional.as_deref() {
        None => (None, false, false, false),
        Some("verify") => (None, true, false, false),
        Some("rollback") => (None, false, true, false),
        Some("crash") => (None, false, false, true),
        Some(value) => {
            let action = args.next();
            (
                Some(value.parse::<usize>()?),
                action.as_deref() == Some("verify"),
                false,
                action.as_deref() == Some("crash"),
            )
        }
    };
    if args.next().is_some() {
        return Err(invalid("unexpected argument".into()));
    }
    Ok((
        backend,
        url,
        fixture,
        high_waters,
        stop_after,
        verify,
        rollback,
        crash,
    ))
}

fn event_id(item: &FixtureEvent, project: &str) -> String {
    if item.has_outbox {
        format!("{project}-outbox-{}", item.legacy_outbox_id)
    } else {
        format!("{project}-audit-legacy-{}", item.legacy_outbox_id)
    }
}

fn audit_row_id(item: &FixtureEvent) -> u64 {
    item.legacy_audit_id.unwrap_or(item.legacy_outbox_id)
}

fn outbox_row_id(item: &FixtureEvent) -> Option<u64> {
    item.has_outbox.then_some(item.legacy_outbox_id)
}

fn parse_time(value: Option<&str>) -> Result<Option<OffsetDateTime>, Box<dyn Error>> {
    value
        .map(|value| OffsetDateTime::parse(value, &Rfc3339).map_err(Into::into))
        .transpose()
}

fn build_event(fixture: &Fixture, item: &FixtureEvent) -> Result<NewEvent, Box<dyn Error>> {
    let stream = fixture
        .streams
        .get(&item.project)
        .ok_or_else(|| invalid(format!("fixture has no stream for {}", item.project)))?;
    let source = fixture
        .source_policy
        .get(&item.project)
        .ok_or_else(|| invalid(format!("fixture has no source for {}", item.project)))?;
    let id = event_id(item, &item.project);
    let mut builder = NewEvent::builder(
        StreamName::new(stream.clone())?,
        EventId::new(id)?,
        EventSource::new(source.clone())?,
        EventType::new(item.event_type.clone())?,
    );
    if let Some(occurred_at) = parse_time(item.occurred_at.as_deref())? {
        builder = builder.time(occurred_at);
    }
    builder = builder
        .datacontenttype(ContentType::new("application/json")?)
        .data(EventData::json(item.payload.as_bytes().to_vec())?);
    Ok(builder.build()?)
}

fn delivery_state(item: &FixtureEvent) -> Result<ImportedDeliveryState, Box<dyn Error>> {
    match item.state.as_str() {
        "pending" => Ok(ImportedDeliveryState::pending()),
        "delivered" => Ok(ImportedDeliveryState::delivered(
            parse_time(item.delivered_at.as_deref())?
                .ok_or_else(|| invalid("delivered fixture row has no delivered_at".into()))?,
        )?),
        state => Err(invalid(format!(
            "fixture state {state:?} is not portable; active/expired claims must be fenced first"
        ))),
    }
}

#[derive(Debug)]
struct SourceEvent {
    item: FixtureEvent,
    audit_row_id: u64,
    outbox_row_id: Option<u64>,
    source_payload: Vec<u8>,
    source_bytes_exact: bool,
    /// Backend export codec used to obtain `source_payload`.
    ///
    /// This is distinct from `FixtureEvent::source_format`: JSONB/JSON
    /// backends cannot promise the original producer spelling, while SQLite
    /// TEXT can preserve it.
    source_export_format: String,
}

#[derive(Debug, sqlx::FromRow)]
struct SqliteKeepsakeRow {
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
struct SqliteGatekeepRow {
    decision_id: i64,
    entry: String,
    outbox_id: Option<i64>,
    outbox_event_type: Option<String>,
    outbox_payload: Option<String>,
    claimed_by: Option<String>,
    claimed_until: Option<String>,
    delivered_at: Option<String>,
}

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

fn parse_chrono_time(value: &str) -> Result<DateTime<Utc>, Box<dyn Error>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn parse_context_attributes(value: &str) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    Ok(serde_json::from_str(value)?)
}

fn reconstruct_keepsake_payload(
    audit_id: i64,
    event_type: &str,
    occurred_at: &str,
    actor_kind: &str,
    actor_id: &str,
    keepsake_id: &str,
    subject_kind: &str,
    subject_id: &str,
    relation_id: &str,
    decision: &str,
    context_attributes: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let input = keepsake_sqlx::LegacyAuditEventV1 {
        audit_id,
        event_type: event_type.to_owned(),
        occurred_at: parse_chrono_time(occurred_at)?,
        actor_kind: actor_kind.to_owned(),
        actor_id: actor_id.to_owned(),
        keepsake_id: keepsake_id.parse()?,
        subject_kind: subject_kind.to_owned(),
        subject_id: subject_id.to_owned(),
        relation_id: relation_id.parse()?,
        decision: serde_json::from_str(decision)?,
        context_attributes: parse_context_attributes(context_attributes)?,
    };
    Ok(keepsake_sqlx::encode_reconstructed_audit_v1(input)?)
}

fn reconstruct_gatekeep_payload(normalized_payload: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let value: serde_json::Value = serde_json::from_str(normalized_payload)?;
    Ok(gatekeep_sqlx::encode_reconstructed_audit_v1(&value)?)
}

// PostgreSQL JSONB and MySQL-family JSON do not retain producer whitespace or
// object-member order. The backend queries export the stored value as text;
// this value codec defines the UTF-8 bytes that are hashed and handed to
// Dovecote. SQLite TEXT takes the separate byte-preserving path above.
fn canonical_json_export(payload: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let value: serde_json::Value = serde_json::from_str(payload)?;
    Ok(serde_json::to_vec(&value)?)
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

fn expected_source(
    fixture: &Fixture,
    project: &str,
    source_id: u64,
    has_outbox: bool,
) -> Result<FixtureEvent, Box<dyn Error>> {
    fixture
        .events
        .iter()
        .find(|item| {
            item.project == project
                && item.legacy_outbox_id == source_id
                && item.has_outbox == has_outbox
        })
        .cloned()
        .ok_or_else(|| {
            invalid(format!(
                "legacy source row {project}/{source_id} is missing from fixture"
            ))
        })
}

fn resolve_source(
    fixture: &Fixture,
    project: &str,
    source_id: u64,
    outbox_id: Option<i64>,
    event_type: Option<String>,
    normalized_payload: Option<String>,
    payload: Option<String>,
    reconstructed_payload: Option<Vec<u8>>,
    occurred_at: Option<String>,
    delivered_at: Option<String>,
    source_export_format: &str,
    exact_source_bytes: bool,
    audit_id: u64,
) -> Result<SourceEvent, Box<dyn Error>> {
    let has_outbox = outbox_id.is_some();
    let expected = expected_source(fixture, project, source_id, has_outbox)?;
    // For a row without a legacy outbox payload, the owning project's public,
    // versioned migration codec is the only source of bytes. The runner does
    // not maintain a lookalike envelope or infer a codec locally.
    let source_payload = if has_outbox {
        let payload = payload.ok_or_else(|| invalid("legacy outbox row has no payload".into()))?;
        if exact_source_bytes {
            payload.into_bytes()
        } else {
            canonical_json_export(&payload)?
        }
    } else {
        reconstructed_payload
            .ok_or_else(|| invalid("reconstructed source row has no codec output".into()))?
    };

    let event_type = event_type.unwrap_or_else(|| expected.event_type.clone());
    if event_type != expected.event_type {
        return Err(invalid(format!(
            "source event type mismatch for {project}/{source_id}: expected {}, got {event_type}",
            expected.event_type
        )));
    }

    let expected_value: serde_json::Value = serde_json::from_slice(expected.payload.as_bytes())?;
    let source_value: serde_json::Value = serde_json::from_slice(&source_payload)?;
    let payload_matches = if exact_source_bytes || !has_outbox {
        expected.payload.as_bytes() == source_payload
    } else {
        expected_value == source_value
    };
    if !payload_matches {
        return Err(invalid(format!(
            "source payload differs from the checked-in fixture for {project}/{source_id}"
        )));
    }

    let state = if delivered_at.is_some() {
        "delivered"
    } else {
        "pending"
    };

    if state != expected.state {
        return Err(invalid(format!(
            "source delivery state mismatch for {project}/{source_id}: expected {}, got {state}",
            expected.state
        )));
    }

    if !has_outbox {
        let codec = expected.codec_version.as_deref().ok_or_else(|| {
            invalid(format!(
                "reconstructed {project}/{source_id} has no project codec version"
            ))
        })?;
        if codec != expected.source_format {
            return Err(invalid(format!(
                "reconstructed {project}/{source_id} codec/source format differ"
            )));
        }

        let _normalized_payload = normalized_payload
            .ok_or_else(|| invalid("reconstructed source row has no normalized payload".into()))?;
    }

    let mut item = expected;
    item.payload = String::from_utf8(source_payload.clone())?;
    item.event_type = event_type;
    item.occurred_at = occurred_at;
    item.delivered_at = delivered_at;
    item.state = state.to_owned();
    Ok(SourceEvent {
        item,
        audit_row_id: u64::try_from(audit_id)?,
        outbox_row_id: outbox_id.map(u64::try_from).transpose()?,
        source_payload,
        source_bytes_exact: exact_source_bytes,
        source_export_format: source_export_format.to_owned(),
    })
}

async fn resolve_sqlite(
    pool: &sqlx::SqlitePool,
    fixture: &Fixture,
    cursors: SourceCursors,
    high_waters: SourceHighWaters,
    batch_size: usize,
) -> Result<Vec<SourceEvent>, Box<dyn Error>> {
    let keepsake = sqlx::query_as::<_, SqliteKeepsakeRow>(
        "SELECT a.id AS audit_id, a.decision, a.occurred_at, a.actor_kind, a.actor_id, a.keepsake_id, a.subject_kind, a.subject_id, a.relation_id, a.event_type, COALESCE((SELECT json_group_object(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}') AS context_attributes, o.id AS outbox_id, o.event_type AS outbox_event_type, o.payload AS outbox_payload, o.claimed_by, o.claimed_until, o.delivered_at FROM keepsake_audit_events a JOIN keepsake_audit_outbox o ON o.audit_event_id = a.id WHERE o.id > ? AND o.id <= ? ORDER BY o.id LIMIT ?",
    )
    .bind(i64::try_from(cursors.keepsake.outbox)?)
    .bind(i64::try_from(high_waters.keepsake_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let keepsake_audit_only = sqlx::query_as::<_, SqliteKeepsakeRow>(
        "SELECT a.id AS audit_id, a.decision, a.occurred_at, a.actor_kind, a.actor_id, a.keepsake_id, a.subject_kind, a.subject_id, a.relation_id, a.event_type, COALESCE((SELECT json_group_object(c.key, c.value) FROM keepsake_audit_context_attributes c WHERE c.audit_event_id = a.id), '{}') AS context_attributes, NULL AS outbox_id, NULL AS outbox_event_type, NULL AS outbox_payload, NULL AS claimed_by, NULL AS claimed_until, NULL AS delivered_at FROM keepsake_audit_events a WHERE a.id > ? AND a.id <= ? AND NOT EXISTS (SELECT 1 FROM keepsake_audit_outbox o WHERE o.audit_event_id = a.id) ORDER BY a.id LIMIT ?",
    )
    .bind(i64::try_from(cursors.keepsake.audit)?)
    .bind(i64::try_from(high_waters.keepsake_audit)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep = sqlx::query_as::<_, SqliteGatekeepRow>(
        "SELECT a.id AS decision_id, a.entry, o.id AS outbox_id, o.event_type AS outbox_event_type, o.payload AS outbox_payload, o.claimed_by, o.claimed_until, o.delivered_at FROM gatekeep_audit_decisions a JOIN gatekeep_audit_outbox o ON o.decision_id = a.id WHERE o.id > ? AND o.id <= ? ORDER BY o.id LIMIT ?",
    )
    .bind(i64::try_from(cursors.gatekeep.outbox)?)
    .bind(i64::try_from(high_waters.gatekeep_outbox)?)
    .bind(i64::try_from(batch_size)?)
    .fetch_all(pool)
    .await?;
    let gatekeep_audit_only = sqlx::query_as::<_, SqliteGatekeepRow>(
        "SELECT a.id AS decision_id, a.entry, NULL AS outbox_id, NULL AS outbox_event_type, NULL AS outbox_payload, NULL AS claimed_by, NULL AS claimed_until, NULL AS delivered_at FROM gatekeep_audit_decisions a WHERE a.id > ? AND a.id <= ? AND NOT EXISTS (SELECT 1 FROM gatekeep_audit_outbox o WHERE o.decision_id = a.id) ORDER BY a.id LIMIT ?",
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
        events.push(resolve_source(
            fixture,
            "keepsake",
            source_id,
            row.outbox_id,
            row.outbox_event_type,
            Some(row.decision.clone()),
            row.outbox_payload.clone(),
            if row.outbox_id.is_none() {
                Some(reconstruct_keepsake_payload(
                    row.audit_id,
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
                )?)
            } else {
                None
            },
            Some(row.occurred_at),
            row.delivered_at,
            if row.outbox_id.is_some() {
                "sqlite-text-v1"
            } else {
                "keepsake.audit.json.v1"
            },
            row.outbox_id.is_some(),
            u64::try_from(row.audit_id)?,
        )?);
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
    }

    let mut gatekeep_rows = gatekeep;
    gatekeep_rows.extend(gatekeep_audit_only);
    for row in gatekeep_rows {
        let source_id = u64::try_from(row.outbox_id.unwrap_or(row.decision_id))?;
        events.push(resolve_source(
            fixture,
            "gatekeep",
            source_id,
            row.outbox_id,
            row.outbox_event_type,
            Some(row.entry.clone()),
            row.outbox_payload.clone(),
            if row.outbox_id.is_none() {
                Some(reconstruct_gatekeep_payload(&row.entry)?)
            } else {
                None
            },
            None,
            row.delivered_at,
            if row.outbox_id.is_some() {
                "sqlite-text-v1"
            } else {
                "gatekeep-audit-json-v1"
            },
            row.outbox_id.is_some(),
            u64::try_from(row.decision_id)?,
        )?);
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
    }
    events.sort_by_key(|event| event.item.legacy_outbox_id);
    events.truncate(batch_size);
    Ok(events)
}

async fn resolve_postgres(
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
            "keepsake",
            source_id,
            row.outbox_id,
            row.outbox_event_type,
            Some(row.decision.clone()),
            row.outbox_payload.clone(),
            if row.outbox_id.is_none() {
                Some(reconstruct_keepsake_payload(
                    row.audit_id,
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
                )?)
            } else {
                None
            },
            Some(row.occurred_at),
            row.delivered_at,
            if row.outbox_id.is_some() {
                "postgres-jsonb-canonical-v1"
            } else {
                "keepsake.audit.json.v1"
            },
            // PostgreSQL JSONB has already discarded the original spelling;
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
                Some(reconstruct_gatekeep_payload(&row.entry)?)
            } else {
                None
            },
            None,
            row.delivered_at,
            if row.outbox_id.is_some() {
                "postgres-jsonb-canonical-v1"
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

async fn resolve_mysql(
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
                Some(reconstruct_keepsake_payload(
                    row.audit_id,
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
                Some(reconstruct_gatekeep_payload(&row.entry)?)
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[derive(Debug, Serialize, Deserialize)]
struct LedgerRow {
    project: String,
    source_identity: String,
    legacy_row_id: u64,
    source_format: String,
    source_export_format: String,
    source_payload_len: usize,
    source_payload_sha256: String,
    imported_row_id: i64,
    delivery_state: String,
    #[serde(default)]
    keepsake_audit_high_water: u64,
    #[serde(default)]
    keepsake_outbox_high_water: u64,
    #[serde(default)]
    gatekeep_audit_high_water: u64,
    #[serde(default)]
    gatekeep_outbox_high_water: u64,
    source_bytes_exact: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProgressRow {
    #[serde(default)]
    keepsake_audit_high_water: u64,
    #[serde(default)]
    keepsake_outbox_high_water: u64,
    #[serde(default)]
    gatekeep_audit_high_water: u64,
    #[serde(default)]
    gatekeep_outbox_high_water: u64,
    imported_rows: usize,
    /// Last owning audit row committed for Keepsake.
    #[serde(default)]
    keepsake_audit_cursor: u64,
    /// Last legacy outbox row committed for Keepsake.
    #[serde(default)]
    keepsake_outbox_cursor: u64,
    /// Last owning decision row committed for Gatekeep.
    #[serde(default)]
    gatekeep_audit_cursor: u64,
    /// Last legacy outbox row committed for Gatekeep.
    #[serde(default)]
    gatekeep_outbox_cursor: u64,
    /// Pre-independent-cursor progress fields.  They are read only so a
    /// resumed run can safely advance an older runner's checkpoint.
    #[serde(default, skip_serializing)]
    keepsake_cursor: u64,
    #[serde(default, skip_serializing)]
    gatekeep_cursor: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProjectSourceCursor {
    audit: u64,
    outbox: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SourceCursors {
    keepsake: ProjectSourceCursor,
    gatekeep: ProjectSourceCursor,
}

const RESOLUTION_BATCH_SIZE: usize = 1_000;

fn persist_ledger(
    path: &str,
    events: &[(&SourceEvent, i64)],
    high_waters: SourceHighWaters,
) -> Result<(), Box<dyn Error>> {
    let mut rows = BTreeMap::<String, LedgerRow>::new();
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let row: LedgerRow = serde_json::from_str(line)?;
        rows.insert(format!("{}:{}", row.project, row.source_identity), row);
    }

    for (event, imported_row_id) in events {
        let identity = event_id(&event.item, &event.item.project);
        rows.insert(
            format!("{}:{identity}", event.item.project),
            LedgerRow {
                project: event.item.project.clone(),
                source_identity: identity,
                legacy_row_id: event.item.legacy_outbox_id,
                source_format: event.item.source_format.clone(),
                source_export_format: event.source_export_format.clone(),
                source_payload_len: event.source_payload.len(),
                source_payload_sha256: sha256_hex(&event.source_payload),
                imported_row_id: *imported_row_id,
                delivery_state: event.item.state.clone(),
                keepsake_audit_high_water: high_waters.keepsake_audit,
                keepsake_outbox_high_water: high_waters.keepsake_outbox,
                gatekeep_audit_high_water: high_waters.gatekeep_audit,
                gatekeep_outbox_high_water: high_waters.gatekeep_outbox,
                source_bytes_exact: event.source_bytes_exact,
            },
        );
    }

    let mut output = String::new();
    for row in rows.values() {
        output.push_str(&serde_json::to_string(row)?);
        output.push('\n');
    }

    let temporary = format!("{path}.tmp");
    fs::write(&temporary, output)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn persist_progress(
    path: &str,
    high_waters: SourceHighWaters,
    events: &[(&SourceEvent, i64)],
    previous: SourceCursors,
) -> Result<(), Box<dyn Error>> {
    let progress_path = format!("{path}.progress");
    let mut output = match fs::read_to_string(&progress_path) {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    let mut cursors = previous;
    for (event, _) in events {
        match event.item.project.as_str() {
            "keepsake" => {
                if let Some(outbox_id) = event.outbox_row_id {
                    cursors.keepsake.outbox = cursors.keepsake.outbox.max(outbox_id);
                } else {
                    cursors.keepsake.audit = cursors.keepsake.audit.max(event.audit_row_id);
                }
            }
            "gatekeep" => {
                if let Some(outbox_id) = event.outbox_row_id {
                    cursors.gatekeep.outbox = cursors.gatekeep.outbox.max(outbox_id);
                } else {
                    cursors.gatekeep.audit = cursors.gatekeep.audit.max(event.audit_row_id);
                }
            }
            project => return Err(invalid(format!("unknown project {project}"))),
        }
    }
    output.push_str(&serde_json::to_string(&ProgressRow {
        keepsake_audit_high_water: high_waters.keepsake_audit,
        keepsake_outbox_high_water: high_waters.keepsake_outbox,
        gatekeep_audit_high_water: high_waters.gatekeep_audit,
        gatekeep_outbox_high_water: high_waters.gatekeep_outbox,
        imported_rows: events.len(),
        keepsake_audit_cursor: cursors.keepsake.audit,
        keepsake_outbox_cursor: cursors.keepsake.outbox,
        gatekeep_audit_cursor: cursors.gatekeep.audit,
        gatekeep_outbox_cursor: cursors.gatekeep.outbox,
        keepsake_cursor: 0,
        gatekeep_cursor: 0,
    })?);
    output.push('\n');
    let temporary = format!("{progress_path}.tmp");
    fs::write(&temporary, output)?;
    fs::rename(temporary, progress_path)?;
    Ok(())
}

fn read_source_cursors(path: &str) -> Result<SourceCursors, Box<dyn Error>> {
    let progress_path = format!("{path}.progress");
    let contents = match fs::read_to_string(progress_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(SourceCursors::default()),
        Err(error) => return Err(error.into()),
    };

    let rows = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<ProgressRow>)
        .collect::<Result<Vec<_>, _>>()?;
    rows.last().map_or(Ok(SourceCursors::default()), |row| {
        Ok(SourceCursors {
            keepsake: ProjectSourceCursor {
                audit: row.keepsake_audit_cursor.max(row.keepsake_cursor),
                outbox: row.keepsake_outbox_cursor.max(row.keepsake_cursor),
            },
            gatekeep: ProjectSourceCursor {
                audit: row.gatekeep_audit_cursor.max(row.gatekeep_cursor),
                outbox: row.gatekeep_outbox_cursor.max(row.gatekeep_cursor),
            },
        })
    })
}

fn verify_progress(
    path: &str,
    fixture: &Fixture,
    expected_high_waters: SourceHighWaters,
) -> Result<(), Box<dyn Error>> {
    let progress_path = format!("{path}.progress");
    let last = fs::read_to_string(progress_path)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<ProgressRow>)
        .collect::<Result<Vec<_>, _>>()?
        .pop()
        .ok_or_else(|| invalid("migration progress ledger is empty".into()))?;
    let expected_cursors = fixture
        .events
        .iter()
        .filter(|item| {
            let audit_high_water = match item.project.as_str() {
                "keepsake" => expected_high_waters.keepsake_audit,
                "gatekeep" => expected_high_waters.gatekeep_audit,
                _ => 0,
            };
            let outbox_high_water = match item.project.as_str() {
                "keepsake" => expected_high_waters.keepsake_outbox,
                "gatekeep" => expected_high_waters.gatekeep_outbox,
                _ => 0,
            };
            audit_row_id(item) <= audit_high_water
                && outbox_row_id(item).is_none_or(|id| id <= outbox_high_water)
        })
        .fold(SourceCursors::default(), |mut cursors, item| {
            let audit_id = audit_row_id(item);
            let outbox_id = outbox_row_id(item);
            match item.project.as_str() {
                "keepsake" => {
                    if let Some(outbox_id) = outbox_id {
                        cursors.keepsake.outbox = cursors.keepsake.outbox.max(outbox_id);
                    } else {
                        cursors.keepsake.audit = cursors.keepsake.audit.max(audit_id);
                    }
                }
                "gatekeep" => {
                    if let Some(outbox_id) = outbox_id {
                        cursors.gatekeep.outbox = cursors.gatekeep.outbox.max(outbox_id);
                    } else {
                        cursors.gatekeep.audit = cursors.gatekeep.audit.max(audit_id);
                    }
                }
                _ => {}
            }
            cursors
        });
    if last.keepsake_audit_high_water != expected_high_waters.keepsake_audit
        || last.keepsake_outbox_high_water != expected_high_waters.keepsake_outbox
        || last.gatekeep_audit_high_water != expected_high_waters.gatekeep_audit
        || last.gatekeep_outbox_high_water != expected_high_waters.gatekeep_outbox
        || last.imported_rows == 0
        || last.keepsake_audit_cursor < expected_cursors.keepsake.audit
        || last.keepsake_outbox_cursor < expected_cursors.keepsake.outbox
        || last.gatekeep_audit_cursor < expected_cursors.gatekeep.audit
        || last.gatekeep_outbox_cursor < expected_cursors.gatekeep.outbox
    {
        return Err(invalid(format!(
            "migration progress cursor is {:?}, expected high-waters {:?}",
            last, expected_high_waters
        )));
    }
    Ok(())
}

fn verify_ledger(
    path: &str,
    events: &[SourceEvent],
    paged_rows: &[dovecote::PagedEvent],
) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)
        .map_err(|error| invalid(format!("cannot read migration provenance ledger: {error}")))?;
    let rows = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str::<LedgerRow>)
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() != events.len() {
        return Err(invalid(format!(
            "provenance ledger has {} rows for {} source occurrences",
            rows.len(),
            events.len()
        )));
    }

    for row in rows {
        let event = events
            .iter()
            .find(|event| event_id(&event.item, &event.item.project) == row.source_identity)
            .ok_or_else(|| {
                invalid(format!(
                    "ledger has unknown identity {}",
                    row.source_identity
                ))
            })?;
        if row.project != event.item.project
            || row.legacy_row_id != event.item.legacy_outbox_id
            || row.source_payload_len != event.source_payload.len()
            || row.source_payload_sha256.is_empty()
            || row.source_payload_sha256 != sha256_hex(&event.source_payload)
            || row.source_format != event.item.source_format
            || row.source_export_format != event.source_export_format
            || row.delivery_state != event.item.state
            || row.source_bytes_exact != event.source_bytes_exact
        {
            return Err(invalid(format!(
                "provenance ledger mismatch for {}",
                row.source_identity
            )));
        }

        let paged = paged_rows
            .iter()
            .find(|paged| paged.event().id().as_str() == row.source_identity)
            .ok_or_else(|| {
                invalid(format!(
                    "ledger identity not paged: {}",
                    row.source_identity
                ))
            })?;
        if row.imported_row_id != paged.row_id().get() {
            return Err(invalid(format!(
                "ledger row ID mismatch for {}: recorded {}, paged {}",
                row.source_identity,
                row.imported_row_id,
                paged.row_id().get()
            )));
        }
    }
    Ok(())
}

fn fixture_with_source(fixture: &Fixture, events: &[SourceEvent]) -> Fixture {
    let mut expected = fixture.clone();
    for event in events {
        if let Some(item) = expected.events.iter_mut().find(|item| {
            item.project == event.item.project
                && item.legacy_outbox_id == event.item.legacy_outbox_id
                && item.has_outbox == event.item.has_outbox
        }) {
            *item = event.item.clone();
        }
    }
    expected
}

#[derive(Debug, PartialEq)]
struct PublicationObservation {
    source: String,
    id: String,
    event_type: String,
    time: Option<OffsetDateTime>,
    payload: Vec<u8>,
}

fn publication_observation(
    event: &dovecote::StoredEvent,
) -> Result<PublicationObservation, Box<dyn Error>> {
    Ok(PublicationObservation {
        source: event.source().as_str().to_owned(),
        id: event.id().as_str().to_owned(),
        event_type: event.event_type().as_str().to_owned(),
        time: event.time(),
        payload: event
            .data()
            .ok_or_else(|| invalid("publication event has no payload".into()))?
            .as_bytes()
            .to_vec(),
    })
}

fn legacy_publication_observation(
    fixture: &Fixture,
    events: &[SourceEvent],
) -> Result<(String, PublicationObservation), Box<dyn Error>> {
    let publication = fixture
        .at_least_once_publications
        .first()
        .ok_or_else(|| invalid("fixture has no publication observation".into()))?;
    let event = events
        .iter()
        .find(|event| event_id(&event.item, &event.item.project) == publication.id)
        .ok_or_else(|| {
            invalid(format!(
                "legacy publication identity {} is not in source rows",
                publication.id
            ))
        })?;
    if !event.item.has_outbox {
        return Err(invalid(format!(
            "at-least-once publication {} must originate in a legacy outbox row",
            publication.id
        )));
    }

    let source = fixture
        .source_policy
        .get(&event.item.project)
        .ok_or_else(|| invalid("fixture has no source for publication".into()))?;
    let observation = PublicationObservation {
        source: source.clone(),
        id: publication.id.clone(),
        event_type: event.item.event_type.clone(),
        time: parse_time(event.item.occurred_at.as_deref())?,
        payload: event.source_payload.clone(),
    };
    if observation.source != publication.source {
        return Err(invalid(format!(
            "legacy publication source differs for {}",
            publication.id
        )));
    }
    Ok((publication.id.clone(), observation))
}

fn verify_publication_boundary(
    publication_id: &str,
    legacy: &PublicationObservation,
    rows: &[dovecote::PagedEvent],
) -> Result<(), Box<dyn Error>> {
    // The caller captured the legacy publisher's authoritative identity and
    // bytes before opening Dovecote paging or asking Dovecote for a claim.
    // This models the transport-success/ack-loss boundary and prevents a
    // second Dovecote read from masquerading as the legacy publication.
    let row = rows
        .iter()
        .find(|row| row.event().id().as_str() == publication_id)
        .ok_or_else(|| {
            invalid(format!(
                "publication identity {} is not paged",
                publication_id
            ))
        })?;
    let observation = publication_observation(row.event())?;
    if &observation != legacy {
        return Err(invalid(format!(
            "legacy and Dovecote publications were not byte-identical for {}",
            publication_id
        )));
    }
    Ok(())
}

fn check_fixture_shape(fixture: &Fixture) -> Result<(), Box<dyn Error>> {
    if fixture.events.len() != 16 {
        return Err(invalid(format!(
            "fixture must contain sixteen source occurrences, got {}",
            fixture.events.len()
        )));
    }

    if fixture.high_water_marks
        != [
            SourceHighWaters {
                keepsake_audit: 104,
                keepsake_outbox: 104,
                gatekeep_audit: 104,
                gatekeep_outbox: 104,
            },
            SourceHighWaters {
                keepsake_audit: 206,
                keepsake_outbox: 206,
                gatekeep_audit: 1_000,
                gatekeep_outbox: 206,
            },
        ]
    {
        return Err(invalid(format!(
            "fixture high-water marks must be [104, 206], got {:?}",
            fixture.high_water_marks
        )));
    }

    if fixture.at_least_once_publications.len() != 2
        || fixture.at_least_once_publications[0].source
            != fixture.at_least_once_publications[1].source
        || fixture.at_least_once_publications[0].id != fixture.at_least_once_publications[1].id
    {
        return Err(invalid(
            "at-least-once fixture publications must repeat one (source,id)".into(),
        ));
    }

    if fixture
        .codec_versions
        .get("keepsake_reconstructed")
        .is_none_or(|version| version != "keepsake.audit.json.v1")
        || fixture
            .codec_versions
            .get("gatekeep_reconstructed")
            .is_none_or(|version| version != "gatekeep-audit-json-v1")
    {
        return Err(invalid(
            "fixture reconstruction codec versions do not match the project-owned codecs".into(),
        ));
    }

    for item in &fixture.events {
        serde_json::from_slice::<serde_json::Value>(item.payload.as_bytes()).map_err(|error| {
            invalid(format!(
                "{} payload is not valid JSON: {error}",
                event_id(item, &item.project)
            ))
        })?;
        if !item.has_outbox {
            let codec = item.codec_version.as_deref().ok_or_else(|| {
                invalid(format!(
                    "reconstructed {} has no codec version",
                    event_id(item, &item.project)
                ))
            })?;
            if codec != item.source_format {
                return Err(invalid(format!(
                    "reconstructed {} has mismatched codec provenance",
                    event_id(item, &item.project)
                )));
            }
            // Reconstructed bytes are project-owned codec output, not bytes
            // copied from a database column. They must never claim exact
            // source-byte preservation in a fixture.
        }

        if item.state == "delivered" && item.delivered_at.is_none() {
            return Err(invalid(format!(
                "delivered {} has no authoritative delivery time",
                event_id(item, &item.project)
            )));
        }
    }
    Ok(())
}

async fn run_imports_sqlite(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote_sqlx_sqlite::SqliteDovecote;
    use sqlx::sqlite::SqlitePoolOptions;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    let adapter = SqliteDovecote::new(pool);
    adapter.check_schema().await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_sqlite(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.begin_write().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}

async fn run_imports_postgres(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote_sqlx_postgres::PostgresDovecote;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let adapter = PostgresDovecote::new(pool);
    adapter.check_schema().await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_postgres(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.pool().begin().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}

async fn run_imports_mysql(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote_sqlx_mysql::MySqlDovecote;
    use sqlx::mysql::MySqlPoolOptions;

    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await?;
    // The MySQL-family migration contains trigger bodies without a client
    // delimiter directive. Install it with SQLx's own statement splitter,
    // exactly as the adapter's live conformance test does, when this fixture
    // is pointed at a fresh schema.
    let has_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'",
    )
    .fetch_one(&pool)
    .await?;
    if has_events == 0 {
        use dovecote_sqlx_mysql::MIGRATIONS;
        let mut trigger = false;
        let mut buffered = String::new();
        for fragment in MIGRATIONS[0].sql().split(';') {
            let fragment = fragment.trim();
            if fragment.is_empty() {
                continue;
            }

            let upper = fragment.to_ascii_uppercase();
            if upper.starts_with("CREATE TRIGGER") || trigger {
                if !buffered.is_empty() {
                    buffered.push(';');
                }
                buffered.push_str(fragment);
                trigger = !upper.ends_with("END");
                if trigger {
                    continue;
                }

                let statement: &'static str =
                    Box::leak(buffered.trim().to_owned().into_boxed_str());
                sqlx::raw_sql(statement).execute(&pool).await?;
                buffered.clear();
            } else {
                sqlx::query(fragment).execute(&pool).await?;
            }
        }
    }

    let adapter = MySqlDovecote::new(pool);
    adapter.check_schema().await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_mysql(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.pool().begin().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}

async fn verify_sqlite(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, WorkerId};
    use dovecote_sqlx_sqlite::SqliteDovecote;
    use sqlx::sqlite::SqlitePoolOptions;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    let adapter = SqliteDovecote::new(pool);
    adapter.check_schema().await?;
    let source_events = resolve_sqlite(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    // Rerunning exact immutable content is an explicit no-op.
    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.begin_write().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    // Same identity with changed immutable bytes must stop with a typed
    // conflict, and the failed transaction must leave the source row intact.
    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.begin_write().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;

    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.begin_write().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;

    // The public claim API must never return the two delivered imports.
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }

    // The fixture includes the at-least-once boundary explicitly. Consumers
    // deduplicate `(source,id)`, not delivery row IDs.
    Ok(())
}

async fn verify_postgres(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, WorkerId};
    use dovecote_sqlx_postgres::PostgresDovecote;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let adapter = PostgresDovecote::new(pool);
    adapter.check_schema().await?;
    let source_events = resolve_postgres(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_postgres::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_postgres::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }
    Ok(())
}

async fn verify_mysql(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, WorkerId};
    use dovecote_sqlx_mysql::MySqlDovecote;
    use sqlx::mysql::MySqlPoolOptions;

    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await?;
    let adapter = MySqlDovecote::new(pool);
    adapter.check_schema().await?;
    let source_events = resolve_mysql(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_mysql::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_mysql::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }
    Ok(())
}

fn assert_projection(
    fixture: &Fixture,
    rows: Vec<dovecote::PagedEvent>,
) -> Result<(), Box<dyn Error>> {
    if rows.len() != fixture.events.len() {
        return Err(invalid(format!(
            "Dovecote row count {} does not match fixture source count {}",
            rows.len(),
            fixture.events.len()
        )));
    }

    let mut pending = 0;
    let mut delivered = 0;
    for item in &fixture.events {
        let id = event_id(item, &item.project);
        let row = rows
            .iter()
            .find(|row| row.event().id().as_str() == id)
            .ok_or_else(|| invalid(format!("missing imported event {id}")))?;
        let source = fixture.source_policy.get(&item.project).unwrap();
        let stream = fixture.streams.get(&item.project).unwrap();
        if row.event().source().as_str() != source
            || row.event().stream().as_str() != stream
            || row.event().event_type().as_str() != item.event_type
            || row.event().datacontenttype().map(|value| value.as_str()) != Some("application/json")
        {
            return Err(invalid(format!("CloudEvents identity mismatch for {id}")));
        }

        if row.event().data().map(|value| value.as_bytes()) != Some(item.payload.as_bytes()) {
            return Err(invalid(format!("payload bytes changed for {id}")));
        }

        let expected_time = parse_time(item.occurred_at.as_deref())?;
        if row.event().time() != expected_time {
            return Err(invalid(format!("occurrence time changed for {id}")));
        }

        match (item.state.as_str(), row.delivery()) {
            ("pending", DeliverySnapshot::Pending { .. }) => pending += 1,
            ("delivered", DeliverySnapshot::Delivered { delivered_at, .. }) => {
                delivered += 1;
                if Some(*delivered_at) != parse_time(item.delivered_at.as_deref())? {
                    return Err(invalid(format!("delivery time changed for {id}")));
                }
            }
            (expected, actual) => {
                return Err(invalid(format!(
                    "delivery state for {id}: expected {expected}, got {actual:?}"
                )));
            }
        }

        let digest = sha256_hex(item.payload.as_bytes());
        if digest.len() != 64 || item.payload.is_empty() {
            return Err(invalid(format!("payload digest/length missing for {id}")));
        }
    }

    if pending != 14 || delivered != 2 {
        return Err(invalid(format!(
            "state counts differ: pending={pending}, delivered={delivered}"
        )));
    }
    Ok(())
}

async fn verify(backend: Backend, fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    match backend {
        Backend::Sqlite => verify_sqlite(fixture, url).await,
        Backend::Postgres => verify_postgres(fixture, url).await,
        Backend::MySql => verify_mysql(fixture, url).await,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (backend, url, fixture_path, high_waters, stop_after, verify_after, rollback, crash) =
        parse_args()?;
    let fixture: Fixture = serde_json::from_str(&fs::read_to_string(fixture_path)?)?;
    check_fixture_shape(&fixture)?;
    match backend {
        Backend::Sqlite => {
            run_imports_sqlite(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
        Backend::Postgres => {
            run_imports_postgres(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
        Backend::MySql => {
            run_imports_mysql(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
    }

    if verify_after {
        verify(backend, &fixture, &url).await?;
    }
    println!(
        "migration fixture imported backend={} high_waters={:?}{}",
        match backend {
            Backend::Sqlite => "sqlite",
            Backend::Postgres => "postgres",
            Backend::MySql => "mysql-or-mariadb",
        },
        high_waters,
        if verify_after { " and verified" } else { "" }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Fixture, FixtureEvent, SourceHighWaters, canonical_json_export,
        reconstruct_gatekeep_payload, reconstruct_keepsake_payload, resolve_source, sha256_hex,
    };
    use std::collections::BTreeMap;

    #[test]
    fn json_database_export_is_deterministic_but_not_original_spelling() {
        let original = r#"{"z":1, "text":"café", "optional":null}"#;
        let exported = canonical_json_export(original).expect("valid JSON");
        assert_eq!(
            exported,
            r#"{"z":1,"text":"café","optional":null}"#.as_bytes()
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&exported).expect("valid export"),
            serde_json::from_str::<serde_json::Value>(original).expect("valid source")
        );
        assert_ne!(exported, original.as_bytes());
        assert_eq!(
            sha256_hex(&exported),
            "caa9a6b82806e92bcdf86a765fc07f2b72b5a5b7ef4bc50d8957992dcb262a65"
        );
    }

    #[test]
    fn json_backend_export_path_compares_semantically_and_records_export_digest() {
        let original = r#"{"z":1, "text":"café", "optional":null}"#;
        let fixture = Fixture {
            streams: BTreeMap::from([(String::from("keepsake"), String::from("keepsake-audit"))]),
            source_policy: BTreeMap::from([(
                String::from("keepsake"),
                String::from("https://keepsake.example/audit"),
            )]),
            codec_versions: BTreeMap::new(),
            high_water_marks: vec![SourceHighWaters {
                keepsake_audit: 101,
                keepsake_outbox: 101,
                gatekeep_audit: 101,
                gatekeep_outbox: 101,
            }],
            at_least_once_publications: Vec::new(),
            events: vec![FixtureEvent {
                project: String::from("keepsake"),
                legacy_outbox_id: 101,
                legacy_audit_id: None,
                has_outbox: true,
                state: String::from("pending"),
                source_format: String::from("legacy-outbox-json-v1"),
                codec_version: None,
                event_type: String::from("keepsake.audit_event_recorded"),
                occurred_at: Some(String::from("2026-01-01T00:00:01Z")),
                delivered_at: None,
                payload: original.to_owned(),
            }],
        };
        for export_format in ["postgres-jsonb-canonical-v1", "mysql-json-canonical-v1"] {
            let event = resolve_source(
                &fixture,
                "keepsake",
                101,
                Some(101),
                Some(String::from("keepsake.audit_event_recorded")),
                Some(original.to_owned()),
                Some(original.to_owned()),
                None,
                Some(String::from("2026-01-01T00:00:01Z")),
                None,
                export_format,
                false,
                101,
            )
            .expect("canonical backend export matches the fixture semantically");
            assert_eq!(
                event.source_payload,
                r#"{"z":1,"text":"café","optional":null}"#.as_bytes()
            );
            assert_eq!(
                sha256_hex(&event.source_payload),
                "caa9a6b82806e92bcdf86a765fc07f2b72b5a5b7ef4bc50d8957992dcb262a65"
            );
            assert!(!event.source_bytes_exact);
            assert_eq!(event.source_export_format, export_format);
        }
    }

    #[test]
    fn project_codecs_define_reconstructed_fixture_bytes() {
        let keepsake = reconstruct_keepsake_payload(
            100,
            "apply",
            "2026-01-01T00:00:00.000000Z",
            "service",
            "writer-zero",
            "00000000-0000-0000-0000-000000000002",
            "user",
            "münchen",
            "00000000-0000-0000-0000-000000000001",
            r#"{"type":"applied","duplicate_prevented":false}"#,
            "{}",
        )
        .expect("valid Keepsake normalized row");
        assert_eq!(
            std::str::from_utf8(&keepsake).expect("codec output is UTF-8"),
            r#"{"event_type":"apply","at":"2026-01-01T00:00:00Z","actor":{"kind":"service","id":"writer-zero"},"keepsake_id":"00000000-0000-0000-0000-000000000002","subject":{"kind":"user","id":"münchen"},"relation_id":"00000000-0000-0000-0000-000000000001","decision":{"type":"applied","duplicate_prevented":false},"context":{"attributes":{}}}"#
        );

        let gatekeep = reconstruct_gatekeep_payload(
            r#"{"effect":"permit","context":{"tenant":"東京","optional":[]}}"#,
        )
        .expect("valid Gatekeep normalized row");
        assert_eq!(
            std::str::from_utf8(&gatekeep).expect("codec output is UTF-8"),
            r#"{"effect":"permit","context":{"tenant":"東京","optional":[]}}"#
        );
    }
}
