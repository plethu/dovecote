//! Checked-in fixture model, CLI decoding, and Dovecote event codecs.

use dovecote::{
    ContentType, EventData, EventId, EventSource, EventType, ImportedDeliveryState, NewEvent,
    StreamName,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    io::{self, ErrorKind},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Debug, Deserialize, Clone)]
pub(super) struct Fixture {
    pub(super) streams: BTreeMap<String, String>,
    pub(super) source_policy: BTreeMap<String, String>,
    pub(super) codec_versions: BTreeMap<String, String>,
    pub(super) high_water_marks: Vec<SourceHighWaters>,
    pub(super) at_least_once_publications: Vec<Publication>,
    pub(super) events: Vec<FixtureEvent>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
pub(super) struct SourceHighWaters {
    pub(super) keepsake_audit: u64,
    pub(super) keepsake_outbox: u64,
    pub(super) gatekeep_audit: u64,
    pub(super) gatekeep_outbox: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct Publication {
    pub(super) source: String,
    pub(super) id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct FixtureEvent {
    pub(super) project: String,
    pub(super) legacy_outbox_id: u64,
    /// The owning audit/decision row ID.  Most published rows use the same
    /// number for both tables; the fixture keeps them separate so a late
    /// outbox row cannot hide an audit row in an independent sequence.
    #[serde(default)]
    pub(super) legacy_audit_id: Option<u64>,
    #[serde(default = "default_has_outbox")]
    pub(super) has_outbox: bool,
    pub(super) state: String,
    pub(super) source_format: String,
    #[serde(default)]
    pub(super) codec_version: Option<String>,
    pub(super) event_type: String,
    #[serde(default)]
    pub(super) occurred_at: Option<String>,
    #[serde(default)]
    pub(super) delivered_at: Option<String>,
    pub(super) payload: String,
}

const fn default_has_outbox() -> bool {
    true
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Backend {
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

pub(super) fn invalid(message: String) -> Box<dyn Error> {
    Box::new(io::Error::new(ErrorKind::InvalidData, message))
}

pub(super) fn parse_args() -> Result<
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

pub(super) fn event_id(item: &FixtureEvent, project: &str) -> String {
    if item.has_outbox {
        format!("{project}-outbox-{}", item.legacy_outbox_id)
    } else {
        format!("{project}-audit-legacy-{}", item.legacy_outbox_id)
    }
}

pub(super) fn audit_row_id(item: &FixtureEvent) -> u64 {
    item.legacy_audit_id.unwrap_or(item.legacy_outbox_id)
}

pub(super) fn outbox_row_id(item: &FixtureEvent) -> Option<u64> {
    item.has_outbox.then_some(item.legacy_outbox_id)
}

pub(super) fn parse_time(value: Option<&str>) -> Result<Option<OffsetDateTime>, Box<dyn Error>> {
    value
        .map(|value| OffsetDateTime::parse(value, &Rfc3339).map_err(Into::into))
        .transpose()
}

pub(super) fn build_event(
    fixture: &Fixture,
    item: &FixtureEvent,
) -> Result<NewEvent, Box<dyn Error>> {
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

pub(super) fn delivery_state(item: &FixtureEvent) -> Result<ImportedDeliveryState, Box<dyn Error>> {
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
