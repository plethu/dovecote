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
            _ => Err(invalid(
                "backend must be sqlite, postgres, mysql, mysql-innovation, or mariadb".into(),
            )),
        }
    }
}

pub(super) fn invalid(message: String) -> Box<dyn Error> {
    Box::new(io::Error::new(ErrorKind::InvalidData, message))
}

/// One mutually exclusive migration execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunMode {
    Import,
    Verify,
    Rollback,
    CrashBeforeCheckpoint,
}

impl RunMode {
    fn parse(value: Option<&str>) -> Result<Self, Box<dyn Error>> {
        match value {
            None => Ok(Self::Import),
            Some("verify") => Ok(Self::Verify),
            Some("rollback") => Ok(Self::Rollback),
            Some("crash") => Ok(Self::CrashBeforeCheckpoint),
            Some(_) => Err(invalid("expected verify, rollback, or crash".into())),
        }
    }
}

/// The fixed positional protocol used by the migration shell harness.
/// Credentials deliberately have no `Debug` representation.
pub(super) struct Invocation {
    pub(super) backend: Backend,
    pub(super) url: String,
    pub(super) fixture_path: String,
    pub(super) high_waters: SourceHighWaters,
    pub(super) stop_after: Option<usize>,
    pub(super) mode: RunMode,
}

pub(super) fn parse_args() -> Result<Invocation, Box<dyn Error>> {
    parse_invocation(env::args().skip(1))
}

fn parse_invocation(mut args: impl Iterator<Item = String>) -> Result<Invocation, Box<dyn Error>> {
    let backend = Backend::parse(&required(&mut args, "backend")?)?;
    let url = required(&mut args, "database URL")?;
    let fixture_path = required(&mut args, "fixture path")?;
    let high_waters = SourceHighWaters {
        keepsake_audit: required(&mut args, "Keepsake audit high-water mark")?.parse()?,
        keepsake_outbox: required(&mut args, "Keepsake outbox high-water mark")?.parse()?,
        gatekeep_audit: required(&mut args, "Gatekeep audit high-water mark")?.parse()?,
        gatekeep_outbox: required(&mut args, "Gatekeep outbox high-water mark")?.parse()?,
    };
    let optional = args.next();
    let (stop_after, mode) = match optional.as_deref() {
        None | Some("verify" | "rollback" | "crash") => {
            (None, RunMode::parse(optional.as_deref())?)
        }
        Some(value) => {
            let limit = value.parse()?;
            let action = args.next();
            (Some(limit), RunMode::parse(action.as_deref())?)
        }
    };
    if args.next().is_some() {
        return Err(invalid("unexpected argument".into()));
    }
    Ok(Invocation {
        backend,
        url,
        fixture_path,
        high_waters,
        stop_after,
        mode,
    })
}

fn required(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| invalid(format!("missing {name}")))
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

#[cfg(test)]
mod invocation_tests {
    use super::{RunMode, parse_invocation};

    fn arguments(tail: &[&str]) -> impl Iterator<Item = String> {
        [
            "sqlite",
            "private-database-url",
            "fixture.json",
            "1",
            "2",
            "3",
            "4",
        ]
        .into_iter()
        .chain(tail.iter().copied())
        .map(str::to_owned)
    }

    #[test]
    fn bounded_rollback_is_an_explicit_mode() -> Result<(), Box<dyn std::error::Error>> {
        let invocation = parse_invocation(arguments(&["2", "rollback"]))?;
        assert_eq!(invocation.stop_after, Some(2));
        assert_eq!(invocation.mode, RunMode::Rollback);
        Ok(())
    }

    #[test]
    fn unknown_mode_does_not_silently_import() {
        let result = parse_invocation(arguments(&["2", "private-unknown-mode"]));
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("unknown mode was accepted"),
        };
        assert_eq!(error.to_string(), "expected verify, rollback, or crash");
    }
}
