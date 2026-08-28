//! Fixture shape, projection, and publication-boundary checks.

use super::{
    fixture::{Fixture, SourceHighWaters, event_id, invalid, parse_time},
    ledger::sha256_hex,
    source::SourceEvent,
};
use dovecote::DeliverySnapshot;
use std::error::Error;
use time::OffsetDateTime;

pub(super) fn fixture_with_source(fixture: &Fixture, events: &[SourceEvent]) -> Fixture {
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
pub(super) struct PublicationObservation {
    source: String,
    id: String,
    event_type: String,
    time: Option<OffsetDateTime>,
    payload: Vec<u8>,
}

pub(super) fn publication_observation(
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

pub(super) fn legacy_publication_observation(
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

pub(super) fn verify_publication_boundary(
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

pub(super) fn check_fixture_shape(fixture: &Fixture) -> Result<(), Box<dyn Error>> {
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
pub(super) fn assert_projection(
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
