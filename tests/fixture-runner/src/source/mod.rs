//! Legacy source resolution and backend-specific export codecs.

mod mysql;
mod postgres;
mod sqlite;

pub(crate) use mysql::resolve_mysql;
pub(crate) use postgres::resolve_postgres;
pub(crate) use sqlite::resolve_sqlite;

use super::fixture::{Fixture, FixtureEvent, SourceHighWaters, invalid};
use crate::ledger::sha256_hex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Debug)]
pub(super) struct SourceEvent {
    pub(super) item: FixtureEvent,
    pub(super) audit_row_id: u64,
    pub(super) outbox_row_id: Option<u64>,
    pub(super) source_payload: Vec<u8>,
    pub(super) source_bytes_exact: bool,
    /// Backend export codec used to obtain `source_payload`.
    ///
    /// This is distinct from `FixtureEvent::source_format`: JSONB/JSON
    /// backends cannot promise the original producer spelling, while SQLite
    /// TEXT can preserve it.
    pub(super) source_export_format: String,
}

#[derive(Debug, Deserialize)]
struct GoldenManifest {
    schema: String,
    source_row_digest_scope: String,
    reference: GoldenReferences,
    occurrences: Vec<GoldenOccurrence>,
}

#[derive(Debug, Deserialize)]
struct GoldenReferences {
    keepsake: GoldenReference,
    gatekeep: GoldenReference,
}

#[derive(Debug, Deserialize)]
struct GoldenReference {
    repository: String,
    commit: String,
    implementation: String,
    codec_name: String,
    codec_version: String,
}

#[derive(Debug, Deserialize)]
struct GoldenOccurrence {
    project: String,
    source_id: u64,
    codec_name: String,
    codec_version: String,
    fixture_codec_version: String,
    source_row_sha256: String,
    canonical_payload_sha256: String,
}

const KEEPSAKE_REFERENCE_COMMIT: &str = "b5d1c1fdebb19164c0c569c75f3a2e21c1c667fc";
const GATEKEEP_REFERENCE_COMMIT: &str = "d7450f2c02e2510da38c5e66e5e55954c3005bd6";
const SOURCE_ROW_DIGEST_SCOPE: &str =
    "Keepsake LegacyAuditEventV1 input JSON (field order below); Gatekeep normalized entry JSON";

/// The exact logical input consumed by Keepsake's retired
/// `LegacyAuditEventV1` encoder.  This is intentionally a named structure,
/// rather than a hash of `decision` alone: every field that could affect the
/// reconstructed payload is bound in a stable, documented order.
#[derive(Debug, Serialize)]
pub(super) struct KeepsakeSourceEvidence<'a> {
    pub(super) event_type: &'a str,
    pub(super) occurred_at: &'a str,
    pub(super) actor_kind: &'a str,
    pub(super) actor_id: &'a str,
    pub(super) keepsake_id: &'a str,
    pub(super) subject_kind: &'a str,
    pub(super) subject_id: &'a str,
    pub(super) relation_id: &'a str,
    pub(super) decision: serde_json::Value,
    pub(super) context: BTreeMap<String, String>,
}

pub(super) fn keepsake_source_row_digest(
    evidence: KeepsakeSourceEvidence<'_>,
) -> Result<String, Box<dyn Error>> {
    Ok(sha256_hex(&serde_json::to_vec(&evidence)?))
}

pub(super) fn keepsake_source_row_digest_from_fields(
    event_type: &str,
    occurred_at: &str,
    actor_kind: &str,
    actor_id: &str,
    keepsake_id: &str,
    subject_kind: &str,
    subject_id: &str,
    relation_id: &str,
    decision: &str,
    context: &str,
) -> Result<String, Box<dyn Error>> {
    keepsake_source_row_digest(KeepsakeSourceEvidence {
        event_type,
        occurred_at: &OffsetDateTime::parse(occurred_at, &Rfc3339)?.format(&Rfc3339)?,
        actor_kind,
        actor_id,
        keepsake_id,
        subject_kind,
        subject_id,
        relation_id,
        decision: serde_json::from_str(decision)?,
        context: serde_json::from_str(context)?,
    })
}

pub(super) fn gatekeep_source_row_digest(entry: &str) -> Result<String, Box<dyn Error>> {
    Ok(sha256_hex(&canonical_json_export(entry)?))
}

// PostgreSQL JSONB and MySQL-family JSON do not retain producer whitespace or
// object-member order. The backend queries export the stored value as text;
// this value codec defines the UTF-8 bytes that are hashed and handed to
// Dovecote. SQLite TEXT takes the separate byte-preserving path above.
fn canonical_json_export(payload: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let value: serde_json::Value = serde_json::from_str(payload)?;
    Ok(serde_json::to_vec(&value)?)
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

fn golden_occurrence(project: &str, source_id: u64) -> Result<GoldenOccurrence, Box<dyn Error>> {
    let manifest: GoldenManifest = serde_json::from_str(include_str!(
        "../../../fixtures/reconstructed-payload-golden-v1.json"
    ))?;
    if manifest.schema != "dovecote-reconstructed-payload-golden/v1"
        || manifest.source_row_digest_scope != SOURCE_ROW_DIGEST_SCOPE
        || manifest.occurrences.len() != 4
    {
        return Err(invalid(
            "reconstructed payload golden manifest has an unsupported schema".into(),
        ));
    }

    let (reference, expected_reference) = match project {
        "keepsake" => (
            &manifest.reference.keepsake,
            (
                "plethu/keepsake",
                KEEPSAKE_REFERENCE_COMMIT,
                "crates/keepsake-sqlx/src/repository/dovecote_bridge.rs",
                "keepsake.audit.json",
                "v1",
            ),
        ),
        "gatekeep" => (
            &manifest.reference.gatekeep,
            (
                "plethu/gatekeep",
                GATEKEEP_REFERENCE_COMMIT,
                "crates/gatekeep-sqlx/src/audit/bridge.rs",
                "gatekeep-audit-json",
                "v1",
            ),
        ),
        _ => {
            return Err(invalid(format!(
                "reconstructed {project}/{source_id} has an unsupported project codec"
            )));
        }
    };

    if reference.repository.is_empty()
        || reference.commit.len() != 40
        || !reference
            .commit
            .chars()
            .all(|character| character.is_ascii_hexdigit())
        || reference.implementation.is_empty()
        || (
            reference.repository.as_str(),
            reference.commit.as_str(),
            reference.implementation.as_str(),
            reference.codec_name.as_str(),
            reference.codec_version.as_str(),
        ) != expected_reference
    {
        return Err(invalid(format!(
            "reconstructed {project}/{source_id} golden reference is incomplete"
        )));
    }
    manifest
        .occurrences
        .into_iter()
        .find(|item| item.project == project && item.source_id == source_id)
        .ok_or_else(|| {
            invalid(format!(
                "reconstructed payload golden is missing for {project}/{source_id}"
            ))
        })
}

/// Return the checked-in byte representation for an audit row that predates
/// the legacy outbox.  Keepsake 3.0 and Gatekeep 3.0 deliberately no longer
/// expose the removed v1 migration codecs: these historical payloads are
/// opaque compatibility data, not current domain values.  The fixture owns
/// their exact bytes. The independently reviewed golden manifest records the
/// former codec identity, normalized source digest, and expected output digest;
/// migration inputs remain separately protected by `historical-migrations.sha256`.
fn reconstructed_fixture_payload(
    fixture: &Fixture,
    project: &str,
    source_id: u64,
    source_row_digest: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let expected = expected_source(fixture, project, source_id, false)?;
    let golden = golden_occurrence(project, source_id)?;
    if expected.codec_version.as_deref() != Some(golden.fixture_codec_version.as_str()) {
        return Err(invalid(format!(
            "reconstructed {project}/{source_id} fixture codec version differs from golden manifest"
        )));
    }

    if source_row_digest != golden.source_row_sha256 {
        return Err(invalid(format!(
            "reconstructed {project}/{source_id} source digest differs from golden manifest: expected {}, got {source_row_digest}",
            golden.source_row_sha256
        )));
    }

    let payload = expected.payload.into_bytes();
    if sha256_hex(&payload) != golden.canonical_payload_sha256 {
        return Err(invalid(format!(
            "reconstructed {project}/{source_id} canonical payload digest differs from golden manifest"
        )));
    }

    let expected_codec = match project {
        "keepsake" => ("keepsake.audit.json", "v1"),
        "gatekeep" => ("gatekeep-audit-json", "v1"),
        _ => {
            return Err(invalid(format!(
                "reconstructed {project}/{source_id} has an unsupported project codec"
            )));
        }
    };

    if (golden.codec_name.as_str(), golden.codec_version.as_str()) != expected_codec {
        return Err(invalid(format!(
            "reconstructed {project}/{source_id} golden codec identity differs from project contract"
        )));
    }
    Ok(payload)
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
    // For a row without a legacy outbox payload, the historical fixture owns
    // the opaque bytes produced by the former project codec. Current 3.0
    // project crates no longer expose those retired codecs.
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
        audit_row_id: audit_id,
        outbox_row_id: outbox_id.map(u64::try_from).transpose()?,
        source_payload,
        source_bytes_exact: exact_source_bytes,
        source_export_format: source_export_format.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::ledger::sha256_hex;
    use super::{Fixture, FixtureEvent, SourceHighWaters, canonical_json_export, resolve_source};
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
}
