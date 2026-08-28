//! Durable import ledger, progress checkpoints, and reconciliation.

use super::{
    fixture::{Fixture, SourceHighWaters, audit_row_id, event_id, invalid, outbox_row_id},
    source::SourceEvent,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, error::Error, fmt::Write, fs, io::ErrorKind};

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
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
pub(super) struct ProjectSourceCursor {
    pub(super) audit: u64,
    pub(super) outbox: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SourceCursors {
    pub(super) keepsake: ProjectSourceCursor,
    pub(super) gatekeep: ProjectSourceCursor,
}

pub(super) const RESOLUTION_BATCH_SIZE: usize = 1_000;

pub(super) fn persist_ledger(
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

pub(super) fn persist_progress(
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

pub(super) fn read_source_cursors(path: &str) -> Result<SourceCursors, Box<dyn Error>> {
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

pub(super) fn verify_progress(
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

pub(super) fn verify_ledger(
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
