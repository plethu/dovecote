//! Durable `SQLite` row hydration into core-owned values.

use crate::enqueue::parse_timestamp;
use dovecote::{
    AttemptCount, DeliverySnapshot, EventData, EventSizeLimit, Failure, NewEvent, PagedEvent,
    QuarantineReason, RowId, StoredEvent, TenantId, WorkerId,
};

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct DurableRow {
    pub(crate) row_id: i64,
    pub(crate) tenant_id: String,
    pub(crate) stream: String,
    pub(crate) specversion: String,
    pub(crate) event_id: String,
    pub(crate) source: String,
    pub(crate) event_type: String,
    pub(crate) subject: Option<String>,
    pub(crate) occurred_at: Option<String>,
    pub(crate) enqueued_at: String,
    pub(crate) datacontenttype: Option<String>,
    pub(crate) dataschema: Option<String>,
    pub(crate) partitionkey: Option<String>,
    pub(crate) extensions: String,
    pub(crate) data_kind: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) state: Option<String>,
    pub(crate) available_at: Option<String>,
    pub(crate) attempts: Option<i64>,
    pub(crate) claim_token: Option<Vec<u8>>,
    pub(crate) claimed_by: Option<String>,
    pub(crate) claim_expires_at: Option<String>,
    pub(crate) last_failure_code: Option<String>,
    pub(crate) last_failure_detail: Option<String>,
    pub(crate) delivered_at: Option<String>,
    pub(crate) quarantined_at: Option<String>,
    pub(crate) quarantine_reason: Option<String>,
}

/// Borrowed immutable event columns; delivery state is validated separately.
pub(crate) struct EventRow<'a> {
    pub(crate) stream: &'a str,
    pub(crate) specversion: &'a str,
    pub(crate) event_id: &'a str,
    pub(crate) source: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) subject: Option<&'a str>,
    pub(crate) occurred_at: Option<&'a str>,
    pub(crate) datacontenttype: Option<&'a str>,
    pub(crate) dataschema: Option<&'a str>,
    pub(crate) partitionkey: Option<&'a str>,
    pub(crate) extensions: &'a str,
    pub(crate) data_kind: Option<&'a str>,
    pub(crate) data: Option<&'a [u8]>,
}

impl DurableRow {
    pub(crate) fn event_row(&self) -> EventRow<'_> {
        EventRow {
            stream: &self.stream,
            specversion: &self.specversion,
            event_id: &self.event_id,
            source: &self.source,
            event_type: &self.event_type,
            subject: self.subject.as_deref(),
            occurred_at: self.occurred_at.as_deref(),
            datacontenttype: self.datacontenttype.as_deref(),
            dataschema: self.dataschema.as_deref(),
            partitionkey: self.partitionkey.as_deref(),
            extensions: &self.extensions,
            data_kind: self.data_kind.as_deref(),
            data: self.data.as_deref(),
        }
    }
}

pub(crate) fn hydrate_event(row: &EventRow<'_>) -> Result<StoredEvent, String> {
    if row.specversion != dovecote::SPEC_VERSION {
        return Err("stored event has an unsupported specversion".to_owned());
    }

    let stream =
        dovecote::StreamName::new(row.stream.to_owned()).map_err(|error| error.to_string())?;
    let id = dovecote::EventId::new(row.event_id.to_owned()).map_err(|error| error.to_string())?;
    let source =
        dovecote::EventSource::new(row.source.to_owned()).map_err(|error| error.to_string())?;
    let event_type =
        dovecote::EventType::new(row.event_type.to_owned()).map_err(|error| error.to_string())?;
    let mut builder = NewEvent::builder(stream, id, source, event_type);
    builder = match row.subject {
        Some(value) => builder.subject(
            dovecote::EventSubject::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    if let Some(value) = row.occurred_at {
        builder = builder.time(parse_timestamp(value)?);
    }
    builder = match row.datacontenttype {
        Some(value) => builder.datacontenttype(
            dovecote::ContentType::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.dataschema {
        Some(value) => builder.dataschema(
            dovecote::SchemaUri::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = match row.partitionkey {
        Some(value) => builder.partitionkey(
            dovecote::PartitionKey::new(value.to_owned()).map_err(|error| error.to_string())?,
        ),
        None => builder,
    };
    builder = builder.extensions(
        dovecote::Extensions::from_canonical_json(row.extensions)
            .map_err(|error| error.to_string())?,
    );
    match (row.data_kind, row.data) {
        (None, None) => {}
        (Some("json"), Some(bytes)) => {
            builder =
                builder.data(EventData::json(bytes.to_owned()).map_err(|error| error.to_string())?);
        }
        (Some("binary"), Some(bytes)) => {
            builder = builder.data(EventData::binary(bytes.to_owned()));
        }
        _ => return Err("stored data kind and data columns do not agree".to_owned()),
    }
    builder
        .build_with_limit(EventSizeLimit::new(usize::MAX).expect("non-zero limit"))
        .map_err(|error| error.to_string())?
        .into_stored()
        .map_err(|error| error.to_string())
}

pub(crate) fn hydrate_page(row: DurableRow) -> Result<PagedEvent, String> {
    let tenant_id = TenantId::new(row.tenant_id.clone()).map_err(|error| error.to_string())?;
    let row_id = RowId::new(row.row_id).map_err(|error| error.to_string())?;
    let event = hydrate_event(&row.event_row())?;
    let state = row
        .state
        .ok_or_else(|| "event has no required delivery row".to_owned())?;
    let available_at = row
        .available_at
        .ok_or_else(|| "delivery row has no available_at".to_owned())?;
    let attempts = AttemptCount::new(
        row.attempts
            .ok_or_else(|| "delivery row has no attempts".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    let failure = parse_failure(row.last_failure_code, row.last_failure_detail)?;
    let available_at = parse_timestamp(&available_at)?;
    let delivery = match state.as_str() {
        "pending" => {
            require_absent("pending claim token", row.claim_token.as_ref())?;
            require_absent("pending claimed worker", row.claimed_by.as_ref())?;
            require_absent("pending claim expiry", row.claim_expires_at.as_ref())?;
            require_absent("pending delivered time", row.delivered_at.as_ref())?;
            require_absent("pending quarantine time", row.quarantined_at.as_ref())?;
            require_absent("pending quarantine reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::pending(available_at, attempts, failure)
        }
        "claimed" => {
            require_token_width(row.claim_token.as_deref())?;
            let worker = WorkerId::new(
                row.claimed_by
                    .ok_or_else(|| "claimed delivery has no worker".to_owned())?,
            )
            .map_err(|error| error.to_string())?;
            let expires_at = parse_timestamp(
                &row.claim_expires_at
                    .ok_or_else(|| "claimed delivery has no claim expiry".to_owned())?,
            )?;
            require_absent("claimed delivered time", row.delivered_at.as_ref())?;
            require_absent("claimed quarantine time", row.quarantined_at.as_ref())?;
            require_absent("claimed quarantine reason", row.quarantine_reason.as_ref())?;
            DeliverySnapshot::claimed(available_at, worker, expires_at, attempts, failure)
        }
        "delivered" => {
            require_absent("delivered claim token", row.claim_token.as_ref())?;
            require_absent("delivered claimed worker", row.claimed_by.as_ref())?;
            require_absent("delivered claim expiry", row.claim_expires_at.as_ref())?;
            let delivered_at = parse_timestamp(
                &row.delivered_at
                    .ok_or_else(|| "delivered delivery has no delivered time".to_owned())?,
            )?;
            require_absent("delivered quarantine time", row.quarantined_at.as_ref())?;
            require_absent(
                "delivered quarantine reason",
                row.quarantine_reason.as_ref(),
            )?;
            DeliverySnapshot::delivered(available_at, delivered_at, attempts, failure)
        }
        "quarantined" => {
            require_absent("quarantined claim token", row.claim_token.as_ref())?;
            require_absent("quarantined claimed worker", row.claimed_by.as_ref())?;
            require_absent("quarantined claim expiry", row.claim_expires_at.as_ref())?;
            require_absent("quarantined delivered time", row.delivered_at.as_ref())?;
            let quarantined_at = parse_timestamp(
                &row.quarantined_at
                    .ok_or_else(|| "quarantined delivery has no quarantine time".to_owned())?,
            )?;
            require_absent("quarantined delivered time", row.delivered_at.as_ref())?;
            let reason = QuarantineReason::new(
                row.quarantine_reason
                    .ok_or_else(|| "quarantined delivery has no quarantine reason".to_owned())?,
            )
            .map_err(|error| error.to_string())?;
            DeliverySnapshot::quarantined(available_at, quarantined_at, attempts, failure, reason)
        }
        state => return Err(format!("unknown delivery state {state:?}")),
    }
    .map_err(|error| error.to_string())?;
    PagedEvent::new(
        tenant_id,
        row_id,
        event,
        parse_timestamp(&row.enqueued_at)?,
        delivery,
    )
    .map_err(|error| error.to_string())
}

fn require_absent<T>(field: &str, value: Option<&T>) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{field} must be NULL for its delivery state"))
    } else {
        Ok(())
    }
}
fn require_token_width(value: Option<&[u8]>) -> Result<(), String> {
    match value {
        Some(value) if value.len() == 16 => Ok(()),
        Some(value) => Err(format!(
            "claimed delivery has an invalid claim token width: {}",
            value.len()
        )),
        None => Err("claimed delivery has no claim token".to_owned()),
    }
}
fn parse_failure(code: Option<String>, detail: Option<String>) -> Result<Option<Failure>, String> {
    match (code, detail) {
        (None, None) => Ok(None),
        (Some(code), Some(detail)) => Failure::new(code, detail)
            .map(Some)
            .map_err(|error| error.to_string()),
        _ => Err("delivery failure code and detail must be both NULL or non-NULL".to_owned()),
    }
}
