use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    bounds::{
        MAX_TRACESTATE_BYTES, MAX_TRACESTATE_KEY_BYTES, MAX_TRACESTATE_MEMBERS,
        MAX_TRACESTATE_SYSTEM_ID_BYTES, MAX_TRACESTATE_TENANT_ID_BYTES, MAX_TRACESTATE_VALUE_BYTES,
        TRACEPARENT_FLAGS_CHARS, TRACEPARENT_PARENT_ID_CHARS, TRACEPARENT_TRACE_ID_CHARS,
        TRACEPARENT_VERSION_CHARS,
    },
    error::{ValidationError, ValidationKind},
};

pub(crate) fn validate_string(
    field: &'static str,
    value: &str,
    maximum_bytes: Option<usize>,
    allow_empty: bool,
) -> Result<(), ValidationError> {
    if !allow_empty && value.is_empty() {
        return Err(ValidationError::new(field, ValidationKind::Empty));
    }

    if maximum_bytes.is_some_and(|max| value.len() > max) {
        return Err(ValidationError::new(field, ValidationKind::Length));
    }

    if value.chars().any(|character| {
        character.is_control()
            || (0xFDD0..=0xFDEF).contains(&(u32::from(character)))
            || u32::from(character) & 0xFFFF == 0xFFFF
            || u32::from(character) & 0xFFFF == 0xFFFE
    }) {
        return Err(ValidationError::new(field, ValidationKind::Characters));
    }

    Ok(())
}

pub(crate) fn validate_uri_reference(
    field: &'static str,
    value: &str,
    maximum_bytes: Option<usize>,
    allow_empty: bool,
) -> Result<(), ValidationError> {
    validate_string(field, value, maximum_bytes, allow_empty)?;
    fluent_uri::UriRef::parse(value)
        .map(|_| ())
        .map_err(|_| ValidationError::new(field, ValidationKind::Syntax))
}

pub(crate) fn validate_traceparent(value: &str) -> Result<(), ValidationError> {
    let mut parts = value.splitn(5, '-');
    let invalid = || ValidationError::new("traceparent", ValidationKind::TraceContext);
    let version = parts.next().ok_or_else(invalid)?;
    let trace_id = parts.next().ok_or_else(invalid)?;
    let parent_id = parts.next().ok_or_else(invalid)?;
    let flags = parts.next().ok_or_else(invalid)?;
    let future_fields = parts.next();
    let valid_lower_hex = |part: &str, width: usize| {
        part.len() == width
            && part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if !valid_lower_hex(version, TRACEPARENT_VERSION_CHARS)
        || !valid_lower_hex(trace_id, TRACEPARENT_TRACE_ID_CHARS)
        || !valid_lower_hex(parent_id, TRACEPARENT_PARENT_ID_CHARS)
        || !valid_lower_hex(flags, TRACEPARENT_FLAGS_CHARS)
        || version == "ff"
        || trace_id.bytes().all(|byte| byte == b'0')
        || parent_id.bytes().all(|byte| byte == b'0')
        || (version == "00" && (future_fields.is_some() || !matches!(flags, "00" | "01")))
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn validate_tracestate(value: &str) -> Result<(), ValidationError> {
    if value.len() > MAX_TRACESTATE_BYTES {
        return Err(ValidationError::new(
            "tracestate",
            ValidationKind::TraceContext,
        ));
    }

    let mut keys = std::collections::BTreeSet::new();
    let raw_members = value.split(',').collect::<Vec<_>>();
    if raw_members.len() > MAX_TRACESTATE_MEMBERS {
        return Err(ValidationError::new(
            "tracestate",
            ValidationKind::TraceContext,
        ));
    }

    for raw_member in raw_members {
        let member = raw_member.trim_matches([' ', '\t']);
        if member.is_empty() {
            continue;
        }

        let Some((key, member_value)) = member.split_once('=') else {
            return Err(ValidationError::new(
                "tracestate",
                ValidationKind::TraceContext,
            ));
        };

        if key.is_empty()
            || key.len() > MAX_TRACESTATE_KEY_BYTES
            || member_value.is_empty()
            || member_value.len() > MAX_TRACESTATE_VALUE_BYTES
            || !valid_tracestate_key(key)
            || !member_value
                .bytes()
                .all(|byte| (0x20..=0x7e).contains(&byte) && byte != b'=')
            || !keys.insert(key)
        {
            return Err(ValidationError::new(
                "tracestate",
                ValidationKind::TraceContext,
            ));
        }
    }

    Ok(())
}

fn valid_tracestate_key(key: &str) -> bool {
    match key.split_once('@') {
        Some((tenant, system)) => {
            valid_tracestate_key_part(tenant, MAX_TRACESTATE_TENANT_ID_BYTES, true)
                && valid_tracestate_key_part(system, MAX_TRACESTATE_SYSTEM_ID_BYTES, false)
        }
        None => valid_tracestate_key_part(key, MAX_TRACESTATE_KEY_BYTES, false),
    }
}

fn valid_tracestate_key_part(part: &str, maximum: usize, tenant: bool) -> bool {
    let Some(first) = part.as_bytes().first() else {
        return false;
    };
    part.len() <= maximum
        && (first.is_ascii_lowercase() || (tenant && first.is_ascii_digit()))
        && part.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_*-/".contains(&byte)
        })
}

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    let formatted = value
        .to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("validated timestamps are RFC 3339 representable");
    let Some((whole, fraction)) = formatted.split_once('.') else {
        return formatted;
    };

    let fraction = fraction.strip_suffix('Z').unwrap_or(fraction);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        format!("{whole}Z")
    } else {
        format!("{whole}.{fraction}Z")
    }
}
