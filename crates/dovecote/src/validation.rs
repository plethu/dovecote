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
            || (0xFDD0..=0xFDEF).contains(&(character as u32))
            || character as u32 & 0xFFFF == 0xFFFF
            || character as u32 & 0xFFFF == 0xFFFE
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
    const VERSION_END: usize = TRACEPARENT_VERSION_CHARS;
    const TRACE_ID_START: usize = VERSION_END + 1;
    const TRACE_ID_END: usize = TRACE_ID_START + TRACEPARENT_TRACE_ID_CHARS;
    const PARENT_ID_START: usize = TRACE_ID_END + 1;
    const PARENT_ID_END: usize = PARENT_ID_START + TRACEPARENT_PARENT_ID_CHARS;
    const FLAGS_START: usize = PARENT_ID_END + 1;
    const CURRENT_LENGTH: usize = FLAGS_START + TRACEPARENT_FLAGS_CHARS;

    let bytes = value.as_bytes();
    let valid_lower_hex = |part: &[u8]| {
        part.iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    };
    let invalid = bytes.len() < CURRENT_LENGTH
        || bytes.get(VERSION_END) != Some(&b'-')
        || bytes.get(TRACE_ID_END) != Some(&b'-')
        || bytes.get(PARENT_ID_END) != Some(&b'-')
        || !valid_lower_hex(&bytes[..VERSION_END])
        || !valid_lower_hex(&bytes[TRACE_ID_START..TRACE_ID_END])
        || !valid_lower_hex(&bytes[PARENT_ID_START..PARENT_ID_END])
        || !valid_lower_hex(&bytes[FLAGS_START..CURRENT_LENGTH])
        || &bytes[..VERSION_END] == b"ff"
        || bytes[TRACE_ID_START..TRACE_ID_END]
            .iter()
            .all(|byte| *byte == b'0')
        || bytes[PARENT_ID_START..PARENT_ID_END]
            .iter()
            .all(|byte| *byte == b'0')
        || if &bytes[..VERSION_END] == b"00" {
            bytes.len() != CURRENT_LENGTH
                || !matches!(&bytes[FLAGS_START..CURRENT_LENGTH], b"00" | b"01")
        } else {
            bytes.len() > CURRENT_LENGTH && bytes.get(CURRENT_LENGTH) != Some(&b'-')
        };

    if invalid {
        return Err(ValidationError::new(
            "traceparent",
            ValidationKind::TraceContext,
        ));
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
    let parts: Vec<_> = key.split('@').collect();
    if parts.len() > 2 {
        return false;
    }
    parts.iter().enumerate().all(|(index, part)| {
        !part.is_empty()
            && part.len()
                <= if index == 0 && parts.len() == 2 {
                    MAX_TRACESTATE_TENANT_ID_BYTES
                } else if index == 1 {
                    MAX_TRACESTATE_SYSTEM_ID_BYTES
                } else {
                    MAX_TRACESTATE_KEY_BYTES
                }
            && if parts.len() == 1 {
                part.as_bytes()[0].is_ascii_lowercase()
            } else if index == 0 {
                part.as_bytes()[0].is_ascii_lowercase() || part.as_bytes()[0].is_ascii_digit()
            } else {
                part.as_bytes()[0].is_ascii_lowercase()
            }
            && part.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_*-/".contains(&byte)
            })
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
