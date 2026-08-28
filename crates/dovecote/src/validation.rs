use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    bounds::{
        MAX_TRACESTATE_BYTES, MAX_TRACESTATE_KEY_BYTES, MAX_TRACESTATE_MEMBERS,
        MAX_TRACESTATE_SYSTEM_ID_BYTES, MAX_TRACESTATE_TENANT_ID_BYTES, MAX_TRACESTATE_VALUE_BYTES,
        TRACEPARENT_FIELDS, TRACEPARENT_FLAGS_CHARS, TRACEPARENT_PARENT_ID_CHARS,
        TRACEPARENT_TRACE_ID_CHARS, TRACEPARENT_VERSION_CHARS,
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
    if !value.is_ascii() {
        return Err(ValidationError::new(field, ValidationKind::Syntax));
    }

    if value
        .as_bytes()
        .windows(1)
        .enumerate()
        .any(|(index, byte)| {
            byte[0] == b'%'
                && (index + 2 >= value.len()
                    || !value.as_bytes()[index + 1].is_ascii_hexdigit()
                    || !value.as_bytes()[index + 2].is_ascii_hexdigit())
        })
    {
        return Err(ValidationError::new(field, ValidationKind::Syntax));
    }

    if value.starts_with("//") {
        url::Url::parse(&format!("https:{value}"))
            .map_err(|_| ValidationError::new(field, ValidationKind::Syntax))?;
        return Ok(());
    }

    if let Some(colon) = value.find(':') {
        let first_delimiter = value.find(['/', '?', '#']).unwrap_or(value.len());
        if colon < first_delimiter {
            let scheme = &value[..colon];
            if scheme.is_empty()
                || !scheme.as_bytes()[0].is_ascii_alphabetic()
                || !scheme
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"+-.".contains(&byte))
            {
                return Err(ValidationError::new(field, ValidationKind::Syntax));
            }
            url::Url::parse(value)
                .map_err(|_| ValidationError::new(field, ValidationKind::Syntax))?;
            return Ok(());
        }
    }

    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len()
                    || !bytes[index + 1].is_ascii_hexdigit()
                    || !bytes[index + 2].is_ascii_hexdigit()
                {
                    return Err(ValidationError::new(field, ValidationKind::Syntax));
                }
                index += 3;
            }
            byte if byte.is_ascii_alphanumeric() || b"-._~:/?#@!$&'()*+,;=".contains(&byte) => {
                index += 1;
            }
            _ if bytes[index].is_ascii() => {
                return Err(ValidationError::new(field, ValidationKind::Syntax));
            }
            _ => return Err(ValidationError::new(field, ValidationKind::Syntax)),
        }
    }

    Ok(())
}

pub(crate) fn validate_traceparent(value: &str) -> Result<(), ValidationError> {
    let parts: Vec<_> = value.split('-').collect();
    if parts.len() != TRACEPARENT_FIELDS
        || parts[0].len() != TRACEPARENT_VERSION_CHARS
        || parts[1].len() != TRACEPARENT_TRACE_ID_CHARS
        || parts[2].len() != TRACEPARENT_PARENT_ID_CHARS
        || parts[3].len() != TRACEPARENT_FLAGS_CHARS
        || parts.iter().any(|part| {
            !part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || parts[0].eq_ignore_ascii_case("ff")
        || parts[1].chars().all(|character| character == '0')
        || parts[2].chars().all(|character| character == '0')
    {
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
    let mut members = 0;
    for raw_member in value.split(',') {
        let member = raw_member.trim_matches([' ', '\t']);
        if member.is_empty() {
            continue;
        }

        members += 1;
        let Some((key, member_value)) = member.split_once('=') else {
            return Err(ValidationError::new(
                "tracestate",
                ValidationKind::TraceContext,
            ));
        };

        if members > MAX_TRACESTATE_MEMBERS
            || key.is_empty()
            || key.len() > MAX_TRACESTATE_KEY_BYTES
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
