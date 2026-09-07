//! Pure normalization helpers for catalog expressions.

fn normalize_trigger(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .filter(|c| !c.is_ascii_whitespace() && *c != '`')
        .collect()
}
pub(super) fn trigger_action_matches(kind: &str, action: &str) -> bool {
    let expected = match kind {
        "insert" => {
            "beginifnew.row_id<0thensignal sqlstate'45000'setmessage_text='dovecote row_id must be positive';endif;end"
        }
        "update" => {
            "beginifnew.row_id<=0ornew.row_id<>old.row_idthensignal sqlstate'45000'setmessage_text='dovecote row_id must be positive';endif;end"
        }
        _ => return false,
    };
    normalize_trigger(action) == expected.replace(' ', "")
}
pub(super) fn normalize_check_clause(name: &str, clause: &str) -> String {
    let mut normalized = normalize(clause);

    // MySQL and MariaDB catalog output may decorate the same ASCII binary
    // literal as _binary'...' or _utf8mb4'...'.  These are the only literal
    // introducers accepted here; the literal and its complete expression must
    // still compare equal below.
    normalized = normalized
        .replace("_binary'", "'")
        .replace("_utf8mb4'", "'");

    // MySQL reports OCTET_LENGTH(binary/blob) as LENGTH(binary/blob) on some
    // releases.  Canonicalize only the binary/blob operands used by this
    // migration; LENGTH on another expression remains a different clause.
    normalized = replace_length_aliases(normalized, binary_length_columns(name));

    strip_redundant_outer_parentheses(&normalized)
}

fn binary_length_columns(name: &str) -> &'static [&'static str] {
    match name {
        "dovecote_events_tenant_size" => &["tenant_id"],
        "dovecote_events_tenant_nonempty" => &["tenant_id"],
        "dovecote_events_stream_size" => &["stream"],
        "dovecote_events_event_id_size" => &["event_id"],
        "dovecote_events_source_size" => &["source"],
        "dovecote_events_event_type_size" => &["event_type"],
        "dovecote_events_subject_size" => &["subject"],
        "dovecote_events_content_type_size" => &["datacontenttype"],
        "dovecote_events_schema_size" => &["dataschema"],
        "dovecote_events_partition_size" => &["partitionkey"],
        "dovecote_events_identity_size" => &["source", "event_id"],
        "dovecote_events_content_type" => &["data"],
        "dovecote_deliveries_tenant_size" => &["tenant_id"],
        "dovecote_deliveries_tenant_nonempty" => &["tenant_id"],
        "dovecote_deliveries_token_size" => &["claim_token"],
        "dovecote_deliveries_worker_size" => &["claimed_by"],
        "dovecote_deliveries_failure_code_size" => &["last_failure_code"],
        "dovecote_deliveries_failure_detail_size" => &["last_failure_detail"],
        "dovecote_deliveries_quarantine_size" => &["quarantine_reason"],
        _ => &[],
    }
}

fn strip_redundant_outer_parentheses(mut value: &str) -> String {
    while let Some(inner) = value
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    {
        if !outer_parentheses_enclose_expression(value) {
            break;
        }
        value = inner;
    }
    value.to_owned()
}

#[derive(Clone, Copy)]
enum QuoteState {
    Outside,
    Quoted,
    Escaped,
}

fn outer_parentheses_enclose_expression(value: &str) -> bool {
    let mut bytes = value.bytes().peekable();
    let mut depth = 0_u32;
    let mut quoted = QuoteState::Outside;
    while let Some(byte) = bytes.next() {
        match (quoted, byte) {
            (QuoteState::Escaped, _) => quoted = QuoteState::Quoted,
            (QuoteState::Quoted, b'\\') => quoted = QuoteState::Escaped,
            (QuoteState::Quoted, b'\'') if bytes.peek() == Some(&b'\'') => {
                bytes.next();
            }
            (QuoteState::Quoted, b'\'') => quoted = QuoteState::Outside,
            (QuoteState::Quoted, _) => {}
            (QuoteState::Outside, b'\'') => quoted = QuoteState::Quoted,
            (QuoteState::Outside, b'(') => {
                let Some(next_depth) = depth.checked_add(1) else {
                    return false;
                };
                depth = next_depth;
            }
            (QuoteState::Outside, b')') => {
                let Some(next_depth) = depth.checked_sub(1) else {
                    return false;
                };
                depth = next_depth;
                if depth == 0 && bytes.peek().is_some() {
                    return false;
                }
            }
            (QuoteState::Outside, _) => {}
        }
    }
    matches!(quoted, QuoteState::Outside) && depth == 0
}

pub(super) fn normalize(value: &str) -> String {
    // MySQL's catalog serializes the quote delimiters of binary literals as
    // `\'`; unescape that decoration before tracking SQL string boundaries.
    let value = value.replace("\\'", "'");
    let mut normalized = String::with_capacity(value.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in value.chars() {
        match (in_string, escaped, character) {
            (true, true, '\'') => {
                // Preserve a quote escaped inside a literal.
                normalized.push('\'');
                escaped = false;
            }
            (true, true, _) => {
                normalized.push('\\');
                normalized.push(character);
                escaped = false;
            }
            (true, false, '\\') => escaped = true,
            (true, false, '\'') => {
                normalized.push(character);
                in_string = false;
            }
            (true, false, _) => normalized.push(character),
            (false, _, '\'') => {
                in_string = true;
                normalized.push(character);
            }
            (false, _, character) if character.is_ascii_whitespace() || character == '`' => {}
            (false, _, character) => normalized.push(character.to_ascii_lowercase()),
        }
    }

    if escaped {
        normalized.push('\\');
    }
    normalized
}

pub(super) fn normalize_generated_expression(value: &str) -> String {
    let normalized = normalize(value)
        .replace('\\', "")
        .replace("_binary'", "'")
        .replace("_utf8mb4'", "'");

    // MySQL/MariaDB catalog output may render OCTET_LENGTH on binary columns
    // as LENGTH.  Canonicalize only the two operands in the identity
    // expression; an altered function or operand remains visibly different.
    replace_length_aliases(normalized, &["tenant_id", "source"])
}

fn replace_length_aliases(mut normalized: String, columns: &[&str]) -> String {
    for column in columns {
        let length = format!("length({column})");
        let octet_length = format!("octet_length({column})");
        let mut remaining = normalized.as_str();
        let mut rewritten = String::with_capacity(normalized.len());
        while let Some((prefix, suffix)) = remaining.split_once(&length) {
            rewritten.push_str(prefix);
            rewritten.push_str(if prefix.ends_with("octet_") {
                &length
            } else {
                &octet_length
            });
            remaining = suffix;
        }
        rewritten.push_str(remaining);
        normalized = rewritten;
    }
    normalized
}
