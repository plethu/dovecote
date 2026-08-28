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
    normalize_trigger(action) == expected.replace(" ", "")
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
    for column in binary_length_columns(name) {
        let length = format!("length({column})");
        let octet_length = format!("octet_length({column})");
        let mut offset = 0;
        let mut rewritten = String::with_capacity(normalized.len());
        while let Some(found) = normalized[offset..].find(&length) {
            let start = offset + found;
            rewritten.push_str(&normalized[offset..start]);
            if normalized.as_bytes()[..start].ends_with(b"octet_") {
                rewritten.push_str(&normalized[start..start + length.len()]);
            } else {
                rewritten.push_str(&octet_length);
            }
            offset = start + length.len();
        }
        rewritten.push_str(&normalized[offset..]);
        normalized = rewritten;
    }

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

fn strip_redundant_outer_parentheses(value: &str) -> String {
    let mut value = value;
    while value.starts_with('(')
        && value.ends_with(')')
        && outer_parentheses_enclose_expression(value)
    {
        value = &value[1..value.len() - 1];
    }
    value.to_owned()
}

fn outer_parentheses_enclose_expression(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        // This is a byte-level lexer state machine, not ordered policy.
        // ast-grep-ignore: rust-elseif-cascade
        if in_string {
            let (still_in_string, escaped_next, skip_next) =
                quoted_byte_state(bytes, index, escaped);
            in_string = still_in_string;
            escaped = escaped_next;
            index += usize::from(skip_next);
        // ast-grep-ignore: rust-elseif-cascade
        } else if byte == b'\'' {
            in_string = true;
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            if depth == 0 {
                return false;
            }
            depth -= 1;
            if depth == 0 && index != bytes.len() - 1 {
                return false;
            }
        }
        index += 1;
    }
    !in_string && depth == 0
}

fn quoted_byte_state(bytes: &[u8], index: usize, escaped: bool) -> (bool, bool, bool) {
    match (bytes[index], escaped) {
        (_, true) => (true, false, false),
        (b'\\', false) => (true, true, false),
        (b'\'', false) if bytes.get(index + 1) == Some(&b'\'') => (true, false, true),
        (b'\'', false) => (false, false, false),
        (_, false) => (true, false, false),
    }
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
        let mut offset = 0;
        let mut rewritten = String::with_capacity(normalized.len());
        while let Some(found) = normalized[offset..].find(&length) {
            let start = offset + found;
            rewritten.push_str(&normalized[offset..start]);
            if normalized.as_bytes()[..start].ends_with(b"octet_") {
                rewritten.push_str(&normalized[start..start + length.len()]);
            } else {
                rewritten.push_str(&octet_length);
            }
            offset = start + length.len();
        }
        rewritten.push_str(&normalized[offset..]);
        normalized = rewritten;
    }
    normalized
}
