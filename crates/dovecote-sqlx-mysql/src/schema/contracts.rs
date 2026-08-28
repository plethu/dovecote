//! Schema column and CHECK-constraint contracts.

use super::normalization::normalize_check_clause;

pub(super) struct ColumnSpec {
    pub(super) name: &'static str,
    pub(super) data_type: &'static str,
    pub(super) max: Option<i64>,
    pub(super) exact_column_type: Option<&'static str>,
    pub(super) nullable: bool,
    pub(super) identity: bool,
    pub(super) default: Option<&'static str>,
    pub(super) generated: Option<&'static str>,
    pub(super) generation_expression: Option<&'static str>,
}
impl ColumnSpec {
    pub(super) const fn required(
        name: &'static str,
        data_type: &'static str,
        max: Option<i64>,
        identity: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            max,
            exact_column_type: None,
            nullable: false,
            identity,
            default: None,
            generated: None,
            generation_expression: None,
        }
    }
    pub(super) const fn optional(
        name: &'static str,
        data_type: &'static str,
        max: Option<i64>,
    ) -> Self {
        Self {
            name,
            data_type,
            max,
            exact_column_type: None,
            nullable: true,
            identity: false,
            default: None,
            generated: None,
            generation_expression: None,
        }
    }
    pub(super) const fn required_default(
        name: &'static str,
        data_type: &'static str,
        default: &'static str,
    ) -> Self {
        Self {
            name,
            data_type,
            max: None,
            exact_column_type: None,
            nullable: false,
            identity: false,
            default: Some(default),
            generated: None,
            generation_expression: None,
        }
    }

    pub(super) const fn required_exact_type(
        name: &'static str,
        data_type: &'static str,
        column_type: &'static str,
    ) -> Self {
        Self {
            name,
            data_type,
            max: None,
            exact_column_type: Some(column_type),
            nullable: false,
            identity: false,
            default: None,
            generated: None,
            generation_expression: None,
        }
    }

    pub(super) const fn stored_generated(
        name: &'static str,
        data_type: &'static str,
        max: Option<i64>,
        expression: &'static str,
    ) -> Self {
        Self {
            name,
            data_type,
            max,
            exact_column_type: None,
            nullable: true,
            identity: false,
            default: None,
            generated: Some("STORED GENERATED"),
            generation_expression: Some(expression),
        }
    }
}

pub(super) fn marker_columns() -> [ColumnSpec; 5] {
    [
        ColumnSpec::required("schema_version", "int", None, false),
        ColumnSpec::required("minimum_crate_major", "smallint", None, false),
        ColumnSpec::required("minimum_crate_minor", "smallint", None, false),
        ColumnSpec::required("minimum_crate_patch", "smallint", None, false),
        ColumnSpec::required_exact_type("rolling_compatible", "tinyint", "tinyint(1)"),
    ]
}

pub(super) const IDENTITY_KEY_GENERATION_EXPRESSION: &str = "CONCAT(LPAD(OCTET_LENGTH(tenant_id), 3, '0'), tenant_id, LPAD(OCTET_LENGTH(source), 4, '0'), source, event_id)";
pub(super) const CHECK_NAMES: &[&str] = &[
    "dovecote_events_specversion",
    "dovecote_events_tenant_size",
    "dovecote_events_tenant_nonempty",
    "dovecote_events_stream_size",
    "dovecote_events_event_id_size",
    "dovecote_events_source_size",
    "dovecote_events_event_type_size",
    "dovecote_events_subject_size",
    "dovecote_events_content_type_size",
    "dovecote_events_schema_size",
    "dovecote_events_partition_size",
    "dovecote_events_identity_size",
    "dovecote_events_data_kind",
    "dovecote_events_data_pair",
    "dovecote_events_content_type",
    "dovecote_deliveries_state",
    "dovecote_deliveries_tenant_size",
    "dovecote_deliveries_tenant_nonempty",
    "dovecote_deliveries_attempts",
    "dovecote_deliveries_token_size",
    "dovecote_deliveries_worker_size",
    "dovecote_deliveries_failure_code_size",
    "dovecote_deliveries_failure_detail_size",
    "dovecote_deliveries_quarantine_size",
    "dovecote_deliveries_failure_pair",
    "dovecote_deliveries_state_shape",
];

pub(super) const MARKER_CHECK_NAMES: &[&str] = &[
    "dovecote_schema_version_supported",
    "dovecote_schema_minimum_nonnegative",
];

pub(super) fn marker_check_clause_is_plausible(name: &str, clause: &str) -> bool {
    let Some(expected) = expected_marker_check_clause(name) else {
        return false;
    };
    let actual = normalize_check_clause(name, clause);
    actual == normalize_check_clause(name, expected)
        || marker_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
}

fn expected_marker_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_schema_version_supported" => "schema_version = 2",
        "dovecote_schema_minimum_nonnegative" => {
            "minimum_crate_major >= 0 AND minimum_crate_minor >= 0 AND minimum_crate_patch >= 0"
        }
        _ => return None,
    })
}

// MySQL 8.4 renders this complete CHECK expression with grouping around each
// boolean atom. Keep the catalog form explicit; inner parentheses are not
// generally discarded because they can change the meaning of a CHECK.
fn marker_catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_schema_minimum_nonnegative" => {
            "(minimum_crate_major >= 0) AND (minimum_crate_minor >= 0) AND (minimum_crate_patch >= 0)"
        }
        _ => return None,
    })
}

pub(super) fn check_clause_is_plausible(name: &str, clause: &str) -> bool {
    let Some(expected) = expected_check_clause(name) else {
        return false;
    };

    let actual = normalize_check_clause(name, clause);
    actual == normalize_check_clause(name, expected)
        || catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mariadb_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mysql_unwrapped_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
        || mysql_predicate_grouped_catalog_check_clause(name)
            .is_some_and(|catalog| actual == normalize_check_clause(name, catalog))
}

// These expressions are copied from migrations/0001_dovecote.sql.  The
// migration is the semantic authority; catalog decoration is handled by
// normalize_check_clause, never by looking for a matching fragment.
pub(super) fn expected_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_events_specversion" => "specversion = _binary '1.0'",
        "dovecote_events_tenant_size" => "OCTET_LENGTH(tenant_id) <= 255",
        "dovecote_events_tenant_nonempty" => "OCTET_LENGTH(tenant_id) > 0",
        "dovecote_events_stream_size" => "OCTET_LENGTH(stream) <= 255",
        "dovecote_events_event_id_size" => "OCTET_LENGTH(event_id) <= 1024",
        "dovecote_events_source_size" => "OCTET_LENGTH(source) <= 2048",
        "dovecote_events_event_type_size" => "OCTET_LENGTH(event_type) <= 1024",
        "dovecote_events_subject_size" => "subject IS NULL OR OCTET_LENGTH(subject) <= 2048",
        "dovecote_events_content_type_size" => {
            "datacontenttype IS NULL OR OCTET_LENGTH(datacontenttype) <= 255"
        }
        "dovecote_events_schema_size" => "dataschema IS NULL OR OCTET_LENGTH(dataschema) <= 2048",
        "dovecote_events_partition_size" => {
            "partitionkey IS NULL OR OCTET_LENGTH(partitionkey) <= 255"
        }
        "dovecote_events_identity_size" => "OCTET_LENGTH(source) + OCTET_LENGTH(event_id) <= 2048",
        "dovecote_events_data_kind" => {
            "data_kind IS NULL OR data_kind IN (_binary 'json', _binary 'binary')"
        }
        "dovecote_events_data_pair" => "(data_kind IS NULL) = (data IS NULL)",
        "dovecote_events_content_type" => {
            "data IS NULL OR OCTET_LENGTH(data) = 0 OR datacontenttype IS NOT NULL"
        }
        "dovecote_deliveries_state" => {
            "state IN (_binary 'pending', _binary 'claimed', _binary 'delivered', _binary 'quarantined')"
        }
        "dovecote_deliveries_tenant_size" => "OCTET_LENGTH(tenant_id) <= 255",
        "dovecote_deliveries_tenant_nonempty" => "OCTET_LENGTH(tenant_id) > 0",
        "dovecote_deliveries_attempts" => "attempts >= 0",
        "dovecote_deliveries_token_size" => "claim_token IS NULL OR OCTET_LENGTH(claim_token) = 16",
        "dovecote_deliveries_worker_size" => {
            "claimed_by IS NULL OR OCTET_LENGTH(claimed_by) <= 255"
        }
        "dovecote_deliveries_failure_code_size" => {
            "last_failure_code IS NULL OR OCTET_LENGTH(last_failure_code) <= 128"
        }
        "dovecote_deliveries_failure_detail_size" => {
            "last_failure_detail IS NULL OR OCTET_LENGTH(last_failure_detail) <= 2048"
        }
        "dovecote_deliveries_quarantine_size" => {
            "quarantine_reason IS NULL OR OCTET_LENGTH(quarantine_reason) <= 2048"
        }
        "dovecote_deliveries_failure_pair" => {
            "(last_failure_code IS NULL) = (last_failure_detail IS NULL)"
        }
        "dovecote_deliveries_state_shape" => {
            "(state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)"
        }
        _ => return None,
    })
}

// CHECK_CLAUSE is rendered from the server's expression tree.  MySQL 8.4
// preserves the migration expression while adding grouping around each
// boolean operand; these are the complete captured forms that are accepted as
// an alternative to the migration source expression.  Keep this list exact:
// no part of it is used as a substring predicate.
pub(super) fn catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_events_subject_size" => "(subject IS NULL) OR (OCTET_LENGTH(subject) <= 2048)",
        "dovecote_events_content_type_size" => {
            "(datacontenttype IS NULL) OR (OCTET_LENGTH(datacontenttype) <= 255)"
        }
        "dovecote_events_schema_size" => {
            "(dataschema IS NULL) OR (OCTET_LENGTH(dataschema) <= 2048)"
        }
        "dovecote_events_partition_size" => {
            "(partitionkey IS NULL) OR (OCTET_LENGTH(partitionkey) <= 255)"
        }
        "dovecote_events_identity_size" => {
            "(OCTET_LENGTH(source) + OCTET_LENGTH(event_id)) <= 2048"
        }
        "dovecote_events_data_kind" => {
            "(data_kind IS NULL) OR (data_kind IN (_binary 'json', _binary 'binary'))"
        }
        "dovecote_events_data_pair" => "data_kind IS NULL = (data IS NULL)",
        "dovecote_events_content_type" => {
            "(data IS NULL) OR (OCTET_LENGTH(data) = 0) OR (datacontenttype IS NOT NULL)"
        }
        "dovecote_deliveries_token_size" => {
            "(claim_token IS NULL) OR (OCTET_LENGTH(claim_token) = 16)"
        }
        "dovecote_deliveries_worker_size" => {
            "(claimed_by IS NULL) OR (OCTET_LENGTH(claimed_by) <= 255)"
        }
        "dovecote_deliveries_failure_code_size" => {
            "(last_failure_code IS NULL) OR (OCTET_LENGTH(last_failure_code) <= 128)"
        }
        "dovecote_deliveries_failure_detail_size" => {
            "(last_failure_detail IS NULL) OR (OCTET_LENGTH(last_failure_detail) <= 2048)"
        }
        "dovecote_deliveries_quarantine_size" => {
            "(quarantine_reason IS NULL) OR (OCTET_LENGTH(quarantine_reason) <= 2048)"
        }
        "dovecote_deliveries_failure_pair" => {
            "last_failure_code IS NULL = (last_failure_detail IS NULL)"
        }
        "dovecote_deliveries_state_shape" => {
            "(state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL) OR (state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)"
        }
        _ => return None,
    })
}

pub(super) fn mariadb_catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_deliveries_state_shape" => {
            "state = _binary 'pending' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'claimed' AND claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL AND delivered_at IS NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'delivered' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NOT NULL AND quarantined_at IS NULL AND quarantine_reason IS NULL OR state = _binary 'quarantined' AND claim_token IS NULL AND claimed_by IS NULL AND claim_expires_at IS NULL AND delivered_at IS NULL AND quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL"
        }
        _ => return None,
    })
}

// MySQL 8.4 may remove redundant branch grouping when serializing this
// expression. This is a complete, ordered alternative—not a fragment match.
pub(super) fn mysql_unwrapped_catalog_check_clause(name: &str) -> Option<&'static str> {
    mariadb_catalog_check_clause(name)
}

pub(super) fn mysql_predicate_grouped_catalog_check_clause(name: &str) -> Option<&'static str> {
    Some(match name {
        "dovecote_deliveries_state_shape" => {
            "((state = _binary 'pending') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'claimed') AND (claim_token IS NOT NULL) AND (claimed_by IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'delivered') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NOT NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR ((state = _binary 'quarantined') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NOT NULL) AND (quarantine_reason IS NOT NULL))"
        }
        _ => return None,
    })
}
