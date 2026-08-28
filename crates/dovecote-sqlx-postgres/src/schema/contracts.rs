use super::normalization::normalize_sql;
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub(crate) struct ConstraintInfo {
    pub(crate) table_name: String,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: Option<String>,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) delete_action: Option<String>,
    pub(crate) validated: bool,
    pub(crate) deferrable: bool,
    pub(crate) deferred: bool,
    pub(crate) definition: String,
}

pub(crate) struct ConstraintContract {
    pub(crate) name: &'static str,
    table_name: &'static str,
    kind: &'static str,
    columns: &'static [&'static str],
    referenced_table: Option<&'static str>,
    referenced_columns: &'static [&'static str],
    delete_action: Option<&'static str>,
    definition_variants: &'static [&'static str],
}

impl ConstraintContract {
    pub(crate) fn check(
        name: &'static str,
        table_name: &'static str,
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "c",
            columns: &[],
            referenced_table: None,
            referenced_columns: &[],
            delete_action: None,
            definition_variants,
        }
    }

    pub(crate) fn primary_key(
        name: &'static str,
        table_name: &'static str,
        columns: &'static [&'static str],
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "p",
            columns,
            referenced_table: None,
            referenced_columns: &[],
            delete_action: None,
            definition_variants,
        }
    }

    pub(crate) fn unique(
        name: &'static str,
        table_name: &'static str,
        columns: &'static [&'static str],
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "u",
            columns,
            referenced_table: None,
            referenced_columns: &[],
            delete_action: None,
            definition_variants,
        }
    }

    pub(crate) fn foreign_key(
        name: &'static str,
        table_name: &'static str,
        columns: &'static [&'static str],
        referenced_table: &'static str,
        referenced_columns: &'static [&'static str],
        delete_action: &'static str,
        definition_variants: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            table_name,
            kind: "f",
            columns,
            referenced_table: Some(referenced_table),
            referenced_columns,
            delete_action: Some(delete_action),
            definition_variants,
        }
    }
}

impl ConstraintInfo {
    pub(crate) fn matches(&self, expected: &ConstraintContract) -> bool {
        let definition = normalize_sql(&self.definition);
        let columns_match = self.kind == "c"
            || self.columns
                == expected
                    .columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>();
        self.table_name == expected.table_name
            && self.kind == expected.kind
            && columns_match
            && self.referenced_table.as_deref() == expected.referenced_table
            && self.referenced_columns
                == expected
                    .referenced_columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            && self.delete_action.as_deref() == expected.delete_action
            && self.validated
            && !self.deferrable
            && !self.deferred
            && expected
                .definition_variants
                .iter()
                .any(|variant| definition == normalize_sql(variant))
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct IndexInfo {
    pub(crate) table_name: String,
    pub(crate) name: String,
    pub(crate) access_method: String,
    pub(crate) is_unique: bool,
    pub(crate) is_valid: bool,
    pub(crate) is_ready: bool,
    pub(crate) has_predicate: bool,
    pub(crate) key_columns: i16,
    pub(crate) total_columns: i16,
    pub(crate) options: Vec<i16>,
    pub(crate) columns: Vec<String>,
    pub(crate) collations: Vec<String>,
}

pub(crate) struct IndexContract {
    pub(crate) name: &'static str,
    pub(crate) table_name: &'static str,
    pub(crate) is_unique: bool,
    pub(crate) columns: &'static [&'static str],
    pub(crate) collations: Option<&'static [&'static str]>,
}

impl IndexContract {
    pub(crate) fn new(
        name: &'static str,
        table_name: &'static str,
        is_unique: bool,
        columns: &'static [&'static str],
        collations: Option<&'static [&'static str]>,
    ) -> Self {
        Self {
            name,
            table_name,
            is_unique,
            columns,
            collations,
        }
    }
}

impl IndexInfo {
    pub(crate) fn matches(&self, expected: &IndexContract) -> bool {
        self.table_name == expected.table_name
            && self.access_method == "btree"
            && self.is_unique == expected.is_unique
            && self.is_valid
            && self.is_ready
            && !self.has_predicate
            && self.key_columns == i16::try_from(expected.columns.len()).unwrap_or(i16::MAX)
            && self.total_columns == self.key_columns
            && self.options == vec![0; expected.columns.len()]
            && self.columns
                == expected
                    .columns
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            && expected.collations.is_none_or(|collations| {
                self.collations
                    == collations
                        .iter()
                        .map(|value| (*value).to_owned())
                        .collect::<Vec<_>>()
            })
    }
}

pub(crate) fn expected_constraints() -> Vec<ConstraintContract> {
    vec![
        ConstraintContract::check(
            "dovecote_schema_version_supported",
            "dovecote_schema",
            &["CHECK ((schema_version = 2))"],
        ),
        ConstraintContract::check(
            "dovecote_schema_minimum_nonnegative",
            "dovecote_schema",
            &[
                "CHECK (((minimum_crate_major >= 0) AND (minimum_crate_minor >= 0) AND (minimum_crate_patch >= 0)))",
            ],
        ),
        ConstraintContract::primary_key(
            "dovecote_schema_pkey",
            "dovecote_schema",
            &["schema_version"],
            &["PRIMARY KEY (schema_version)"],
        ),
        ConstraintContract::check(
            "dovecote_events_row_id_positive",
            "dovecote_events",
            &["CHECK ((row_id > 0))"],
        ),
        ConstraintContract::check(
            "dovecote_events_tenant_size",
            "dovecote_events",
            &["CHECK ((octet_length((tenant_id)) <= 255))"],
        ),
        ConstraintContract::check(
            "dovecote_events_tenant_nonempty",
            "dovecote_events",
            &["CHECK ((octet_length((tenant_id)) > 0))"],
        ),
        ConstraintContract::primary_key(
            "dovecote_events_pkey",
            "dovecote_events",
            &["row_id"],
            &["PRIMARY KEY (row_id)"],
        ),
        ConstraintContract::unique(
            "dovecote_events_tenant_row_unique",
            "dovecote_events",
            &["tenant_id", "row_id"],
            &["UNIQUE (tenant_id, row_id)"],
        ),
        ConstraintContract::check(
            "dovecote_events_specversion",
            "dovecote_events",
            &["CHECK (((specversion) = '1.0'))"],
        ),
        ConstraintContract::check(
            "dovecote_events_stream_size",
            "dovecote_events",
            &["CHECK ((octet_length((stream)) <= 255))"],
        ),
        ConstraintContract::check(
            "dovecote_events_event_id_size",
            "dovecote_events",
            &["CHECK ((octet_length((event_id)) <= 1024))"],
        ),
        ConstraintContract::check(
            "dovecote_events_source_size",
            "dovecote_events",
            &["CHECK ((octet_length((source)) <= 2048))"],
        ),
        ConstraintContract::check(
            "dovecote_events_event_type_size",
            "dovecote_events",
            &["CHECK ((octet_length((event_type)) <= 1024))"],
        ),
        ConstraintContract::check(
            "dovecote_events_subject_size",
            "dovecote_events",
            &["CHECK (((subject IS NULL) OR (octet_length((subject)) <= 2048)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_content_type_size",
            "dovecote_events",
            &["CHECK (((datacontenttype IS NULL) OR (octet_length((datacontenttype)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_schema_size",
            "dovecote_events",
            &["CHECK (((dataschema IS NULL) OR (octet_length((dataschema)) <= 2048)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_partition_size",
            "dovecote_events",
            &["CHECK (((partitionkey IS NULL) OR (octet_length((partitionkey)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_identity_size",
            "dovecote_events",
            &["CHECK (((octet_length((source)) + octet_length((event_id))) <= 2048))"],
        ),
        ConstraintContract::check(
            "dovecote_events_data_kind",
            "dovecote_events",
            &["CHECK (((data_kind IS NULL) OR ((data_kind) = ANY ((ARRAY['json', 'binary'])))))"],
        ),
        ConstraintContract::check(
            "dovecote_events_data_pair",
            "dovecote_events",
            &["CHECK (((data_kind IS NULL) = (data IS NULL)))"],
        ),
        ConstraintContract::check(
            "dovecote_events_content_type",
            "dovecote_events",
            &[
                "CHECK (((data IS NULL) OR (octet_length(data) = 0) OR (datacontenttype IS NOT NULL)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_state",
            "dovecote_deliveries",
            &[
                "CHECK (((state) = ANY ((ARRAY['pending', 'claimed', 'delivered', 'quarantined']))))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_tenant_size",
            "dovecote_deliveries",
            &["CHECK ((octet_length((tenant_id)) <= 255))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_tenant_nonempty",
            "dovecote_deliveries",
            &["CHECK ((octet_length((tenant_id)) > 0))"],
        ),
        ConstraintContract::primary_key(
            "dovecote_deliveries_pkey",
            "dovecote_deliveries",
            &["event_row_id"],
            &["PRIMARY KEY (event_row_id)"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_attempts",
            "dovecote_deliveries",
            &["CHECK ((attempts >= 0))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_token_size",
            "dovecote_deliveries",
            &["CHECK (((claim_token IS NULL) OR (octet_length(claim_token) = 16)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_worker_size",
            "dovecote_deliveries",
            &["CHECK (((claimed_by IS NULL) OR (octet_length((claimed_by)) <= 255)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_code_size",
            "dovecote_deliveries",
            &[
                "CHECK (((last_failure_code IS NULL) OR (octet_length((last_failure_code)) <= 128)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_detail_size",
            "dovecote_deliveries",
            &[
                "CHECK (((last_failure_detail IS NULL) OR (octet_length((last_failure_detail)) <= 2048)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_quarantine_size",
            "dovecote_deliveries",
            &[
                "CHECK (((quarantine_reason IS NULL) OR (octet_length((quarantine_reason)) <= 2048)))",
            ],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_failure_pair",
            "dovecote_deliveries",
            &["CHECK (((last_failure_code IS NULL) = (last_failure_detail IS NULL)))"],
        ),
        ConstraintContract::check(
            "dovecote_deliveries_state_shape",
            "dovecote_deliveries",
            &[
                "CHECK (((((state) = 'pending') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'claimed') AND (claim_token IS NOT NULL) AND (claimed_by IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'delivered') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NOT NULL) AND (quarantined_at IS NULL) AND (quarantine_reason IS NULL)) OR (((state) = 'quarantined') AND (claim_token IS NULL) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL) AND (delivered_at IS NULL) AND (quarantined_at IS NOT NULL) AND (quarantine_reason IS NOT NULL))))",
            ],
        ),
        ConstraintContract::foreign_key(
            "dovecote_deliveries_event_fk",
            "dovecote_deliveries",
            &["tenant_id", "event_row_id"],
            "dovecote_events",
            &["tenant_id", "row_id"],
            "r",
            &[
                "FOREIGN KEY (tenant_id, event_row_id) REFERENCES dovecote_events (tenant_id, row_id) ON DELETE RESTRICT",
            ],
        ),
    ]
}

pub(crate) fn expected_indexes() -> Vec<IndexContract> {
    vec![
        IndexContract::new(
            "dovecote_events_tenant_source_event_id",
            "dovecote_events",
            true,
            &["tenant_id", "source", "event_id"],
            Some(&["C", "C", "C"]),
        ),
        IndexContract::new(
            "dovecote_deliveries_claimable",
            "dovecote_deliveries",
            false,
            &["tenant_id", "state", "available_at", "event_row_id"],
            Some(&["C", "default", "default", "default"]),
        ),
        IndexContract::new(
            "dovecote_deliveries_expired_claims",
            "dovecote_deliveries",
            false,
            &["tenant_id", "state", "claim_expires_at", "event_row_id"],
            Some(&["C", "default", "default", "default"]),
        ),
    ]
}
