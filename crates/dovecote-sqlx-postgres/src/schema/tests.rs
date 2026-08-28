use super::contracts::{ConstraintContract, IndexContract};
use super::normalization::normalize_sql;
use super::*;
use crate::migration::{MIGRATIONS, SCHEMA_VERSION, current_crate_version, marker_compatibility};

#[test]
fn schema_marker_uses_the_shipped_compatibility_range() {
    let migration = MIGRATIONS
        .iter()
        .find(|migration| migration.version() == SCHEMA_VERSION)
        .expect("the v2 migration is shipped");
    let marker = SchemaMarker {
        schema_version: 2,
        minimum_crate_major: 0,
        minimum_crate_minor: 2,
        minimum_crate_patch: 0,
        rolling_compatible: false,
    };
    assert_eq!(marker_compatibility(&marker), Ok(migration.compatibility()));
    assert!(marker_matches_migration(&marker, *migration).is_ok());
    assert!(migration.compatibility().contains(current_crate_version()));

    let wrong_version = SchemaMarker {
        schema_version: 1,
        ..marker
    };
    assert!(marker_matches_migration(&wrong_version, *migration).is_err());

    let too_new = SchemaMarker {
        minimum_crate_major: 9,
        ..marker
    };
    assert!(marker_matches_migration(&too_new, *migration).is_err());

    let malformed = SchemaMarker {
        minimum_crate_minor: -1,
        ..marker
    };
    assert!(marker_compatibility(&malformed).is_err());
}

#[test]
fn constraint_and_index_contracts_require_their_live_semantics() {
    let constraint = ConstraintInfo {
        table_name: "dovecote_events".to_owned(),
        name: "dovecote_events_pkey".to_owned(),
        kind: "p".to_owned(),
        columns: vec!["row_id".to_owned()],
        referenced_table: None,
        referenced_columns: Vec::new(),
        delete_action: None,
        validated: true,
        deferrable: false,
        deferred: false,
        definition: "PRIMARY KEY (row_id)".to_owned(),
    };
    let expected = ConstraintContract::primary_key(
        "dovecote_events_pkey",
        "dovecote_events",
        &["row_id"],
        &["PRIMARY KEY (row_id)"],
    );
    assert!(constraint.matches(&expected));
    let wrong_relation = ConstraintInfo {
        table_name: "other".to_owned(),
        ..constraint
    };
    assert!(!wrong_relation.matches(&expected));

    let check = ConstraintInfo {
        table_name: "dovecote_schema".to_owned(),
        name: "dovecote_schema_version_supported".to_owned(),
        kind: "c".to_owned(),
        columns: vec!["schema_version".to_owned()],
        referenced_table: None,
        referenced_columns: Vec::new(),
        delete_action: None,
        validated: true,
        deferrable: false,
        deferred: false,
        definition: "CHECK ((schema_version = 1))".to_owned(),
    };
    let expected_check = ConstraintContract::check(
        "dovecote_schema_version_supported",
        "dovecote_schema",
        &["CHECK ((schema_version = 1))"],
    );
    assert!(check.matches(&expected_check));

    let index = IndexInfo {
        table_name: "dovecote_events".to_owned(),
        name: "dovecote_events_tenant_source_event_id".to_owned(),
        access_method: "btree".to_owned(),
        is_unique: true,
        is_valid: true,
        is_ready: true,
        has_predicate: false,
        key_columns: 3,
        total_columns: 3,
        options: vec![0, 0, 0],
        columns: vec![
            "tenant_id".to_owned(),
            "source".to_owned(),
            "event_id".to_owned(),
        ],
        collations: vec!["C".to_owned(), "C".to_owned(), "C".to_owned()],
    };
    let expected_index = IndexContract::new(
        "dovecote_events_tenant_source_event_id",
        "dovecote_events",
        true,
        &["tenant_id", "source", "event_id"],
        Some(&["C", "C", "C"]),
    );
    assert!(index.matches(&expected_index));
    let wrong_order = IndexInfo {
        options: vec![1, 0, 0],
        ..index
    };
    assert!(!wrong_order.matches(&expected_index));
}

#[test]
fn pg17_constraint_renderings_match_the_shipped_contracts() {
    let fixtures = [(
        ConstraintInfo {
            table_name: "dovecote_events".to_owned(),
            name: "dovecote_events_identity_size".to_owned(),
            kind: "c".to_owned(),
            columns: vec!["source".to_owned(), "event_id".to_owned()],
            referenced_table: None,
            referenced_columns: Vec::new(),
            delete_action: None,
            validated: true,
            deferrable: false,
            deferred: false,
            definition:
                "CHECK (((octet_length((source)::text) + octet_length((event_id)::text)) <= 2048))"
                    .to_owned(),
        },
        ConstraintContract::check(
            "dovecote_events_identity_size",
            "dovecote_events",
            &["CHECK (((octet_length((source)) + octet_length((event_id))) <= 2048))"],
        ),
    )];

    for (actual, expected) in fixtures {
        assert!(actual.matches(&expected));
    }
}

#[test]
fn sql_normalization_preserves_boolean_grouping() {
    assert_ne!(
        normalize_sql("CHECK ((left IS NULL OR right IS NULL))"),
        normalize_sql("CHECK (((left IS NULL OR right IS NULL)))")
    );
    assert_ne!(
        normalize_sql("CHECK (((left IS NULL) OR (right IS NULL)))"),
        normalize_sql("CHECK (((left IS NULL) AND (right IS NULL)))")
    );
}

#[test]
fn schema_catalog_name_matching_rejects_unexpected_objects() {
    let expected = [
        "dovecote_events_tenant_source_event_id",
        "dovecote_deliveries_claimable",
    ];
    assert!(is_expected_name(
        "dovecote_events_tenant_source_event_id",
        &expected
    ));
    assert!(!is_expected_name("dovecote_events_probe", &expected));
}
