use super::{
    catalog::{default_matches, non_generated_metadata_matches},
    contracts::{
        CHECK_NAMES, ColumnSpec, MARKER_CHECK_NAMES, catalog_check_clause,
        check_clause_is_plausible, expected_check_clause, mariadb_catalog_check_clause,
        marker_check_clause_is_plausible, marker_columns, mysql_unwrapped_catalog_check_clause,
    },
    normalization::{normalize, normalize_generated_expression},
};

#[test]
fn normalization_ignores_catalog_decoration() {
    assert_eq!(normalize("CHECK ((`x` = 1))"), "check((x=1))");
}

#[test]
fn mariadb_nullable_null_default_is_accepted_only_for_nullable_columns() {
    let nullable = ColumnSpec::optional("subject", "varbinary", Some(2048));
    let required = ColumnSpec::required("row_id", "bigint", None, false);
    assert!(default_matches(&nullable, Some("NULL")));
    assert!(default_matches(&nullable, None));
    assert!(!default_matches(&required, Some("NULL")));
    assert!(default_matches(&required, None));
}

#[test]
fn non_generated_columns_accept_empty_generation_metadata_only() {
    assert!(non_generated_metadata_matches("", None, false, false));
    assert!(non_generated_metadata_matches("", Some(""), false, false));
    assert!(non_generated_metadata_matches(
        "",
        Some(" \t\n"),
        false,
        false
    ));
    assert!(non_generated_metadata_matches(
        "DEFAULT_GENERATED",
        Some(""),
        false,
        true
    ));
    assert!(!non_generated_metadata_matches(
        "DEFAULT_GENERATED",
        Some(""),
        false,
        false
    ));
    assert!(!non_generated_metadata_matches(
        "",
        Some("tenant_id"),
        false,
        false
    ));
    assert!(!non_generated_metadata_matches(
        "VIRTUAL GENERATED",
        Some(""),
        false,
        false
    ));
    assert!(!non_generated_metadata_matches(
        "STORED GENERATED",
        Some(""),
        false,
        false
    ));
    assert!(!non_generated_metadata_matches(
        "OTHER",
        Some(""),
        false,
        false
    ));
}

#[test]
fn auto_increment_is_accepted_only_for_identity_columns() {
    assert!(non_generated_metadata_matches(
        "AUTO_INCREMENT",
        Some(""),
        true,
        false
    ));
    assert!(!non_generated_metadata_matches(
        "AUTO_INCREMENT",
        Some(""),
        false,
        false
    ));
}

#[test]
fn every_migration_check_clause_matches() {
    for name in CHECK_NAMES {
        let clause = expected_check_clause(name).expect("every name has a migration clause");
        assert!(
            check_clause_is_plausible(name, clause),
            "migration clause should match {name}"
        );
    }
}

#[test]
fn marker_check_clauses_are_exact() {
    for name in MARKER_CHECK_NAMES {
        let clause = match *name {
            "dovecote_schema_version_supported" => "schema_version = 2",
            "dovecote_schema_minimum_nonnegative" => {
                "minimum_crate_major >= 0 AND minimum_crate_minor >= 0 AND minimum_crate_patch >= 0"
            }
            _ => unreachable!("unknown marker check {name}"),
        };
        assert!(marker_check_clause_is_plausible(name, clause));
        assert!(!marker_check_clause_is_plausible(
            name,
            &format!("{clause} OR 1 = 1")
        ));
    }
}

#[test]
fn mysql_marker_catalog_grouping_is_exact() {
    let catalog = "((minimum_crate_major >= 0) and (minimum_crate_minor >= 0) and (minimum_crate_patch >= 0))";
    assert!(marker_check_clause_is_plausible(
        "dovecote_schema_minimum_nonnegative",
        catalog
    ));
    assert!(!marker_check_clause_is_plausible(
        "dovecote_schema_minimum_nonnegative",
        &catalog.replace(">= 0", "<= 0")
    ));
    assert!(!marker_check_clause_is_plausible(
        "dovecote_schema_minimum_nonnegative",
        "((minimum_crate_minor >= 0) and (minimum_crate_major >= 0) and (minimum_crate_patch >= 0))"
    ));
}

#[test]
fn marker_schema_version_is_not_auto_increment() {
    let columns = marker_columns();
    assert_eq!(columns[0].name, "schema_version");
    assert!(!columns[0].identity);
}

#[test]
fn captured_mysql_catalog_clauses_match_as_complete_expressions() {
    for name in CHECK_NAMES {
        let Some(clause) = catalog_check_clause(name) else {
            continue;
        };
        assert!(
            check_clause_is_plausible(name, clause),
            "captured catalog clause should match {name}"
        );
    }
}

#[test]
fn catalog_decoration_and_binary_length_aliases_match() {
    assert!(check_clause_is_plausible(
        "dovecote_events_specversion",
        r#"(`specversion` = _binary\'1.0\')"#
    ));
    assert!(check_clause_is_plausible(
        "dovecote_events_subject_size",
        "((`subject` IS NULL) OR (LENGTH(`subject`) <= 2048))"
    ));
    assert!(check_clause_is_plausible(
        "dovecote_deliveries_state",
        "(`state` IN (_utf8mb4'pending', _utf8mb4'claimed', _utf8mb4'delivered', _utf8mb4'quarantined'))"
    ));
}

#[test]
fn generated_identity_expression_matches_catalog_decoration_exactly() {
    let migration = "CONCAT(LPAD(OCTET_LENGTH(tenant_id), 3, '0'), tenant_id, LPAD(OCTET_LENGTH(source), 4, '0'), source, event_id)";
    let catalog = "concat(lpad(octet_length(`tenant_id`),3,_utf8mb4'0'),`tenant_id`,lpad(octet_length(`source`),4,_utf8mb4'0'),`source`,`event_id`)";
    let mysql_length_catalog = r#"concat(lpad(length(`tenant_id`),3,_utf8mb4\'0\'),`tenant_id`,lpad(length(`source`),4,_utf8mb4\'0\'),`source`,`event_id`)"#;
    assert_eq!(
        normalize_generated_expression(migration),
        normalize_generated_expression(catalog)
    );
    assert_eq!(
        normalize_generated_expression(migration),
        normalize_generated_expression(mysql_length_catalog)
    );
    assert_ne!(
        normalize_generated_expression(migration),
        normalize_generated_expression("CONCAT(tenant_id, source, event_id)")
    );
}

#[test]
fn mariadb_state_shape_precedence_form_matches_only_as_a_whole() {
    let clause = mariadb_catalog_check_clause("dovecote_deliveries_state_shape").unwrap();
    assert!(check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        clause
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        &clause.replace(" OR state =", " OR (state =")
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        &clause.replace("quarantine_reason IS NOT NULL", "quarantine_reason IS NULL")
    ));
}

#[test]
fn mysql_unwrapped_state_shape_catalog_form_matches_exactly() {
    let clause = mysql_unwrapped_catalog_check_clause("dovecote_deliveries_state_shape")
        .expect("captured MySQL alternative");
    assert!(check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        clause
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        &format!("{clause} AND 1 = 1")
    ));
}

#[test]
fn altered_or_true_and_false_are_rejected() {
    assert!(!check_clause_is_plausible(
        "dovecote_events_stream_size",
        "OCTET_LENGTH(stream) <= 255 OR true"
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_attempts",
        "attempts >= 0 AND false"
    ));
}

#[test]
fn missing_or_reordered_state_branches_are_rejected() {
    let expected = expected_check_clause("dovecote_deliveries_state_shape").unwrap();
    let branches = expected.split(" OR ").collect::<Vec<_>>();
    assert_eq!(branches.len(), 4);
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        &branches[..3].join(" OR ")
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_deliveries_state_shape",
        &branches
            .iter()
            .rev()
            .copied()
            .collect::<Vec<_>>()
            .join(" OR ")
    ));
}

#[test]
fn changed_bounds_grouping_and_removed_constraints_are_rejected() {
    assert!(!check_clause_is_plausible(
        "dovecote_events_source_size",
        "OCTET_LENGTH(source) <= 2047"
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_events_data_pair",
        "data_kind IS NULL = data IS NULL"
    ));
    assert!(!check_clause_is_plausible(
        "dovecote_events_row_id_positive",
        "row_id > 0"
    ));
}
