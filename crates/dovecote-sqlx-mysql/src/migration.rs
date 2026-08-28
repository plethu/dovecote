//! Versioned MySQL/MariaDB migration metadata.

/// Schema version adapters compare before using these migration artifacts.
pub const SCHEMA_VERSION: u32 = 2;

/// Numeric crate version used to evaluate migration compatibility without
/// parsing free-form requirement strings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CrateVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl CrateVersion {
    /// Creates a crate version.
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Returns the major component.
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Returns the minor component.
    pub const fn minor(self) -> u16 {
        self.minor
    }

    /// Returns the patch component.
    pub const fn patch(self) -> u16 {
        self.patch
    }

    const fn is_less_than(self, other: Self) -> bool {
        self.major < other.major
            || (self.major == other.major
                && (self.minor < other.minor
                    || (self.minor == other.minor && self.patch < other.patch)))
    }
}

/// Returned when a migration's maximum supported release precedes its minimum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCompatibilityError;

impl std::fmt::Display for MigrationCompatibilityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("migration compatibility maximum precedes minimum")
    }
}

impl std::error::Error for MigrationCompatibilityError {}

/// A checked crate-version range for a migration artifact.
///
/// Use [`Self::contains`] when deciding whether the running adapter may apply it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCompatibility {
    minimum: CrateVersion,
    maximum: Option<CrateVersion>,
}

impl MigrationCompatibility {
    const fn new(minimum: CrateVersion, maximum: Option<CrateVersion>) -> Self {
        Self { minimum, maximum }
    }

    /// Creates a checked compatibility range.
    pub const fn try_new(
        minimum: CrateVersion,
        maximum: Option<CrateVersion>,
    ) -> Result<Self, MigrationCompatibilityError> {
        if let Some(maximum) = maximum
            && maximum.is_less_than(minimum)
        {
            return Err(MigrationCompatibilityError);
        }
        Ok(Self::new(minimum, maximum))
    }

    /// Returns the minimum compatible crate version.
    pub const fn minimum(self) -> CrateVersion {
        self.minimum
    }

    /// Returns the optional maximum compatible crate version.
    pub const fn maximum(self) -> Option<CrateVersion> {
        self.maximum
    }

    /// Reports whether a crate version is in this range.
    pub const fn contains(self, version: CrateVersion) -> bool {
        !version.is_less_than(self.minimum)
            && match self.maximum {
                Some(maximum) => !maximum.is_less_than(version),
                None => true,
            }
    }
}

/// An immutable SQL artifact whose compatibility metadata cannot be fabricated
/// by callers of the adapter crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Migration {
    version: u32,
    sql: &'static str,
    compatibility: MigrationCompatibility,
    rolling_compatible: bool,
}

impl Migration {
    const fn new(
        version: u32,
        sql: &'static str,
        compatibility: MigrationCompatibility,
        rolling_compatible: bool,
    ) -> Self {
        Self {
            version,
            sql,
            compatibility,
            rolling_compatible,
        }
    }

    /// Returns this migration's durable schema version.
    pub const fn version(self) -> u32 {
        self.version
    }

    /// Returns the immutable SQL artifact.
    pub const fn sql(self) -> &'static str {
        self.sql
    }

    /// Returns the crate compatibility range.
    pub const fn compatibility(self) -> MigrationCompatibility {
        self.compatibility
    }

    /// Reports whether this migration is safe during rolling deployment.
    pub const fn rolling_compatible(self) -> bool {
        self.rolling_compatible
    }
}

/// The migration sequence shipped with this adapter; entries are append-only.
pub const MIGRATIONS: &[Migration] = &[Migration::new(
    2,
    include_str!("../migrations/0002_dovecote_tenant_baseline.sql"),
    MigrationCompatibility::new(CrateVersion::new(0, 2, 0), None),
    false,
)];

/// The immutable schema version 1 artifact for pre-tenant deployments.
pub const LEGACY_MIGRATION: Migration = Migration::new(
    1,
    include_str!("../migrations/0001_dovecote.sql"),
    MigrationCompatibility::new(CrateVersion::new(0, 1, 0), None),
    false,
);

/// SQL that adds nullable tenant columns to a version 1 deployment.
pub const V1_TENANT_PREPARE_SQL: &str =
    include_str!("../migrations/0002_dovecote_tenant_prepare.sql");

/// SQL that validates backfill and activates tenant constraints.
pub const V1_TENANT_ACTIVATE_SQL: &str =
    include_str!("../migrations/0002_dovecote_tenant_activate.sql");

pub(crate) fn current_migration() -> Result<Migration, String> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.version() == SCHEMA_VERSION)
        .copied()
        .ok_or_else(|| format!("adapter does not ship schema version {SCHEMA_VERSION}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_ordered_and_typed() {
        assert_eq!(MIGRATIONS[0].version(), SCHEMA_VERSION);
        assert!(!MIGRATIONS[0].sql().is_empty());
        assert_eq!(
            MIGRATIONS[0].compatibility().minimum(),
            CrateVersion::new(0, 2, 0)
        );
        assert!(!MIGRATIONS[0].rolling_compatible());
        assert!(
            MIGRATIONS[0]
                .compatibility()
                .contains(CrateVersion::new(0, 9, 0))
        );
        assert!(
            MigrationCompatibility::try_new(
                CrateVersion::new(1, 0, 0),
                Some(CrateVersion::new(0, 9, 0))
            )
            .is_err()
        );
    }

    #[test]
    fn tenant_baseline_preserves_row_id_guards() {
        let sql = MIGRATIONS[0].sql();
        assert!(sql.contains("dovecote_events_row_id_positive_insert"));
        assert!(sql.contains("dovecote_events_row_id_positive_update"));
        assert_eq!(sql.matches("CREATE TRIGGER").count(), 2);
    }

    #[test]
    fn tenant_baseline_catalog_contract_has_ordered_identity_keys() {
        let sql = MIGRATIONS[0].sql();
        assert!(sql.contains("identity_key VARBINARY(2310) GENERATED ALWAYS AS"));
        assert!(sql.contains("LPAD(OCTET_LENGTH(tenant_id), 3, '0')"));
        assert!(sql.contains("LPAD(OCTET_LENGTH(source), 4, '0')"));
        assert!(
            sql.contains("CONSTRAINT dovecote_events_tenant_row_unique UNIQUE (tenant_id, row_id)")
        );
        assert!(sql.contains(
            "CONSTRAINT dovecote_deliveries_event_fk FOREIGN KEY (tenant_id, event_row_id) REFERENCES dovecote_events (tenant_id, row_id)"
        ));
        assert!(sql.contains(
            "KEY dovecote_deliveries_claimable (tenant_id, state, available_at, event_row_id)"
        ));
        assert!(sql.contains(
            "KEY dovecote_deliveries_expired_claims (tenant_id, state, claim_expires_at, event_row_id)"
        ));
        assert!(sql.contains("UNIQUE KEY dovecote_events_tenant_source_event_id (identity_key)"));
    }

    #[test]
    fn tenant_activation_upgrades_the_actual_v1_shape() {
        assert!(V1_TENANT_PREPARE_SQL.contains("ADD COLUMN tenant_id VARBINARY(255)"));
        assert!(!V1_TENANT_ACTIVATE_SQL.contains("ADD COLUMN tenant_id VARBINARY(255)"));
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .contains("CREATE TEMPORARY TABLE dovecote_tenant_activation_guard")
        );
        assert!(V1_TENANT_ACTIVATE_SQL.contains("OCTET_LENGTH(tenant_id) > 255"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("d.tenant_id <> e.tenant_id"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("GROUP BY tenant_id, source, event_id"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_marker_catalog_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_marker_data_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_events_columns_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_deliveries_columns_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_checks_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_required_checks_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_unexpected_checks_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_check_shapes_valid"));
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .contains("CREATE TEMPORARY TABLE dovecote_tenant_activation_statistics")
        );
        assert!(V1_TENANT_ACTIVATE_SQL.contains("stats_source.NON_UNIQUE"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_identity_prerequisites_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_triggers_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("dovecoterow_idmustbepositive"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_fk_index_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("r.update_rule IN ('RESTRICT', 'NO ACTION')"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_claimable_shape_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("@dovecote_expired_shape_valid"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("dovecote_deliveries_state_shape"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("LOWER(c.column_default) = 'null'"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("LOWER(column_default) = 'null'"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("COUNT(*) = 0 OR (COUNT(*) = 1"));
        let guard_position = V1_TENANT_ACTIVATE_SQL
            .find("INSERT INTO dovecote_tenant_activation_guard")
            .expect("activation guard preflight");
        let first_durable_ddl = V1_TENANT_ACTIVATE_SQL
            .find("'ALTER TABLE dovecote_events")
            .expect("activation DDL");
        assert!(guard_position < first_durable_ddl);
        assert!(V1_TENANT_ACTIVATE_SQL.contains("CHAR(92)"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("'length(tenant_id)'"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("'length(source)'"));
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .matches("column_default IS NULL")
                .count()
                >= 2
        );
        assert!(V1_TENANT_ACTIVATE_SQL.contains("PREPARE dovecote_activation_statement"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("ON DUPLICATE KEY UPDATE"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("DROP FOREIGN KEY dovecote_deliveries_event_fk"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("DROP INDEX dovecote_events_source_event_id"));
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .contains("ADD COLUMN identity_key VARBINARY(2310) GENERATED ALWAYS AS")
        );
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .contains("ADD UNIQUE KEY dovecote_events_tenant_source_event_id (identity_key)")
        );
        assert!(V1_TENANT_ACTIVATE_SQL.contains("DROP INDEX dovecote_deliveries_claimable"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("FOREIGN KEY (tenant_id, event_row_id)"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("CREATE TABLE IF NOT EXISTS dovecote_schema"));
        assert!(
            V1_TENANT_ACTIVATE_SQL
                .matches("dovecote_events_tenant_nonempty")
                .count()
                >= 1
        );
    }
}
