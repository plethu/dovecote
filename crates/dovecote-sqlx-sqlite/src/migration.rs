//! Versioned SQLite migration metadata.

/// Schema version implemented by this adapter.
pub const SCHEMA_VERSION: u32 = 2;

/// Numeric crate version used to evaluate migration compatibility.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CrateVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl CrateVersion {
    /// Creates a numeric crate version.
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

/// Error returned when a migration compatibility range is inverted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCompatibilityError;
impl std::fmt::Display for MigrationCompatibilityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("migration compatibility maximum precedes minimum")
    }
}
impl std::error::Error for MigrationCompatibilityError {}

/// A checked crate-version range for a migration artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCompatibility {
    minimum: CrateVersion,
    maximum: Option<CrateVersion>,
}
impl MigrationCompatibility {
    const fn new(minimum: CrateVersion, maximum: Option<CrateVersion>) -> Self {
        Self { minimum, maximum }
    }
    /// Creates a compatibility range, rejecting an upper bound below its lower bound.
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
    /// Returns the minimum supported crate version.
    pub const fn minimum(self) -> CrateVersion {
        self.minimum
    }
    /// Returns the maximum supported crate version, when bounded.
    pub const fn maximum(self) -> Option<CrateVersion> {
        self.maximum
    }
    /// Returns whether a crate version is inside this compatibility range.
    pub const fn contains(self, version: CrateVersion) -> bool {
        !version.is_less_than(self.minimum)
            && match self.maximum {
                Some(maximum) => !maximum.is_less_than(version),
                None => true,
            }
    }
}

/// One immutable, versioned SQLite migration artifact.
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
    /// Returns the schema version introduced by this migration.
    pub const fn version(self) -> u32 {
        self.version
    }
    /// Returns the migration SQL exactly as shipped.
    pub const fn sql(self) -> &'static str {
        self.sql
    }
    /// Returns the crate-version compatibility range.
    pub const fn compatibility(self) -> MigrationCompatibility {
        self.compatibility
    }
    /// Returns whether this migration supports rolling deployment.
    pub const fn rolling_compatible(self) -> bool {
        self.rolling_compatible
    }
}

/// All migration artifacts shipped by this adapter, in version order.
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

pub(crate) fn current_crate_version() -> CrateVersion {
    CrateVersion::new(
        env!("CARGO_PKG_VERSION_MAJOR")
            .parse()
            .expect("Cargo version is numeric"),
        env!("CARGO_PKG_VERSION_MINOR")
            .parse()
            .expect("Cargo version is numeric"),
        env!("CARGO_PKG_VERSION_PATCH")
            .parse()
            .expect("Cargo version is numeric"),
    )
}
pub(crate) fn current_migration() -> Result<Migration, String> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.version() == SCHEMA_VERSION)
        .copied()
        .ok_or_else(|| format!("adapter does not ship schema version {SCHEMA_VERSION}"))
}
pub(crate) fn migration_is_usable(migration: Migration) -> Result<(), String> {
    if !migration.compatibility().contains(current_crate_version()) {
        return Err("current crate is outside migration compatibility range".to_owned());
    }
    Ok(())
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
        assert!(MIGRATIONS[0].sql().contains("dovecote_schema"));
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
    fn tenant_baseline_retains_v1_durable_bounds() {
        let sql = MIGRATIONS[0].sql();
        for bound in [
            "stream AS BLOB)) <= 255",
            "event_id AS BLOB)) <= 1024",
            "source AS BLOB)) <= 2048",
            "event_type AS BLOB)) <= 1024",
            "subject AS BLOB)) <= 2048",
            "datacontenttype AS BLOB)) <= 255",
            "dataschema AS BLOB)) <= 2048",
            "partitionkey AS BLOB)) <= 255",
            "claimed_by AS BLOB)) <= 255",
            "last_failure_code AS BLOB)) <= 128",
            "last_failure_detail AS BLOB)) <= 2048",
            "quarantine_reason AS BLOB)) <= 2048",
        ] {
            assert!(sql.contains(bound), "missing durable bound: {bound}");
        }
        assert!(sql.contains("source AS BLOB)) + length(CAST(event_id AS BLOB)) <= 2048"));
    }

    #[test]
    fn tenant_activation_is_a_transactional_rebuild() {
        assert!(V1_TENANT_ACTIVATE_SQL.contains("BEGIN IMMEDIATE"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("CREATE TEMP TABLE"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("CREATE TABLE dovecote_events_v2"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("ALTER TABLE dovecote_events_v2 RENAME"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("LEFT JOIN dovecote_events AS e"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("e.row_id IS NULL OR d.tenant_id <> e.tenant_id"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("COMMIT"));
        assert!(V1_TENANT_ACTIVATE_SQL.contains("INSERT INTO dovecote_deliveries_v2"));
    }
}
