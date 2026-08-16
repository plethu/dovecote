//! SQLite schema and SQLx boundary for Carrier.
//!
//! The eventual implementation will use explicit `BEGIN IMMEDIATE` claim
//! transactions and bounded busy handling. SQLite's single-writer model is a
//! distinct support contract, not an approximation of server-database locks.

/// Schema version adapters compare before using these migration artifacts.
pub const SCHEMA_VERSION: u32 = 1;

/// Numeric crate version used to evaluate migration compatibility without
/// parsing free-form requirement strings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CrateVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl CrateVersion {
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
    pub const fn major(self) -> u16 {
        self.major
    }
    pub const fn minor(self) -> u16 {
        self.minor
    }
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
    pub const fn minimum(self) -> CrateVersion {
        self.minimum
    }
    pub const fn maximum(self) -> Option<CrateVersion> {
        self.maximum
    }

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
    pub const fn version(self) -> u32 {
        self.version
    }
    pub const fn sql(self) -> &'static str {
        self.sql
    }
    pub const fn compatibility(self) -> MigrationCompatibility {
        self.compatibility
    }
    pub const fn rolling_compatible(self) -> bool {
        self.rolling_compatible
    }
}

/// The migration sequence shipped with this adapter; entries are append-only.
pub const MIGRATIONS: &[Migration] = &[Migration::new(
    1,
    include_str!("../migrations/0001_carrier.sql"),
    MigrationCompatibility::new(CrateVersion::new(0, 1, 0), None),
    false,
)];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn migrations_are_ordered_and_typed() {
        assert_eq!(MIGRATIONS[0].version(), SCHEMA_VERSION);
        assert!(!MIGRATIONS[0].sql().is_empty());
        assert_eq!(
            MIGRATIONS[0].compatibility().minimum(),
            CrateVersion::new(0, 1, 0)
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
}
