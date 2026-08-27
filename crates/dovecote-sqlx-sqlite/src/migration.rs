//! Versioned SQLite migration metadata.

pub const SCHEMA_VERSION: u32 = 1;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCompatibilityError;
impl std::fmt::Display for MigrationCompatibilityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("migration compatibility maximum precedes minimum")
    }
}
impl std::error::Error for MigrationCompatibilityError {}

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

pub const MIGRATIONS: &[Migration] = &[Migration::new(
    1,
    include_str!("../migrations/0001_dovecote.sql"),
    MigrationCompatibility::new(CrateVersion::new(0, 1, 0), None),
    false,
)];

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
            CrateVersion::new(0, 1, 0)
        );
        assert!(!MIGRATIONS[0].sql().contains("dovecote_schema"));
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
