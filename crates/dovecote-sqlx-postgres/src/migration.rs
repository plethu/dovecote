//! Versioned PostgreSQL migration metadata.

use sqlx::FromRow;

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

pub(crate) fn current_crate_version() -> CrateVersion {
    CrateVersion::new(
        env!("CARGO_PKG_VERSION_MAJOR")
            .parse()
            .expect("Cargo supplies a numeric major version"),
        env!("CARGO_PKG_VERSION_MINOR")
            .parse()
            .expect("Cargo supplies a numeric minor version"),
        env!("CARGO_PKG_VERSION_PATCH")
            .parse()
            .expect("Cargo supplies a numeric patch version"),
    )
}

#[derive(Debug, FromRow)]
pub(crate) struct SchemaMarker {
    pub(crate) schema_version: i32,
    pub(crate) minimum_crate_major: i16,
    pub(crate) minimum_crate_minor: i16,
    pub(crate) minimum_crate_patch: i16,
    pub(crate) rolling_compatible: bool,
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
    include_str!("../migrations/0001_dovecote.sql"),
    MigrationCompatibility::new(CrateVersion::new(0, 1, 0), None),
    false,
)];

pub(crate) fn current_migration() -> Result<Migration, String> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.version() == SCHEMA_VERSION)
        .copied()
        .ok_or_else(|| format!("adapter does not ship schema version {SCHEMA_VERSION}"))
}

pub(crate) fn marker_compatibility(
    marker: &SchemaMarker,
) -> Result<MigrationCompatibility, String> {
    let minimum = CrateVersion::new(
        u16::try_from(marker.minimum_crate_major)
            .map_err(|_| "schema marker minimum major version is negative".to_owned())?,
        u16::try_from(marker.minimum_crate_minor)
            .map_err(|_| "schema marker minimum minor version is negative".to_owned())?,
        u16::try_from(marker.minimum_crate_patch)
            .map_err(|_| "schema marker minimum patch version is negative".to_owned())?,
    );
    MigrationCompatibility::try_new(minimum, None)
        .map_err(|error| format!("schema marker compatibility is invalid: {error}"))
}

pub(crate) fn marker_matches_migration(
    marker: &SchemaMarker,
    migration: Migration,
) -> Result<(), String> {
    let marker_version = u32::try_from(marker.schema_version)
        .map_err(|_| "schema marker version is negative".to_owned())?;
    let compatibility = marker_compatibility(marker)?;
    if marker_version != migration.version() {
        return Err(format!(
            "schema marker version {} does not match installed version {}",
            marker_version,
            migration.version()
        ));
    }

    if compatibility != migration.compatibility() {
        return Err("schema marker compatibility range is incompatible".to_owned());
    }

    if marker.rolling_compatible != migration.rolling_compatible() {
        return Err("schema marker rolling compatibility is incompatible".to_owned());
    }

    if !migration.compatibility().contains(current_crate_version()) {
        return Err("current crate is outside the migration compatibility range".to_owned());
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
