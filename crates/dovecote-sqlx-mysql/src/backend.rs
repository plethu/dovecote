//! MySQL/MariaDB server identification and capability policy.

use crate::error::SchemaError;
use sqlx::{FromRow, MySqlConnection, MySqlPool, query_as};

/// Backend family reported by the server, kept distinct because their
/// catalogs and release numbering are not interchangeable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BackendKind {
    /// Oracle MySQL.
    MySql,
    /// MariaDB.
    MariaDb,
}

/// Numeric server release. Calendar-versioned MySQL Innovation releases such
/// as 26.7 are represented without assuming a sequential major number.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServerVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl ServerVersion {
    /// Creates a server version from its numeric components.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
    /// Returns the major component.
    pub const fn major(self) -> u32 {
        self.major
    }
    /// Returns the minor component.
    pub const fn minor(self) -> u32 {
        self.minor
    }
    /// Returns the patch component.
    pub const fn patch(self) -> u32 {
        self.patch
    }
}

/// Capabilities selected from a verified family and release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Capabilities {
    /// Whether SKIP LOCKED is available.
    pub skip_locked: bool,
    /// Whether the server enforces CHECK constraints.
    pub enforced_checks: bool,
    /// Whether repeatable-read snapshots are supported.
    pub repeatable_read_snapshot: bool,
}

/// Verified connection identity and SQL capability set.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BackendInfo {
    /// Detected server family.
    pub kind: BackendKind,
    /// Detected server release.
    pub version: ServerVersion,
    /// Capabilities validated for this connection.
    pub capabilities: Capabilities,
    /// Active transaction isolation level.
    pub transaction_isolation: String,
}

#[derive(Debug, FromRow)]
struct ServerProbe {
    version: String,
    version_comment: String,
    time_zone: String,
    transaction_isolation: String,
    sql_mode: String,
    character_set_client: String,
    character_set_connection: String,
    character_set_results: String,
    collation_connection: String,
}

/// Detects the server family, release and required session settings.
///
/// Dovecote uses UTC civil timestamps and repeatable-read InnoDB snapshots;
/// accepting a non-UTC session would silently reinterpret every instant.
/// Detection is capability-based and may accept newer releases that meet the
/// adapter's minimum requirements. The support matrix advertises only the
/// exact releases covered by conformance evidence.
pub async fn detect(pool: &MySqlPool) -> Result<BackendInfo, SchemaError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| SchemaError::sql("acquire backend detection connection", source))?;
    detect_on_connection(&mut connection).await
}

pub(crate) async fn detect_on_connection(
    connection: &mut MySqlConnection,
) -> Result<BackendInfo, SchemaError> {
    let probe = query_as::<_, ServerProbe>(
        "SELECT VERSION() AS version, @@version_comment AS version_comment, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@sql_mode AS sql_mode, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@collation_connection AS collation_connection",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|source| SchemaError::sql("detect MySQL/MariaDB backend", source))?;
    let version_lower = probe.version.to_ascii_lowercase();
    let comment_lower = probe.version_comment.to_ascii_lowercase();
    let kind = if version_lower.contains("mariadb")
        || probe
            .version_comment
            .to_ascii_lowercase()
            .contains("mariadb")
    {
        BackendKind::MariaDb
    } else if comment_lower.contains("mysql")
        && !comment_lower.contains("percona")
        && !comment_lower.contains("tidb")
        && !comment_lower.contains("vitess")
    {
        BackendKind::MySql
    } else {
        return Err(SchemaError::BackendMismatch {
            detail: format!(
                "server is not an identified Oracle MySQL or MariaDB build: {:?}",
                probe.version_comment
            ),
        });
    };
    let version =
        parse_server_version(&probe.version).ok_or_else(|| SchemaError::BackendMismatch {
            detail: format!("cannot parse {kind:?} server version {:?}", probe.version),
        })?;
    if !supported(kind, version) {
        return Err(SchemaError::BackendMismatch {
            detail: format!(
                "unsupported {kind:?} server release {}.{}.{}",
                version.major, version.minor, version.patch
            ),
        });
    }

    if !matches!(
        probe.time_zone.trim().to_ascii_uppercase().as_str(),
        "+00:00" | "UTC"
    ) {
        return Err(SchemaError::BackendMismatch {
            detail: format!("session time_zone must be UTC, got {:?}", probe.time_zone),
        });
    }

    let capabilities = capabilities(kind, version);
    if !capabilities.skip_locked || !capabilities.enforced_checks {
        return Err(SchemaError::BackendMismatch {
            detail: "server lacks required SKIP LOCKED or enforced CHECK support".to_owned(),
        });
    }

    if !probe.sql_mode.split(',').any(|mode| {
        mode.eq_ignore_ascii_case("STRICT_TRANS_TABLES")
            || mode.eq_ignore_ascii_case("STRICT_ALL_TABLES")
    }) {
        return Err(SchemaError::BackendMismatch {
            detail: format!("strict SQL mode is required, got {:?}", probe.sql_mode),
        });
    }

    if probe
        .sql_mode
        .split(',')
        .any(|mode| mode.eq_ignore_ascii_case("NO_AUTO_VALUE_ON_ZERO"))
    {
        return Err(SchemaError::BackendMismatch {
            detail: "NO_AUTO_VALUE_ON_ZERO is incompatible with positive AUTO_INCREMENT row IDs"
                .to_owned(),
        });
    }

    if !probe
        .transaction_isolation
        .eq_ignore_ascii_case("REPEATABLE-READ")
    {
        return Err(SchemaError::BackendMismatch {
            detail: format!(
                "transaction isolation must be REPEATABLE-READ, got {:?}",
                probe.transaction_isolation
            ),
        });
    }

    for (name, value) in [
        ("character_set_client", &probe.character_set_client),
        ("character_set_connection", &probe.character_set_connection),
        ("character_set_results", &probe.character_set_results),
    ] {
        if !value.eq_ignore_ascii_case("utf8mb4") {
            return Err(SchemaError::BackendMismatch {
                detail: format!("{name} must be utf8mb4, got {value:?}"),
            });
        }
    }

    if !probe
        .collation_connection
        .to_ascii_lowercase()
        .starts_with("utf8mb4_")
    {
        return Err(SchemaError::BackendMismatch {
            detail: format!(
                "connection collation must be utf8mb4, got {:?}",
                probe.collation_connection
            ),
        });
    }
    Ok(BackendInfo {
        kind,
        version,
        capabilities,
        transaction_isolation: probe.transaction_isolation,
    })
}

pub(crate) fn parse_server_version(value: &str) -> Option<ServerVersion> {
    let mut parts = value.split(|c: char| !c.is_ascii_digit() && c != '.');
    let numeric = parts.find(|part| !part.is_empty())?;
    let mut numbers = numeric.split('.').map(|part| part.parse::<u32>().ok());
    Some(ServerVersion::new(
        numbers.next()??,
        numbers.next()??,
        numbers.next().unwrap_or(Some(0))?,
    ))
}

pub(crate) const fn supported(kind: BackendKind, version: ServerVersion) -> bool {
    match kind {
        BackendKind::MySql => version.major > 8 || (version.major == 8 && version.minor >= 4),
        BackendKind::MariaDb => version.major > 11 || (version.major == 11 && version.minor >= 8),
    }
}

pub(crate) const fn capabilities(kind: BackendKind, version: ServerVersion) -> Capabilities {
    let skip_locked = match kind {
        BackendKind::MySql => version.major >= 8,
        BackendKind::MariaDb => version.major > 10 || (version.major == 10 && version.minor >= 6),
    };
    let enforced_checks = match kind {
        BackendKind::MySql => version.major >= 8,
        BackendKind::MariaDb => version.major > 10 || (version.major == 10 && version.minor >= 2),
    };
    Capabilities {
        skip_locked,
        enforced_checks,
        repeatable_read_snapshot: true,
    }
}

impl std::fmt::Display for BackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MySql => "MySQL",
            Self::MariaDb => "MariaDB",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_handle_calendar_mysql_and_mariadb_suffixes() {
        assert_eq!(
            parse_server_version("26.7.1-innovation"),
            Some(ServerVersion::new(26, 7, 1))
        );
        assert_eq!(
            parse_server_version("11.8.2-MariaDB"),
            Some(ServerVersion::new(11, 8, 2))
        );
        assert_eq!(
            parse_server_version("8.4.7"),
            Some(ServerVersion::new(8, 4, 7))
        );
        assert!(supported(BackendKind::MySql, ServerVersion::new(26, 7, 0)));
        assert!(supported(
            BackendKind::MariaDb,
            ServerVersion::new(11, 8, 0)
        ));
        assert!(!supported(
            BackendKind::MariaDb,
            ServerVersion::new(11, 7, 0)
        ));
    }
}
