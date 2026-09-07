//! Cross-project migration fixture runner.
//!
//! This is intentionally a test-only package. It reads the checked-in fixture
//! description, calls the public Dovecote migration importer, and checks the
//! public paging and claim boundaries. Legacy schemas are installed by the
//! shell harness from the real sibling migration files; this runner never
//! duplicates a backend schema or a Dovecote insert statement.

mod checks;
mod fixture;
mod imports;
mod ledger;
mod source;
mod verify;

use checks::check_fixture_shape;
use fixture::{Backend, Fixture, Invocation, RunMode, parse_args};
use imports::{run_imports_mysql, run_imports_postgres, run_imports_sqlite};
use std::{error::Error, fs, io, process::ExitCode};
use verify::verify;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "migration fixture failed: {}",
                failure_category(error.as_ref())
            );
            ExitCode::FAILURE
        }
    }
}

// Driver errors and invalid fixture rows can contain credentials or source data.
// Keep their typed causes internal instead of formatting them at the CLI boundary.
fn failure_category(error: &(dyn Error + 'static)) -> &'static str {
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return match error.kind() {
            io::ErrorKind::Interrupted => "simulated crash before checkpoint",
            io::ErrorKind::InvalidData => "fixture validation failed",
            _ => "fixture I/O failed",
        };
    }

    if error.is::<serde_json::Error>() {
        return "fixture JSON is invalid";
    }

    "database or fixture operation failed"
}

async fn run() -> Result<(), Box<dyn Error>> {
    let Invocation {
        backend,
        url,
        fixture_path,
        high_waters,
        stop_after,
        mode,
    } = parse_args()?;
    let fixture: Fixture = serde_json::from_str(&fs::read_to_string(fixture_path)?)?;
    check_fixture_shape(&fixture)?;
    match backend {
        Backend::Sqlite => {
            run_imports_sqlite(&fixture, &url, high_waters, stop_after, mode).await?
        }
        Backend::Postgres => {
            run_imports_postgres(&fixture, &url, high_waters, stop_after, mode).await?
        }
        Backend::MySql => run_imports_mysql(&fixture, &url, high_waters, stop_after, mode).await?,
    }

    if mode == RunMode::Verify {
        verify(backend, &fixture, &url).await?;
    }
    println!(
        "migration fixture completed backend={} high_waters={:?} mode={mode:?}",
        match backend {
            Backend::Sqlite => "sqlite",
            Backend::Postgres => "postgres",
            Backend::MySql => "mysql-or-mariadb",
        },
        high_waters,
    );
    Ok(())
}

#[cfg(test)]
mod diagnostic_tests {
    use super::failure_category;
    use std::io;

    #[test]
    fn diagnostic_category_does_not_format_private_error_details() {
        let source = sqlx::Error::Protocol("secret-token:password@host private payload".into());
        assert_eq!(
            failure_category(&source),
            "database or fixture operation failed"
        );
        let fixture = io::Error::new(io::ErrorKind::InvalidData, "private fixture metadata");
        assert_eq!(failure_category(&fixture), "fixture validation failed");
    }
}
