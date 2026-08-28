//! Cross-project migration fixture runner.
//!
//! This is intentionally a test-only package. It reads the checked-in fixture
//! description, calls the public Dovecote migration importer, and checks the
//! public paging and claim boundaries. Legacy schemas are installed by the
//! shell harness from the real sibling migration files; this runner never
//! duplicates a backend schema or a Dovecote insert statement.

#![allow(
    clippy::excessive_nesting,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

mod checks;
mod fixture;
mod imports;
mod ledger;
mod source;
mod verify;

use checks::check_fixture_shape;
use fixture::{Backend, Fixture, parse_args};
use imports::{run_imports_mysql, run_imports_postgres, run_imports_sqlite};
use std::{error::Error, fs};
use verify::verify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (backend, url, fixture_path, high_waters, stop_after, verify_after, rollback, crash) =
        parse_args()?;
    let fixture: Fixture = serde_json::from_str(&fs::read_to_string(fixture_path)?)?;
    check_fixture_shape(&fixture)?;
    match backend {
        Backend::Sqlite => {
            run_imports_sqlite(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
        Backend::Postgres => {
            run_imports_postgres(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
        Backend::MySql => {
            run_imports_mysql(&fixture, &url, high_waters, stop_after, rollback, crash).await?
        }
    }

    if verify_after {
        verify(backend, &fixture, &url).await?;
    }
    println!(
        "migration fixture imported backend={} high_waters={:?}{}",
        match backend {
            Backend::Sqlite => "sqlite",
            Backend::Postgres => "postgres",
            Backend::MySql => "mysql-or-mariadb",
        },
        high_waters,
        if verify_after { " and verified" } else { "" }
    );
    Ok(())
}
