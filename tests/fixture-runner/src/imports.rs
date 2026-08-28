//! Backend-specific import orchestration.

use super::{
    fixture::{Fixture, SourceHighWaters, build_event, delivery_state, invalid},
    ledger::{RESOLUTION_BATCH_SIZE, persist_ledger, persist_progress, read_source_cursors},
    source::{resolve_mysql, resolve_postgres, resolve_sqlite},
};
use std::{
    env,
    error::Error,
    io::{self, ErrorKind},
};

pub(super) async fn run_imports_sqlite(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote::TenantId;
    use dovecote_sqlx_sqlite::SqliteDovecote;
    use sqlx::sqlite::SqlitePoolOptions;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    let adapter = SqliteDovecote::new(pool).for_tenant(TenantId::new("fixture")?);
    adapter.check_schema().await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_sqlite(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.begin_write().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}

pub(super) async fn run_imports_postgres(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote::TenantId;
    use dovecote_sqlx_postgres::PostgresDovecote;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let root = PostgresDovecote::new(pool);
    root.check_schema().await?;
    let adapter = root.for_tenant(TenantId::new("fixture")?);
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_postgres(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.pool().begin().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}

pub(super) async fn run_imports_mysql(
    fixture: &Fixture,
    url: &str,
    high_waters: SourceHighWaters,
    stop_after: Option<usize>,
    rollback: bool,
    crash: bool,
) -> Result<(), Box<dyn Error>> {
    use dovecote::TenantId;
    use dovecote_sqlx_mysql::MySqlDovecote;
    use sqlx::mysql::MySqlPoolOptions;

    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await?;
    // The MySQL-family migration contains trigger bodies without a client
    // delimiter directive. When this fixture is pointed at a fresh schema,
    // install the complete artifact through SQLx's raw/unprepared protocol.
    let has_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'dovecote_events'",
    )
    .fetch_one(&pool)
    .await?;
    if has_events == 0 {
        use dovecote_sqlx_mysql::MIGRATIONS;
        // The MySQL-family artifact contains trigger bodies and semicolons in
        // comments. Send the complete artifact through SQLx's raw/unprepared
        // protocol; splitting it would corrupt both kinds of semicolon.
        sqlx::raw_sql(MIGRATIONS[0].sql()).execute(&pool).await?;
    }

    let root = MySqlDovecote::new(pool);
    root.check_schema().await?;
    let adapter = root.for_tenant(TenantId::new("fixture")?);
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let cursors = read_source_cursors(&ledger)?;
    let selected = resolve_mysql(
        adapter.pool(),
        fixture,
        cursors,
        high_waters,
        stop_after.unwrap_or(RESOLUTION_BATCH_SIZE).max(1),
    )
    .await?;
    let mut transaction = adapter.pool().begin().await?;
    let mut imported = Vec::new();
    for (index, item) in selected.iter().enumerate() {
        if stop_after.is_some_and(|limit| index >= limit) {
            break;
        }

        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                build_event(fixture, &item.item)?,
                delivery_state(&item.item)?,
            )
            .await?;
        let imported_row_id = match outcome {
            dovecote::ImportOutcome::Imported { row_id }
            | dovecote::ImportOutcome::AlreadyImported { row_id } => row_id.get(),
            _ => return Err(invalid("unsupported import outcome".into())),
        };
        imported.push((item, imported_row_id));
    }

    if rollback {
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(adapter.pool())
            .await?;
        if count != 0 {
            return Err(invalid(
                "rolled-back fixture batch left Dovecote rows".into(),
            ));
        }

        return Ok(());
    }

    transaction.commit().await?;
    if crash {
        return Err(Box::new(io::Error::new(
            ErrorKind::Interrupted,
            "fixture runner crashed after committing the batch before external checkpoint",
        )));
    }

    persist_ledger(&ledger, &imported, high_waters)?;
    persist_progress(&ledger, high_waters, &imported, cursors)?;
    Ok(())
}
