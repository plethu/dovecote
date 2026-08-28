//! Backend-specific verification of imported projections and recovery behavior.

use super::{
    checks::{
        assert_projection, fixture_with_source, legacy_publication_observation,
        publication_observation, verify_publication_boundary,
    },
    fixture::{Backend, Fixture, build_event, delivery_state, event_id, invalid, parse_time},
    ledger::{RESOLUTION_BATCH_SIZE, SourceCursors, verify_ledger, verify_progress},
    source::{resolve_mysql, resolve_postgres, resolve_sqlite},
};
use dovecote::{ImportedDeliveryState, Limit};
use std::{env, error::Error, time::Duration};

pub(super) async fn verify_sqlite(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, TenantId, WorkerId};
    use dovecote_sqlx_sqlite::SqliteDovecote;
    use sqlx::sqlite::SqlitePoolOptions;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    let adapter = SqliteDovecote::new(pool).for_tenant(TenantId::new("fixture")?);
    adapter.check_schema().await?;
    let source_events = resolve_sqlite(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    // Rerunning exact immutable content is an explicit no-op.
    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.begin_write().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    // Same identity with changed immutable bytes must stop with a typed
    // conflict, and the failed transaction must leave the source row intact.
    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.begin_write().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;

    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.begin_write().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;

    // The public claim API must never return the two delivered imports.
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }

    // The fixture includes the at-least-once boundary explicitly. Consumers
    // deduplicate `(source,id)`, not delivery row IDs.
    Ok(())
}

pub(super) async fn verify_postgres(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, TenantId, WorkerId};
    use dovecote_sqlx_postgres::PostgresDovecote;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new().max_connections(4).connect(url).await?;
    let root = PostgresDovecote::new(pool);
    root.check_schema().await?;
    let adapter = root.for_tenant(TenantId::new("fixture")?);
    let source_events = resolve_postgres(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_postgres::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_postgres::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }
    Ok(())
}
pub(super) async fn verify(
    backend: Backend,
    fixture: &Fixture,
    url: &str,
) -> Result<(), Box<dyn Error>> {
    match backend {
        Backend::Sqlite => verify_sqlite(fixture, url).await,
        Backend::Postgres => verify_postgres(fixture, url).await,
        Backend::MySql => verify_mysql(fixture, url).await,
    }
}

pub(super) async fn verify_mysql(fixture: &Fixture, url: &str) -> Result<(), Box<dyn Error>> {
    use dovecote::{Delay, Lease, TenantId, WorkerId};
    use dovecote_sqlx_mysql::MySqlDovecote;
    use sqlx::mysql::MySqlPoolOptions;

    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await?;
    let root = MySqlDovecote::new(pool);
    root.check_schema().await?;
    let adapter = root.for_tenant(TenantId::new("fixture")?);
    let source_events = resolve_mysql(
        adapter.pool(),
        fixture,
        SourceCursors::default(),
        fixture.high_water_marks[1],
        RESOLUTION_BATCH_SIZE,
    )
    .await?;
    let ledger = env::var("DOVECOTE_FIXTURE_LEDGER")
        .map_err(|_| invalid("DOVECOTE_FIXTURE_LEDGER is required".into()))?;
    let expected = fixture_with_source(fixture, &source_events);
    let (publication_id, first_publication) =
        legacy_publication_observation(&expected, &source_events)?;
    let mut pager = adapter.begin_snapshot().await?;
    let mut rows = Vec::new();
    loop {
        let page = pager.next_page(Limit::new(3)?).await?;
        let done = page.is_empty();
        rows.extend(page);
        if done {
            break;
        }
    }
    pager.finish().await?;
    verify_progress(&ledger, fixture, fixture.high_water_marks[1])?;
    verify_ledger(&ledger, &source_events, &rows)?;
    assert_projection(&expected, rows.clone())?;
    verify_publication_boundary(&publication_id, &first_publication, &rows)?;

    let first = expected
        .events
        .first()
        .ok_or_else(|| invalid("fixture has no first event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, first)?,
            delivery_state(first)?,
        )
        .await?;
    if !matches!(outcome, dovecote::ImportOutcome::AlreadyImported { .. }) {
        return Err(invalid(format!(
            "exact rerun was not idempotent: {outcome:?}"
        )));
    }
    transaction.commit().await?;

    let mut changed = first.clone();
    changed.payload = "{\"changed\":true}".to_owned();
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(fixture, &changed)?,
            delivery_state(first)?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_mysql::ImportError::IdentityConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed immutable content did not return IdentityConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let pending = expected
        .events
        .iter()
        .find(|item| item.state == "pending")
        .ok_or_else(|| invalid("fixture has no pending event".into()))?;
    let mut transaction = adapter.pool().begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            build_event(&expected, pending)?,
            ImportedDeliveryState::delivered(
                parse_time(Some("2026-01-04T00:00:00Z"))?.expect("fixed timestamp"),
            )?,
        )
        .await;
    if !matches!(
        conflict,
        Err(dovecote_sqlx_mysql::ImportError::ImportConflict { .. })
    ) {
        return Err(invalid(format!(
            "changed delivery state did not return ImportConflict: {conflict:?}"
        )));
    }
    transaction.rollback().await?;
    let delivered = expected
        .events
        .iter()
        .filter(|item| item.state == "delivered")
        .map(|item| event_id(item, &item.project))
        .collect::<Vec<_>>();
    let claimed = adapter
        .claim(
            WorkerId::new("migration-fixture-verifier")?,
            Lease::new(Duration::from_secs(30))?,
            Limit::new(32)?,
        )
        .await?;
    let zero_delay = Delay::new(Duration::ZERO)?;
    let mut second_publication = None;
    for item in claimed {
        if item.event().id().as_str() == publication_id {
            second_publication = Some(publication_observation(item.event())?);
        }

        if delivered.iter().any(|id| id == item.event().id().as_str()) {
            return Err(invalid(format!(
                "delivered event {} was claimable",
                item.event().id().as_str()
            )));
        }
        adapter
            .release(item.row_id(), item.claim_token(), zero_delay)
            .await?;
    }

    if second_publication.as_ref() != Some(&first_publication) {
        return Err(invalid(
            "legacy and Dovecote publications were not byte-identical".into(),
        ));
    }
    Ok(())
}
