//! A small, runnable SQLite walkthrough.
//!
//! This example keeps the worker loop deliberately visible and incomplete. It
//! demonstrates the boundaries an application owns: schema installation,
//! caller-transaction enqueue, token-fenced retry/ack, and finite paging.

use dovecote::{
    ContentType, Delay, EventData, EventId, EventSource, EventType, Failure, Lease, Limit,
    NewEvent, StreamName, WorkerId,
};
use dovecote_sqlx_sqlite::{MIGRATIONS, SqliteDovecote};
use sqlx::{raw_sql, sqlite::SqlitePoolOptions};
use std::{error::Error, io, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(dovecote::TenantId::new("example")?);

    // Migrations are application-owned. The adapter only checks their result.
    for migration in MIGRATIONS {
        raw_sql(migration.sql()).execute(&pool).await?;
    }
    adapter.check_schema().await?;

    sqlx::query("CREATE TABLE application_state (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        .execute(&pool)
        .await?;

    let event = NewEvent::builder(
        StreamName::new("example")?,
        EventId::new("sqlite-example-1")?,
        EventSource::new("https://example.test/sqlite")?,
        EventType::new("com.example.recorded")?,
    )
    .datacontenttype(ContentType::new("application/json")?)
    .data(EventData::json(br#"{"ok":true}"#.to_vec())?)
    .build()?;

    // The application row and Dovecote rows share one caller-owned commit.
    let mut transaction = adapter.begin_enqueue().await?;
    sqlx::query("INSERT INTO application_state (id, note) VALUES (1, 'committed with event')")
        .execute(&mut *transaction)
        .await?;
    let outcome = adapter.enqueue(&mut transaction, event).await?;
    transaction.commit().await?;
    println!("enqueue: {outcome:?}");

    // A transient failure returns the row to pending; the next claim gets a
    // fresh token. The example then acknowledges that reclaimed claim.
    let worker = WorkerId::new("sqlite-example-worker")?;
    let lease = Lease::new(Duration::from_secs(30))?;
    let limit = Limit::new(10)?;
    let claimed = adapter.claim(worker.clone(), lease, limit).await?;
    let first = claimed
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::other("expected one claim"))?;
    let failure = Failure::new("example.transient", "demo failure")?;
    adapter
        .retry(
            first.row_id(),
            first.claim_token(),
            &failure,
            Delay::new(Duration::ZERO)?,
        )
        .await?;

    let reclaimed = adapter.claim(worker, lease, limit).await?;
    let second = reclaimed
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::other("expected reclaimed claim"))?;
    adapter.ack(second.row_id(), second.claim_token()).await?;
    println!(
        "recovery: retried and acknowledged row {}",
        second.row_id().get()
    );

    // A snapshot has a finite row-id ceiling and must be explicitly finished
    // (or rolled back) by the application.
    let mut snapshot = adapter.begin_snapshot().await?;
    while !snapshot.is_exhausted() {
        for row in snapshot.next_page(Limit::new(1)?).await? {
            println!(
                "snapshot: row {} state {:?} event {}",
                row.row_id().get(),
                row.delivery().state(),
                row.event().id().as_str()
            );
        }
    }
    snapshot.finish().await?;

    pool.close().await;
    Ok(())
}
