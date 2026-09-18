# dovecote

> A holder. A recipient.
>
> — Ursula K. Le Guin, “The Carrier Bag Theory of Fiction” (1986)

`dovecote` stores an event in the same database transaction as your application
change. Your worker claims it, sends it, then acknowledges delivery. Failed
attempts can be retried without losing the original event.

Dovecote is pre-1.0; API and schema changes may require migration. See the
[backend requirements](docs/support-matrix.md) and
[migration guide](docs/migrations/keepsake-gatekeep.md) before deploying it.

## A transaction

Build the event, then enqueue it in the transaction that owns the application
change:

```rust
use dovecote::{EventId, EventSource, EventType, NewEvent, StreamName, TenantId};
use dovecote_sqlx_postgres::PostgresDovecote;
use sqlx::PgPool;

async fn record(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let event = NewEvent::builder(
        StreamName::new("audit")?,
        EventId::new("evt-123")?,
        EventSource::new("https://example.test/audit")?,
        EventType::new("com.example.audit.recorded")?,
    )
    .json_data(br#"{"ok":true}"#.to_vec())?
    .build()?;

    let adapter = PostgresDovecote::new(pool.clone())
        .for_tenant(TenantId::new("tenant-a")?);
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO application_audit_log (event_id) VALUES ($1)")
        .bind("evt-123")
        .execute(&mut *transaction)
        .await?;
    adapter.enqueue(&mut transaction, event).await?;
    transaction.commit().await?;
    Ok(())
}
```

The commit makes the application change and event visible together. Publication
happens later, through your worker. Delivery is at least once, so consumers must
deduplicate by tenant, source and event ID. Leased claim tokens fence delivery
updates; Dovecote does not promise FIFO or exactly-once delivery.

Try the complete SQLite example without setting up a server:

```sh
cargo run -p dovecote-sqlx-sqlite --example basic
```

It covers enqueue, claim, acknowledgement, retry and bounded history paging.

The core `dovecote` crate is synchronous and has no runtime or SQLx dependency.
Its SQLx adapters support PostgreSQL, MySQL/MariaDB, and SQLite without hiding
their different transaction, locking, clock, or migration behaviour.

## Documentation

- [SPEC.md](SPEC.md) defines the contract.
- [Operations](docs/operations.md), [recovery](docs/recovery.md), and the
  [support matrix](docs/support-matrix.md) cover production use.
- [Integration mappings](docs/integrations.md) cover HTTP, Kafka, NATS
  JetStream, Azure Event Grid, and Debezium.
- The [migration runbook](docs/migrations/keepsake-gatekeep.md) moves existing
  [Keepsake](https://github.com/plethu/keepsake) and
  [Gatekeep](https://github.com/plethu/gatekeep) data into Dovecote.
- [Contributing](CONTRIBUTING.md) covers development;
  [security](SECURITY.md) explains private vulnerability reporting.

## Development

The project uses the tools pinned in `.mise.toml`:

```sh
mise install
mise run check
```

Licensed under `MIT OR Apache-2.0`.
