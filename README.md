# dovecote

> A holder. A recipient.
>
> — Ursula K. Le Guin, “The Carrier Bag Theory of Fiction” (1986)

`dovecote` is a transactional outbox for Rust applications. It writes a
validated CloudEvents-compatible event in the same database transaction as
application state, then keeps its delivery state for an application-owned
worker.

Claims are leased, and delivery mutations require the matching claim token.
Delivery is at least once, so consumers deduplicate on `(source, id)`. Dovecote
does not run workers, choose transports, apply migrations, or promise FIFO or
exactly-once delivery.

> [!WARNING]
> Dovecote is pre-release (`0.1.0`). Rust APIs, the durable schema, and migration
> tooling may change before v1. Backend support is version-specific; see the
> [support matrix](docs/support-matrix.md). Existing Keepsake deployments on
> MariaDB use the documented [maintenance-window migration
> route](docs/migrations/keepsake-gatekeep.md#mariadb-maintenance-window-route-for-existing-keepsake-deployments).

## A transaction

Build the event, then enqueue it in the transaction that owns the application
change:

```rust
use dovecote::{ContentType, EventData, EventId, EventSource, EventType, NewEvent, StreamName};
use dovecote_sqlx_postgres::PostgresDovecote;
use sqlx::PgPool;

async fn record(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let event = NewEvent::builder(
        StreamName::new("audit")?,
        EventId::new("evt-123")?,
        EventSource::new("https://example.test/audit")?,
        EventType::new("com.example.audit.recorded")?,
    )
    .datacontenttype(ContentType::new("application/json")?)
    .data(EventData::json(br#"{"ok":true}"#.to_vec())?)
    .build()?;

    let adapter = PostgresDovecote::new(pool.clone());
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
happens later, through a worker owned by the application.

The `dovecote` crate is synchronous, runtime-free, and SQLx-free. Concrete SQLx
adapters are provided for PostgreSQL, MySQL/MariaDB, and SQLite; each keeps its
database's transaction, locking, clock, and migration behaviour explicit.

## Documentation

- [SPEC.md](SPEC.md) is the accepted contract.
- [Operations](docs/operations.md), [recovery](docs/recovery.md), and the
  [support matrix](docs/support-matrix.md) cover deployment and backend evidence.
- [Integration mappings](docs/integrations.md) cover HTTP, Kafka, NATS
  JetStream, Azure Event Grid, and Debezium boundaries.
- The [Keepsake and Gatekeep migration
  runbook](docs/migrations/keepsake-gatekeep.md) covers paused and rolling
  cutovers, including the MariaDB maintenance-window route.
- [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md) describe the
  project and its private reporting route.

## Development

The project uses the tools pinned in `.mise.toml`:

```sh
mise install
mise run check
```

Licensed under `MIT OR Apache-2.0`.
