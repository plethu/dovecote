# dovecote

> A holder. A recipient.
>
> — Ursula K. Le Guin, “The Carrier Bag Theory of Fiction” (1986)

`dovecote` is a transactional outbox for Rust applications. It writes a
validated CloudEvents-compatible event in the same database transaction as
application state, then keeps its delivery state for an application-owned
worker. In schema version 2, durable event identity is scoped to the tenant
handle as `(tenant_id, source, id)`.

Claims are leased, and only the matching claim token can change a delivery.
Delivery is at least once. For one tenant, consumers can deduplicate on
`(source, id)`; a shared destination must include the tenant routing domain.
Your application runs the worker, chooses the transport, and applies
migrations. Dovecote promises neither FIFO nor exactly-once delivery.

> [!WARNING]
> Dovecote is pre-release (`0.2.x`). Expect the Rust API, durable schema, and
> migration tooling to change before v1. Backend support is version-specific;
> see the [support matrix](docs/support-matrix.md). Existing Keepsake
> deployments on MariaDB use the documented [maintenance-window migration
> route](docs/migrations/keepsake-gatekeep.md#mariadb-maintenance-window-route-for-existing-keepsake-deployments).

## A transaction

Build the event, then enqueue it in the transaction that owns the application
change:

```rust
use dovecote::{ContentType, EventData, EventId, EventSource, EventType, NewEvent, StreamName, TenantId};
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
happens later, through a worker owned by the application.

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
- [1.0 readiness](docs/1.0-readiness.md),
  [contributing](CONTRIBUTING.md), and [security](SECURITY.md) cover the project
  itself.

## Development

The project uses the tools pinned in `.mise.toml`:

```sh
mise install
mise run check
```

Licensed under `MIT OR Apache-2.0`.
