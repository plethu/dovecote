# dovecote

> A holder. A recipient.
>
> — Ursula K. Le Guin, “The Carrier Bag Theory of Fiction” (1986)

`dovecote` is a transactional outbox for Rust applications. The event insert
is the easy bit. Claims expire, acknowledgements race, sends become ambiguous,
and somebody still has to work out what happened at three in the morning.

Dovecote stores a validated event in the same database transaction as the
application state that produced it, then keeps delivery state explicit until a
worker finishes the job. It does not run that worker or choose a transport.
Delivery is at least once; consumers deduplicate with the CloudEvents
`source + id` pair.

The accepted contract is [SPEC.md](SPEC.md). The runtime-free value model and
all three SQLx adapter families include caller-transaction-bound enqueue,
read-only schema verification, leased claims, claim-token-fenced lifecycle
mutations, and live and finite snapshot paging. Cross-backend conformance and
independent projection validation now exist.

> [!WARNING]
> Dovecote is pre-release (`0.1.0`). Rust APIs, durable schema, and migration
> tooling may change before v1. The adapters and backend matrix remain
> unadvertised until their exact CI and release evidence passes. Existing
> Keepsake users on MariaDB use the maintenance-window route in the migration
> runbook; MariaDB can require downtime, and the route does not replay
> Keepsake's MySQL migration on MariaDB 11.8.

Dovecote does not provide exactly-once publication, FIFO ordering, tenant
authorization, retention, automatic migrations, or a worker runtime.

## First transaction

Build the event without a runtime or database, then enqueue it in the same
caller-owned transaction as the application change. This PostgreSQL example
deliberately stops before migration execution, transport delivery, and worker
supervision:

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
    // An application-owned mutation in the same transaction.
    sqlx::query("INSERT INTO application_audit_log (event_id) VALUES ($1)")
        .bind("evt-123")
        .execute(&mut *transaction)
        .await?;
    adapter.enqueue(&mut transaction, event).await?;
    // Commit this only after the application state and event are both ready.
    transaction.commit().await?;
    Ok(())
}
```

The commit makes the application change and event visible together. It does
not send a message, create a worker, provide exactly-once publication or FIFO,
authorize tenants, apply migrations, or choose a transport. The consumer still
deduplicates ambiguous retries with `source + id`; see the [operations
guide](docs/operations.md) for the deployment boundary.

## Documentation

- [Acceptance specification](SPEC.md) — the normative event, schema, lifecycle,
  projection, integration, migration, and release contract.
- [Operations and compatibility](docs/operations.md) — schema installation,
  `check_schema`, versioning, worker shutdown, signals, privacy, retention, and
  a fake-transport recovery loop.
- [Integration mappings](docs/integrations.md) — HTTP, Kafka, NATS JetStream,
  Azure Event Grid, and Debezium boundaries.
- [Keepsake/Gatekeep migration runbook](docs/migrations/keepsake-gatekeep.md)
  — identity mapping, the paused cutover, the MariaDB maintenance-window
  route, bridge limits, rollback, and evidence.
- [Architecture](docs/architecture.md) and [recovery boundaries](docs/recovery.md)
  — ownership and lifecycle shape.
- [Backend support matrix](docs/support-matrix.md) — exact versions, settings,
  evidence, and advertisement status.
- [Security reporting policy](SECURITY.md) — the private reporting route and
  its per-release verification gate.

The adapters remain pre-release and no backend is advertised until its exact CI
job and the required release fixtures pass.

## Workspace

- `dovecote` is synchronous, runtime-free, and SQLx-free. It owns finalized
  events, validated extensions, projections, lifecycle values, typed bounds,
  and stable validation codes with English diagnostic `Display` output.
- `dovecote-sqlx-postgres` owns PostgreSQL migrations, schema verification,
  caller-transaction-bound enqueue, leased claims, fenced lifecycle mutations,
  and live and finite snapshot paging. Its `runtime-tokio` feature is the
  explicit default runtime policy for the async adapter surface.
- `dovecote-sqlx-mysql` owns MySQL/MariaDB dialect migrations and their concrete
  operations. MySQL and MariaDB remain separately verified claims; their exact
  release evidence is recorded in the support matrix.
- `dovecote-sqlx-sqlite` owns SQLite migrations, concrete operations, and
  bounded busy handling. Callers use `begin_write`/`begin_enqueue` (or perform
  an application write first) before enqueue; a `BusyConfig` policy waits at
  most `(retries + 1) * timeout` for writer-lock acquisition. Snapshot paging
  is finite, retains one read transaction, and uses SQLite database time at
  millisecond resolution. Deployments set bounded page/time budgets, explicitly
  close or roll back abandoned snapshots, and restart reconciliation from a new
  snapshot. Its migration constraint smoke test runs in the repository gate,
  but SQLite is not a release-advertised backend yet.

The adapter crates intentionally do not share a repository trait. The database
transaction, locking model, clock behaviour, and SQL dialect are part of each
adapter's correctness boundary.

`NewEvent::builder(...).build()` returns a finalized event. A larger explicit
event-size profile uses `build_with_limit`; the selected limit stays attached to
the finalized input until it becomes a stored event. `ValidationError` exposes
`kind()` and `code()` for programmatic handling and `to_english()` for the
locale-neutral diagnostic projection used by the local Rust libraries.

## Development

The project uses the pinned tools in `.mise.toml`. With `mise` installed:

```sh
mise install
mise run fmt
mise run check
```

The direct equivalents are `just fmt` and `just verify`. The verification gate
uses Cargo, Taplo, Typos, a repository-wide ast-grep scan, and a warning-only
Rust file-size review. PostgreSQL integration tests use an isolated temporary
schema when `DOVECOTE_POSTGRES_URL` is set. They skip locally when it is absent;
CI/release runs fail when `CI=true` or `DOVECOTE_RELEASE_MODE=1` unless
`DOVECOTE_POSTGRES_OPTIONAL=1` is explicitly set. `DOVECOTE_POSTGRES_REQUIRED=1`
also makes the test mandatory.

For live backend tests, provide the matching URL and required flag:

```sh
DOVECOTE_POSTGRES_URL=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  DOVECOTE_POSTGRES_REQUIRED=1 cargo test --workspace --all-features

DOVECOTE_MYSQL_URL=mysql://root:password@127.0.0.1:3306/dovecote_test \
  DOVECOTE_MYSQL_REQUIRED=1 cargo test --workspace --all-features
```

`DOVECOTE_POSTGRES_URL` selects the PostgreSQL server used by the adapter
tests. `DOVECOTE_MYSQL_URL` selects the MySQL or MariaDB server; the adapter
detects which server it reached, but MySQL 8.4, MySQL Innovation, and MariaDB
must still be tested as separate backend targets. `DOVECOTE_POSTGRES_REQUIRED=1`
and `DOVECOTE_MYSQL_REQUIRED=1` turn a missing URL into a failure. In CI or
release mode, the corresponding URL is required unless the matching
`DOVECOTE_POSTGRES_OPTIONAL=1` or `DOVECOTE_MYSQL_OPTIONAL=1` flag is set for a
non-target job. SQLite uses the linked SQLx runtime and has no URL control.

These controls decide whether a test may skip; they do not make a backend a
release claim. The exact images, session settings, and evidence gates are in
the [backend support matrix](docs/support-matrix.md). Database adapter release
gates and the separate, optional [CDC release gate](docs/support-matrix.md#cdc-release-gate)
are not interchangeable.

## Versioning

The initial MSRV is Rust 1.94. Crate semver, durable schema version, tagged
extension encoding, and projection format are separate contracts. Schema
migrations are application-controlled and forward-only; library startup never
changes a database.

The project is licensed under `MIT OR Apache-2.0`.

## Publishing

The repository gate verifies the `dovecote` package archive and only constructs
the SQLx adapter archives locally. Adapter construction uses `--no-verify`
because Cargo normalizes their path dependency on the as-yet-unpublished
`dovecote` crate to its registry form. The gate labels those archives as
unverified; it does not treat construction as release verification.

The release order is deliberately explicit: publish `dovecote` first and wait
for the registry to serve that version, then run
`cargo package --package <adapter> --locked` without `--no-verify` for every
adapter. Publish an adapter only after its normal package verification passes.
