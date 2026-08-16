# Carrier

Carrier is a storage-only transactional outbox for applications that need to
commit an event beside application state and deliver it later. It owns durable
insertion, inspection, leased claims, claim-token fencing, retry state, and
quarantine. It does not deliver messages or run a worker.

The accepted contract is [SPEC.md](SPEC.md). This repository is at the initial
scaffold stage: the public value model and backend migration artifacts are
present, while SQLx operations and their conformance suites are deliberately
the next implementation boundary.

Carrier does not provide exactly-once publication, FIFO ordering, a transport
client, tenant authorization, retention, automatic migrations, or a worker
runtime. Delivery is at least once after an ambiguous send; consumers use the
CloudEvents `source + id` pair for duplicate identity.

## Workspace

- `carrier` is synchronous, runtime-free, and SQLx-free. It owns finalized
  events, validated extensions, projections, lifecycle values, typed bounds,
  and stable validation codes with English diagnostic `Display` output.
- `carrier-sqlx-postgres` owns PostgreSQL migrations and will own concrete
  PostgreSQL operations. Its `runtime-tokio` feature is the explicit default
  runtime policy for the future async adapter surface.
- `carrier-sqlx-mysql` owns MySQL/MariaDB dialect migrations and will own their
  concrete operations. MySQL and MariaDB remain separately verified claims.
- `carrier-sqlx-sqlite` owns SQLite migrations and will own bounded busy
  handling and concrete operations. Its migration constraint smoke test runs in
  the repository gate.

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
Rust file-size review. It does not claim database conformance until the backend
matrix and integration fixtures are implemented.

## Versioning

The initial MSRV is Rust 1.94. Crate semver, durable schema version, tagged
extension encoding, and projection format are separate contracts. Schema
migrations are application-controlled and forward-only; library startup never
changes a database.

The project is licensed under `MIT OR Apache-2.0`.
