# Changelog

## [0.2.0] - 2026-08-28

### Core

- Canonicalize accepted timestamps to UTC at event, extension, import, and
  delivery-state construction boundaries.
- Distinguish empty values from oversized values in the public validation
  taxonomy, with complete core API rustdoc and dependency hygiene.
- Mark public errors, outcomes, and backend metadata `#[non_exhaustive]` ahead
  of 1.0 so their APIs can evolve without making exhaustive matches brittle.
- Added a 1.0 readiness checklist, a runnable SQLite walkthrough, and clarified
  all-stream publication ownership and application-owned operational queries.
- Added validated `TenantId` values and tenant metadata on claimed and paged
  state. All SQLx adapters now expose tenant-scoped and explicit admin handles;
  durable identity is tenant-scoped as `(tenant_id, source, id)` while the
  projected CloudEvents identity remains `(source, id)`.
- Added clean tenant-aware baselines and explicit v1 prepare/backfill/activate
  paths for PostgreSQL, MySQL/MariaDB, and SQLite.

## [0.1.1] - 2026-08-28

### Fixed

- Live and snapshot paging now includes every event row and reports a typed
  serialization error when its delivery row is missing.
- PostgreSQL schema checks reject unexpected columns, and MySQL/MariaDB
  readiness consistently requires `REPEATABLE-READ` for finite snapshots.
- Migration imports return a typed error if a future imported delivery state is
  not supported by this adapter.

## [0.1.0] - 2026-08-27

### Added

- The runtime-free `dovecote` event, validation, extension, serialization,
  projection, lifecycle, and typed-error model.
- PostgreSQL, MySQL/MariaDB, and SQLite SQLx adapters with caller-owned
  transaction enqueue, schema checks, leased claims, token-fenced lifecycle
  mutations, and live and finite snapshot paging.
- A narrowly named `import_for_migration` path for pending or delivered
  legacy history, including complete identity and delivery-state comparison.
- Backend support notes, operations and recovery guidance, CloudEvents and
  Debezium integration boundaries, and the Keepsake/Gatekeep migration
  runbook.
- MSRV and stable Rust CI jobs with separately attributable PostgreSQL, MySQL
  8.4, MySQL Innovation, MariaDB, and SQLite coverage.

### Boundaries

- Dovecote does not publish messages, run workers, provide a transport client,
  authorize tenants, apply migrations automatically, enforce retention, or
  provide FIFO or exactly-once publication.
- Delivery is at least once after an ambiguous send. Consumers must deduplicate
  the CloudEvents `source + id` identity.
- Backend support is version-specific and recorded in the support matrix. The
  Debezium configuration in this repository is a reference fixture, not live
  CDC evidence.
- Historical migrations are forward-only and remain byte-identical; legacy
  audit tables are not automatically dropped during sibling-library migration.
