# Changelog

## [Unreleased]

The first Dovecote release is not published. Local and live backend gates,
complete Keepsake/Gatekeep migration fixtures, independent reviews, and the
private security-reporting route are complete. Publication follows an
exact-revision GitHub CI run and the documented registry order.

## [0.1.0] - Unreleased

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
- No database backend or CDC integration is advertised until its exact release
  gates and required fixtures pass. The Debezium configuration in this
  repository is a reference fixture, not live CDC evidence.
- Historical migrations are forward-only and remain byte-identical; legacy
  audit tables are not automatically dropped during sibling-library migration.
