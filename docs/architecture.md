# Architecture and ownership

Dovecote has one logical event lifecycle and three database-specific effects.
The core crate owns values that can be validated without a runtime or a
database. Each SQLx adapter owns its database's schema, transaction shape,
clock, locking, migration check, and error translation.

```text
application transaction
        |
        +--> dovecote::NewEvent --validated--> dovecote_events + dovecote_deliveries
        |
        +--> migration exporter --validated--> import_for_migration (same tables)
                                                   |
                                      adapter claims and fences mutations
                                                   |
                                      application-owned transport worker
```

The application owns the outer transaction. Dovecote never migrates at startup,
creates a worker, sends a message, chooses a destination, or deletes a row.
The [migration runbook](migrations/keepsake-gatekeep.md) covers imports from
older [Keepsake](https://github.com/plethu/keepsake) and
[Gatekeep](https://github.com/plethu/gatekeep) tables. Dovecote is the only
maintained SQL audit and outbox shape; legacy tables remain read-only migration
material.
The delivery row is mutable; the event row is immutable and is the only table
intended for CDC observation. The claim API scans the complete Dovecote table
set, so an application chooses one publication mode for that table set: a
leased worker or CDC. Claiming covers the complete table set, so publication
ownership cannot be split by stream.

## Design choices

- The workspace has no common async repository trait. PostgreSQL, MySQL /
  MariaDB, and SQLite have materially different lock and clock contracts.
- `DeliverySnapshot` is an enum whose variants own state-specific fields. A
  collection of optional claim and terminal fields would permit contradictory
  states in the Rust API.
- `ClaimToken` has fixed 128-bit storage and redacted `Debug` output. Adapters
  source tokens from operating-system randomness and fence every mutation by
  row ID, state, token, and unexpired database time.
- Durable extensions are a lexicographically ordered tagged JSON object. JSON
  payload bytes remain exact at rest; structured CloudEvents projection may
  parse and reserialize them deterministically.
- Validation is a finalization boundary: raw input is assembled through
  `NewEventBuilder`, then only the checked `NewEvent` enters adapter APIs.
  Validation errors retain a stable kind/code while `Display` and
  `to_english()` provide the local, locale-neutral diagnostic projection.
- Adapter migrations are exposed as ordered versioned artifacts with explicit
  compatibility metadata. They are never applied implicitly.
- `import_for_migration` is a separate, concrete adapter boundary:
  the application owns legacy-row extraction and the caller transaction, while
  Dovecote owns schema validation, complete immutable identity comparison, and
  canonical pending/delivered state comparison. It never imports a claim.

Schema version 2 makes `tenant_id` a storage-authority key on both
event and delivery rows, and scopes durable identity to
`(tenant_id, source, event_id)`. Ordinary operations use a validated
tenant-scoped handle; all-tenant reads and explicitly named administrative
writes require an admin handle. Claimed and paged state carries the tenant
needed for safe worker routing. Projected CloudEvents retain their `(source,
id)` wire identity; optional PostgreSQL RLS supplements the adapter predicates.
MySQL/MariaDB has no RLS profile and should use a separate database for the
strongest boundary.
SQLite treats its database file as the security boundary; regulated
deployments should use one file per tenant.

All three SQLx adapter families implement read-only schema verification,
caller-transaction-bound enqueue and migration import, leased claims, claim-token-fenced lifecycle
mutations, and live and finite snapshot paging. SQLite write and claim paths use
`BEGIN IMMEDIATE`; its caller transaction must already own the writer slot, and
its bounded `BusyConfig` has a total lock-wait budget of at most
`(retries + 1) * timeout`. Snapshot pages retain one finite read transaction
and use database-generated millisecond timestamps. The 0.2.0 backend and
migration evidence is recorded in the [support matrix](support-matrix.md);
CDC remains an optional, separately advertised integration and is not live
evidence for the database adapters.
SQLite deployments bound snapshot page/time budgets because retained read
snapshots can delay vacuum or cleanup; abandoned pagers are explicitly closed
or rolled back and reconciliation restarts from a new snapshot/checkpoint.
