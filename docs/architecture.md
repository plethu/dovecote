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
During the Keepsake/Gatekeep 1.x bridge releases, legacy and Dovecote records
are temporarily written together (with a pending Dovecote delivery), and the
legacy publisher remains the owner.
After the 2.0 cutover, Dovecote is the sole maintained SQL audit/outbox shape;
legacy tables are read-only historical source material.
The delivery row is mutable; the event row is immutable and is the only table
intended for CDC observation.

## Decisions carried by the scaffold

- The workspace has no common async repository trait. PostgreSQL, MySQL /
  MariaDB, and SQLite have materially different lock and clock contracts.
- `DeliverySnapshot` is an enum whose variants own state-specific fields. A
  collection of optional claim and terminal fields would permit contradictory
  states in the Rust API.
- `ClaimToken` has fixed 128-bit storage and redacted `Debug` output. Adapters
  will source tokens from operating-system randomness and fence every mutation
  by row ID, state, token, and unexpired database time.
- Durable extensions are a lexicographically ordered tagged JSON object. JSON
  payload bytes remain exact at rest; structured CloudEvents projection may
  parse and reserialize them deterministically.
- Validation is a finalization boundary: raw input is assembled through
  `NewEventBuilder`, then only the checked `NewEvent` enters adapter APIs.
  Validation errors retain a stable kind/code while `Display` and
  `to_english()` provide the local, locale-neutral diagnostic projection.
- Adapter migrations are exposed as ordered versioned artifacts with explicit
  compatibility metadata. They are never applied implicitly.
- `import_for_migration` is a deliberately separate, concrete adapter boundary:
  the application owns legacy-row extraction and the caller transaction, while
  Dovecote owns schema validation, complete immutable identity comparison, and
  canonical pending/delivered state comparison. It never imports a claim.

All three SQLx adapter families implement read-only schema verification,
caller-transaction-bound enqueue and migration import, leased claims, claim-token-fenced lifecycle
mutations, and live and finite snapshot paging. SQLite write and claim paths use
`BEGIN IMMEDIATE`; its caller transaction must already own the writer slot, and
its bounded `BusyConfig` has a total lock-wait budget of at most
`(retries + 1) * timeout`. Snapshot pages retain one finite read transaction
and use database-generated millisecond timestamps. Cross-backend conformance,
race, locking, migration, projection, CDC, and release evidence remain
incomplete as described in [SPEC.md](../SPEC.md).
SQLite deployments bound snapshot page/time budgets because retained read
snapshots can delay vacuum or cleanup; abandoned pagers are explicitly closed
or rolled back and reconciliation restarts from a new snapshot/checkpoint.
