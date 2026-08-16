# Architecture and ownership

Carrier has one logical event lifecycle and three database-specific effects.
The core crate owns values that can be validated without a runtime or a
database. Each SQLx adapter owns its database's schema, transaction shape,
clock, locking, migration check, and error translation.

```text
application transaction
        |
        +--> carrier::NewEvent --validated--> carrier_events + carrier_deliveries
                                                   |
                                      adapter claims and fences mutations
                                                   |
                                      application-owned transport worker
```

The application owns the outer transaction. Carrier never migrates at startup,
creates a worker, sends a message, chooses a destination, or deletes a row.
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

The SQLx operation surface is intentionally the next slice. It will be added
only with the shared conformance contract, race tests, and backend-specific
locking tests described in [SPEC.md](../SPEC.md).
