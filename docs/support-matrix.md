# Backend support matrix

This scaffold advertises no database backend yet. A backend becomes a support
claim only after CI runs its conformance, race, locking, migration, and
projection fixtures and records the exact image, SQLx version, Rust version,
settings, and test date.

| Backend | Target release | Scaffold status | Release gate |
| --- | --- | --- | --- |
| PostgreSQL | exact CI-pinned version | schema artifact only | not advertised |
| MySQL | 8.4 LTS and current Innovation line | schema artifact only | not advertised |
| MariaDB | 11.8 LTS | schema artifact only | not advertised |
| SQLite | exact CI-pinned version | schema artifact only | not advertised |

The migration files are durable artifacts, not proof that the adapter contract
has been implemented. Do not add a backend to this table's advertised status
until `check_schema`, transaction-bound enqueue, fenced lifecycle operations,
live and snapshot paging, and the shared contract are all exercised.
