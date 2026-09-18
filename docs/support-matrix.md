# Backend support matrix

Backend requirements are listed below; CI owns the exact test images and Rust
toolchain matrix. Release results belong with the corresponding release.

All jobs use SQLx 0.9.0 and run the workspace against Rust 1.94.0 (the MSRV)
and latest stable Rust. The backend job sets only its own `*_REQUIRED=1`
variable; other live adapter suites receive their `*_OPTIONAL=1` variable and
skip. This makes a green job attributable to one backend rather than to an
accidental service or a missing URL.

| Backend | Exact CI target | Required settings recorded by the test contract |
| --- | --- | --- |
| PostgreSQL | `postgres:17.11` | `READ COMMITTED`; finite statement/lock waits; lock-timeout fixture uses a 50 ms session override; schema v2 tenant predicates and optional RLS profile |
| MySQL 8.4 LTS | `mysql:8.4.11` | `REPEATABLE-READ`; `time_zone=+00:00`; strict SQL mode without `NO_AUTO_VALUE_ON_ZERO`; `utf8mb4` client/connection/results and an `utf8mb4_*` collation; InnoDB |
| MySQL Innovation | `mysql:26.7.0` | same MySQL settings; the image tag is pinned independently of the moving Innovation series |
| MariaDB LTS | `mariadb:11.8.6` | `REPEATABLE-READ`; `time_zone=+00:00`; strict SQL mode without `NO_AUTO_VALUE_ON_ZERO`; `utf8mb4` client/connection/results and an `utf8mb4_*` collation; InnoDB |
| SQLite | SQLx linked runtime `3.46.0` | foreign keys on every connection; `BEGIN IMMEDIATE`; default `BusyConfig` is 5 s × (3 retries + initial attempt) = 20 s maximum lock-wait budget; deployments set explicit page/time budgets for retained snapshots |

## Additional checks

Set `DOVECOTE_HIGH_CARDINALITY=1` to enable the ignored many-tenant fixtures.
Their local timings are workload measurements, not a deployment SLO.

The three adapters also expose the migration-only
`import_for_migration` and `finalize_pending_delivery_for_migration`
operations. SQLite has local contract suites covering pending and delivered
imports, exact reruns, immutable-event and delivery-state conflicts, legacy
delivery finalization, rollback, schema mismatch, timestamp endpoints, and
precision rejection. PostgreSQL and MySQL/MariaDB have corresponding database
test modules; they remain environment-gated and do not turn an unset live URL
into backend evidence. Import/finalization support is therefore not advertised
independently of the exact backend conformance and migration fixtures listed
below.

The MySQL and MariaDB adapters reject a non-UTC session, missing strict mode,
unsupported isolation, non-`utf8mb4` connection settings, and non-InnoDB
schemas. They also verify `SKIP LOCKED` and enforced checks before operations.
PostgreSQL and SQLite expose their different clock, transaction, and locking
models rather than treating them as interchangeable.

The PostgreSQL RLS role-boundary proof is an explicit opt-in live test. Set
`DOVECOTE_POSTGRES_RLS_URL` to a disposable PostgreSQL 17.11 URL authenticated
as a superuser and run
`cargo test -p dovecote-sqlx-postgres --test postgres tenancy::postgres_rls_live_role_boundary_is_enforced_when_configured`.
The test creates an isolated schema and a temporary `NO BYPASSRLS` login role,
grants only the table, sequence, and schema privileges it needs, then drops
both objects. Set `DOVECOTE_POSTGRES_RLS_REQUIRED=1` to fail when the URL is
missing or is not a superuser; otherwise an ordinary application URL is
reported as a deliberate skip. The ordinary `DOVECOTE_POSTGRES_URL` setting
does not imply the privilege to create roles or prove RLS boundaries.

## MariaDB migration evidence

The MariaDB migration fixture differs from the ordinary MySQL-family schema
fixture. It creates the historical Keepsake and Gatekeep
tables on pinned MariaDB 10.3.17 from the hash-checked published migration
artifacts, cleanly stops that server, starts MariaDB 11.8.6 on the same data
volume, runs `mariadb-upgrade`, and invokes the public Dovecote importer against
the already-existing source tables. MariaDB 10.3.17 is an evidence source, not
a supported deployment target: it is the pinned release on which the real
historical generated-column artifact can be installed before later MariaDB
made that SQL-mode dependency fatal. The target phase never applies the
immutable Keepsake MySQL migration files. `mariadb-upgrade` reports the inherited
generated-column warning, and the fixture then proves the legacy rows remain
readable for complete-history import.

This is a reproducible evidence path for the pinned server transition, not a
claim that an arbitrary deployed schema can be rebuilt from source migrations
or that every MariaDB release is interchangeable. Existing Keepsake users must
follow the [maintenance-window route in the migration runbook](migrations/keepsake-gatekeep.md#mariadb-maintenance-window-route-for-existing-keepsake-deployments),
including a verified backup, claim resolution or fencing, complete-history
import, zero-delta reconciliation, and read-only retention of legacy tables.
Do not replay the historical Keepsake migration directly on MariaDB 11.8.6;
use the maintenance-window route for retained legacy data.

## Database release gate

Database support is a per-backend claim. Before advertising one row in the
matrix, the release review must have all of the following for that exact
server/runtime combination:

- the pinned CI job passing on Rust 1.94.0 and stable Rust;
- backend conformance, locking/race, schema, timestamp, error, and migration
  evidence, including the complete Keepsake/Gatekeep fixtures;
- package archive inspection and the normal `cargo package --locked`
  verification permitted by the Dovecote-first publication order; and
- independent review of the implementation and its evidence.

A missing or failing CDC connector does not invalidate database adapter
evidence. Conversely, a passing database job does not establish CDC support.

## CDC release gate

The repository gate additionally checks the checked-in Debezium reference
configuration at
[`docs/debezium/dovecote-outbox.properties`](debezium/dovecote-outbox.properties)
and each crate's Cargo package archive. The properties fixture proves the
literal section 10.5 field selection and that only `dovecote_events` is
selected; it does not pretend to operate Kafka Connect or prove a downstream
converter. It is a repository correctness check, not a CDC release gate.

Database and CDC advertisement are separate decisions. To advertise CDC for a
backend, a separate review must additionally record:

- a live connector/converter fixture for that exact backend;
- the final transformed CloudEvent, including optional-field absence, exact
  JSON/binary payload bytes, and the documented timestamp precision;
- proof that only `dovecote_events` is watched and delivery lifecycle updates
  do not emit watched-table changes; and
- independent review of the connector, converter, and downstream evidence.

The structured projection already has exact CloudEvents v1.0.2 JSON Schema
coverage plus an external SDK parser check; those checks do not constitute
broker or HTTP-server execution evidence.

CDC is optional and remains unadvertised. Its missing connector/converter
fixtures and live connector validation do not block database adapter evidence,
but they are mandatory before Dovecote advertises CDC for any backend. If CDC
is advertised later, fixtures must cover each advertised backend and prove the
final transformed event, not only the raw Debezium envelope.
