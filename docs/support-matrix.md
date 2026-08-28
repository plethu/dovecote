# Backend support matrix

Evidence snapshot: 2026-08-28. The current results below are local
0.2.0 release-candidate evidence from disposable backend instances, not
release CI. They supplement the historical 0.1.1 CI evidence; they do not
replace the exact CI job, package inspection, or final reviewed release
commits. The tenant-aware 0.2.0 API remains unadvertised and pending those
gates. This is not a claim for a neighbouring database release.

All jobs use SQLx 0.9.0 and run the workspace against Rust 1.94.0 (the MSRV)
and latest stable Rust. The backend job sets only its own `*_REQUIRED=1`
variable; other live adapter suites receive their `*_OPTIONAL=1` variable and
skip. This makes a green job attributable to one backend rather than to an
accidental service or a missing URL.

| Backend | Exact CI target | Required settings recorded by the test contract | Current evidence | Release status |
| --- | --- | --- | --- | --- |
| PostgreSQL | `postgres:17.11` | `READ COMMITTED`; finite statement/lock waits; lock-timeout fixture uses a 50 ms session override; schema v2 tenant predicates and optional RLS profile | Historical 0.1.1 pre-tenant suite passed in CI. Local 0.2.0 RC: 43/43 serialized backend tests passed, including the configured live RLS role-boundary proof, and the complete-history fixture passed against PostgreSQL 17.11. | 0.2.0 tenant-aware API remains unadvertised; CI, package, and final reviewed-commit gates are pending |
| MySQL 8.4 LTS | `mysql:8.4.11` | `REPEATABLE-READ`; `time_zone=+00:00`; strict SQL mode without `NO_AUTO_VALUE_ON_ZERO`; `utf8mb4` client/connection/results and an `utf8mb4_*` collation; InnoDB | Historical 0.1.1 suite and complete-history fixture passed in CI. Local 0.2.0 RC: 25/25 backend tests and the complete-history fixture passed against MySQL 8.4.11. The dedicated disposable v1-to-v2 activation/preflight/interruption/rerun rehearsal passed 1/1 on MySQL 8.4.11. | 0.2.0 tenant-aware API remains unadvertised; CI, package, and final reviewed-commit gates are pending |
| MySQL Innovation | `mysql:26.7.0` | same MySQL settings; the image tag is pinned independently of the moving Innovation series | Historical 0.1.1 suite and complete-history fixture passed separately from MySQL 8.4 in CI. Local 0.2.0 RC: 24/24 backend tests and the complete-history fixture passed against MySQL 26.7.0. | 0.2.0 tenant-aware API remains unadvertised; CI, package, and final reviewed-commit gates are pending |
| MariaDB LTS | `mariadb:11.8.6` | `REPEATABLE-READ`; `time_zone=+00:00`; strict SQL mode without `NO_AUTO_VALUE_ON_ZERO`; `utf8mb4` client/connection/results and an `utf8mb4_*` collation; InnoDB | Historical 0.1.1 suite and pinned 10.3.17-to-11.8.6 maintenance-window fixture passed in CI. Local 0.2.0 RC: 25/25 backend tests passed, with five repeated competing-import race runs, and the complete-history maintenance path passed from MariaDB 10.3.17 to 11.8.6. The dedicated disposable v1-to-v2 activation/preflight/interruption/rerun rehearsal passed 1/1 on MariaDB 11.8.6. | 0.2.0 tenant-aware API remains unadvertised; CI, package, and final reviewed-commit gates are pending. Existing Keepsake deployments use the maintenance-window route below. |
| SQLite | SQLx linked runtime `3.46.0` | foreign keys on every connection; `BEGIN IMMEDIATE`; default `BusyConfig` is 5 s × (3 retries + initial attempt) = 20 s maximum lock-wait budget; deployments set explicit page/time budgets for retained snapshots | Historical 0.1.1 linked-runtime suite and migration smoke test passed on stable and MSRV. Local 0.2.0 RC current adapter suites and the complete-history fixture passed. | 0.2.0 tenant-aware API remains unadvertised; CI, package, and final reviewed-commit gates are pending |

## Local opt-in high-cardinality evidence

The ignored `DOVECOTE_HIGH_CARDINALITY=1` fixture passed locally on this
release candidate: PostgreSQL 17.11 completed 1/1 in 3.10 seconds, and SQLite
linked runtime 3.46.0 completed 1/1 in 1.14 seconds. Each run populated 10,000
tenants with one shared CloudEvents `(source, event_id)` identity per tenant,
then added 64 events for one hot tenant: 10,064 event rows and 10,064 delivery
rows. These are bounded, disposable local opt-in results, not CI evidence, a
hardware-independent latency SLO, or a general throughput claim.

The local 0.2.0 complete-history matrix covers SQLite, PostgreSQL 17.11,
MySQL 8.4.11, MySQL Innovation 26.7.0, and the MariaDB 10.3.17 to 11.8.6
maintenance-window route. These are reproducible local release-candidate
results only; they do not change the release status above or establish a
deployment claim before the CI, package, and final review gates pass.

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

The MariaDB migration fixture is deliberately different from the ordinary
MySQL-family schema fixture. It creates the historical Keepsake and Gatekeep
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
The 0.1.1 evidence supports that exact maintenance-window route on MariaDB
11.8.6. It does not support replaying the historical Keepsake migration directly
on MariaDB 11.8.6.

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
