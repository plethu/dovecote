# Operations and compatibility

Dovecote is a storage library, not a migration runner or a worker service. This
page is the deployment path for the first schema version; [SPEC.md](../SPEC.md)
remains the normative contract. A database adapter is not release-advertised
until its exact backend, CI, projection, migration, and independent-review
evidence passes. CDC fixtures are required only for a separate CDC
advertisement; see the [support matrix](support-matrix.md).

## Install and verify the schema

Each SQLx adapter publishes an append-only `MIGRATIONS` slice. The application
owns the migration transaction and must execute each artifact in order using
its existing migration process. Do not apply a migration from library startup,
and do not edit a published artifact in place.

After installation, run the adapter's read-only `check_schema` function as a
deployment/startup gate. A mismatch is a failed deployment: it is not an
invitation to repair tables or to continue with a guessed schema.

| Backend | Migration and check | Installation boundary |
| --- | --- | --- |
| PostgreSQL | `dovecote_sqlx_postgres::MIGRATIONS`; `dovecote_sqlx_postgres::check_schema(&pool).await` | Creates the PostgreSQL schema marker and the two Dovecote domain tables. The marker is checked against the adapter's schema and crate compatibility metadata. |
| MySQL or MariaDB | `dovecote_sqlx_mysql::MIGRATIONS`; `dovecote_sqlx_mysql::check_schema(&pool).await` | Detects the server family and verifies the exact dialect-sensitive shape. Both tables must be InnoDB. MySQL evidence does not cover MariaDB. |
| SQLite | `dovecote_sqlx_sqlite::MIGRATIONS`; `dovecote_sqlx_sqlite::check_schema(&pool).await` | Creates only the two domain tables. Enable foreign keys on every connection. Use `SqliteDovecote::begin_write`/`begin_enqueue` for the caller transaction. |

The MySQL/MariaDB migration creates two validation triggers. The account that
installs the schema therefore needs trigger DDL authority. On a MySQL server
with binary logging enabled, an administrator may also need to enable
`log_bin_trust_function_creators` for the installation, according to the
deployment's replication policy. The ordinary application account does not
need that setting merely to use an already-installed Dovecote schema.

The SQL is available as `migration.sql()` on each public migration artifact.
The application migration runner may need to split or execute the artifact
according to its own backend rules; the adapter does not assume that one
`sqlx::query` call can execute a multi-statement file. If Keepsake and Gatekeep
share one application database, install the Dovecote schema once in that
database and have both libraries use the same adapter boundary.

Run `check_schema` after the migration and on every deployment. It verifies
the tables, columns, defaults, indexes, constraints, foreign key, backend
settings, and—on PostgreSQL—the schema marker. It never changes data. Keep
database time in UTC and use the settings recorded for the exact backend in
the [support matrix](support-matrix.md).

For an existing Keepsake deployment on MariaDB, do not replay Keepsake's
immutable MySQL migrations on MariaDB 11.8. Stop writers and the legacy
publisher, resolve or fence claims, take and verify a snapshot, then install
and check Dovecote additively before importing complete history from the
existing tables. The full maintenance-window sequence, zero-delta checks, and
read-only legacy retention policy are in the
[Keepsake/Gatekeep migration runbook](migrations/keepsake-gatekeep.md#mariadb-maintenance-window-route-for-existing-keepsake-deployments).

## Import legacy outbox rows

The adapters' `import_for_migration` operation is migration infrastructure for
the legacy-outbox-to-Dovecote cutover; it is not an enqueue shortcut. The application
must export each legacy row into an already validated `dovecote::NewEvent` and
choose `ImportedDeliveryState::Pending` or
`ImportedDeliveryState::Delivered { delivered_at }`. Legacy claims, retries,
and quarantines are not portable. An active unexpired claim must first finish,
expire, or be explicitly fenced; only then may the source row be mapped to
pending under the cutover policy.

Call it with the application's concrete transaction and commit that transaction
only after the application state and both Dovecote rows are ready. The importer
does not commit or roll back this caller-owned transaction. On a typed error,
the caller must roll it back (including surrounding application writes) before
retrying; this differs from adapter-owned lifecycle operations, which roll back
their own short transaction on failure. Schema validation occurs before the
first mutation. Pending imports use one
database-authoritative operation timestamp for `enqueued_at` and
`available_at`, and exact replay compares their stored backend representations;
SQLite's clock is millisecond-resolution with three trailing
fractional zeroes. A delivered timestamp is authoritative source data and is
stored at exact microsecond precision; a value outside the common range or with
sub-microsecond precision is rejected.

The first import returns `ImportOutcome::Imported`. An exact replay returns
`AlreadyImported` only for the same complete immutable event and canonical
zero-attempt delivery shape. A changed event returns the typed
`IdentityConflict`; a changed or previously claimed/retried delivery returns
the distinct `ImportConflict`. Backend SQL failures retain their normal
database-specific transient categories. Keep the operation bounded to the
named migration window and remove migration callers after reconciliation.

## Record a legacy delivery

When a legacy publisher successfully delivers a row that was dual-written as a
pending Dovecote delivery, call the adapter's explicit migration finalizer in
the same caller-owned transaction used to record the legacy acknowledgement:

```text
finalize_pending_delivery_for_migration(
    caller_transaction,
    dovecote_delivery_row_id,
    authoritative_delivered_at,
)
```

The PostgreSQL, MySQL/MariaDB, and SQLite adapters expose this operation with
that exact name. It is migration infrastructure, not a replacement for the
ordinary token-fenced `ack` operation and must not be used by normal 2.0
writers. The caller supplies the legacy publisher's authoritative delivery
instant; Dovecote validates the common range and microsecond precision and
the caller still owns commit or rollback.

The finalizer locks the event and delivery, then permits only the canonical
pending import shape: `state = pending`, zero attempts, no claim, failure, or
quarantine fields, and `available_at = enqueued_at`. A successful transition
returns `FinalizeOutcome::Finalized`. Repeating it with the same timestamp
returns `AlreadyFinalized`; a changed timestamp, claimed row, retry/failure,
quarantine, delayed availability, or any other non-canonical state returns a
typed `StateConflict`. It never imports or recreates a legacy claim. Delivered
rows are terminal and cannot be claimed by Dovecote.

PostgreSQL stores the value in `timestamptz(6)` semantics and MySQL/MariaDB in
UTC `DATETIME(6)` semantics. SQLite stores canonical RFC3339 text; its database
clock is millisecond-resolution, but a supplied authoritative timestamp is
retained at the validated microsecond precision. Schema validation runs before
the first mutation on every backend. A finalization error leaves the decision
to the caller, which must roll back the surrounding transaction before retrying.

## What can be published together

Four versions are separate contracts:

| Contract | First-release value | Changes require |
| --- | --- | --- |
| Rust crate semver | `0.1.1` | Normal Rust API compatibility and release notes. The initial MSRV is Rust 1.94. |
| Durable schema | Version `1` | A forward-only migration, preceding-version fixtures, compatibility metadata, and an application migration plan. |
| Tagged extension encoding | Version 1 encoding in the durable schema | Round-trip fixtures preserving abstract extension types and values. |
| CloudEvents projection | CloudEvents 1.0-compatible deterministic output | Updated golden vectors, official v1.0.2 JSON Schema validation, external SDK parsing, and transport-binding validation when bytes or meaning change. |

The schema version 1 migration metadata has a minimum crate floor of `0.1.0`
and is not marked rolling-compatible. A deployment must therefore use a
documented compatible crate/schema pair and a maintenance window whenever a schema change cannot
tolerate old and new processes together. `check_schema` rejects a crate that
is too old or too new; it never applies a migration.

For a future rolling-compatible change, use expand/migrate/contract ordering:

1. Add structures that the old crate ignores.
2. Deploy a crate that reads both forms and writes the new form.
3. Backfill and verify, with an explicit reconciliation checkpoint.
4. Remove the old form only in a later application-controlled migration.

Every advertised old/new pair needs a migration fixture. Rollback means
returning to a documented compatible pair or restoring and reconciling from a
backup. It does not erase an event already published to a downstream system.

## Worker pressure and shutdown

The application-owned worker should claim no more events than its bounded
in-flight transport capacity. Set the lease longer than the transport timeout
plus a measured scheduling margin; renew only active work with its current
claim token. A batch limit is a safety ceiling, not a concurrency setting.

Graceful shutdown is a short, explicit sequence:

1. Stop claiming new rows.
2. Drain in-flight sends for a bounded interval.
3. Acknowledge only sends the transport accepted while the claim is valid.
4. Release only work known not to have been attempted and only with its valid
   token.
5. Let cancelled or ambiguous sends expire. Never acknowledge or release an
   ambiguous send as if it were unsent.

Process termination after the drain interval uses ordinary lease expiry and
reclaim. It does not require a hidden shutdown state. The resulting duplicate
is expected and is deduplicated by the consumer's CloudEvents `source + id`
identity.

SQLite has one writer. Its default `BusyConfig` waits at most 20 seconds per
writer-lock operation (5 seconds, then three complete retries); deployments may
choose another finite policy. A retained SQLite snapshot is finite but holds a
read transaction: set page/time budgets, close or roll back abandoned pagers,
and restart reconciliation from a new snapshot.

## Signals and tracing

Dovecote adds no telemetry runtime to the core crate. An integration should
publish bounded, low-cardinality signals for:

- pending count and oldest pending age by stream;
- claimed count, oldest claim age, and expired-lease count;
- attempts and retry/quarantine transitions;
- claim, renewal, and lifecycle-mutation latency;
- `LostClaim`, illegal transition, overflow, entropy, busy/lock, deadlock,
  serialization, connection, and other SQL failures;
- transport send latency and classified outcomes; and
- ambiguous send-before-ack exits.

Do not use event IDs, sources, subjects, partition keys, worker IDs, payloads,
failure detail, quarantine prose, or trace baggage as metric labels. Apply the
same boundary to logs. When OpenTelemetry is present, instrument the
application's send and messaging boundary using its messaging semantic
conventions. Trace propagation is opt-in; if an integration emits both
`traceparent`/`tracestate` as CloudEvents extensions and protocol tracing
headers, keep the two views consistent. Dovecote never synthesizes a trace.
W3C Baggage is not persisted by default; any propagation needs an explicit
allowlist, access policy, and retention decision.

## Payload, privacy, and tenants

The default logical event-size profile is 65,536 bytes. It is an upper bound
for Dovecote's event material, not a promise about request lines, TLS, HTTP/2
or HTTP/3 compression, Kafka framing/batching, broker limits, or CDC
converters. An integration must calculate its exact message and reject or
route an oversized event before sending it. A larger event limit is an explicit
application choice and is not portable by default; Dovecote 1 has no blob,
chunking, or claim-check service.

Operational context is commonly visible to logs, brokers, and intermediaries.
Do not put credentials, bearer tokens, encryption keys, secrets, personal data,
or special-category data in CloudEvents context, routing fields, worker names,
failure summaries, quarantine reasons, or trace state. A payload still needs
the application's access controls, encryption, and retention policy.

Schema version 1 has no `tenant_id` and Dovecote does not authorize tenants.
For database-enforced isolation, use separate databases or schemas and
separately authorized pools, applying the Dovecote schema once in each
boundary. A shared schema is appropriate only for a trusted producer/worker
tier authorized to process all its rows. Stream filtering in application code
is not tenant isolation, and tenants must not receive direct access to shared
Dovecote tables.

## Retention and deletion

Retention, archival, and deletion belong to the application. Dovecote 1 has no
delete, purge, TTL, partition manager, or automatic quarantine job. Before a
bounded deletion batch, the owning application must:

1. Select delivered rows only, or quarantined rows covered by a separate
   resolution policy. Pending and claimed rows are never retention input.
2. Keep terminal rows beyond the greatest consumer deduplication window.
3. Prove CDC connectors have advanced past the candidate rows and are not
   snapshotting or replaying them.
4. Satisfy backup/restore, audit, legal-hold, and incident-investigation rules.
5. Dry-run candidate counts and row-ID ranges and record approval.
6. Delete delivery rows before event rows in bounded transactions; the foreign
   key is restrictive.
7. Verify counts and CDC health after every batch before reclaiming database
   space.

Dovecote does not infer a safe cutoff, and acknowledgement never means delete.

## A fake-transport recovery loop

Use a fake transport in a conformance or staging test so the ambiguous case is
observable. Give the fake transport a scripted result per `source + id` and a
set of identities it has accepted:

1. Enqueue one event in the application's transaction and commit it.
2. Claim one bounded batch with a lease longer than the fake send timeout.
3. Return `accepted` for the first send, then crash the worker before its
   acknowledgement commits. Record that the fake transport accepted the
   `source + id`; do not call this Dovecote delivery success.
4. Wait for the database-authoritative lease to expire. Reclaim the event with
   a new token. The event's identity and payload are unchanged.
5. Have the fake transport return `duplicate` for the identity it already
   accepted. The consumer-side idempotency check observes the same
   `source + id`, applies the effect once, and the worker acknowledges with
   the new valid token.
6. Repeat with a scripted transient result: retry with bounded backoff, then
   acknowledge only after an accepted result. Repeat with a permanent result:
   quarantine it with a bounded, redacted reason.
7. Force a stale worker to mutate after reclaim and assert `LostClaim`; it must
   stop and must not report success.

This loop demonstrates at-least-once delivery and recovery from an ambiguous
send. It does not demonstrate exactly-once publication or relieve the consumer
of deduplication.
