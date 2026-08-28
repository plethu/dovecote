# Operations and compatibility

Dovecote is a storage library, not a migration runner or a worker service. This
page is the deployment path for the current schema version; [SPEC.md](../SPEC.md)
remains the normative contract. A database adapter is not release-advertised
until its exact backend, CI, projection, migration, and independent-review
evidence passes. CDC fixtures are required only for a separate CDC
advertisement; see the [support matrix](support-matrix.md).

## Install and verify the schema

Each SQLx adapter publishes migration artifacts. The application
owns the migration transaction and must execute each artifact in order using
its existing migration process. Do not apply a migration from library startup,
and do not edit a published artifact in place.

After installation, run the adapter's read-only `check_schema` function as a
deployment/startup gate. A mismatch is a failed deployment: it is not an
invitation to repair tables or to continue with a guessed schema.

| Backend | Migration and check | Installation boundary |
| --- | --- | --- |
| PostgreSQL | `dovecote_sqlx_postgres::MIGRATIONS`; `dovecote_sqlx_postgres::check_schema(&pool).await` | Clean installs use schema v2 with required tenant columns. The marker is checked against the adapter's schema and crate compatibility metadata. |
| MySQL or MariaDB | `dovecote_sqlx_mysql::MIGRATIONS`; `dovecote_sqlx_mysql::check_schema(&pool).await` | Detects the server family and verifies the exact dialect-sensitive shape. Both tables must be InnoDB. MySQL evidence does not cover MariaDB. |
| SQLite | `dovecote_sqlx_sqlite::MIGRATIONS`; `dovecote_sqlx_sqlite::check_schema(&pool).await` | Creates the domain tables plus the schema marker. Enable foreign keys on every connection. Use `SqliteDovecote::for_tenant` and its `begin_write`/`begin_enqueue` for caller transactions. The database file is the security boundary. |

The MySQL/MariaDB migration creates two validation triggers. The account that
installs the schema therefore needs trigger DDL authority. On a MySQL server
with binary logging enabled, an administrator may also need to enable
`log_bin_trust_function_creators` for the installation, according to the
deployment's replication policy. The ordinary application account does not
need that setting merely to use an already-installed Dovecote schema.

The SQL is available as `migration.sql()` on each public migration artifact.
For MySQL/MariaDB, execute the complete artifact through the raw/unprepared
protocol (for SQLx, `sqlx::raw_sql`) so the trigger bodies are sent as one
multi-statement request. The artifact deliberately contains no client-side
`DELIMITER` directives: a MySQL or MariaDB command-line wrapper must supply
those directives itself, while SQLx callers must not split the bytes on
semicolons. Splitting would corrupt trigger bodies and semicolons in comments.
The adapter does not assume that one `sqlx::query` call can execute a
multi-statement file. If Keepsake and Gatekeep share one application database,
install the Dovecote schema once in that database and have both libraries use
the same adapter boundary.

MySQL and MariaDB schema v2 enforce the tenant-scoped identity with the
`dovecote_events_tenant_source_event_id` unique index over the internal,
stored-generated `identity_key VARBINARY(2310)` column. The key is the exact
length-prefixed byte encoding
`len(tenant_id, 3 ASCII digits) || tenant_id || len(source, 4 ASCII digits) || source || event_id`.
Its maximum is `3 + 255 + 4 + 2,048 = 2,310` bytes, below the 3,072-byte
InnoDB index limit. Fixed prefixes make the encoding collision-free even for a
direct SQL writer; application lookups continue to predicate on
`tenant_id`, `source`, and `event_id`. The stable index name is also the
duplicate-key classifier used by the adapter.

Run `check_schema` after the migration and on every deployment. It verifies
the tables, columns, defaults, indexes, constraints, foreign key, backend
settings, and—on PostgreSQL—the schema marker. It never changes data. Keep
database time in UTC and use the settings recorded for the exact backend in
the [support matrix](support-matrix.md).

### PostgreSQL tenant upgrade and handles

The PostgreSQL v2 baseline is for clean tenant-aware installations. A v1
deployment must run `V1_TENANT_PREPARE_SQL`, assign every event and delivery a
validated tenant in an operator-owned backfill, verify that each pair agrees,
then run `V1_TENANT_ACTIVATE_SQL`. Activation fails while any tenant is null;
no default or guessed tenant is used. The v1 migration bytes remain available
as `LEGACY_MIGRATION` and are not rewritten.

Construct `PostgresDovecote::for_tenant(tenant_id)` for ordinary enqueue,
import, finalization, claim, mutation, page, and snapshot operations.
`PostgresDovecote::admin()` is an explicit privileged surface: writes and
mutations name a tenant, while its page, snapshot, and claim operations span
all tenants. Returned claimed and paged values include their tenant ID.
`bind_tenant` and `RLS_PROFILE_SQL` provide an optional PostgreSQL RLS profile;
RLS requires reviewed roles and a `BYPASSRLS` administrator, and never removes
the adapter's predicates.

MySQL/MariaDB and SQLite expose the same `for_tenant` and `admin` handle shape.
Their adapters always apply tenant predicates but do not claim database RLS:
use a separate MySQL/MariaDB database for the strongest boundary, and treat a
SQLite file as the security boundary (one file per tenant for regulated
isolation). Each v1 upgrade uses its adapter's explicit prepare, operator-owned
backfill, and activation artifacts; no tenant is guessed.

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
| Rust crate semver | `0.2.0` | Normal Rust API compatibility and release notes. The initial MSRV is Rust 1.94. |
| Durable schema | Version `2` for PostgreSQL | A forward-only migration, preceding-version fixtures, compatibility metadata, and an application migration plan. |
| Tagged extension encoding | Version 1 encoding in the durable schema | Round-trip fixtures preserving abstract extension types and values. |
| CloudEvents projection | CloudEvents 1.0-compatible deterministic output | Updated golden vectors, official v1.0.2 JSON Schema validation, external SDK parsing, and transport-binding validation when bytes or meaning change. |

The PostgreSQL schema version 2 migration metadata has a minimum crate floor of `0.2.0`
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
is expected and is deduplicated by the consumer's tenant-scoped CloudEvents
identity policy: `(source + id)` within one tenant, plus its tenant routing
domain when destinations are shared.

SQLite has one writer. Its default `BusyConfig` waits at most 20 seconds per
writer-lock operation (5 seconds, then three complete retries); deployments may
choose another finite policy. A retained SQLite snapshot is finite but holds a
read transaction: set page/time budgets, close or roll back abandoned pagers,
and restart reconciliation from a new snapshot.

## Signals and tracing

Dovecote adds no telemetry runtime to the core crate and exposes no status-query
method. Applications own bounded, read-only operational SQL against the
documented `dovecote_events` and `dovecote_deliveries` tables, including
backend-specific time functions, parameters, permissions, and indexes. The
`page` and snapshot APIs are event inspection/reconciliation tools, not status
counters. An integration should publish bounded, low-cardinality signals for:

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

Schema version 2 carries a required validated `tenant_id` on both event and
delivery rows for every adapter. Use an adapter's `for_tenant` handle for
ordinary operations; its `admin` handle is an explicit privileged surface
whose writes and mutations name a tenant. Claimed and paged values retain
tenant metadata for safe routing. Durable identity is scoped to
`(tenant_id, source, event_id)`, while the projected CloudEvents identity
remains `(source, id)`. A destination shared by multiple tenants must partition
its deduplication by tenant; a destination isolated to one tenant can
deduplicate on `(source, id)`. PostgreSQL's optional `RLS_PROFILE_SQL` and `bind_tenant` helper
supplement these predicates and require reviewed roles, including `BYPASSRLS`
for admin. MySQL/MariaDB has no RLS claim and should use a separate database
for the strongest boundary; SQLite's file is the security boundary and a file
per tenant is the regulated-isolation choice. Version 1 upgrades require
explicit prepare, operator-owned backfill, and activation; no tenant is
guessed. Stream filtering in application code is never tenant isolation, and
tenants must not receive direct access to shared Dovecote tables.

Tenant assignment is an application trust decision. Dovecote validates the
identifier and preserves it as storage metadata, but it does not authenticate
the caller or decide tenant membership; only a trusted producer or an
authorized administrative migration may choose it. The `(tenant_id, source,
event_id)` tuple is the durable identity oracle within a tenant. Reusing the
same source and event ID in another tenant creates a separate event and delivery
row. With the optional PostgreSQL RLS profile, an unset or mismatched
transaction-local tenant setting denies ordinary scoped access; it does not
expand the handle's authority. RLS conflicts are expected to surface as the
adapter's normal database error, and the application must use a separately
authorized `BYPASSRLS` pool for explicit administrative operations.

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
