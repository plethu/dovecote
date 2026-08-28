# Keepsake and Gatekeep migration runbook

This runbook moves the generic delivery outbox and its SQL audit records to
Dovecote while leaving each library's domain state and audit meaning in place.
It consumes Keepsake 1.1 and Gatekeep 1.0 source schemas and is prepared for
the verified bridge releases Keepsake 1.2.x and Gatekeep 1.1.x. Their
historical migrations remain byte-for-byte immutable. Each application adds a
new, forward-only migration and creates the shared Dovecote schema once in its
own database boundary.

The 1.x bridge releases are temporary dual-persistence and publication bridges:
they write the legacy record and a pending Dovecote event/delivery in one
caller-owned transaction, and the legacy publisher remains the owner. The 2.0 cutover makes Dovecote the sole
maintained SQL audit/outbox shape and removes the legacy APIs. Legacy tables and
rows may remain read-only historical source material under application
retention policy; they are removed, if ever, only by a later separately
approved cleanup. Domain state and domain audit meaning remain sibling-owned.

> **Evidence status (27 August 2026):** complete-history fixtures pass locally
> on SQLite, PostgreSQL 17.11, MySQL 8.4.11, MySQL Innovation 26.7.0, and the
> pinned MariaDB 10.3.17-to-11.8.6 maintenance path. The sibling bridge and 2.0
> worktrees contain the finalizer, activation APIs, and removal audits described
> below, with independent code review and backend-focused tests. Exact-revision
> GitHub CI still requires authorized sibling commits and pinned commit SHAs;
> publication and a real deployment cutover remain later operator actions.

## Identity and content mapping

Before migration, configure one stable absolute CloudEvents `source` per
producer. It must be controlled by the producer and must not be derived from a
deployment hostname, ephemeral instance, or database name. Keepsake and
Gatekeep use distinct streams in a shared schema; the defaults are
`keepsake-audit` and `gatekeep-audit`.

Legacy IDs become deterministic ASCII event IDs:

```text
keepsake-outbox-<legacy decimal row id>
gatekeep-outbox-<legacy decimal row id>
```

When an older normalized audit row has no outbox row, use the reserved
migration-only identity instead:

```text
keepsake-audit-legacy-<legacy audit row id>
gatekeep-audit-legacy-<legacy decision row id>
```

The source plus this ID is stable across retries and resumptions.
The importer must also provide the owning Dovecote `tenant_id` explicitly:
durable identity is `(tenant_id, source, event_id)`, so this source/ID mapping
is unique within that tenant. If one migration or downstream destination serves
multiple tenants, retain that tenant routing context alongside the imported
event; Dovecote does not add it to the projected CloudEvents identity.

Keep four independent source cursors and four captured high-water marks:
Keepsake audit rows, Keepsake outbox rows, Gatekeep decision-audit rows, and
Gatekeep outbox rows. These table sequences can overlap or diverge, so neither
one cursor per library nor one shared cursor is safe. A committed row advances
only the cursor for the source table that selected it.

| Legacy value | Dovecote value |
| --- | --- |
| Library source configuration | `source` |
| Legacy outbox row ID | `event_id` with the library prefix above |
| Application stream configuration | `stream` |
| Legacy event type | `event_type` |
| Legacy outbox payload bytes | `EventData::Json` with explicit `application/json`, preserving the source bytes and digest |
| Normalized row without an outbox payload | Output of the owning project's named versioned codec (`keepsake.audit.json.v1` or `gatekeep-audit-json-v1`); record codec provenance and do not call it original source bytes |
| Legacy `created_at` | `occurred_at` only when producer policy establishes it as occurrence time; otherwise omit |
| No legacy value | `subject`, `dataschema`, `partitionkey`, and extensions remain absent |

Legacy JSON columns vary by backend. When an outbox payload exists, read its
bytes as the authority, record their SHA-256 digest, and insert those exact
bytes into Dovecote. A database cast that reformats JSON between digest and
insert is not acceptable. For a normalized row without a payload, call only
the owning project's documented versioned migration codec; its deterministic
output is a reconstruction and must not be labelled as an original database
byte sequence. The complete-history fixture's independent
`tests/fixtures/reconstructed-payload-golden-v1.json` records the four v1
reference outputs and their digests. The current 3.0 sibling crates no longer
export those retired codecs, so the fixture compares normalized source values
and the recorded digest rather than silently treating its checked-in payload
as a newly generated current-project value.

The application passes the resulting validated `dovecote::NewEvent` to the
adapter's `import_for_migration` operation in the same caller-owned
transaction. Every source row is imported after its legacy delivery state is
resolved: a row with no active claim maps to `ImportedDeliveryState::Pending`,
while a delivered row maps to `Delivered { delivered_at }` using its
authoritative timestamp. The importer never accepts or recreates a legacy
claim.

If a bridge row is imported as pending and the legacy publisher later
succeeds, the bridge's acknowledgement transaction must call the adapter's
`finalize_pending_delivery_for_migration` with the Dovecote delivery row ID
and the authoritative legacy delivery timestamp. An exact rerun is
idempotent; a changed timestamp or any non-canonical, claimed, or quarantined
row is a typed conflict. This finalizer is migration infrastructure, not
ordinary application acknowledgement, and the caller retains commit/rollback
control.

### Current 3.0 occurrence-time mapping

The 3.0 producers establish the audit identity before durable enqueue and reuse
it when retrying the same logical operation. Keepsake's `AuditEventId` and
Gatekeep's `DecisionAuditId` are producer-owned identities, not database row
IDs or delivery cursors. The adapters map them to the following Dovecote
values:

| Producer value | Dovecote and CloudEvents mapping |
|---|---|
| Keepsake `AuditEvent.id` | `event_id = keepsake-audit-<UUID>`; the same ID is reused across retries. |
| Keepsake `AuditEvent.at` | `occurred_at` and CloudEvents `time`. |
| Gatekeep `DecisionAuditOccurrence.decision_audit_id` | `event_id = gatekeep-audit-<DecisionAuditId>`; a caller-supplied occurrence is retained across retries. |
| Gatekeep `DecisionAuditOccurrence.occurred_at` | `occurred_at` and CloudEvents `time`. |
| Gatekeep's application-owned `Clock` | The same clock governs context validation, fact observation, decision receipt/freshness checks, and generated audit occurrences at the Axum boundary. Explicit occurrences remain authoritative. |
| Database `created_at`, `recorded_at`, and `enqueued_at` | Persistence times only; they must not substitute for occurrence time. |

Policy evaluation itself remains clock-free. A 1.x historical row without a
current producer identity still uses the migration-only IDs documented above;
that compatibility mapping is separate from the current 3.0 event identity.

Every legacy audit occurrence through the recorded high-water mark is copied,
including rows without an outbox payload and delivered history. Never trust a legacy claim across the cutover: it
has no Dovecote claim token. Before the state snapshot, an active unexpired
claim must finish, expire, or be explicitly fenced; stopping workers alone does
not make it importable. Once that precondition is met, the source row maps to
Dovecote `pending`, available at migration database time. Delivered rows
require an authoritative `delivered_at` that is representable at exact
microsecond precision; otherwise the migration stops for reconciliation.

## Backend execution notes

The three backends share the identity ledger and pause protocol, but their
legacy JSON and time representations are different. The following notes are
the minimum backend-specific part of an application migration; they do not
replace a fixture against the real Keepsake or Gatekeep schema.

### PostgreSQL

Keepsake and Gatekeep store their audit payloads and JSON decision/context
values in `jsonb`; their audit and legacy outbox times are `timestamptz`. That
is useful for application queries but is not the producer's original byte
representation. Export the value through the documented deterministic JSON
value codec (`postgres-jsonb-canonical-v1`), hash those UTF-8 bytes, and pass
the same bytes to `EventData::Json`. Compare the exported value semantically to
the migration manifest; never compare `payload::text` as though JSONB retained
producer whitespace or member order.

Apply the PostgreSQL Dovecote artifact from
`dovecote_sqlx_postgres::MIGRATIONS` through the application's migration
runner, then run `dovecote_sqlx_postgres::check_schema(&pool).await` on the
same namespace. Use the documented `READ COMMITTED` transaction profile and
finite statement/lock waits. During the pause, enforce the producer boundary
in the application or with a database permission/maintenance control; a
high-water mark recorded by an unlocked read is not sufficient.

For a bounded ledger pass, the shape is:

Capture and persist four independent bounds for the audit and outbox tables of
both libraries. The example shows the Keepsake outbox bound; use the matching
bound for each table rather than sharing one numeric cursor.

```sql
SELECT COUNT(*), MIN(id), MAX(id)
FROM keepsake_audit_outbox
WHERE id <= :high_water;
```

Use the same query for `gatekeep_audit_outbox` and record event-type counts in
the exporter. Do not use `payload::text` as the digest source: PostgreSQL may
render `jsonb` differently from the original producer bytes. The application
ledger's byte length and SHA-256 digest are authoritative.

### MySQL and MariaDB

The sibling MySQL migrations use JSON columns and `datetime(6)`/`timestamp(6)`
values. A JSON value returned by the server is not an archival byte contract;
export it once through the documented deterministic JSON value codec
(`mysql-json-canonical-v1`), before hashing, and keep those UTF-8 bytes
unchanged through enqueue. Compare the exported value semantically to the
migration manifest. Do not compare a database-side cast or reformatted JSON
string with a digest recorded for another export.

Apply the Dovecote artifact from `dovecote_sqlx_mysql::MIGRATIONS`, then run
`dovecote_sqlx_mysql::check_schema(&pool).await`. The adapter detects MySQL
versus MariaDB separately; both require the exact advertised release, UTC,
strict SQL mode, `REPEATABLE-READ`, `utf8mb4`, InnoDB, and finite lock waits.
Run the migration's count pass in bounded transactions after the write pause;
the application or database must prevent new legacy inserts rather than
assuming a transaction's isolation level creates a cutover boundary.

The count ledger uses the same shape as PostgreSQL, with the bound captured
independently for each table:

```sql
SELECT COUNT(*), MIN(id), MAX(id)
FROM gatekeep_audit_outbox
WHERE id <= :high_water;
```

Substitute `keepsake_audit_outbox` for Keepsake and retain event-type, byte
length, and exporter digest totals outside SQL. `OCTET_LENGTH(payload)` is not
an acceptable substitute when the JSON driver can re-encode the value.

### MariaDB maintenance-window route for existing Keepsake deployments

An existing Keepsake deployment on MariaDB uses a maintenance window; MariaDB
can require downtime for this cutover. The deployed source schema is
authoritative: do not recreate it from the checked-in
Keepsake MySQL migrations, and do not replay those immutable files against
MariaDB 11.8. They describe how one release created a schema; they are not a
portable reconstruction of a database that is already in service.

The supported route is:

1. Stop Keepsake writers and the legacy publisher. Let in-flight transactions
   finish, then finish, expire, or explicitly fence every active legacy claim.
   A stopped process is not a claim fence, and Dovecote never imports a legacy
   claim token.
2. Take a database snapshot or backup of the stopped, claim-resolved source and
   prove that it can be restored. If the server is being upgraded to MariaDB
   11.8 at the same time, follow the vendor's
   [major-version upgrade procedure](https://mariadb.com/docs/server/server-management/install-and-upgrade-mariadb/upgrading/upgrading-between-major-mariadb-versions)
   (including `mariadb-upgrade`) on the restored rehearsal first. A server
   upgrade must not be used as an excuse to rerun application migrations.
3. On the target database, install the Dovecote migration artifact and the
   additive terminal Keepsake 1.2 bridge migration through the application's
   migration runner. The bridge migration creates reconciliation state and
   upgrade evidence tables; enabling its runtime dual-write feature is not
   required for a paused cutover. Run
   `dovecote_sqlx_mysql::check_schema(&pool).await` and stop if it does not
   pass. Both migrations are additive: the existing Keepsake tables remain
   untouched.
4. Export and import the complete Keepsake history from the existing tables,
   including delivered rows and audit rows without an outbox row. Capture one
   inclusive high-water mark per source table. Use exact outbox bytes when the
   source representation preserves bytes; otherwise use the named versioned
   JSON-value or normalized-audit codec and record that provenance. Use the
   final 1.x bridge import API, which calls Dovecote's migration importer in
   bounded caller-owned transactions and maps only pending or delivered state
   after the claim precondition in step 1.
5. Rerun the import and produce a zero-delta reconciliation. Compare every
   source identity, event type, occurrence time, payload length, SHA-256 digest,
   delivery state, and per-table count. Prove that no legacy row exists above
   its captured bound and that no writer or publisher committed during the
   window. While the external writer fence remains enforced, call
   `finalize_upgrade_reconciliation()`; it performs the complete reread and is
   the only supported writer of the evidence consumed by Keepsake 2.0
   activation. Any delta stops the cutover; it is not silently skipped.
6. Run Keepsake 2.0's explicit `upgrade_migrate()` and
   `activate_upgrade()`, then deploy the Dovecote-only writer and start its one
   publication owner for the Dovecote table set. Keep the legacy publisher stopped. Retain
   the Keepsake tables and the exported ledger read-only through the rollback
   and consumer deduplication windows; later deletion is a separate,
   explicitly approved cleanup.

The repository's MariaDB maintenance-window fixture creates the source tables
from the hash-checked published artifacts on MariaDB 10.3.17, where MariaDB
still admitted the historical SQL-mode-dependent generated column. It cleanly
stops that server, mounts the same data volume in MariaDB 11.8.6, runs
`mariadb-upgrade`, and imports the existing tables without replaying a
historical Keepsake migration on 11.8. MariaDB reports the inherited generated
column as SQL-mode-dependent during upgrade; the fixture records that warning
and proves the source rows remain readable for the bounded import. This is
evidence for that pinned transition only, not a claim that every MariaDB release
or deployed schema can be upgraded without its own rehearsal. The operator
route above remains the production contract.

### SQLite

The sibling SQLite migrations store audit JSON as checked `TEXT` and times as
UTC text. Read the legacy text bytes through the documented exporter, validate
them as JSON, and hash the exact UTF-8 sequence that will become
`EventData::Json`; do not round-trip it through a JSON database function.

Apply `dovecote_sqlx_sqlite::MIGRATIONS` with the application's DDL runner,
ensure `PRAGMA foreign_keys = ON` on every connection, and run
`dovecote_sqlx_sqlite::check_schema(&pool).await`. Use
`SqliteDovecote::begin_write` or `begin_enqueue` for each write transaction;
these use `BEGIN IMMEDIATE` and the configured finite `BusyConfig`. Pause
legacy writers at the application/database boundary before recording the
high-water mark, then migrate bounded batches and commit each one. A busy
timeout is not permission to continue a partially paused cutover.

The verification query remains (using the Keepsake audit bound; use the
corresponding bound for each other source table):

```sql
SELECT COUNT(*), MIN(id), MAX(id)
FROM keepsake_audit_outbox
WHERE id <= :high_water;
```

Use the Gatekeep table where appropriate. SQLite's `length(CAST(payload AS
BLOB))` may help inspect legacy text, but the exporter's exact byte length and
digest remain authoritative and must be compared after enqueue.

## Paused cutover

Name the maintenance window, owner, backup, restore check, rollback window,
high-water marks, and consumer deduplication owner before starting. Then:

1. Inventory schema versions, row counts by legacy state, configured sources
   and streams, database time zone, and running workers.
2. Back up the database and prove the documented restore check.
3. Apply the Dovecote migration once and run the adapter's read-only
   `check_schema` gate. See [operations.md](../operations.md#install-and-verify-the-schema).
4. Deploy code that can read Dovecote while legacy producers and workers still
   exist. Do not switch publication ownership yet.
5. Stop or drain legacy workers. Pause both legacy producer write paths, wait
   for in-flight transactions to finish, and enforce the pause at the
   application or database boundary so no new legacy row can commit.
6. While the pause is enforced, record each legacy table's inclusive maximum
   row ID. Migrate every row, including delivered history, through that
   high-water mark in bounded transactions, preserving outbox bytes (or calling
   the declared project codec for pre-outbox rows), recording lengths/digests
   and codec provenance, and calling `import_for_migration` for each
   deterministic identity and mapped state.
7. Rerun the same high-water ranges. Identical identities and canonical
   imported state must return `AlreadyImported`; changed immutable content must
   stop with `IdentityConflict`, and changed imported state with
   `ImportConflict`.
8. Compare per-library and total counts by delivery state, row IDs, event
   types, byte lengths, and SHA-256 payload digests. Prove that no legacy row
   exists above its corresponding recorded high-water mark.
9. Switch both producer write paths to Dovecote while the pause remains
   enforced. Resume producers, then start Dovecote workers. There is no
   supported interval in which a legacy producer can commit between the
   migration snapshot and the write-path switch.
10. Confirm that both libraries coexist in the shared tables under distinct
    streams. Monitor legacy writes, Dovecote claims, lost claims, retries,
    quarantine, and duplicate consumer identities.
11. Remove migration-only code only after its named verification and rollback
    window. Keep legacy tables read-only until the application's later
    retention decision; they are no longer a maintained audit publication.

Failure to enforce the producer pause aborts cutover. Do not patch around a
high-water discrepancy by silently skipping rows.

## Rolling bridge cutover (17 steps)

Use this path only for the opt-in bridge releases on the final 1.x lines:
Keepsake 1.2.x and Gatekeep 1.1.x. Existing 1.x defaults remain legacy-only;
the bridge is explicit configuration. Dovecote deliveries stay pending and
the legacy publisher remains the sole publication owner until step 13. The
bridge is zero application downtime, not zero operational coordination.

Record the named owner, release versions, backend image, source and stream
configuration, reconciliation counters, alert, rollback window, and bridge
deletion release in the deployment record. Every step below has a required
evidence item; an absent item blocks the next step.

1. **Install Dovecote additively.** Apply the selected Dovecote migration in
   the application's database namespace. Do not edit or remove a historical
   Keepsake or Gatekeep migration, and do not drop a legacy table.
   **Evidence:** migration ID, backend/version, schema checksum, and the
   byte-level checksums of every historical source migration.
2. **Check the installed schema.** Run the concrete adapter's read-only
   `check_schema` before the first import or bridge write. It must validate the
   complete installed shape, not only table names.
   **Evidence:** successful `check_schema` output for the exact namespace and
   adapter version.
3. **Deploy the opt-in 1.x bridge.** Configure one producer-controlled stable
   absolute `source`, the distinct default stream (`keepsake-audit` or
   `gatekeep-audit`), and the bridge's legacy high-water state. The bridge
   writes legacy and Dovecote pending forms in one caller transaction.
   **Evidence:** configuration review and an atomic test showing that a
   rollback removes both forms.
4. **Keep legacy publication ownership.** Do not start Dovecote workers or a
   Dovecote CDC publication for this table set. Legacy workers remain the only
   publishers while dual writes accumulate pending Dovecote deliveries.
   **Evidence:** worker/connector inventory, table-set publication-owner record, and a query
   showing bridge deliveries are pending rather than claimed for publication.
5. **Run a bounded high-water complete-history import.** Capture an inclusive
   high-water mark for each legacy source table—Keepsake audit and outbox plus
   Gatekeep decision and outbox—and import every row at or below its mark,
   including delivered and terminal history. Prefer legacy outbox bytes; use
   only the declared versioned codec for older rows without stored bytes.
   **Evidence:** source-row counts, IDs, event types, byte lengths, and SHA-256
   digests, plus importer outcomes and a ledger of deferred active claims.
6. **Repeat reconciliation while old writers remain.** Re-run bounded passes
   above each previous high-water mark as old 1.x processes continue to write.
   Interrupted batches resume by deterministic identity; exact reruns return
   `AlreadyImported`; changed content or imported state stops with its typed
   conflict.
   **Evidence:** each batch's input range, outcome counts, conflict log, and
   the absence of an unexplained reconciliation delta.
7. **Prove every writer is bridge-aware or 2.0-capable.** Inventory every
   application process, scheduled task, repair tool, and publisher that can
   create an audit occurrence. An unupgraded legacy-only writer blocks the
   rolling path and requires the paused cutover.
   **Evidence:** deployed-version inventory, write-path test results, and a
   signed owner acknowledgement for the complete writer set.
8. **Fence creation of new legacy-only audit rows.** Enforce the boundary at
   the application or database permission level so a legacy-only write cannot
   commit. Keep the bridge's dual-write path available until the final switch.
   **Evidence:** a rejected legacy-only write (or equivalent enforced
   permission), the enforcement timestamp, and a query showing no new
   legacy-only rows after that boundary.
9. **Resolve active legacy claims.** Let each active claim finish or expire, or
   explicitly fence it before importing that source row. Never copy a live
   legacy claim into Dovecote and never treat stopping a worker alone as a
   fence. A successfully delivered bridge row is finalized with its
   authoritative legacy delivery time.
   **Evidence:** claim-state export, fence/expiry records, delivered timestamp
   ledger, and zero unfenced active claims eligible for cutover.
10. **Run the final high-water pass.** With legacy-only creation fenced and
    active claims resolved, record the final inclusive high-water marks and
    import every remaining source row. Include rows produced by bridge-aware
    writers between earlier passes and this boundary.
    **Evidence:** final ranges, importer outcomes, and proof that no source row
    at or below a range was omitted.
11. **Require zero reconciliation delta.** Compare legacy and Dovecote rows by
    source, deterministic ID, event type, exact payload length and digest,
    occurrence time, and delivery state. Compare per-library and combined
    counts, including delivered, pending, expired-claim-to-pending, and every
    terminal state. Stop on any mismatch.
    **Evidence:** zero-delta report, state/count/digest ledger, distinct
    Keepsake/Gatekeep stream check, and release-owner sign-off.
12. **Stop the legacy publisher.** Only after step 11, stop legacy workers and
    prevent them from claiming new rows. Preserve source tables and the
    publisher's final acknowledgement ledger for rollback/reconciliation.
    **Evidence:** stop/fence timestamp, last legacy claim and acknowledgement,
    and no active legacy publisher process.
13. **Switch publication ownership to Dovecote.** Start exactly one Dovecote
    publication owner for the table set: a leased worker or an explicitly
    advertised CDC path, never both. Because claims are all-stream, Dovecote
    cannot safely split publication modes by stream. Dovecote may now claim and
    publish pending rows.
    **Evidence:** owner configuration, first successful Dovecote claim and
    acknowledgement, and monitoring for lost claims/retries/quarantine.
14. **Deploy Dovecote-only 2.0 writers.** Disable the 1.x bridge and deploy
    writers that create project-owned stable audit IDs before enqueue, preserve
    typed payload bytes, and map authoritative occurrence time. They must not
    write active legacy audit/outbox state.
    **Evidence:** 2.0 migration/API check, one-event-per-occurrence test, and
    a query proving no new legacy audit/outbox writes.
15. **Keep legacy tables read-only through the rollback window.** Retain the
    exact source rows, exported digests, and delivery ledger. Reconciliation
    and incident work may read them, but no runtime path may mutate or publish
    them.
    **Evidence:** database permissions or application enforcement, backup and
    restore check, and the recorded rollback-window end date.
16. **Retire the bridge at its documented deletion release.** Remove bridge
    flags, state, and migration-only callers only after the named release,
    zero-delta evidence, and rollback window. Keep migration documentation and
    reconciliation evidence with the release record.
    **Evidence:** deletion-release change record and a public API audit showing
    no ordinary enqueue shortcut or legacy publisher dependency remains.
17. **Drop legacy tables only through later explicit cleanup.** Require a
    separately approved, operator-controlled migration after retention, legal
    hold, backup, consumer deduplication, and rollback requirements are met.
    Dropping is never part of bridge cutover and never automatic.
    **Evidence:** cleanup approval, row-range/count report, backup/restore
    result, and post-cleanup reconciliation.

### At-least-once boundary

The bridge cannot promise duplicate-free cutover. A legacy worker may be
accepted by a transport immediately before step 12 and fail before recording
its legacy acknowledgement; Dovecote may publish the same pending event after
step 13. Both publications must carry the identical CloudEvents `(source, id)`
and exact payload, and consumers must deduplicate that identity. If consumer
deduplication cannot be established, use the paused cutover instead.

## Rollback and zero-downtime bridge

Before producer cutover, restore the backup or remove only verified,
migration-owned Dovecote rows under the application's written procedure. After
Dovecote publication begins, stop publishers and reconcile by CloudEvents
`source + id`; a downstream effect cannot be made as though it never happened.

If a real deployment cannot pause writes, follow the complete 17-step rolling
bridge above. The bridge must write the legacy row and Dovecote event with its
pending delivery in the same caller transaction. Both rows carry the identical
producer-configured `source + id`, and the legacy publisher must expose that
identity unchanged. If the legacy path cannot do this, use the paused cutover
instead.

## Verification checklist

Record these artifacts with the release review:

- backup and restore evidence;
- adapter/backend, schema version, and `check_schema` result;
- source, stream, high-water mark, count, byte length, and digest ledgers for
  each library;
- a complete-history rerun proving `AlreadyImported`, plus changed-content and
  changed-state fixtures proving `IdentityConflict` and `ImportConflict`;
- no rows above the high-water marks and no legacy writes after the pause;
- distinct Keepsake/Gatekeep streams in the shared tables;
- consumer deduplication evidence for an ambiguous send or bridge duplicate;
- named rollback and deletion owners; and
- the date on which migration-only code and old rows may be reconsidered.

The legacy outbox is not a second publication owner after cutover. Keep one
owner for the Dovecote table set and follow
[SPEC section 12](../../SPEC.md#12-keepsake-and-gatekeep-migration)
for the full contract.
