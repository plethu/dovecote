# Dovecote acceptance specification

- Status: accepted design for initial implementation
- Specification date: 16 August 2026
- Durable schema version: 1
- CloudEvents compatibility target: 1.0
- Initial MSRV: Rust 1.94
- Licence: `MIT OR Apache-2.0`

## 1. Purpose and authority

Dovecote is a storage-only transactional outbox for Keepsake, Gatekeep, and
other applications that need to commit an event beside application state and
deliver it later. It owns durable insertion, deterministic inspection, leased
claims, claim-token fencing, retry state, and quarantine. It does not deliver
messages itself.

This document is the acceptance contract for Dovecote's first implementation.
It is standalone: an implementer must not need the earlier ecosystem plan to
resolve an API, schema, lifecycle, interoperability, migration, or testing
decision described here.

Dovecote is the canonical project and package-family name.

Normative words such as **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry
their usual RFC 2119 meanings.

## 2. Scope and guarantees

Dovecote provides one logical delivery lifecycle for each stored event:

1. an application inserts an event using its existing database transaction;
2. committing that transaction makes both the application's state and the
   Dovecote event visible;
3. a worker may claim the event under a bounded lease;
4. only the current, unexpired claim may renew, acknowledge, retry, release, or
   quarantine it;
5. an expired claim may be reclaimed atomically with a fresh token.

Dovecote guarantees:

- atomic enqueue when the caller commits its transaction;
- rollback of the enqueue when the caller rolls back;
- CloudEvents identity and context validation before insertion;
- idempotent replay of identical content under the same `source + id`;
- exclusive durable delivery state;
- at most one current, unexpired claim for an event;
- fencing of every post-claim mutation by event row ID and claim token;
- database-authoritative lifecycle time;
- deterministic row-ID paging; and
- backend-specific SQL tested against a shared conformance contract.

Dovecote does **not** guarantee:

- delivery, exactly-once effects, or exactly-once publication;
- FIFO ordering, including within a stream or partition key;
- a worker will finish before its lease expires;
- that a successful transport send followed by a process crash can be
  distinguished from a failed send;
- broker deduplication, consumer idempotency, or transport availability;
- simultaneous publication of one stream through both CDC and leased workers;
  or
- application-level audit, event-schema, retention, or privacy correctness.

Delivery is necessarily at least once when workers retry after ambiguous
failure. Consumers must use the CloudEvents identity pair, `source + id`, as
their duplicate identity. An application MUST designate one publication owner
for each stream: either a leased-worker integration or CDC, never both at the
same time.

## 3. Project and crate boundaries

The package family is:

| Crate | Responsibility |
|---|---|
| `dovecote` | Validated events and extensions, claims, failures, states, outcomes, and deterministic CloudEvents projections. |
| `dovecote-sqlx-postgres` | PostgreSQL schema, migrations, transaction-bound enqueue and migration import, claims, lifecycle mutations, and paging. |
| `dovecote-sqlx-mysql` | MySQL and MariaDB schemas, migrations, dialect handling, enqueue and migration import, claims, lifecycle mutations, and paging. |
| `dovecote-sqlx-sqlite` | SQLite schema, migrations, enqueue and migration import, claims, lifecycle mutations, paging, and bounded busy handling. |

All crates use `MIT OR Apache-2.0`. The workspace declares Rust 1.94 as its
initial MSRV and tests that MSRV in CI. Raising the MSRV follows the published
MSRV policy and is not coupled to durable schema versions.

The `dovecote` crate MUST be synchronous, runtime-free, and SQLx-free. It MAY
depend privately on focused parsing or serialization crates, but public event,
URI, media-type, extension, and projection types are Dovecote-owned. It MUST NOT
expose a CloudEvents SDK type or a CloudEvents SDK as a public dependency.
`time::OffsetDateTime` is the deliberate public type for instants;
`std::time::Duration` is used for leases, delay, and backoff.

The adapter crates expose concrete SQLx entry points. Dovecote does not define a
common async repository, store, or worker trait in version 1. A reusable worker
can be generic over its own narrow closure or adapter, but that concern does
not widen Dovecote's public storage contract.

## 4. Core event model

### 4.1 CloudEvents context

`NewEvent` contains these validated values:

```rust
pub struct NewEvent {
    pub stream: StreamName,
    pub id: EventId,
    pub source: EventSource,
    pub event_type: EventType,
    pub subject: Option<EventSubject>,
    pub time: Option<OffsetDateTime>,
    pub datacontenttype: Option<ContentType>,
    pub dataschema: Option<SchemaUri>,
    pub partitionkey: Option<PartitionKey>,
    pub extensions: Extensions,
    pub data: Option<EventData>,
}

pub enum EventData {
    Json(Vec<u8>),
    Binary(Vec<u8>),
}
```

The exact final Rust field visibility and constructors may use accessors, but
the model and distinctions above are public contract. In particular, absent
data, present empty data, JSON data, and opaque binary data are distinct.

Dovecote fixes `specversion` to `1.0` for schema version 1. Callers do not set it.
Dovecote validates the CloudEvents 1.0 required attributes at construction:

- `id` is a non-empty CloudEvents String;
- `source` is a non-empty URI-reference; an absolute URI is recommended;
- `event_type` is a non-empty CloudEvents String; a reverse-DNS prefix is
  recommended;
- `subject`, when present, is non-empty;
- `time`, when present, is an RFC 3339-representable instant;
- `datacontenttype`, when present, is a syntactically valid media type;
- `dataschema`, when present, is a non-empty absolute URI; and
- all String values reject forbidden control characters, Unicode
  noncharacters, and unpaired surrogates as required by CloudEvents.

`EventData::Json` MUST contain exactly one valid JSON value encoded as UTF-8 and
MUST have a JSON media type: a subtype of `json` or one ending in `+json`, after
parameters are removed. `EventData::Binary` is never parsed or transcoded.
Every non-empty data value requires `datacontenttype`. Empty binary data MAY
omit it. Absent data and a zero-length binary value remain distinguishable in
storage and projection.

CloudEvents structured JSON permits an omitted `datacontenttype` to imply
`application/json` for a `data` member. Dovecote deliberately does not use that
permission for non-empty stored data: requiring an explicit media type is its
stricter portable-storage profile, not a claim about the CloudEvents minimum.

### 4.2 Routing metadata

`stream` selects an application-defined logical destination. It is outside the
CloudEvent because a CloudEvent describes an occurrence rather than a specific
routing destination. A stream is a non-empty UTF-8 value of at most 255 bytes
containing only ASCII letters, digits, `.`, `_`, and `-`; it starts with an
ASCII letter or digit. This portable form can be mapped safely to configured
topic or subject names, though an integration still owns the actual
destination configuration.

`partitionkey` is the registered CloudEvents partitioning extension when a
CloudEvent is projected, but Dovecote stores it in its own dedicated routing
column. It is at most 255 UTF-8 bytes, has CloudEvents String validity, and is
not part of Dovecote's claim ordering. It MUST also appear as the
`partitionkey` extension in structured and binary CloudEvents projections.
Callers MUST NOT separately insert an extension named `partitionkey`.

Neither field implies FIFO. A future FIFO mode would require a separately
specified sequence and head-of-line claim rule; schema version 1 has neither.

### 4.3 Extension attributes

Unknown valid CloudEvents extensions are preserved. Their names obey the
CloudEvents naming rules:

- one to 20 lowercase ASCII letters or digits;
- the first character SHOULD be a letter;
- not `data`, `data_base64`, a core attribute name, or `partitionkey`; and
- unique within the event.

An extension value is one of the CloudEvents abstract types:

```rust
pub enum ExtensionValue {
    Boolean(bool),
    Integer(i32),
    String(String),
    Binary(Vec<u8>),
    Uri(AbsoluteUri),
    UriReference(UriReference),
    Timestamp(OffsetDateTime),
}
```

Dovecote preserves the name, abstract type, and value. The durable extension
encoding is a compact UTF-8 JSON object ordered lexicographically by extension
name. Each member has exactly `type` and `value`; type is one of `boolean`,
`integer`, `string`, `binary`, `uri`, `uri-reference`, or `timestamp`. Binary is
padded RFC 4648 base64, timestamps use Dovecote's canonical timestamp form,
and all other values use their natural JSON or string form. This tagged form is
part of durable schema version 1 and prevents a URI or binary value from being
silently recovered as an ordinary string.

Arbitrary HTTP, Kafka, NATS, or vendor headers are not extensions and MUST NOT
be accepted through an unvalidated metadata map. An integration may construct
known extensions explicitly, then owns any transport-only headers separately.

CloudEvents context, routing fields, worker names, failure summaries, and
quarantine reasons are commonly visible to logs and intermediaries. They MUST
NOT contain credentials, bearer tokens, encryption keys, secrets, or personal
or special-category data. Such data belongs in an appropriately protected event
payload, if it should be emitted at all. Trace context is subject to the same
boundary.

### 4.4 Size profile

CloudEvents defines event size at the wire, but dynamic HTTP compression, TLS
records, Kafka batching/compression, and lower protocol frames are chosen below
Dovecote's integration boundary and cannot be predicted before send. Dovecote
therefore defines and enforces a logical, uncompressed binding size. It does not
label an event portable from payload length alone or claim to count lower
transport frames.

`MAX_PORTABLE_EVENT_BYTES` is 65,536. Before insertion, Dovecote computes an
event-material upper bound as the greater of:

1. the exact byte length of its structured JSON body plus the bytes in
   `Content-Type: application/cloudevents+json` under the same name, separator,
   and CRLF accounting used below; and
2. the binary body length plus, for every context attribute, the UTF-8 byte
   length of its binding name, four bytes for `: ` and CRLF, and three times the
   UTF-8 byte length of its canonical string value. The same calculation
   includes `Content-Type` for HTTP or `ce-datacontenttype` for NATS, whichever
   is longer.

The factor of three is the maximum expansion of one UTF-8 byte to `%XX` in the
HTTP and NATS bindings. Attribute names use the longer applicable `ce-<name>` or
`ce_<name>` spelling. Absent data contributes zero body bytes; present empty data
also contributes zero but remains distinct in the projection type.

Dovecote's default configured event limit rejects an event when this upper bound
exceeds `MAX_PORTABLE_EVENT_BYTES`. Passing that check means the logical
CloudEvent material fits the 64 KiB profile. It explicitly excludes request
lines and targets, routing subjects/topics, authentication and other application
headers, TLS records, HTTP/2 or HTTP/3 dynamic compression state, Kafka batch or
request framing and compression, and other lower transport frames.

Every integration MUST form its exact logical message—body, serialized
CloudEvents headers, routing key, and application headers—and compare that with
the destination's documented size-accounting rule and lower configured limit.
It rejects or routes an oversized message elsewhere before transport send and
never acknowledges it. Dovecote does not promise a byte count for framing that
only the transport implementation or peer creates.

Applications MAY configure a larger finite limit. The chosen limit is explicit
at adapter construction, is enforced before insertion, and is recorded in
operational documentation. Larger events are not portable by default: brokers,
HTTP servers, CDC converters, and other intermediaries impose different
ceilings. Dovecote version 1 does not provide blob storage, payload chunking, or
a claim-check service.

## 5. Durable schema

### 5.1 General rules

Dovecote owns exactly two domain tables: immutable `dovecote_events` and mutable
`dovecote_deliveries`. Application or SQLx migration bookkeeping is not a
Dovecote domain table. The PostgreSQL adapter also installs one
`dovecote_schema` bookkeeping table containing the durable schema version and
minimum-crate compatibility marker; it is not part of the event or delivery
domain. Both domain tables are created once per application database and shared
by all producers.

Applications execute adapter migrations under their own migration process.
Dovecote exposes migration artifacts and schema inspection but never migrates at
library initialization or application startup. Published migration files are
immutable. Durable schema versions evolve independently of crate semver.

The common accepted instant range is
`1970-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z`. All stored and returned
instants use UTC and microsecond representation. An occurrence time supplied by
a caller must fall in that range and be exactly representable at microsecond
precision; Dovecote rejects, rather than rounds or truncates, a value with
non-zero sub-microsecond precision.

Storage precision is not clock resolution. PostgreSQL and configured MySQL or
MariaDB clocks provide microsecond-capable values. SQLite's built-in current-time
source has millisecond resolution: its database operation time is stored in the
microsecond representation with the final three fractional digits zero. Dovecote
does not synthesize finer SQLite clock readings from the worker clock.
Applications must configure MySQL and MariaDB sessions to UTC; adapters verify
that condition and use a temporal type/arithmetic path covering the common
range rather than accepting server-local civil timestamps.

### 5.2 `dovecote_events`

`dovecote_events` is insert-only after creation. Dovecote supplies no update API,
trigger, or lifecycle mutation for it.

| Column | Logical type | Rule |
|---|---|---|
| `row_id` | signed 64-bit, database-generated | Primary key; positive, immutable, monotonically increasing cursor. Gaps are valid. |
| `stream` | string, 255 bytes | Required routing stream. |
| `specversion` | string, 8 bytes | Required; exactly `1.0` in schema version 1. |
| `event_id` | string, 1024 bytes | Required CloudEvents `id`; combined with `source`, at most 2,048 bytes. |
| `source` | string, 2048 bytes | Required CloudEvents `source` URI-reference; combined with `event_id`, at most 2,048 bytes. |
| `event_type` | string, 1024 bytes | Required CloudEvents `type`. |
| `subject` | nullable string, 2048 bytes | Optional CloudEvents `subject`. |
| `occurred_at` | nullable instant | Optional CloudEvents `time`. |
| `datacontenttype` | nullable string, 255 bytes | Required when `data` is non-empty. |
| `dataschema` | nullable string, 2048 bytes | Optional absolute CloudEvents schema URI. |
| `partitionkey` | nullable string, 255 bytes | Optional routing key and projected extension. |
| `extensions` | canonical tagged JSON text | Required; `{}` when empty. |
| `data_kind` | nullable enum | `json` or `binary`; null exactly when `data` is absent. |
| `data` | nullable bytes | Exact event bytes; null means absent, zero length means present empty data. |
| `enqueued_at` | database-generated instant | Required insertion time; not caller supplied. |

The database enforces:

- primary-key and positive-row constraints;
- unique `(source, event_id)` identity;
- allowed `specversion` and `data_kind` values;
- `data_kind IS NULL` if and only if `data IS NULL`; and
- `datacontenttype IS NOT NULL` whenever byte length of `data` is greater than
  zero.

Lengths and full CloudEvents syntax are checked by constructors before SQL.
In addition to the individual bounds, the UTF-8 byte lengths of `source` and
`event_id` together MUST NOT exceed 2,048 bytes. This portable identity-key
ceiling leaves index-tuple overhead below the smallest full B-tree entry limit
in the tested backend matrix; it is a Dovecote profile limit, not a CloudEvents
limit. Adapters repeat the individual and combined byte constraints in the
database. Identity comparison is bytewise over the validated UTF-8 `source` and
`event_id`, with equivalent binary collations on every backend. Backend DDL
uses bounded/indexable types and a full composite unique key. Prefix indexes,
digest-only uniqueness, or any scheme that could equate a digest collision are
forbidden. The tagged extension value is stored as text, not a backend JSON
type that can rewrite its canonical bytes. Event data is always stored as
bytes, including when `data_kind` is `json`.

The row ID is an inspection cursor, not event identity. Sequence allocation
may leave gaps after rollback and does not express commit order across
transactions.

### 5.3 `dovecote_deliveries`

There is exactly one delivery row for every event row. It is inserted in the
same statement sequence and caller transaction as its event. The foreign key is
`ON DELETE RESTRICT`; Dovecote exposes no deletion operation.

| Column | Logical type | Rule |
|---|---|---|
| `event_row_id` | signed 64-bit | Primary key and foreign key to `dovecote_events(row_id)`. |
| `state` | enum | Exactly `pending`, `claimed`, `delivered`, or `quarantined`. |
| `available_at` | instant | Required; initial value is database enqueue time. |
| `attempts` | non-negative signed 64-bit | Starts at zero; checked increment on each successful claim or reclaim. |
| `claim_token` | nullable 16 bytes | Fresh opaque 128-bit token for the active claim. |
| `claimed_by` | nullable string, 255 bytes | Validated operational worker identity. |
| `claim_expires_at` | nullable instant | Database time plus lease duration. |
| `last_failure_code` | nullable string, 128 bytes | Stable, non-sensitive operational category. |
| `last_failure_detail` | nullable string, 2048 bytes | Redacted, bounded diagnostic summary. |
| `delivered_at` | nullable instant | Database-generated terminal time. |
| `quarantined_at` | nullable instant | Database-generated terminal time. |
| `quarantine_reason` | nullable string, 2048 bytes | Required redacted terminal reason. |

The table has a claim index beginning with `state`, `available_at`, and
`event_row_id`, with backend-specific additions for expired claims. It also has
an index suitable for finding `claimed` rows by `claim_expires_at`.

Database checks make `state` the only state model:

| State | Required | Forbidden |
|---|---|---|
| `pending` | none beyond common fields | claim token, worker, claim expiry, both terminal timestamps, quarantine reason |
| `claimed` | claim token, worker, claim expiry | both terminal timestamps, quarantine reason |
| `delivered` | `delivered_at` | claim token, worker, claim expiry, `quarantined_at`, quarantine reason |
| `quarantined` | `quarantined_at`, quarantine reason | claim token, worker, claim expiry, `delivered_at` |

`available_at`, `attempts`, and optional last-failure fields remain populated in
all states. Terminal states are immutable through Dovecote operations. Nullable
timestamps do not create a second implicit lifecycle.

`last_failure_code` and `last_failure_detail` are either both null or both
present; the database enforces that pairing. Enqueue obtains one database time
value and uses it for both `dovecote_events.enqueued_at` and the initial
`dovecote_deliveries.available_at`.

### 5.4 Idempotent enqueue

The unique CloudEvents identity is `(source, event_id)`, not stream plus an
application key. Enqueue compares every caller-controlled immutable field:
stream, specversion, ID, source, type, subject, occurrence time, content type,
schema URI, partition key, extension names/types/values, data kind, and exact
data bytes. Database-generated `row_id` and `enqueued_at` are excluded.

- A new identity inserts both rows and returns `Enqueued { row_id }`.
- An existing identity with identical normalized content returns
  `AlreadyEnqueued { row_id }` and makes no change.
- An existing identity with any different immutable content returns
  `IdempotencyConflict { existing_row_id }` and makes no change.

URI references, schema URIs, media types, IDs, and event types retain their
exact validated UTF-8 spelling; Dovecote does not invent semantic URI or media-
type normalization. Instants use the canonical UTC representation defined in
this specification, and extensions use their canonical tagged representation.
JSON payload bytes are **not** canonicalized: whitespace, object member order,
and number spelling remain content and therefore may conflict. Implementations
compare complete content, not a digest alone. A private digest may accelerate
comparison only if an equal digest is followed by collision-safe field and byte
comparison.

Concurrent insertion of the same identity must resolve to one of these three
outcomes without leaking a backend uniqueness error as ordinary control flow.
An existing event without its required delivery row is `MigrationMismatch`, not
`AlreadyEnqueued` and not an opportunity for silent repair. The caller still
owns commit and rollback.

### 5.5 Tenant boundary

Durable schema version 1 has no `tenant_id` and Dovecote does not implement row-
level tenant authorization. `stream`, `source`, `subject`, and extension values
are not security boundaries. An application needing database-enforced tenant
isolation uses separate databases or schemas and separately authorized pools;
it applies Dovecote migrations once in each boundary and never gives a tenant
direct access to shared Dovecote tables.

An application may use one shared schema only when its own trusted producer and
worker tier is explicitly authorized to process all rows in that schema and
tenant-sensitive material stays out of operational context. If a real consumer
needs tenant-scoped claim, page, retention, or database authorization within one
table set, Dovecote must add a first-class tenant key and matching indexes in a
new durable schema design before that deployment. Filtering by stream in
application code is not an acceptable substitute.

## 6. Public operations

Each SQLx adapter provides the following concrete capabilities using that
backend's pool, connection, and transaction types:

```text
enqueue(caller_transaction, NewEvent) -> Result<EnqueueOutcome, EnqueueError>
import_for_migration(caller_transaction, NewEvent, ImportedDeliveryState)
    -> Result<ImportOutcome, ImportError>
finalize_pending_delivery_for_migration(
    caller_transaction, row_id, delivered_at,
) -> Result<FinalizeOutcome, FinalizeError>
claim(worker, lease_for, limit) -> Result<Vec<ClaimedEvent>, ClaimError>
renew(row_id, claim_token, lease_for) -> Result<(), MutationError>
ack(row_id, claim_token) -> Result<(), MutationError>
retry(row_id, claim_token, failure, backoff) -> Result<(), MutationError>
release(row_id, claim_token, delay) -> Result<(), MutationError>
quarantine(row_id, claim_token, reason) -> Result<(), MutationError>
page(after_row_id, limit) -> Result<Vec<PagedEvent>, PageError>
begin_snapshot() -> Result<SnapshotPager, PageError>
```

Adapters additionally expose embedded migration artifacts and a read-only
`check_schema` operation. These are concrete functions or methods, not a common
async trait.

The core crate owns the values exchanged by those operations. Their minimum
public shape is:

```rust
pub enum DeliveryState {
    Pending,
    Claimed,
    Delivered,
    Quarantined,
}

pub enum EnqueueOutcome {
    Enqueued { row_id: RowId },
    AlreadyEnqueued { row_id: RowId },
}

pub enum ImportedDeliveryState {
    Pending,
    Delivered { delivered_at: OffsetDateTime },
}

pub enum ImportOutcome {
    Imported { row_id: RowId },
    AlreadyImported { row_id: RowId },
}

pub enum FinalizeOutcome {
    Finalized { row_id: RowId },
    AlreadyFinalized { row_id: RowId },
}

pub struct ClaimedEvent {
    pub row_id: RowId,
    pub event: StoredEvent,
    pub attempts: AttemptCount,
    pub claim_token: ClaimToken,
    pub claimed_by: WorkerId,
    pub claim_expires_at: OffsetDateTime,
}

pub struct PagedEvent {
    pub row_id: RowId,
    pub event: StoredEvent,
    pub enqueued_at: OffsetDateTime,
    pub delivery: DeliverySnapshot,
}
```

`StoredEvent` contains the validated immutable fields from section 4 without
`stream` being mistaken for CloudEvents context. `DeliverySnapshot` is an enum
whose variants own only fields legal for that state; it is not a bag of
parallel booleans and optional terminal timestamps. `RowId` and `AttemptCount`
are checked non-negative newtypes. `ClaimToken` owns exactly 16 bytes, does not
implement an unredacted `Display`, and is generated by adapters from the
operating system's cryptographic randomness. `Failure`, `QuarantineReason`, and
all bounded string types validate before an adapter begins SQL.

`IdempotencyConflict` is an error, not an `EnqueueOutcome`, because callers must
not mistake different content for successful replay.

`IdentityConflict` and `ImportConflict` are importer errors. The former means
that the complete immutable event content changed under an existing identity;
the latter means that the delivery state changed or is no longer in the
canonical zero-attempt imported shape. `StateConflict` is the corresponding
typed migration-finalization error when a pending delivery is no longer in
that shape, has already been delivered with a different timestamp, or has
otherwise acquired delivery authority outside the finalizer.

### 6.1 Input bounds

- `limit` is in `1..=1000` for claim and page.
- `lease_for` is an exact whole number of milliseconds, greater than zero, and
  no more than 24 hours.
- `delay` and `backoff` are exact whole numbers of milliseconds and no more than
  30 days; zero is valid.
- converting a `Duration` to database microseconds is checked and never wraps,
  truncates into a negative value, or uses floating-point arithmetic.
- a worker identity is a non-empty CloudEvents-valid String of at most 255 UTF-8
  bytes and contains no secrets or personal data.

The bound values are public constants so callers can validate configuration at
startup.

### 6.2 Claim

Within one short database transaction, `claim`:

1. reads database time once as the operation time;
2. selects up to `limit` eligible rows ordered by ascending `event_row_id`;
3. treats a `pending` row as eligible when `available_at <= operation_time`;
4. treats a `claimed` row as eligible when
   `claim_expires_at <= operation_time`;
5. increments `attempts` with a checked integer operation;
6. generates a distinct, cryptographically random 128-bit claim token per row;
7. sets `state = claimed`, the supplied worker, and
   `claim_expires_at = operation_time + lease_for`; and
8. returns event content, attempts, token, and the database-computed expiry only
   after the claim transaction commits.

On reclaim, the new token MUST differ from the token currently stored for that
row; an implementation regenerates the random value in the vanishingly unlikely
case of equality. Tokens returned for separate rows in one batch are also
distinct.

Token generation is fallible. Adapters generate every token for the selected
batch before updating any delivery row. If operating-system randomness fails,
the claim transaction rolls back without changing attempts, state, tokens, or
expiry and returns `EntropyUnavailable`. Dovecote never substitutes timestamps,
row IDs, deterministic pseudorandomness, or a zero token.

Ascending selection makes behaviour inspectable but is not a FIFO promise:
locks, rollback, availability, expired claims, and concurrent workers can alter
delivery order. A process crash after the claim commits leaves the event
claimed until expiry; it never implies delivery.

If the next selected row's attempt counter cannot be incremented, the claim
transaction changes no rows and returns `CounterOverflow { row_id }`. Operators
must inspect and repair or migrate this invariant breach; Dovecote never wraps
the counter or silently delivers the row.

### 6.3 Fenced lifecycle mutations

Every post-claim mutation is one atomic conditional update whose predicate
includes:

```text
event_row_id = ?
AND state = 'claimed'
AND claim_token = ?
AND claim_expires_at > database_operation_time
```

Matching only the row ID is a correctness defect. An expired token loses its
authority even before another worker reclaims the row.

- `renew` sets expiry to database operation time plus `lease_for`. It does not
  add to the old expiry and cannot revive an expired claim.
- `ack` sets state to `delivered`, sets database `delivered_at`, and clears all
  claim fields.
- `retry` records the bounded redacted failure, sets state to `pending`, sets
  `available_at` to database operation time plus `backoff`, and clears all claim
  fields. It does not increment attempts; the next successful claim does.
- `release` sets state to `pending`, sets `available_at` to database operation
  time plus `delay`, clears all claim fields, and leaves the previous failure
  unchanged.
- `quarantine` sets state to `quarantined`, records database
  `quarantined_at` and the bounded redacted reason, and clears all claim fields.

No method accepts caller-supplied wall-clock time. Renewal, acknowledgement,
retry, release, and quarantine each obtain database time within their statement
or transaction.

When the conditional update affects no row, the adapter classifies the outcome
inside the same short transaction while holding or obtaining the row lock. The
precedence is exact:

1. no delivery row: `NotFound`;
2. delivery state is not `claimed`: `IllegalTransition { state }`;
3. delivery is claimed but the token differs or the lease is expired at the
   operation's database time: `LostClaim`; or
4. a claimed row still satisfies the predicate: retry the conditional mutation
   once inside the lock, with any further database failure reported as `Sql`.

This makes repeated acknowledgement of a delivered row an illegal transition,
while a worker superseded by reclaim receives `LostClaim`. Classification and
mutation use the same database operation time and cannot be separated by a
concurrent state change.

### 6.4 Paging

`page(after_row_id, limit)` returns a left-to-right join of immutable event and
current delivery state where `row_id > after_row_id`, ordered strictly by
ascending `row_id`. `None` starts before the first row. The next cursor is the
last returned row ID. Empty pages do not advance the cursor.

The join includes every event row. A missing delivery row is durable
inconsistency and MUST be surfaced as a typed serialization or migration error;
it MUST NOT disappear from live or snapshot paging. Claims scan delivery rows
for their hot path; reconciliation uses paging to discover orphan events.

Paging is for inspection, export, and reconciliation. It does not claim rows,
hide delivered or quarantined events, lock delivery rows, or promise a snapshot
across separate live `page` calls. Because databases may allocate row IDs before
commit, a later transaction can commit row 6 while an earlier transaction still
holds uncommitted row 5. Advancing a live cursor through 6 can therefore miss 5
when it later commits. An upper row-ID bound alone does not repair commit
inversion.

Consumers that must see every row in a finite export use `begin_snapshot`.
`SnapshotPager` owns one backend-specific consistent read transaction and pages
within that same snapshot until completion or explicit cancellation. PostgreSQL
uses `REPEATABLE READ`; MySQL and MariaDB use a consistent InnoDB snapshot at
their documented repeatable-read isolation; SQLite begins one read transaction
before reading its upper bound. The pager records its upper row ID only after
the snapshot is established and never advances beyond it. It is not `Send`
across unrelated executors or connections and releases the transaction on
completion, cancellation, or drop.

Snapshot paging is deliberately finite because a long read transaction delays
vacuum or history cleanup. Documentation requires a page/time budget and shows
how to restart an abandoned export from a new snapshot with application-level
reconciliation. SQLite deployments MUST set a bounded page/time budget because
its retained read snapshot can delay vacuum or cleanup. On abandonment, the
application explicitly closes or rolls back the pager and restarts from a new
snapshot according to its reconciliation or checkpoint policy; it MUST NOT
resume from an old pager cursor as if that cursor were a durable checkpoint.
Live `page` remains suitable for inspection where concurrent commit inversion
and later reconciliation are acceptable.

## 7. Errors and outcomes

Core and adapter APIs expose typed, actionable categories. Display strings are
diagnostic text and are never parsed to recover a category.

| Category | Meaning | Caller response |
|---|---|---|
| `InvalidEvent` | Required attribute, extension, JSON, size, or operational-field bound failed validation. | Correct the producer input. |
| `InvalidLimit` | Claim/page limit is outside the public range. | Correct configuration or input. |
| `InvalidDuration` | Lease, delay, or backoff is zero where forbidden, too large, or cannot be represented. | Correct configuration or input. |
| `IdempotencyConflict` | `source + id` already names different immutable content. | Treat as a producer identity defect; do not retry unchanged. |
| `IdentityConflict` | Migration import found different immutable content under an existing identity. | Stop the migration and reconcile the legacy/export ledger. |
| `ImportConflict` | Migration import found a changed, claimed, retried, or otherwise non-canonical delivery state. | Stop the migration and reconcile state ownership. |
| `StateConflict` | Migration finalization found a pending delivery that is claimed, retried, quarantined, delayed, already delivered with a different timestamp, or otherwise not canonical. | Stop the migration acknowledgement and reconcile state ownership. |
| `InvalidState` | The supplied imported delivery state or authoritative timestamp is invalid. | Correct the exporter state and retry the bounded migration step. |
| `LostClaim` | A claimed row's token is wrong, expired, or superseded. | Stop work and do not report success. |
| `NotFound` | No such event row exists. | Correct the row reference or reconcile retention/manual changes. |
| `IllegalTransition` | The requested operation cannot legally follow the stored non-claimed state. | Stop the stale/repeated action; investigate only if the caller did not expect that state. |
| `CounterOverflow` | Attempts cannot be incremented safely. | Investigate and repair; never wrap. |
| `EntropyUnavailable` | The operating system could not provide secure claim-token randomness. | Retry only after the entropy source is healthy; the batch is unchanged. |
| `MigrationMismatch` | Tables, columns, constraints, or durable schema version are missing or incompatible. | Apply the documented application migration. |
| `BackendMismatch` | A MySQL adapter is used against an unsupported MariaDB/MySQL variant or another wrong backend. | Use the correct adapter/configuration. |
| `Serialization` | A validated value cannot be encoded or decoded according to durable format version. | Investigate data or library defect. |
| `Sql` | The database rejected or failed an operation for another reason. | Inspect preserved backend code and source error; classify operationally. |

Adapter error enums retain the SQLx source error and operation context without
exposing database error strings as domain categories. Duplicate identity is
translated only after content comparison. Lock timeouts, deadlocks, connection
loss, and busy exhaustion remain distinguishable through typed SQLx sources or
adapter subcategories where callers need retry policy.

`Failure` contains a stable code and redacted detail with the bounds in the
delivery schema. `QuarantineReason` is separately typed because quarantine is a
terminal operator or transport-policy decision. Dovecote validates bounds; it
cannot prove that caller prose was correctly redacted.

## 8. Backend requirements

Dovecote never retries a PostgreSQL, MySQL, or MariaDB deadlock, serialization
failure, statement timeout, or lock timeout internally in version 1. The adapter
returns a typed transient SQL category after the whole short transaction has
rolled back; callers may retry the complete operation under their own bounded
policy. A retry never reuses a token from the rolled-back attempt. Deployments
set finite backend statement and lock-wait limits, and the published support
matrix records the test values. SQLite's bounded busy handling is the one
documented adapter-level retry because busy acquisition is its ordinary
single-writer contention model.

Schema locks, connection acquisition, and pool exhaustion are not hidden behind
unbounded waits. Tests inject deadlock, serialization/lock timeout where the
backend supports it, connection failure, and SQLite busy exhaustion and verify
complete rollback plus the advertised typed category.

### 8.1 PostgreSQL

PostgreSQL uses `timestamptz`, `bytea`, full-value unique indexes, check
constraints, and a short claim transaction with `FOR UPDATE SKIP LOCKED`.
Conformance tests run at the documented default `READ COMMITTED` isolation and
at every additional isolation level the release claims to support. Tests verify
that row locks are held only through selection/update and commit, not during
transport work.

### 8.2 MySQL and MariaDB

The MySQL adapter contains explicit dialect paths where MySQL and MariaDB differ.
It uses UTC `DATETIME(6)`-class instant columns and arithmetic rather than the
narrower legacy `TIMESTAMP` range, binary claim tokens, full-value unique
identity indexes, enforced check constraints on supported releases, and short
locking transactions using verified `SKIP LOCKED` behaviour.

Each Dovecote release publishes results for all of:

- MySQL 8.4 LTS;
- the current MySQL Innovation series at release time; and
- MariaDB 11.8 LTS.

The exact current Innovation version is pinned in CI and the published support
matrix rather than frozen into this durable specification. MySQL success never
stands in for MariaDB success, or vice versa. Tests state transaction isolation,
SQL mode, time zone, character set, and storage engine. InnoDB is required for
MySQL/MariaDB support.

### 8.3 SQLite

SQLite uses integer row IDs, blobs, checked text enums, canonical UTC timestamp
text, foreign keys enabled on every connection, and an explicit short
`BEGIN IMMEDIATE` write transaction for claims. It has bounded, configurable
busy handling with a documented default and returns busy exhaustion as a typed
database failure. It commits or rolls back before returning claimed events.

SQLite's single-writer serialization is part of the support contract, not
described as equivalent to server-database concurrency. Claim conformance still
proves that concurrent callers never receive overlapping claims.

### 8.4 Support matrix publication

Before each release, the repository publishes a machine-readable or Markdown
matrix containing exact database image/version, SQLx version, Rust/MSRV version,
isolation/settings, conformance result, and test date. A backend is advertised
only while CI exercises it. A failing current Innovation line may be marked
temporarily unsupported only in a release note with an owner and re-test
condition; it may not be silently replaced by an older green image.

## 9. CloudEvents projections

Dovecote's projections implement CloudEvents 1.0 semantics without exposing the
CloudEvents SDK. Wire `specversion` remains `1.0`; conformance fixtures are
pinned to the current stable CloudEvents 1.0.2 core, JSON-format, and binding
artifacts by repository tag or commit rather than following a moving `main`
branch silently. Updating that fixture pin requires a compatibility review.
Projection methods are pure and deterministic.

Dovecote canonical timestamps are UTC RFC 3339: `Z` is used for UTC; the
fraction is omitted when zero and otherwise uses the shortest decimal fraction
that preserves the stored instant. Extensions are ordered lexicographically.
Structured JSON uses a fixed member order: `specversion`, `id`, `source`,
`type`, optional core attributes in the order `subject`, `time`,
`datacontenttype`, `dataschema`, then `partitionkey`, remaining extensions, and
finally `data` or `data_base64`. Consumers must not rely on JSON member order,
but fixed output keeps golden vectors and digests reproducible.

### 9.1 Structured JSON

The structured projection has content type `application/cloudevents+json` and a
compact UTF-8 JSON object.

- Core and extension attributes become top-level members.
- `partitionkey` is a top-level extension member when present.
- `EventData::Json` becomes a top-level `data` member containing the parsed JSON
  value, never a quoted JSON document. Dovecote parses and deterministically
  serializes that value; the projected value is semantically equal to the
  stored JSON but need not preserve its whitespace, object-member order, or
  number spelling.
- `EventData::Binary` becomes `data_base64` containing padded RFC 4648 base64,
  even when its media type is JSON.
- absent data emits neither member;
- present empty binary data emits `"data_base64":""`; and
- `data` and `data_base64` are mutually exclusive.

### 9.2 Binary mode

The protocol-neutral binary projection contains:

- raw data bytes as the body, with absent and present-empty represented
  distinctly in the Dovecote type;
- `datacontenttype` as the transport content type when present; and
- every other core and extension attribute as an ordered context map using the
  CloudEvents canonical string encoding.

The context map includes `specversion`, `id`, `source`, `type`, optional
`subject`, `time`, `dataschema`, `partitionkey`, and all other extensions.
Dovecote does not accept or emit arbitrary transport headers in this map.
Protocol integrations add the binding-specific prefix and own authentication,
authorization, tracing-hop headers, and broker configuration.

### 9.3 Required golden vectors

Golden fixtures cover, separately and in combination:

- no data, present empty binary data, UTF-8 text bytes, arbitrary binary bytes,
  a JSON object, and a scalar JSON value;
- `datacontenttype` parameters and `+json` media types;
- `dataschema`, `subject`, occurrence time, and `partitionkey`;
- every extension abstract type and an unknown valid extension;
- optional `traceparent` and `tracestate`; and
- absent optional attributes.

Each fixture includes the durable extension encoding, exact structured JSON
bytes, and protocol-neutral binary projection. Structured output is validated
against the CloudEvents 1.0 JSON schema or an independent conforming
implementation. Durable tagged-extension encode/decode round trips preserve
every extension's abstract type and value. Binary projection preserves exact
stored data bytes, including JSON bytes. Structured JSON projection preserves
the semantic JSON value and has byte-for-byte deterministic Dovecote output; it
does not claim to reproduce the producer's original JSON spelling after a
generic CloudEvents parser has materialized that value.

CloudEvents structured JSON maps `String`, `Binary`, `URI`, `URI-reference`, and
`Timestamp` extension values to JSON strings. A generic parser therefore cannot
recover an unknown extension's abstract type from structured JSON alone. The
bounded Dovecote migration importer does not parse CloudEvents or infer
extension types: it accepts an already validated `NewEvent` produced by the
application's legacy exporter.

### 9.4 Migration importer

Each SQLx adapter exposes `import_for_migration` as a concrete, caller-owned
transaction operation. It is distinct from `enqueue` and is intended only for
the finite legacy-outbox cutover. The caller supplies one of
`ImportedDeliveryState::Pending` or
`ImportedDeliveryState::Delivered { delivered_at }`; the importer accepts no
legacy claim, retry, or quarantine state. The migration caller must resolve or
explicitly fence an active legacy claim before mapping that source row to
pending. Pending availability and the event's `enqueued_at` use one
database-authoritative operation timestamp, and an exact replay requires the
stored backend representations of those timestamps to be equal as well.
Delivered timestamps are authoritative source values and MUST be in the common
UTC range at exact microsecond precision; adapters preserve that precision.

The operation validates the installed schema before its first mutation and
inserts the immutable event and delivery row in the caller transaction. A
replay returns the typed `ImportOutcome::AlreadyImported` only when the
complete immutable event content matches and the existing delivery has the
canonical imported shape: zero attempts, no claim, failure, or quarantine
fields, and either pending state or the same delivered timestamp. A changed
event returns `IdentityConflict`; a changed or non-canonical delivery returns
the distinct `ImportConflict`. The importer never commits or rolls back the
caller-owned transaction: after any typed error, the caller MUST roll it back
before retrying or committing the surrounding application work. This differs
from adapter-owned lifecycle operations, which own their short transaction and
roll it back internally on failure. The caller commits or rolls back the
complete application/import transaction.

### 9.5 Migration delivery finalization

Each SQLx adapter also exposes the concrete, migration-only operation
`finalize_pending_delivery_for_migration(caller_transaction, row_id,
delivered_at)`. It is used when a legacy publisher has delivered a row that
was dual-written as a pending Dovecote delivery. It is not an ordinary
acknowledgement shortcut and is not used by the maintained 2.0 writer path.
The caller supplies the Dovecote `RowId` returned by the bridge mapping and
the legacy publisher's authoritative delivery occurrence time. The caller
owns commit or rollback; after any error it MUST roll back the surrounding
transaction before retrying.

The adapter validates the timestamp against Dovecote's common UTC range and
microsecond precision, checks the installed schema before mutation, and locks
the event and delivery before inspecting them. It permits exactly this
delivery shape:

```text
state = pending
attempts = 0
claim_token = NULL, claimed_by = NULL, claim_expires_at = NULL
last_failure_code = NULL, last_failure_detail = NULL
delivered_at = NULL, quarantined_at = NULL, quarantine_reason = NULL
available_at = enqueued_at
```

The transition sets `state = delivered` and the supplied authoritative
`delivered_at`, clearing no unrelated fields because the canonical predicate
already requires them to be empty. A successful first transition returns
`FinalizeOutcome::Finalized`; an exact rerun with the same timestamp returns
`AlreadyFinalized`. A changed timestamp, active or expired claim, retry,
failure, quarantine, delayed availability, missing delivery row, or any other
non-canonical state returns a typed conflict or migration mismatch. A row
finalized this way is terminal and is never eligible for a Dovecote claim.

PostgreSQL preserves the value with `timestamptz` microsecond semantics;
MySQL and MariaDB use UTC `DATETIME(6)`; SQLite stores canonical RFC3339 text
and its database-generated operation clock is millisecond-resolution. The
authoritative supplied timestamp remains at validated microsecond precision on
all three adapters. Adapter errors retain backend SQL and transient
categories, and SQLite requires a caller-owned write transaction (normally
`BEGIN IMMEDIATE`).

### 9.6 Trace context

Dovecote recognizes `traceparent` and `tracestate` as ordinary validated String
extensions following the W3C Trace Context grammar. `tracestate` is rejected
without `traceparent`. Trace propagation is opt-in per producer and destination.
The CloudEvents tracing extension does not replace protocol-specific tracing
headers; a single-hop integration that emits both keeps them consistent.

Trace IDs can correlate activity across systems and may widen visibility.
Applications document retention and access, must not put user or case
identifiers into trace state, and may omit trace context at a privacy boundary.
Dovecote never synthesizes trace context.

## 10. Integration mappings

### 10.1 HTTP

For structured HTTP, the body is Dovecote's structured JSON and `Content-Type`
is `application/cloudevents+json`. For binary HTTP, the body is the exact event
data, `Content-Type` is `datacontenttype` when present, and every other context
attribute becomes a `ce-<name>` header. Binary HTTP MUST NOT also emit
`ce-datacontenttype`.

Each HTTP context-header value begins as the CloudEvents canonical string. The
binding then percent-encodes space, double quote, percent, and every character
outside printable ASCII U+0021 through U+007E. Non-ASCII characters are first
encoded as UTF-8 and each byte becomes uppercase `%XX`. A decoder removes
HTTP quoted-string escaping when present for legacy compatibility, performs
exactly one percent-decoding pass, and rejects invalid UTF-8. Header names and
percent encoding are output mappings, not stored event metadata.

The HTTP integration owns request method and URL, authentication, response
classification, timeouts, retries, and maximum request size. A 2xx response is
not acknowledged until the integration's explicit policy accepts it.

### 10.2 Kafka

Structured mode places Dovecote's JSON projection in the record value and sets
the record content-type header to `application/cloudevents+json`. Binary mode
places exact event data in the record value, maps the content type, and maps
context attributes to `ce_<name>` UTF-8 headers as required by the CloudEvents
Kafka binding.

In binary mode, absent event data maps to a null Kafka record value. On a
log-compacted topic that is a tombstone, not merely an event with no payload.
Dovecote's Kafka integration rejects this combination by default. An application
either uses structured mode, whose envelope is a non-null value even without
data, or explicitly enables `allow_compaction_tombstone` for a destination where
deletion is the intended meaning. Present empty binary data maps to a non-null
zero-length value and is not treated as absent.

When `partitionkey` is present, the opt-in key mapper uses its UTF-8 bytes as
the Kafka record key and leaves the extension in the CloudEvent. When absent,
Dovecote supplies no key. Topic selection comes from an application-owned map
from `stream` to topic; a stream is not blindly treated as a deployment topic.
Kafka key partitioning can improve per-key broker order but does not turn
Dovecote's claim lifecycle into FIFO.

### 10.3 NATS JetStream

The integration maps every binary context attribute, including
`datacontenttype`, to `ce-<name>` NATS headers; unlike HTTP, binary NATS uses
`ce-datacontenttype`, not `Content-Type`. NATS header values use the same
single-pass CloudEvents percent-encoding rules stated for HTTP. Structured mode
sends Dovecote's structured JSON according to the CloudEvents NATS binding. The
integration maps `stream` through application destination configuration and MAY
use `partitionkey` as subject or consumer routing input without removing it from
the event.

For JetStream duplicate suppression, `Nats-Msg-Id` is the lowercase hex SHA-256
of the unambiguous length-prefixed UTF-8 sequence `source || event_id`. The
length prefix is an unsigned 64-bit big-endian byte count before each value.
This is a deterministic transport mapping of the CloudEvents duplicate identity,
not a replacement for consumer idempotency. The integration documents the
JetStream duplicate window; duplicates outside it remain possible.

### 10.4 Azure Event Grid

An Event Grid integration configures CloudEvents 1.0 as the input schema and
uses Dovecote's structured JSON projection through Event Grid's documented HTTP
publishing envelope. `id`, `source`, `type`, `subject`, occurrence `time`,
`datacontenttype`, `dataschema`, extensions, and data retain their CloudEvents
meanings. `stream` selects the configured Event Grid topic or domain route and
is not added to the CloudEvent unless the application deliberately defines a
separate valid extension.

The integration owns Azure authentication, endpoint/batch framing, response
classification, and service limits. Dovecote does not pretend that an accepted
publish is an exactly-once consumer effect.

### 10.5 Debezium Outbox Event Router

Debezium watches only `dovecote_events`. All Dovecote worker operations update
only `dovecote_deliveries`, so they produce no change event for the watched
table. Enqueue produces exactly one insert into `dovecote_events`; the associated
delivery insert is outside the connector include list.

The connector include list or SMT predicate MUST select only
`dovecote_events`. Configure the stable Outbox Event Router fields as follows
(property names are Debezium's):

| Debezium property or output | Dovecote field | Meaning |
|---|---|---|
| `table.field.event.id` | `event_id` | Debezium `id` header; CloudEvents event ID after the downstream binding maps it to `ce_id`. Consumers pair it with `source`. |
| `table.field.event.type` | `event_type` | Debezium `type` header; CloudEvents type after the downstream binding maps it to `ce_type`. |
| `route.by.field` | `stream` | Logical route input. |
| `route.topic.replacement` | application template using `${routedByValue}` | Explicit stream-to-topic convention. |
| `table.field.event.key` | `partitionkey` | Kafka key when present; null means no key. |
| `table.field.event.timestamp` | `enqueued_at` | Kafka record timestamp truncated to whole milliseconds by Debezium. |
| `table.field.event.payload` | `data` | Exact payload column; converter settings must preserve bytes. |
| additional header `ce_specversion` | `specversion` | CloudEvents version. |
| additional header `ce_source` | `source` | Completes duplicate identity and CloudEvents context. |
| additional header `ce_subject` | `subject` | Optional CloudEvents subject. |
| additional header `ce_time` | `occurred_at` | Optional occurrence time, distinct from record timestamp. |
| additional header `content-type` | `datacontenttype` | Optional data media type. |
| additional header `ce_dataschema` | `dataschema` | Optional schema URI. |
| additional header `ce_partitionkey` | `partitionkey` | Preserves the extension when the field is also the key. |
| additional envelope `dovecote_extensions` | `extensions` | Tagged extension JSON for a downstream CloudEvents-aware transformer. |
| additional envelope `dovecote_data_kind` | `data_kind` | Distinguishes JSON from opaque binary projection. |
| additional envelope `dovecote_row_id` | `row_id` | Reconciliation cursor, not event identity. |
| additional envelope `dovecote_enqueued_at` | `enqueued_at` | Source logical timestamp retained for exact enqueue-time recovery when the converter preserves Debezium's microsecond value. |

The reference SMT configuration contains these literal fields; connector table
selection and the application-owned topic prefix are configured alongside it:

```properties
transforms=outbox
transforms.outbox.type=io.debezium.transforms.outbox.EventRouter
transforms.outbox.table.op.invalid.behavior=fatal
transforms.outbox.table.field.event.id=event_id
transforms.outbox.table.field.event.type=event_type
transforms.outbox.table.field.event.key=partitionkey
transforms.outbox.table.field.event.timestamp=enqueued_at
transforms.outbox.table.field.event.payload=data
transforms.outbox.table.expand.json.payload=false
transforms.outbox.route.by.field=stream
transforms.outbox.route.topic.replacement=outbox.event.${routedByValue}
transforms.outbox.table.fields.additional.placement=specversion:header:ce_specversion,source:header:ce_source,subject:header:ce_subject,occurred_at:header:ce_time,datacontenttype:header:content-type,dataschema:header:ce_dataschema,partitionkey:header:ce_partitionkey,extensions:envelope:dovecote_extensions,data_kind:envelope:dovecote_data_kind,row_id:envelope:dovecote_row_id,enqueued_at:envelope:dovecote_enqueued_at
```

Kafka record timestamps have millisecond precision. When Debezium receives a
microsecond source timestamp for `enqueued_at`, the Outbox Event Router divides
it by 1,000 and discards the sub-millisecond remainder for the record timestamp.
This is a deterministic truncation, not Dovecote timestamp equality. A consumer
that requires the exact enqueue instant reads `dovecote_enqueued_at` from the
envelope using a converter configuration proven to retain the source logical
microsecond timestamp; otherwise that deployment documents millisecond-only CDC
precision.

Debezium additional-field placement does not promise to omit a configured
nullable column. Depending on placement and converter, a database null remains
a null envelope field or a null-valued Kafka header. The
`table.field.additional.missing` option concerns an absent column, not a null
column value. A deployment that needs complete CloudEvents output from CDC MUST
use a tested downstream transform that:

- removes null optional attributes rather than stringifying them;
- maps Debezium's `id` and `type` headers to the chosen CloudEvents binding;
- decodes `dovecote_extensions` and respects `dovecote_data_kind`;
- preserves exact binary payload bytes; and
- produces the structured or binary mapping in section 9.

The generic Debezium SMT cannot explode the tagged extension object into
arbitrary CloudEvents headers by itself.

CDC tests deliberately run `claim`, `renew`, `ack`, `retry`, `release`, and
`quarantine` and assert that no watched-table update is emitted. Connector and
converter fixtures inspect both raw SMT output and the final transformed
CloudEvent. They cover JSON, binary, null partition keys, every nullable
additional field, an enqueue time with non-zero sub-millisecond microseconds,
the expected record-timestamp truncation, exact envelope timestamp recovery when
advertised, and exact binary payload bytes for each advertised CDC backend.

Dovecote does not run Debezium, Kafka Connect, or schema-registry infrastructure.
CDC is an integration path for applications already willing to operate it, not
a hidden dependency of ordinary Dovecote use.

### 10.6 Other standards

AsyncAPI MAY describe an application's transport-facing destinations and
CloudEvents messages. It does not describe Dovecote's SQL lease protocol and is
not required by the storage crates. An HTTP producer integration MAY accept the
IETF `Idempotency-Key` header under its own policy, but that transport key does
not replace or redefine durable CloudEvents `source + id` identity.

Dovecote does not implement CloudEvents SQL/CESQL, CNCF Serverless Workflow, a
schema registry, or a vendor retry-header vocabulary. They solve querying,
workflow, payload governance, or transport policy rather than this storage
contract. Broker-specific duplicate suppression remains a useful integration
aid and never becomes Dovecote's correctness guarantee.

## 11. Retention and operational recovery

### 11.1 Retention

Retention, deletion, and archival are application policy. Dovecote version 1
provides no delete, purge, TTL, partition-management, or automatic quarantine
job. Applications may implement later deletion only under an explicit policy
that accounts for consumer deduplication, audit needs, CDC lag, backups, and
foreign references. Direct deletion is outside Dovecote's API and support unless
a later specification adds it.

Release documentation nevertheless includes an application-owned retention
runbook. Before deleting anything, it must:

1. select only delivered rows, or quarantined rows whose separate resolution
   policy permits removal; pending and claimed rows are never retention input;
2. keep terminal rows longer than the greatest consumer deduplication window;
3. prove every configured CDC connector has advanced beyond the candidate event
   rows and is not snapshotting or replaying them;
4. satisfy backup, restore, audit, legal-hold, and incident-investigation policy;
5. dry-run bounded candidate counts and row-ID ranges and record approval;
6. delete delivery rows before their referenced immutable event rows in bounded
   application transactions, because the foreign key is restrictive; and
7. verify counts and CDC health after each batch before database-specific
   vacuum or space reclamation.

Dovecote does not infer a safe cutoff or turn acknowledgement into deletion.

### 11.2 Recovery example

Dovecote also excludes worker supervision. Documentation must nevertheless show
a complete recovery loop using a fake transport:

1. claim a bounded batch;
2. attempt each send;
3. acknowledge only on the fake transport's accepted result;
4. retry a classified transient failure with bounded backoff;
5. quarantine a classified permanent rejection;
6. stop mutating on `LostClaim`;
7. simulate a crash after send and before ack; and
8. show the reclaimed duplicate and consumer-side identity deduplication.

The example must make the ambiguous crash visible rather than describing it as
delivery success.

### 11.3 Backpressure and shutdown

A worker integration claims no more rows than it has bounded in-flight capacity
to send. It does not hoard leased rows in an unbounded channel. Its configured
lease exceeds the transport timeout plus a documented scheduling margin, and it
renews only work that is still active and whose current token it owns. Dovecote's
maximum batch limit is a safety ceiling, not a recommended concurrency level.

Graceful shutdown first stops new claims, then gives in-flight sends a bounded
drain interval. A confirmed accepted send may be acknowledged while its claim
is valid. Work known not to have been attempted may be released only with its
current valid token. Cancelled or ambiguous sends are never acknowledged or
released as though unsent; the integration lets their leases expire and accepts
possible duplicate delivery. Process termination after the drain bound relies
on the same lease recovery rather than a hidden shutdown state.

### 11.4 Operational signals

Dovecote adds no telemetry runtime to the core crate. Adapter documentation
provides bounded status queries, and integrations instrument sends using the
OpenTelemetry messaging semantic conventions where OpenTelemetry is present.
Production guidance requires at least:

- pending count and oldest pending age by stream;
- claimed count, oldest claim age, and expired-lease count;
- attempt distribution and retry/quarantine transition totals;
- claim, renewal, and mutation latency;
- `LostClaim`, `IllegalTransition`, overflow, entropy, busy/lock, deadlock,
  serialization, connection, and other SQL failure totals;
- transport send latency and classified result totals; and
- an integration-owned count of ambiguous send-before-ack exits.

Metric labels are bounded and low-cardinality. Event IDs, sources, subjects,
partition keys, worker IDs, failure details, quarantine prose, trace baggage,
personal data, and payloads are not metric labels. Logs apply the same privacy
boundary. W3C Baggage is not persisted as Dovecote metadata by default; an
integration may propagate it only under an explicit allowlist and privacy/
retention policy.

## 12. Keepsake and Gatekeep migration

### 12.1 Release coordination

Migration support consumes the Keepsake 1.1 and Gatekeep 1.0 source schemas;
those historical migration files remain byte-for-byte immutable. The verified
bridge releases are Keepsake 1.2.x and Gatekeep 1.1.x. Each project adds a new,
forward-only application-consumable migration; the shared Dovecote schema is
created once even when both libraries are present.

The 1.x bridge releases are temporary dual-persistence and publication bridges:
they write the legacy record and a pending Dovecote event/delivery in one
caller-owned transaction, and the legacy publisher remains the owner. The 2.0 cutover makes Dovecote the sole
maintained SQL audit/outbox shape and removes the legacy APIs. Legacy
audit/outbox tables and rows may remain as read-only historical source material
under application retention policy and are removed, if ever, only by a later
application-owned cleanup. Domain state and domain audit meaning remain owned
by the respective library; Dovecote owns the shared durable audit event and
delivery record.

### 12.2 Legacy identity and content mapping

The application configures a stable absolute `source` for each producer before
migration. It must not derive source from a deployment hostname, ephemeral
instance, or database name. Recommended examples are
`https://example.org/keepsake` and `https://example.org/gatekeep` under a domain
the producer controls.

Legacy event IDs are deterministic ASCII strings:

```text
keepsake-outbox-<legacy decimal row id>
gatekeep-outbox-<legacy decimal row id>
```

When a historical audit occurrence has no legacy outbox row, its migration
identity uses the reserved legacy-audit namespace instead:

```text
keepsake-audit-legacy-<legacy audit row id>
gatekeep-audit-legacy-<legacy decision row id>
```

An outbox identity is authoritative whenever an outbox row exists. These
legacy-audit identities are migration-only and are distinct from the 2.0
project-owned audit identities; a 2.0 producer must never derive an event ID
from a Dovecote row ID or an absent legacy outbox row.

The source plus this ID is stable across retries and resumptions. Migration
maps:

| Legacy field | Dovecote field |
|---|---|
| library-specific source config | `source` |
| prefixed legacy outbox row ID | `event_id` |
| application-configured distinct stream (`keepsake-audit` or `gatekeep-audit` by default) | `stream` |
| legacy `event_type` | `event_type` |
| legacy outbox payload in byte-preserving TEXT storage | exact UTF-8 bytes as `data = EventData::Json` |
| legacy outbox payload in JSONB/JSON storage | deterministic UTF-8 bytes from the documented backend JSON-value export codec as `data = EventData::Json`; compare source manifests semantically because producer spelling was not retained |
| normalized audit columns with no legacy outbox payload | output of the owning project's named versioned migration codec; `data = EventData::Json` and provenance records the codec version; these bytes are not represented as original legacy bytes |
| legacy outbox payload absent | reserved `*-audit-legacy-<legacy audit row id>` identity; it is not a Dovecote storage-row identity |
| explicit `application/json` | `datacontenttype` |
| legacy `created_at` | `occurred_at` only when producer policy establishes it as occurrence time; otherwise omit |
| none | subject, schema URI, partition key, extensions |

The resumable importer MUST checkpoint four independent source cursors and
their four captured high-water marks: Keepsake audit rows, Keepsake outbox
rows, Gatekeep decision-audit rows, and Gatekeep outbox rows. Project and table
row-ID sequences are unrelated and may overlap or diverge; neither one cursor
per project nor one cursor across projects is a safe progress key. A committed
row advances only the cursor for the source table that selected it.

The planned 2.0 producer mapping makes occurrence time explicit:

| 2.0 producer value | Dovecote and CloudEvents mapping |
|---|---|
| Keepsake `AuditEvent.at` | Will continue to map to `occurred_at` and CloudEvents `time`. |
| Gatekeep 2.0 explicit decision-time captured authoritatively at the audit/orchestration boundary | Will map to `occurred_at` and CloudEvents `time`. |
| Clock access inside deterministic Gatekeep policy evaluation | Will remain absent; the evaluation will not read a clock. |
| Database `created_at`, `recorded_at`, and `enqueued_at` | Will remain persistence times and MUST NOT substitute for occurrence time. |

Although legacy columns are JSON/JSONB/text depending on backend, migration
must preserve a defined byte representation. For an outbox payload in
byte-preserving TEXT storage, the application exporter reads those bytes as the
authority, records their SHA-256 digest, and inserts the same exact UTF-8 bytes
into Dovecote. For JSONB/JSON storage, the exporter parses the stored value and
uses the documented deterministic backend JSON-value export codec
(`postgres-jsonb-canonical-v1` or `mysql-json-canonical-v1`); it records and
inserts those exported bytes, while comparing the source manifest semantically.
Database-side casts that silently reformat JSON outside that codec are
forbidden. When an older normalized audit row has no outbox payload, the
owning project reconstructs its typed event through one named, versioned
migration codec (Keepsake `keepsake.audit.json.v1` or Gatekeep
`gatekeep-audit-json-v1`). Reconstructed output is deterministic and its
version is recorded in migration provenance, but it MUST NOT be labelled as
original source bytes or compared to a byte sequence that never existed.

### 12.3 State mapping

Every legacy audit occurrence through the recorded high-water mark is copied,
including normalized rows without an outbox payload and delivered history. Its
Dovecote delivery state is chosen as follows:

| Legacy row | Dovecote state |
|---|---|
| never claimed | `pending`, available at migration database time |
| claim finished, expired, or explicitly fenced before the state snapshot | `pending`, available at migration database time |
| delivered with authoritative `delivered_at` | `delivered`, preserving the exact timestamp |

No legacy claim is trusted across cutover because it has no Dovecote claim token.
Workers are stopped or drained before the state snapshot, but stopping workers
alone does not make an active unexpired claim importable. Such a claim must
finish, expire, or be explicitly fenced first; only then is the row imported as
pending at Dovecote database time. A delivered row without an authoritative,
microsecond-representable timestamp is a migration error, not a pending row and
not a silently skipped row. Legacy tables become read-only historical source
material after cutover and are removed, if ever, only by a later
application-owned migration.

### 12.4 Runbook and cutover

The published runbook is backend-specific and resumable:

1. inventory schema versions, row counts by legacy state, configured sources,
   streams, database time zone, and current workers;
2. back up the database and prove the documented restore check;
3. apply Dovecote's schema once and run `check_schema`;
4. deploy code capable of reading Dovecote but leave legacy producers/workers in
   place;
5. enter a declared maintenance window: stop or drain legacy workers, pause both
   legacy producer write paths, wait for their in-flight transactions to finish,
   and enforce the pause at the application or database boundary so no new
   legacy outbox row can commit;
6. while the pause is enforced, record each legacy table's maximum row ID and
   migrate every row (including delivered history) through that inclusive
   high-water mark in bounded transactions, producing canonical export bytes
   and digests and calling `import_for_migration` with its mapped state;
7. rerun the migration through the same high-water marks: identical identities
   and canonical imported state must return `AlreadyImported`, while changed
   immutable content must stop the migration with `IdentityConflict` and
   changed imported state with `ImportConflict`;
8. compare per-library and total source counts, row IDs, event types, byte
   lengths, and SHA-256 payload digests, then prove there are no legacy rows
   above either high-water mark before cutover;
9. switch both producer write paths to Dovecote while the pause remains enforced,
   resume producers, and only then start Dovecote workers;
10. confirm both libraries coexist in the shared tables under distinct streams;
11. monitor legacy writes, Dovecote claims, lost claims, retries, quarantine,
    and duplicate consumer identities; and
12. remove migration-only code after its named verification and rollback window.

There is no interval in the default runbook where a legacy producer can commit
after the migration snapshot and before its write path changes. Failure to
enforce the pause aborts cutover.

Rollback before producer cutover restores the backup or removes only verified
migration-owned Dovecote rows under the application's written procedure.
Rollback after Dovecote publication begins cannot pretend downstream effects did
not occur; it stops publishers, reconciles by `source + id`, and follows the
application incident plan.

If an application genuinely requires zero-downtime bridging, it first deploys a
bounded producer version that atomically writes both the legacy row and the
Dovecote event with its pending delivery in the same caller transaction. Both
rows carry the identical producer-configured CloudEvents `source + id`, and the
legacy publisher MUST
expose that identity unchanged to the downstream consumer. If the legacy path
cannot do so, this zero-downtime bridge is unsupported; use the paused cutover.

The bridge then records legacy high-water marks, migrates older rows including
delivered history, and repeatedly reconciles every legacy identity through the
moving high-water marks. A dual-written row may be delivered by a legacy worker
while its Dovecote delivery remains pending. The bridge acknowledgement path
records that authoritative legacy delivery by calling
`finalize_pending_delivery_for_migration` in its caller-owned transaction; it
must not use a Dovecote claim token or ordinary `ack` for that row. Dovecote
will publish an unfenced pending event again after cutover; this duplicate is
expected and is safe only because the consumer deduplicates the identical
`source + id`. Cutover stops legacy
workers, proves a zero-row reconciliation delta, switches publication
ownership to Dovecote, and disables the legacy write in one named release. The
bridge has a named owner, start and end releases, reconciliation metric, alert,
rollback procedure, and deletion condition. It is not part of Dovecote's
permanent API and MUST NOT become indefinite dual-write compatibility.

### 12.5 MariaDB maintenance-window route for existing Keepsake deployments

An existing Keepsake deployment on MariaDB MUST use a maintenance window for
the Dovecote cutover; MariaDB can require downtime here. The deployed Keepsake
source schema is authoritative.
Migration tooling MUST NOT recreate it from, or replay, the immutable Keepsake
MySQL migration files against MariaDB 11.8. Those files describe one release's
schema creation and are not a reconstruction of an already-deployed database.

The supported route is:

1. stop Keepsake writers and the legacy publisher; let in-flight transactions
   finish, then finish, expire, or explicitly fence every active legacy claim;
2. take a database snapshot or backup of that stopped, claim-resolved source
   and prove the restore check; if the MariaDB server is upgraded to 11.8,
   follow the vendor-supported upgrade procedure and run `mariadb-upgrade`;
3. install Dovecote's migration artifact and the additive final Keepsake 1.x
   bridge migration through the application migration runner, then run
   `dovecote_sqlx_mysql::check_schema(&pool).await`;
4. export and import complete Keepsake history from the existing tables,
   including delivered rows and audit rows without an outbox row, with one
   inclusive high-water mark per source table. Use exact source bytes where
   they exist; otherwise use the named versioned exporter codec and record its
   provenance. Use the final 1.x bridge importer, which calls
   `import_for_migration` in bounded caller-owned transactions after the claim
   precondition in step 1;
5. produce a zero-delta reconciliation covering identities, event types,
   occurrence times, payload lengths and SHA-256 digests, delivery states, and
   per-table counts. Prove there are no rows above the captured bounds and no
   legacy writer or publisher committed during the window, then call the final
   1.x bridge's `finalize_upgrade_reconciliation()` to write the only accepted
   Keepsake 2.0 activation evidence; and
6. run Keepsake 2.0 `upgrade_migrate()` and `activate_upgrade()`, then deploy
   the Dovecote-only writer and its one publication owner. Keep the legacy
   publisher stopped and retain the legacy tables and export ledger read-only
   through the rollback and consumer-deduplication windows.

The repository's MariaDB fixture provides one pinned evidence path: it creates
the historical source tables from hash-checked published artifacts on MariaDB
10.3.17, where the historical SQL-mode-dependent generated column was still
admitted, cleanly stops that server, opens the same data volume on MariaDB
11.8.6, runs `mariadb-upgrade`, and then imports the existing tables without
applying a historical Keepsake migration on 11.8. The upgrade reports the
inherited generated-column warning; the fixture proves the source remains
readable for complete-history import. This evidence covers that transition
only; it does not make arbitrary deployed schemas or neighbouring MariaDB
releases interchangeable.

## 13. Excluded responsibilities

Dovecote version 1 does not own or provide:

- retention, deletion, archival, table partitioning, or vacuum policy;
- worker task spawning, process supervision, cancellation, or shutdown;
- Tokio or another async runtime in `dovecote`;
- transport clients or retry classification for Kafka, NATS, HTTP, Event Grid,
  Restate, object storage, or ledgers;
- destination URLs, credentials, broker topics, NATS subjects, or stream-to-
  destination configuration;
- event payload schemas or a schema registry;
- application audit meaning, authorization meaning, tenant policy, or evidence
  claims;
- payload encryption, compression, blob storage, chunking, or claim-check;
- tracing policy or automatic trace propagation;
- FIFO or exactly-once delivery; or
- automatic migrations at process startup.

These exclusions are API boundaries, not deferred methods on a generic trait.

## 14. Acceptance and verification

### 14.1 Shared backend conformance

The same behavioural suite runs against every advertised database target and
proves:

- caller transaction commit makes event and delivery visible together;
- caller rollback leaves neither row;
- identical replay returns the original row ID without mutation;
- conflicting duplicate identity returns `IdempotencyConflict`;
- identities exactly at the 2,048-byte combined boundary insert and deduplicate
  through the full unique key on every backend, while a 2,049-byte identity is
  rejected before SQL;
- paging is strictly ordered, stable across gaps, bounded, and includes every
  state;
- a snapshot pager retains one consistent read across pages, while live paging
  explicitly makes no completeness claim during concurrent commits;
- direct invalid state combinations are rejected by database constraints;
- event and extension constructors reject invalid CloudEvents values;
- tagged durable extensions round-trip all abstract types, while outbound
  projections serialize each type according to CloudEvents without claiming
  that an unknown JSON string recovers its original abstract type; and
- absent, empty, JSON, and binary data stay distinct.

### 14.2 Race and crash tests

Barrier-controlled tests with separate connections prove:

- concurrent claims never overlap;
- locked rows do not prevent other eligible rows being claimed where the
  backend advertises skip-locked concurrency;
- expired claims are atomically reclaimed with a fresh token;
- an expired token cannot renew or mutate before or after reclaim;
- stale tokens cannot ack, retry, release, or quarantine;
- repeated mutation of delivered, quarantined, and pending rows returns
  `IllegalTransition`, while a wrong or expired token on a claimed row returns
  `LostClaim`;
- renewal is based on current database time, not old expiry or worker time;
- entropy failure rolls back the whole claim batch without incrementing an
  attempt or changing a token, state, or expiry;
- a crash before claim commit exposes no claim;
- a crash after claim commit leaves a reclaimable claimed row; and
- a crash after transport success but before ack never implies delivery and may
  produce a duplicate.

A commit-inversion paging race allocates row 5 in an open transaction, commits
row 6, begins both a live scan and a consistent snapshot at controlled points,
then commits row 5. It proves the documented live-scan limitation and that every
row visible in the snapshot is returned exactly once before its upper bound.

### 14.3 Retry and terminal-state tests

Tests prove:

- initial availability and all backoff/delay calculations use database time;
- attempts start at zero and increment exactly once per successful claim or
  reclaim, not on retry/release;
- invalid or overflowing durations fail without mutation;
- sub-millisecond leases and positive delays/backoffs are rejected on every
  backend rather than rounded differently;
- occurrence-time range endpoints round-trip and values just outside the common
  range are rejected before SQL;
- checked attempt overflow fails without wrap or partial claim;
- failure codes/details and quarantine reasons enforce UTF-8 byte bounds;
- retry records redacted failure fields and schedules availability;
- release preserves the last failure and applies its delay;
- explicit quarantine is terminal;
- delivered and quarantined rows reject every worker mutation; and
- timestamps and cleared claim fields satisfy the exclusive-state checks.

### 14.4 Backend locking tests

- PostgreSQL exercises short `FOR UPDATE SKIP LOCKED` transactions and stated
  isolation behaviour.
- MySQL 8.4 LTS, the pinned current Innovation line, and MariaDB 11.8 LTS each
  exercise their own `SKIP LOCKED`, isolation, time-zone, check-constraint, and
  deadlock behaviour. Their results are reported separately.
- SQLite exercises separate connections, `BEGIN IMMEDIATE`, bounded busy retry,
  busy exhaustion, commit, rollback, single-writer non-overlap, millisecond
  database-clock resolution, and microsecond storage representation with three
  trailing zero digits for database-generated operation times.

No test holds a database transaction open while the fake transport runs.

### 14.5 CDC tests

For every advertised CDC backend, an integration fixture proves:

- enqueue creates one watched-table insert;
- inserting the delivery row does not enter the connector include list;
- every lifecycle operation emits no watched-table update;
- configured event ID, route, key, timestamp, payload, and additional fields
  match section 10.5;
- JSON and arbitrary binary payload bytes survive the connector/converter path;
- nullable optional fields and absent partition keys remain absent/null rather
  than stringified in raw SMT output, and the downstream transform removes null
  CloudEvents attributes;
- update handling fails the fixture if anything updates `dovecote_events`.

### 14.6 Projection tests

Golden vectors are checked byte-for-byte on the MSRV and latest stable Rust.
An independent CloudEvents 1.0 validator or SDK parses structured output.
Binary projections preserve all context and exact stored data bytes. Structured
JSON tests require semantic equality for JSON data and byte-for-byte equality
with Dovecote's deterministic projected output, not the producer's original JSON
spelling. HTTP and Kafka binding fixtures verify header naming, content type,
body, and Kafka key. Kafka fixtures distinguish absent data from a present
zero-length value and prove that binary no-data events are rejected for compacted
topics unless tombstone semantics are explicitly enabled. HTTP and NATS vectors
cover spaces, double quotes, literal
percent signs, and non-ASCII values, and verify uppercase single-pass percent
encoding. NATS fixtures also verify `ce-datacontenttype` and the deterministic
duplicate ID. Boundary vectors cover structured base64 expansion, worst-case
percent-encoded context, both normative logical-size calculations, explicit
exclusion of lower dynamic transport frames, destination-specific size checks,
and rejection immediately above each configured limit.

### 14.7 Migration fixtures

Fixtures begin with representative Keepsake 1.1 and Gatekeep 1.0 databases for
every shared backend. Each contains never-claimed, currently claimed (requiring
resolution or fencing before import), expired, and delivered legacy rows, plus
non-ASCII and formatting-sensitive JSON.

Tests verify:

- historical migrations are unchanged;
- Dovecote schema creation is shared and idempotently coordinated;
- complete legacy history migrates; only claims that finished, expired, or were
  explicitly fenced before the state snapshot become pending, and delivered
  rows retain their authoritative delivery timestamp;
- deterministic identities and configured sources remain stable on rerun;
- counts, byte lengths, and SHA-256 digests match before cutover;
- interrupted batches resume through `AlreadyImported`;
- changed immutable content stops with `IdentityConflict`, and changed delivery
  state stops with `ImportConflict`;
- no unfenced legacy claim crosses cutover;
- the default maintenance window rejects or blocks a concurrent legacy producer
  write until the Dovecote producer path is active;
- a zero-downtime bridge fixture inserts rows before and after successive
  high-water marks and proves every identity is present before legacy writes are
  disabled;
- that bridge fixture lets a legacy worker deliver a dual-written row before
  cutover, finalizes its pending Dovecote row with the authoritative legacy
  timestamp, and proves any Dovecote later duplicate carries the identical
  `source + id` and is deduplicable downstream;
- Keepsake and Gatekeep coexist under distinct streams; and
- delivered legacy rows remain untouched.

### 14.8 Documentation and release gate

Before the first release, documentation includes:

- the non-guarantees in section 2 near the first usage example;
- crate semver, MSRV, durable schema, and projection-format versioning policy;
- the exact tested backend matrix;
- schema installation and `check_schema` instructions;
- the fake-transport recovery example from section 11;
- HTTP, Kafka, NATS, Event Grid, and Debezium mappings;
- payload-size and operational-field privacy boundaries;
- bounded backpressure, graceful shutdown, operational signals, and
  OpenTelemetry integration guidance;
- the application-owned retention/deletion runbook and tenant-isolation
  boundary;
- rolling schema compatibility and `check_schema` version-pair policy;
- the complete Keepsake/Gatekeep migration runbook; and
- a responsible security reporting route.

The implementation, schema, race suite, golden vectors, CDC mapping, and
migration runbook receive an independent review before the first crate is
published. Empty name-reservation releases are forbidden.

## 15. Versioning policy

Crate semver governs Rust API compatibility. Durable schema version, tagged
extension encoding, and projection format are separately versioned contracts.
A crate release may support more than one durable version during an explicit
migration, but it never silently rewrites stored rows.

Schema changes are forward-only migrations. A change that alters identity,
event bytes, extension meaning, state constraints, or projection output needs:

- a new durable version or a demonstrated backward-compatible interpretation;
- fixtures from the preceding version;
- an application-controlled migration and rollback/reconciliation plan;
- updated golden vectors and support matrix; and
- release notes naming affected consumers.

Every migration declares its compatible old/new crate versions and whether a
rolling deployment is supported. A rolling-compatible change follows
expand/migrate/contract ordering: first add structures tolerated by old code,
then deploy code that can read both representations and writes the new one,
then backfill and verify, and only in a later application-controlled migration
remove the old representation. `check_schema` runs as a read-only startup or
deployment gate and rejects a crate that is too old or too new for the installed
schema; it never applies the migration itself.

A migration that cannot support concurrent old and new processes requires a
declared maintenance window. Rollback means returning to a documented compatible
crate/schema pair or restoring and reconciling from backup; down-migrations do
not pretend that already-published events or transformed durable values can be
unwritten. Migration fixtures exercise every advertised rolling-version pair
and the explicit rejection of unsupported pairs.

The implementation may change private SQL or serialization machinery without a
durable version bump only when all stored values, observable outcomes, ordering,
and golden projection bytes remain compatible.

## 16. References

- [CloudEvents 1.0 specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
- [CloudEvents JSON format](https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md)
- [CloudEvents HTTP binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/http-protocol-binding.md)
- [CloudEvents Kafka binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md)
- [CloudEvents NATS binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md)
- [CloudEvents partitioning extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/partitioning.md)
- [CloudEvents distributed tracing extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/)
- [AsyncAPI 3.0 specification](https://www.asyncapi.com/docs/reference/specification/v3.0.0)
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [MySQL release model](https://dev.mysql.com/doc/refman/en/mysql-releases.html)
- [MariaDB maintenance policy](https://mariadb.org/about/maintenance-policy/)
