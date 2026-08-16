# Carrier acceptance specification

- Status: accepted design for initial implementation
- Specification date: 16 August 2026
- Durable schema version: 1
- CloudEvents compatibility target: 1.0
- Initial MSRV: Rust 1.94
- Licence: `MIT OR Apache-2.0`

## 1. Purpose and authority

Carrier is a storage-only transactional outbox for Keepsake, Gatekeep, and
other applications that need to commit an event beside application state and
deliver it later. It owns durable insertion, deterministic inspection, leased
claims, claim-token fencing, retry state, and quarantine. It does not deliver
messages itself.

This document is the acceptance contract for Carrier's first implementation.
It is standalone: an implementer must not need the earlier ecosystem plan to
resolve an API, schema, lifecycle, interoperability, migration, or testing
decision described here.

The project was previously called Carry. Carrier is now the canonical project
and package-family name. The historical wording in
`send-app-ecosystem-foundations.md` remains unchanged until a separate edit.

Normative words such as **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry
their usual RFC 2119 meanings.

## 2. Scope and guarantees

Carrier provides one logical delivery lifecycle for each stored event:

1. an application inserts an event using its existing database transaction;
2. committing that transaction makes both the application's state and the
   Carrier event visible;
3. a worker may claim the event under a bounded lease;
4. only the current, unexpired claim may renew, acknowledge, retry, release, or
   quarantine it;
5. an expired claim may be reclaimed atomically with a fresh token.

Carrier guarantees:

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

Carrier does **not** guarantee:

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
| `carrier` | Validated events and extensions, claims, failures, states, outcomes, and deterministic CloudEvents projections. |
| `carrier-sqlx-postgres` | PostgreSQL schema, migrations, transaction-bound enqueue, claims, lifecycle mutations, and paging. |
| `carrier-sqlx-mysql` | MySQL and MariaDB schemas, migrations, dialect handling, enqueue, claims, lifecycle mutations, and paging. |
| `carrier-sqlx-sqlite` | SQLite schema, migrations, enqueue, claims, lifecycle mutations, paging, and bounded busy handling. |

All crates use `MIT OR Apache-2.0`. The workspace declares Rust 1.94 as its
initial MSRV and tests that MSRV in CI. Raising the MSRV follows the published
MSRV policy and is not coupled to durable schema versions.

The `carrier` crate MUST be synchronous, runtime-free, and SQLx-free. It MAY
depend privately on focused parsing or serialization crates, but public event,
URI, media-type, extension, and projection types are Carrier-owned. It MUST NOT
expose a CloudEvents SDK type or a CloudEvents SDK as a public dependency.
`time::OffsetDateTime` is the deliberate public type for instants;
`std::time::Duration` is used for leases, delay, and backoff.

The adapter crates expose concrete SQLx entry points. Carrier does not define a
common async repository, store, or worker trait in version 1. A reusable worker
can be generic over its own narrow closure or adapter, but that concern does
not widen Carrier's public storage contract.

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

Carrier fixes `specversion` to `1.0` for schema version 1. Callers do not set it.
Carrier validates the CloudEvents 1.0 required attributes at construction:

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
`application/json` for a `data` member. Carrier deliberately does not use that
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
CloudEvent is projected, but Carrier stores it in its own dedicated routing
column. It is at most 255 UTF-8 bytes, has CloudEvents String validity, and is
not part of Carrier's claim ordering. It MUST also appear as the
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

Carrier preserves the name, abstract type, and value. The durable extension
encoding is a compact UTF-8 JSON object ordered lexicographically by extension
name. Each member has exactly `type` and `value`; type is one of `boolean`,
`integer`, `string`, `binary`, `uri`, `uri-reference`, or `timestamp`. Binary is
padded RFC 4648 base64, timestamps use Carrier's canonical timestamp form,
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
Carrier's integration boundary and cannot be predicted before send. Carrier
therefore defines and enforces a logical, uncompressed binding size. It does not
label an event portable from payload length alone or claim to count lower
transport frames.

`MAX_PORTABLE_EVENT_BYTES` is 65,536. Before insertion, Carrier computes an
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

Carrier's default configured event limit rejects an event when this upper bound
exceeds `MAX_PORTABLE_EVENT_BYTES`. Passing that check means the logical
CloudEvent material fits the 64 KiB profile. It explicitly excludes request
lines and targets, routing subjects/topics, authentication and other application
headers, TLS records, HTTP/2 or HTTP/3 dynamic compression state, Kafka batch or
request framing and compression, and other lower transport frames.

Every integration MUST form its exact logical message—body, serialized
CloudEvents headers, routing key, and application headers—and compare that with
the destination's documented size-accounting rule and lower configured limit.
It rejects or routes an oversized message elsewhere before transport send and
never acknowledges it. Carrier does not promise a byte count for framing that
only the transport implementation or peer creates.

Applications MAY configure a larger finite limit. The chosen limit is explicit
at adapter construction, is enforced before insertion, and is recorded in
operational documentation. Larger events are not portable by default: brokers,
HTTP servers, CDC converters, and other intermediaries impose different
ceilings. Carrier version 1 does not provide blob storage, payload chunking, or
a claim-check service.

## 5. Durable schema

### 5.1 General rules

Carrier owns exactly two domain tables: immutable `carrier_events` and mutable
`carrier_deliveries`. Application or SQLx migration bookkeeping is not a
Carrier domain table. Both tables are created once per application database and
shared by all producers.

Applications execute adapter migrations under their own migration process.
Carrier exposes migration artifacts and schema inspection but never migrates at
library initialization or application startup. Published migration files are
immutable. Durable schema versions evolve independently of crate semver.

The common accepted instant range is
`1970-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z`. All stored and returned
instants use UTC and microsecond representation. An occurrence time supplied by
a caller must fall in that range and be exactly representable at microsecond
precision; Carrier rejects, rather than rounds or truncates, a value with
non-zero sub-microsecond precision.

Storage precision is not clock resolution. PostgreSQL and configured MySQL or
MariaDB clocks provide microsecond-capable values. SQLite's built-in current-time
source has millisecond resolution: its database operation time is stored in the
microsecond representation with the final three fractional digits zero. Carrier
does not synthesize finer SQLite clock readings from the worker clock.
Applications must configure MySQL and MariaDB sessions to UTC; adapters verify
that condition and use a temporal type/arithmetic path covering the common
range rather than accepting server-local civil timestamps.

### 5.2 `carrier_events`

`carrier_events` is insert-only after creation. Carrier supplies no update API,
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
in the tested backend matrix; it is a Carrier profile limit, not a CloudEvents
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

### 5.3 `carrier_deliveries`

There is exactly one delivery row for every event row. It is inserted in the
same statement sequence and caller transaction as its event. The foreign key is
`ON DELETE RESTRICT`; Carrier exposes no deletion operation.

| Column | Logical type | Rule |
|---|---|---|
| `event_row_id` | signed 64-bit | Primary key and foreign key to `carrier_events(row_id)`. |
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
all states. Terminal states are immutable through Carrier operations. Nullable
timestamps do not create a second implicit lifecycle.

`last_failure_code` and `last_failure_detail` are either both null or both
present; the database enforces that pairing. Enqueue obtains one database time
value and uses it for both `carrier_events.enqueued_at` and the initial
`carrier_deliveries.available_at`.

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
exact validated UTF-8 spelling; Carrier does not invent semantic URI or media-
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

Durable schema version 1 has no `tenant_id` and Carrier does not implement row-
level tenant authorization. `stream`, `source`, `subject`, and extension values
are not security boundaries. An application needing database-enforced tenant
isolation uses separate databases or schemas and separately authorized pools;
it applies Carrier migrations once in each boundary and never gives a tenant
direct access to shared Carrier tables.

An application may use one shared schema only when its own trusted producer and
worker tier is explicitly authorized to process all rows in that schema and
tenant-sensitive material stays out of operational context. If a real consumer
needs tenant-scoped claim, page, retention, or database authorization within one
table set, Carrier must add a first-class tenant key and matching indexes in a
new durable schema design before that deployment. Filtering by stream in
application code is not an acceptable substitute.

## 6. Public operations

Each SQLx adapter provides the following concrete capabilities using that
backend's pool, connection, and transaction types:

```text
enqueue(caller_transaction, NewEvent) -> Result<EnqueueOutcome, EnqueueError>
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
expiry and returns `EntropyUnavailable`. Carrier never substitutes timestamps,
row IDs, deterministic pseudorandomness, or a zero token.

Ascending selection makes behaviour inspectable but is not a FIFO promise:
locks, rollback, availability, expired claims, and concurrent workers can alter
delivery order. A process crash after the claim commits leaves the event
claimed until expiry; it never implies delivery.

If the next selected row's attempt counter cannot be incremented, the claim
transaction changes no rows and returns `CounterOverflow { row_id }`. Operators
must inspect and repair or migrate this invariant breach; Carrier never wraps
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
reconciliation. Live `page` remains suitable for inspection where concurrent
commit inversion and later reconciliation are acceptable.

## 7. Errors and outcomes

Core and adapter APIs expose typed, actionable categories. Display strings are
diagnostic text and are never parsed to recover a category.

| Category | Meaning | Caller response |
|---|---|---|
| `InvalidEvent` | Required attribute, extension, JSON, size, or operational-field bound failed validation. | Correct the producer input. |
| `InvalidLimit` | Claim/page limit is outside the public range. | Correct configuration or input. |
| `InvalidDuration` | Lease, delay, or backoff is zero where forbidden, too large, or cannot be represented. | Correct configuration or input. |
| `IdempotencyConflict` | `source + id` already names different immutable content. | Treat as a producer identity defect; do not retry unchanged. |
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
terminal operator or transport-policy decision. Carrier validates bounds; it
cannot prove that caller prose was correctly redacted.

## 8. Backend requirements

Carrier never retries a PostgreSQL, MySQL, or MariaDB deadlock, serialization
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

Each Carrier release publishes results for all of:

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

Carrier's projections implement CloudEvents 1.0 semantics without exposing the
CloudEvents SDK. Wire `specversion` remains `1.0`; conformance fixtures are
pinned to the current stable CloudEvents 1.0.2 core, JSON-format, and binding
artifacts by repository tag or commit rather than following a moving `main`
branch silently. Updating that fixture pin requires a compatibility review.
Projection methods are pure and deterministic.

Carrier canonical timestamps are UTC RFC 3339: `Z` is used for UTC; the
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
  value, never a quoted JSON document. Carrier parses and deterministically
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
  distinctly in the Carrier type;
- `datacontenttype` as the transport content type when present; and
- every other core and extension attribute as an ordered context map using the
  CloudEvents canonical string encoding.

The context map includes `specversion`, `id`, `source`, `type`, optional
`subject`, `time`, `dataschema`, `partitionkey`, and all other extensions.
Carrier does not accept or emit arbitrary transport headers in this map.
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
the semantic JSON value and has byte-for-byte deterministic Carrier output; it
does not claim to reproduce the producer's original JSON spelling after a
generic CloudEvents parser has materialized that value.

CloudEvents structured JSON maps `String`, `Binary`, `URI`, `URI-reference`, and
`Timestamp` extension values to JSON strings. A generic parser therefore cannot
recover an unknown extension's abstract type from structured JSON alone.
Carrier version 1 provides outbound projections, not a generic CloudEvent import
API. Any future importer must require a caller-supplied extension-type registry
for unknown string-mapped attributes or retain them as explicitly untyped input;
it must not guess from string contents.

### 9.4 Trace context

Carrier recognizes `traceparent` and `tracestate` as ordinary validated String
extensions following the W3C Trace Context grammar. `tracestate` is rejected
without `traceparent`. Trace propagation is opt-in per producer and destination.
The CloudEvents tracing extension does not replace protocol-specific tracing
headers; a single-hop integration that emits both keeps them consistent.

Trace IDs can correlate activity across systems and may widen visibility.
Applications document retention and access, must not put user or case
identifiers into trace state, and may omit trace context at a privacy boundary.
Carrier never synthesizes trace context.

## 10. Integration mappings

### 10.1 HTTP

For structured HTTP, the body is Carrier's structured JSON and `Content-Type`
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

Structured mode places Carrier's JSON projection in the record value and sets
the record content-type header to `application/cloudevents+json`. Binary mode
places exact event data in the record value, maps the content type, and maps
context attributes to `ce_<name>` UTF-8 headers as required by the CloudEvents
Kafka binding.

In binary mode, absent event data maps to a null Kafka record value. On a
log-compacted topic that is a tombstone, not merely an event with no payload.
Carrier's Kafka integration rejects this combination by default. An application
either uses structured mode, whose envelope is a non-null value even without
data, or explicitly enables `allow_compaction_tombstone` for a destination where
deletion is the intended meaning. Present empty binary data maps to a non-null
zero-length value and is not treated as absent.

When `partitionkey` is present, the opt-in key mapper uses its UTF-8 bytes as
the Kafka record key and leaves the extension in the CloudEvent. When absent,
Carrier supplies no key. Topic selection comes from an application-owned map
from `stream` to topic; a stream is not blindly treated as a deployment topic.
Kafka key partitioning can improve per-key broker order but does not turn
Carrier's claim lifecycle into FIFO.

### 10.3 NATS JetStream

The integration maps every binary context attribute, including
`datacontenttype`, to `ce-<name>` NATS headers; unlike HTTP, binary NATS uses
`ce-datacontenttype`, not `Content-Type`. NATS header values use the same
single-pass CloudEvents percent-encoding rules stated for HTTP. Structured mode
sends Carrier's structured JSON according to the CloudEvents NATS binding. The
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
uses Carrier's structured JSON projection through Event Grid's documented HTTP
publishing envelope. `id`, `source`, `type`, `subject`, occurrence `time`,
`datacontenttype`, `dataschema`, extensions, and data retain their CloudEvents
meanings. `stream` selects the configured Event Grid topic or domain route and
is not added to the CloudEvent unless the application deliberately defines a
separate valid extension.

The integration owns Azure authentication, endpoint/batch framing, response
classification, and service limits. Carrier does not pretend that an accepted
publish is an exactly-once consumer effect.

### 10.5 Debezium Outbox Event Router

Debezium watches only `carrier_events`. All Carrier worker operations update
only `carrier_deliveries`, so they produce no change event for the watched
table. Enqueue produces exactly one insert into `carrier_events`; the associated
delivery insert is outside the connector include list.

The connector include list or SMT predicate MUST select only
`carrier_events`. Configure the stable Outbox Event Router fields as follows
(property names are Debezium's):

| Debezium property or output | Carrier field | Meaning |
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
| additional envelope `carrier_extensions` | `extensions` | Tagged extension JSON for a downstream CloudEvents-aware transformer. |
| additional envelope `carrier_data_kind` | `data_kind` | Distinguishes JSON from opaque binary projection. |
| additional envelope `carrier_row_id` | `row_id` | Reconciliation cursor, not event identity. |
| additional envelope `carrier_enqueued_at` | `enqueued_at` | Source logical timestamp retained for exact enqueue-time recovery when the converter preserves Debezium's microsecond value. |

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
transforms.outbox.table.fields.additional.placement=specversion:header:ce_specversion,source:header:ce_source,subject:header:ce_subject,occurred_at:header:ce_time,datacontenttype:header:content-type,dataschema:header:ce_dataschema,partitionkey:header:ce_partitionkey,extensions:envelope:carrier_extensions,data_kind:envelope:carrier_data_kind,row_id:envelope:carrier_row_id,enqueued_at:envelope:carrier_enqueued_at
```

Kafka record timestamps have millisecond precision. When Debezium receives a
microsecond source timestamp for `enqueued_at`, the Outbox Event Router divides
it by 1,000 and discards the sub-millisecond remainder for the record timestamp.
This is a deterministic truncation, not Carrier timestamp equality. A consumer
that requires the exact enqueue instant reads `carrier_enqueued_at` from the
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
- decodes `carrier_extensions` and respects `carrier_data_kind`;
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

Carrier does not run Debezium, Kafka Connect, or schema-registry infrastructure.
CDC is an integration path for applications already willing to operate it, not
a hidden dependency of ordinary Carrier use.

### 10.6 Other standards

AsyncAPI MAY describe an application's transport-facing destinations and
CloudEvents messages. It does not describe Carrier's SQL lease protocol and is
not required by the storage crates. An HTTP producer integration MAY accept the
IETF `Idempotency-Key` header under its own policy, but that transport key does
not replace or redefine durable CloudEvents `source + id` identity.

Carrier does not implement CloudEvents SQL/CESQL, CNCF Serverless Workflow, a
schema registry, or a vendor retry-header vocabulary. They solve querying,
workflow, payload governance, or transport policy rather than this storage
contract. Broker-specific duplicate suppression remains a useful integration
aid and never becomes Carrier's correctness guarantee.

## 11. Retention and operational recovery

### 11.1 Retention

Retention, deletion, and archival are application policy. Carrier version 1
provides no delete, purge, TTL, partition-management, or automatic quarantine
job. Applications may implement later deletion only under an explicit policy
that accounts for consumer deduplication, audit needs, CDC lag, backups, and
foreign references. Direct deletion is outside Carrier's API and support unless
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

Carrier does not infer a safe cutoff or turn acknowledgement into deletion.

### 11.2 Recovery example

Carrier also excludes worker supervision. Documentation must nevertheless show
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
renews only work that is still active and whose current token it owns. Carrier's
maximum batch limit is a safety ceiling, not a recommended concurrency level.

Graceful shutdown first stops new claims, then gives in-flight sends a bounded
drain interval. A confirmed accepted send may be acknowledged while its claim
is valid. Work known not to have been attempted may be released only with its
current valid token. Cancelled or ambiguous sends are never acknowledged or
released as though unsent; the integration lets their leases expire and accepts
possible duplicate delivery. Process termination after the drain bound relies
on the same lease recovery rather than a hidden shutdown state.

### 11.4 Operational signals

Carrier adds no telemetry runtime to the core crate. Adapter documentation
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
boundary. W3C Baggage is not persisted as Carrier metadata by default; an
integration may propagate it only under an explicit allowlist and privacy/
retention policy.

## 12. Keepsake and Gatekeep migration

### 12.1 Release coordination

Migration support is prepared against Keepsake 1.1 and Gatekeep 1.0. Historical
migration files in both projects remain byte-for-byte immutable. Each project
adds new, forward-only application-consumable migrations; the shared Carrier
schema is created once even when both libraries are present.

Keepsake 2.0 and Gatekeep 2.0 remove copied outbox SQL, claim/export
implementations, and worker lifecycle APIs from their maintained surface. Their
historical migrations and retained legacy rows remain until application policy
removes them; neither library keeps permanent compatibility wrappers around the
legacy outboxes.
Ordinary audit/domain recording remains owned by the respective library; only
the generic delivery lifecycle moves to Carrier.

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

The source plus this ID is stable across retries and resumptions. Migration
maps:

| Legacy field | Carrier field |
|---|---|
| library-specific source config | `source` |
| prefixed legacy outbox row ID | `event_id` |
| application-configured distinct stream (`keepsake-audit` or `gatekeep-audit` by default) | `stream` |
| legacy `event_type` | `event_type` |
| legacy payload bytes | `data = EventData::Json` |
| explicit `application/json` | `datacontenttype` |
| legacy `created_at` | `occurred_at` when producer policy says it represents occurrence; otherwise omit |
| none | subject, schema URI, partition key, extensions |

Although legacy columns are JSON/JSONB/text depending on backend, migration
must preserve a defined byte representation. Before migration, the application
exporter serializes each legacy JSON value once to UTF-8 bytes using the
library's documented legacy export path and records its SHA-256 digest. The
same exact bytes are inserted into Carrier. Database-side casts that silently
reformat JSON are forbidden between digest and insert.

### 12.3 State mapping

Only legacy undelivered rows are copied. Their initial Carrier delivery state is
chosen as follows:

| Legacy row | Carrier state |
|---|---|
| never claimed | `pending`, available at migration database time |
| claim unexpired when workers were stopped | `pending`, available at migration database time |
| claim expired | `pending`, available at migration database time |
| delivered | not copied |

No legacy claim is trusted across cutover because it has no Carrier claim token.
Workers are stopped or drained before the state snapshot. Delivered legacy rows
remain in their existing tables under the application's existing retention
policy and are removed, if ever, only by a later application-owned migration.

### 12.4 Runbook and cutover

The published runbook is backend-specific and resumable:

1. inventory schema versions, row counts by legacy state, configured sources,
   streams, database time zone, and current workers;
2. back up the database and prove the documented restore check;
3. apply Carrier's schema once and run `check_schema`;
4. deploy code capable of reading Carrier but leave legacy producers/workers in
   place;
5. enter a declared maintenance window: stop or drain legacy workers, pause both
   legacy producer write paths, wait for their in-flight transactions to finish,
   and enforce the pause at the application or database boundary so no new
   legacy outbox row can commit;
6. while the pause is enforced, record each legacy table's maximum row ID and
   migrate every undelivered row through that inclusive high-water mark in
   bounded transactions, producing canonical export bytes and digests and
   enqueueing deterministic identities;
7. rerun the migration through the same high-water marks: identical identities
   must return `AlreadyEnqueued`, while any changed payload must stop the
   migration with `IdempotencyConflict`;
8. compare per-library and total source counts, row IDs, event types, byte
   lengths, and SHA-256 payload digests, then prove there are no legacy rows
   above either high-water mark before cutover;
9. switch both producer write paths to Carrier while the pause remains enforced,
   resume producers, and only then start Carrier workers;
10. confirm both libraries coexist in the shared tables under distinct streams;
11. monitor legacy writes, Carrier claims, lost claims, retries, quarantine,
    and duplicate consumer identities; and
12. remove migration-only code after its named verification and rollback window.

There is no interval in the default runbook where a legacy producer can commit
after the migration snapshot and before its write path changes. Failure to
enforce the pause aborts cutover.

Rollback before producer cutover restores the backup or removes only verified
migration-owned Carrier rows under the application's written procedure.
Rollback after Carrier publication begins cannot pretend downstream effects did
not occur; it stops publishers, reconciles by `source + id`, and follows the
application incident plan.

If an application genuinely requires zero-downtime bridging, it first deploys a
bounded producer version that atomically writes both the legacy row and the
Carrier event in the same caller transaction. Both rows carry the identical
producer-configured CloudEvents `source + id`, and the legacy publisher MUST
expose that identity unchanged to the downstream consumer. If the legacy path
cannot do so, this zero-downtime bridge is unsupported; use the paused cutover.

The bridge then records legacy high-water marks, migrates older undelivered
rows, and repeatedly reconciles every legacy identity through the moving
high-water marks. A dual-written row may be delivered by a legacy worker while
its Carrier delivery remains pending. Carrier will publish that event again
after cutover; this duplicate is expected and is safe only because the consumer
deduplicates the identical `source + id`. Cutover stops legacy workers, proves a
zero-row reconciliation delta, switches publication ownership to Carrier, and
disables the legacy write in one named release. The bridge has a named owner,
start and end releases, reconciliation metric, alert, rollback procedure, and
deletion condition. It is not part of Carrier's permanent API and MUST NOT
become indefinite dual-write compatibility.

## 13. Excluded responsibilities

Carrier version 1 does not own or provide:

- retention, deletion, archival, table partitioning, or vacuum policy;
- worker task spawning, process supervision, cancellation, or shutdown;
- Tokio or another async runtime in `carrier`;
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
- update handling fails the fixture if anything updates `carrier_events`.

### 14.6 Projection tests

Golden vectors are checked byte-for-byte on the MSRV and latest stable Rust.
An independent CloudEvents 1.0 validator or SDK parses structured output.
Binary projections preserve all context and exact stored data bytes. Structured
JSON tests require semantic equality for JSON data and byte-for-byte equality
with Carrier's deterministic projected output, not the producer's original JSON
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
every shared backend. Each contains never-claimed, currently claimed, expired,
and delivered legacy rows, plus non-ASCII and formatting-sensitive JSON.

Tests verify:

- historical migrations are unchanged;
- Carrier schema creation is shared and idempotently coordinated;
- only undelivered rows migrate;
- deterministic identities and configured sources remain stable on rerun;
- counts, byte lengths, and SHA-256 digests match before cutover;
- interrupted batches resume through `AlreadyEnqueued`;
- changed content stops with `IdempotencyConflict`;
- no unfenced legacy claim crosses cutover;
- the default maintenance window rejects or blocks a concurrent legacy producer
  write until the Carrier producer path is active;
- a zero-downtime bridge fixture inserts rows before and after successive
  high-water marks and proves every identity is present before legacy writes are
  disabled;
- that bridge fixture lets a legacy worker deliver a dual-written row before
  cutover, then proves Carrier's later duplicate carries the identical
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
