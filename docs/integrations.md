# Integration mappings

Dovecote produces validated CloudEvents projections and owns durable delivery
state. An application integration owns destinations, authentication, transport
timeouts, response classification, retries, and the decision to publish by a
leased worker or by CDC. Choose one publication owner for the complete Dovecote
table set; the all-stream claim API does not support mixing modes per stream.
Do not run both paths at once. The exact wire rules are in [SPEC section 10](../SPEC.md#10-integration-mappings).

| Destination | Structured projection | Binary projection | Routing and duplicate identity |
| --- | --- | --- | --- |
| HTTP | `application/cloudevents+json` body | Exact data body; other context as `ce-<name>` headers | Application-owned URL and method; a tenant-isolated consumer uses `source + id`. |
| Kafka | JSON record value and `application/cloudevents+json` content-type header | Exact data value and `ce_<name>` headers | Application maps `stream` to a topic; optional `partitionkey` becomes the record key. |
| NATS JetStream | CloudEvents NATS structured body | Exact data body and `ce-<name>` headers, including `ce-datacontenttype` | Application maps `stream` to a subject; `Nats-Msg-Id` may carry a deterministic hash of the tenant routing domain plus `source + id`. |
| Azure Event Grid | CloudEvents 1.0 HTTP publishing envelope | Not a separate Event Grid route in this profile | Application maps `stream` to a configured topic or domain route. |
| Debezium Outbox Event Router | Downstream transform required | Downstream transform required to preserve exact bytes | Watches only `dovecote_events`; routes by `stream`; delivery mutations are not CDC events. |

Every binding keeps the CloudEvents identity pair. A transport key, broker
deduplication window, or database row ID is not a replacement for `source + id`
within one tenant. Absent data and present empty binary data remain different
Dovecote values. Tenant IDs are storage metadata, not an implicit CloudEvents
extension. A tenant-scoped worker receives the tenant from its claimed or paged
state and must route it according to application policy. If multiple tenants
share one destination, that policy must partition consumer deduplication by
tenant as well as `(source, id)`; Dovecote does not add tenant
context to the CloudEvents projection.

## HTTP

Structured HTTP sends the deterministic JSON projection with content type
`application/cloudevents+json`. Binary HTTP sends the exact event bytes and
maps context to `ce-<name>` headers; `datacontenttype` is the HTTP content type
and must not also be emitted as `ce-datacontenttype`. The integration owns the
request target, authentication, timeout, response policy, and size limit. It
acknowledges only after its policy accepts the response.

Header values use Dovecote's canonical CloudEvents string form followed by one
binding-specific percent-encoding pass. Do not store request headers as
unvalidated Dovecote extensions.

## Kafka

Structured mode uses the JSON envelope as a non-null record value. Binary mode
uses exact event bytes and `ce_<name>` headers. A binary event with absent data
maps to a null value; on a compacted topic that is a tombstone. Reject that
combination by default, or explicitly opt in only where deletion is intended.
Present empty binary data is a non-null zero-length value.

Map `stream` through an application-owned stream-to-topic table. If the
application opts into partition routing, use `partitionkey` as the UTF-8
record key while retaining it in the CloudEvent. Kafka key partitioning does
not make Dovecote FIFO.

## NATS JetStream

Map binary context attributes, including the content type, to `ce-<name>` NATS
headers. Structured mode follows the CloudEvents NATS binding. Route `stream`
through application configuration and retain `partitionkey` as an extension
even when it also informs subject or consumer routing.

For JetStream duplicate suppression, the application may set `Nats-Msg-Id` to
the lowercase hexadecimal SHA-256 of an unambiguous length-prefixed UTF-8
sequence containing the tenant routing domain, `source`, and `event_id` when
destinations are shared. A tenant-isolated destination may hash `source ||
event_id`. This is a transport aid with a finite duplicate window, not consumer
idempotency or a Dovecote guarantee.

## Azure Event Grid

Configure Event Grid for CloudEvents 1.0 and send Dovecote's structured JSON
through the documented HTTP publishing envelope. Preserve `id`, `source`,
`type`, `subject`, `time`, `datacontenttype`, `dataschema`, extensions, and
data. Map `stream` to an application-owned topic or domain route; do not add it
to the CloudEvent unless the application deliberately defines a valid
extension. Azure authentication, batching, service limits, and response
classification stay outside Dovecote.

## Debezium Outbox Event Router

The checked-in [reference properties](debezium/dovecote-outbox.properties) is a
configuration fixture, not a runnable Kafka Connect deployment. Supply
credentials, converter choices, topic prefixes, and any downstream
CloudEvents transform in the application deployment.

Configure the connector include list or SMT predicate to select only
`dovecote_events`. Enqueue creates one insert into that immutable table;
claims, renewals, acknowledgements, retries, releases, and quarantine update
only `dovecote_deliveries` and must not produce CDC events. The reference
router maps `event_id`, `event_type`, `stream`, `partitionkey`, `enqueued_at`,
and `data`; additional fields carry CloudEvents context and Dovecote's tagged
extensions, data kind, row ID, and source enqueue time.

The generic SMT does not turn the tagged extension object into arbitrary
CloudEvents headers. The checked-in reference fixture declares a
`dovecote-json-envelope-base64-v1` converter shape: CE context remains in
headers, while one value envelope contains `payload` plus only the four
`dovecote_*` fields. Its JSON converter represents `payload` as a base64 string;
this is a fixture assumption that must be replaced or confirmed by live
converter evidence. Test a downstream transform that removes null optional
attributes, maps `id`/`type`/`source`, decodes the tagged extensions, preserves
binary bytes, and emits the selected structured or binary binding. Kafka record
timestamps have millisecond precision; retain the envelope enqueue timestamp
when exact microseconds matter. CDC fixtures must prove the final transformed
event, not just the raw SMT envelope.

Until those fixtures pass for each advertised backend, CDC is an integration
option under application ownership, not a release claim. Dovecote does not run
Debezium, Kafka Connect, or a schema registry.

The checked-in projection vectors under
`crates/dovecote/tests/fixtures/projections.json` and their HTTP/Kafka/NATS
helpers are local reference mappings over Dovecote's transport-neutral public
projection. The exact structured JSON is additionally validated against the
official [CloudEvents v1.0.2 JSON Schema](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/cloudevents.json)
with the external `jsonschema` validator and parsed with the external
[`cloudevents-sdk`](https://docs.rs/cloudevents-sdk/0.9.0/) implementation. The
SDK probe normalizes media-type parameters only for its parser compatibility
gap; the official schema validates the parsed form of the exact projection and
all six data cases,
including UTF-8 text, arbitrary binary, present-empty, and absent data.
These checks validate the structured event format, not HTTP/Kafka/NATS broker
execution, live Debezium, converter behaviour, or transport delivery.
The vendored schema is the tagged upstream path above and is pinned by SHA-256
`e28a6d252d7b7238d176618f6bbf6cde570b26a867bc5241563aed34c9dd1d83`; the test
asserts that digest before validation.
