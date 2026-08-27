# Debezium reference-transform fixtures

The checked-in [`cdc.json`](../../crates/dovecote/tests/fixtures/cdc.json)
fixture is a deterministic, runtime-free conformance model for the mapping in
SPEC section 10.5. It is deliberately not a Kafka Connect or Debezium runner.

The model declares the converter shape `dovecote-json-envelope-base64-v1`:
record headers carry the router's `id` and `type`, the CloudEvents context, and
`content-type`, while one converter-owned value envelope contains `payload`
plus only `dovecote_extensions`, `dovecote_data_kind`, `dovecote_row_id`, and
`dovecote_enqueued_at`. Under this fixture's JSON-converter assumption,
`payload` is a base64 string so binary bytes remain exact. This is an explicit
fixture assumption, not a claim about the default output of every Debezium
converter.

Within that declared shape, the model checks a selected `dovecote_events`
insert:

- only the watched event table emits a record; a `dovecote_deliveries` insert is
  ignored and an update to `dovecote_events` fails;
- the route, optional Kafka key, exact payload bytes, and additional fields
  agree with the checked-in Outbox Event Router properties;
- nullable optional attributes remain null in raw SMT-shaped output and are
  omitted by the downstream CloudEvents transform;
- JSON payload bytes remain exact in the raw record while structured projection
  uses Dovecote's deterministic JSON spelling; and
- a microsecond enqueue timestamp is truncated to milliseconds for the record
  timestamp while the exact source timestamp remains in the envelope.

The tests prove this declared reference mapping and Dovecote's own projection
code only.
They do not prove Debezium connector execution, Kafka Connect converter
behaviour, schema-registry behaviour, database-log capture, or transport
delivery. Each advertised CDC backend still needs a live connector/converter
fixture before it can be advertised in the support matrix.
