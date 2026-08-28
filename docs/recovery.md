# Recovery and operational boundaries

Dovecote stores delivery state; it does not supervise a worker or decide what a
transport error means. An application-owned worker should claim no more than
its bounded in-flight capacity, use a lease longer than its transport timeout
and scheduling margin, and stop claiming before graceful shutdown.

The recovery loop is deliberately explicit:

1. claim a bounded batch;
2. send each event through an application-owned transport;
3. acknowledge only an accepted send while the claim is still valid;
4. retry a classified transient failure with a bounded backoff;
5. quarantine a classified permanent rejection;
6. stop mutating when Dovecote reports `LostClaim`; and
7. let an ambiguous send-before-ack crash expire and be reclaimed.

That last case can publish a duplicate. Consumers deduplicate with the
CloudEvents `source + id` pair within the tenant routing domain. Dovecote's
durable idempotency identity is `(tenant_id, source, event_id)`; a shared
destination must preserve tenant routing context when it deduplicates the
projected CloudEvents. A successful transport send is not durable delivery
until the acknowledgement commits, and an acknowledgement is not a transport
send.

Retention remains application policy. Pending and claimed rows are never
retention input. Before deleting terminal rows, the application proves CDC
progress, deduplication-window coverage, backup and legal-hold requirements,
then deletes delivery rows before their referenced event rows in bounded
transactions.
