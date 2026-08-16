# Keepsake and Gatekeep migration boundary

Carrier's migration support is prepared for Keepsake 1.1 and Gatekeep 1.0.
Their historical migrations remain byte-for-byte immutable. Each application
will add forward-only migrations that create the shared Carrier schema once in
its own database boundary.

The default paused cutover uses stable application-configured sources and
deterministic IDs:

```text
keepsake-outbox-<legacy decimal row id>
gatekeep-outbox-<legacy decimal row id>
```

Only undelivered legacy rows move. Existing claims are not trusted because
they have no Carrier token; migrated rows start as pending and available at
the migration database time. Delivered legacy rows stay under their existing
retention policy.

Before the write path changes, the application stops or drains workers, pauses
legacy producer writes, records inclusive high-water marks, exports exact JSON
bytes and digests, migrates in bounded transactions, reruns the same range,
and proves no rows exist above either high-water mark. `AlreadyEnqueued` makes
the rerun resumable; `IdempotencyConflict` stops it when content changed.

The application switches producers to Carrier while the pause remains
enforced, then resumes producers and starts Carrier workers. A zero-downtime
bridge is an application-owned, named, temporary migration mechanism rather
than a permanent Carrier API.
