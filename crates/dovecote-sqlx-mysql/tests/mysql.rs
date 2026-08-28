//! Live MySQL/MariaDB conformance gates.
//!
//! The harness intentionally never drops tables: point DOVECOTE_MYSQL_URL at
//! a disposable database (the matrix creates one per server run). In
//! required mode an omitted URL is a failure rather than an accidental skip.
//!
//! Concurrent same-identity importer races are covered in the migration
//! concern with two independent transactions and a barrier. The assertion is
//! deliberately about the portable uniqueness/idempotency contract rather
//! than release-specific lock timing.

#[path = "mysql/backend_schema.rs"]
mod backend_schema;
#[path = "mysql/concurrency.rs"]
mod concurrency;
#[path = "mysql/enqueue_paging.rs"]
mod enqueue_paging;
#[path = "mysql/lifecycle.rs"]
mod lifecycle;
#[path = "mysql/migration.rs"]
mod migration;
#[path = "mysql/support.rs"]
mod support;
#[path = "mysql/tenant_isolation.rs"]
mod tenant_isolation;
#[path = "mysql/tenant_upgrade.rs"]
mod tenant_upgrade;
