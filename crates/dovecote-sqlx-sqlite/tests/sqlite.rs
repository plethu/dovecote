//! SQLite integration coverage for enqueue, paging, lifecycle, recovery, and concurrency.

#[path = "sqlite/busy_concurrency.rs"]
mod busy_concurrency;
#[path = "sqlite/high_cardinality.rs"]
mod high_cardinality;
#[path = "sqlite/lifecycle.rs"]
mod lifecycle;
#[path = "sqlite/round_trip.rs"]
mod round_trip;
#[path = "sqlite/snapshot.rs"]
mod snapshot;
mod support;
#[path = "sqlite/tenant_isolation.rs"]
mod tenant_isolation;
#[path = "sqlite/support.rs"]
mod test_support;
#[path = "sqlite/transactions_schema.rs"]
mod transactions_schema;
