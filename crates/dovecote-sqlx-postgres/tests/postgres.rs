#[path = "postgres/support.rs"]
mod support;

#[path = "postgres/enqueue_paging.rs"]
mod enqueue_paging;

#[path = "postgres/high_cardinality.rs"]
mod high_cardinality;

#[path = "postgres/lifecycle.rs"]
mod lifecycle;

#[path = "postgres/mutation_outcomes.rs"]
mod mutation_outcomes;

#[path = "postgres/claim_concurrency.rs"]
mod claim_concurrency;

#[path = "postgres/transaction_failures.rs"]
mod transaction_failures;

#[path = "postgres/migration_workflows.rs"]
mod migration_workflows;

#[path = "postgres/migration_validation.rs"]
mod migration_validation;

#[path = "postgres/migration_races.rs"]
mod migration_races;

#[path = "postgres/tenancy.rs"]
mod tenancy;
