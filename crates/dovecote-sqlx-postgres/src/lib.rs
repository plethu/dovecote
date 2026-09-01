//! PostgreSQL schema and SQLx boundary for Dovecote.
//!
//! This crate publishes versioned PostgreSQL migration artifacts and implements
//! the caller-transaction-bound enqueue, schema verification, leased lifecycle
//! operations, and live and finite snapshot paging for Dovecote. The locking,
//! database-time, fencing, and rollback contracts are covered by repository
//! tests; release advertisement remains subject to the published support matrix
//! and release gates.
#![warn(missing_docs)]

mod delivery_state;
mod enqueue;
mod error;
mod finalize;
mod hydrate;
mod import;
mod lifecycle;
mod lifecycle_mutation;
mod migration;
mod page;
mod rls;
mod schema;
mod scope;

pub use error::{
    ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError, SchemaError,
    TransientKind,
};
#[allow(deprecated)]
pub use migration::{
    CrateVersion, LEGACY_MIGRATION, MIGRATIONS, Migration, MigrationCompatibility,
    MigrationCompatibilityError, SCHEMA_VERSION, V1_TENANT_ACTIVATE_SQL, V1_TENANT_ACTIVATE_V2_SQL,
    V1_TENANT_PREPARE_SQL,
};
pub use page::SnapshotPager;
pub use rls::{RLS_PROFILE_SQL, bind_tenant};
pub use schema::check_schema;

use sqlx::PgPool;

pub use scope::{AdminDovecote, TenantDovecote};

/// PostgreSQL adapter for Dovecote's durable event and delivery schema.
#[derive(Clone)]
pub struct PostgresDovecote {
    pool: PgPool,
}

impl PostgresDovecote {
    /// Creates an adapter using the supplied SQLx pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrows the pool used by this adapter.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Creates a handle whose ordinary operations are restricted to `tenant`.
    pub fn for_tenant(&self, tenant: dovecote::TenantId) -> TenantDovecote {
        TenantDovecote::new(self.pool.clone(), tenant)
    }

    /// Creates an explicit all-tenant administrative handle.
    ///
    /// This handle does not provide authorization. Applications must construct
    /// it only around a separately authorized worker or operator pool.
    pub fn admin(&self) -> AdminDovecote {
        AdminDovecote::new(self.pool.clone())
    }

    /// Verifies that the pool's current PostgreSQL schema satisfies Dovecote
    /// migration version 2.
    pub async fn check_schema(&self) -> Result<(), SchemaError> {
        check_schema(&self.pool).await
    }
}
