//! MySQL and MariaDB schema and SQLx boundary for Dovecote.
#![warn(missing_docs)]
//!
//! The adapter deliberately detects the server family and release before
//! using dialect-sensitive locking and catalog operations. MySQL success is
//! not treated as evidence for MariaDB, or the reverse.

mod backend;
mod delivery_state;
mod enqueue;
mod error;
mod finalize;
mod hydrate;
mod import;
mod lifecycle;
mod migration;
mod page;
mod schema;
mod scope;

pub use backend::detect as detect_backend;
pub use backend::{BackendInfo, BackendKind, Capabilities, ServerVersion};
pub use error::{
    ClaimError, EnqueueError, FinalizeError, ImportError, MutationError, PageError, SchemaError,
    TransientKind,
};
pub use migration::{
    CrateVersion, LEGACY_MIGRATION, MIGRATIONS, Migration, MigrationCompatibility,
    MigrationCompatibilityError, SCHEMA_VERSION, V1_TENANT_ACTIVATE_SQL, V1_TENANT_PREPARE_SQL,
};
pub use page::SnapshotPager;
pub use schema::check_schema;
pub use scope::{AdminDovecote, TenantDovecote};

use sqlx::MySqlPool;

/// MySQL/MariaDB adapter for Dovecote's durable event and delivery schema.
#[derive(Clone)]
pub struct MySqlDovecote {
    pool: MySqlPool,
}

impl MySqlDovecote {
    /// Creates an adapter using the supplied SQLx pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    /// Borrows the pool used by this adapter.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Detects and verifies the configured backend and schema.
    pub async fn check_schema(&self) -> Result<(), SchemaError> {
        check_schema(&self.pool).await
    }
    /// Detects the backend family, release and capabilities.
    pub async fn backend_info(&self) -> Result<BackendInfo, SchemaError> {
        backend::detect(&self.pool).await
    }
    /// Creates an ordinary handle restricted to one tenant.
    pub fn for_tenant(&self, tenant_id: dovecote::TenantId) -> TenantDovecote {
        TenantDovecote::new(self.pool.clone(), tenant_id)
    }
    /// Creates the explicit administrative handle for all-tenant reads and named writes.
    pub fn admin(&self) -> AdminDovecote {
        AdminDovecote::new(self.pool.clone())
    }
}
