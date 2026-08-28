//! Optional PostgreSQL row-level-security profile for tenant isolation.

use dovecote::TenantId;
use sqlx::{Postgres, Transaction, query};

/// Installs the opt-in RLS policies for a tenant-aware schema.
///
/// RLS is deliberately separate from the ordinary migration. Applications
/// that enable it must use a role with `BYPASSRLS` for [`crate::AdminDovecote`]
/// and call [`bind_tenant`] at the start of every scoped transaction.
pub const RLS_PROFILE_SQL: &str = include_str!("../migrations/0002_dovecote_tenant_rls.sql");

/// Binds a validated tenant to the current transaction for the RLS profile.
///
/// The setting is transaction-local and cannot outlive the supplied SQLx
/// transaction. It does not replace the adapter's tenant predicates.
pub async fn bind_tenant<'c>(
    transaction: &mut Transaction<'c, Postgres>,
    tenant_id: &TenantId,
) -> Result<(), sqlx::Error> {
    query("SELECT set_config('dovecote.tenant_id', $1, true)")
        .bind(tenant_id.as_str())
        .execute(&mut **transaction)
        .await
        .map(|_| ())
}
