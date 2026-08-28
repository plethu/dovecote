use dovecote::{MAX_TENANT_ID_BYTES, TenantId};
use dovecote_sqlx_postgres::{
    MIGRATIONS, PostgresDovecote, RLS_PROFILE_SQL, V1_TENANT_ACTIVATE_SQL, V1_TENANT_PREPARE_SQL,
    bind_tenant,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_scalar, raw_sql,
};
use std::{
    error::Error,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn tenant_migration_path_requires_explicit_backfill() {
    assert!(V1_TENANT_PREPARE_SQL.contains("ADD COLUMN tenant_id VARCHAR(255)"));
    assert!(V1_TENANT_ACTIVATE_SQL.contains("tenant_id IS NULL"));
    assert!(V1_TENANT_ACTIVATE_SQL.contains("Dovecote tenant backfill is incomplete"));
    assert!(V1_TENANT_ACTIVATE_SQL.starts_with("--"));
    assert!(V1_TENANT_ACTIVATE_SQL.contains("BEGIN;"));
    assert!(V1_TENANT_ACTIVATE_SQL.contains("COMMIT;"));
    assert!(!V1_TENANT_ACTIVATE_SQL.contains("COALESCE(tenant_id"));
}

#[test]
fn rls_profile_is_opt_in_and_transaction_local() {
    assert!(RLS_PROFILE_SQL.contains("FORCE ROW LEVEL SECURITY"));
    assert!(RLS_PROFILE_SQL.contains("current_setting('dovecote.tenant_id', true)"));
    assert!(RLS_PROFILE_SQL.contains("USING (tenant_id = current_setting"));
    assert!(RLS_PROFILE_SQL.contains("WITH CHECK (tenant_id = current_setting"));
    assert!(RLS_PROFILE_SQL.contains("Scoped"));
    assert!(RLS_PROFILE_SQL.contains("application transactions must call bind_tenant"));
    assert!(RLS_PROFILE_SQL.contains("administrative roles must"));
    assert!(RLS_PROFILE_SQL.contains("use BYPASSRLS"));
    assert!(RLS_PROFILE_SQL.contains("not part of the ordinary migration"));
}

#[test]
fn tenant_ids_are_exact_bounded_values() {
    let tenant = TenantId::new("School-A").expect("valid tenant");
    assert_eq!(tenant.as_str(), "School-A");
    assert_ne!(tenant, TenantId::new("school-a").expect("valid tenant"));
    assert!(TenantId::new("").is_err());
    assert!(TenantId::new("line\nbreak").is_err());
    assert!(TenantId::new("x".repeat(MAX_TENANT_ID_BYTES + 1)).is_err());
}

#[tokio::test]
async fn tenant_handles_isolate_reads_claims_and_mutations_when_configured()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database) = super::support::isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = super::support::PostgresDovecote::new(database.pool.clone());
        let alpha = adapter.for_tenant(TenantId::new("alpha")?);
        let beta = adapter.for_tenant(TenantId::new("beta")?);

        let alpha_row = {
            let mut transaction = database.pool.begin().await?;
            let outcome = alpha
                .enqueue(
                    &mut transaction,
                    super::support::event("tenant-alpha", "com.example.tenant"),
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
                other => return Err(format!("expected alpha insertion, got {other:?}").into()),
            }
        };
        {
            let mut transaction = database.pool.begin().await?;
            let outcome = beta
                .enqueue(
                    &mut transaction,
                    super::support::event("tenant-beta", "com.example.tenant"),
                )
                .await?;
            transaction.commit().await?;
            assert!(matches!(outcome, dovecote::EnqueueOutcome::Enqueued { .. }));
        }

        let alpha_page = alpha.page(None, dovecote::Limit::new(10)?).await?;
        let beta_page = beta.page(None, dovecote::Limit::new(10)?).await?;
        assert_eq!(alpha_page.len(), 1);
        assert_eq!(beta_page.len(), 1);
        assert_eq!(alpha_page[0].tenant_id(), alpha.tenant_id());
        assert_eq!(beta_page[0].tenant_id(), beta.tenant_id());

        let alpha_claim = alpha
            .claim(
                dovecote::WorkerId::new("tenant-alpha-worker")?,
                dovecote::Lease::new(std::time::Duration::from_secs(5))?,
                dovecote::Limit::new(1)?,
            )
            .await?
            .pop()
            .ok_or("alpha claim was missing")?;
        let beta_claim = beta
            .claim(
                dovecote::WorkerId::new("tenant-beta-worker")?,
                dovecote::Lease::new(std::time::Duration::from_secs(5))?,
                dovecote::Limit::new(1)?,
            )
            .await?
            .pop()
            .ok_or("beta claim was missing")?;
        assert_eq!(beta_claim.tenant_id(), beta.tenant_id());
        assert!(matches!(
            beta.ack(alpha_row, alpha_claim.claim_token()).await,
            Err(dovecote_sqlx_postgres::MutationError::NotFound)
        ));
        alpha.ack(alpha_row, alpha_claim.claim_token()).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn postgres_rls_live_role_boundary_is_enforced_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(url) = std::env::var_os("DOVECOTE_POSTGRES_RLS_URL") else {
        if super::support::env_flag("DOVECOTE_POSTGRES_RLS_REQUIRED") {
            return Err(
                "DOVECOTE_POSTGRES_RLS_URL is required for the PostgreSQL RLS proof".into(),
            );
        }
        eprintln!(
            "skipping PostgreSQL RLS live proof: DOVECOTE_POSTGRES_RLS_URL is unset; configure a disposable superuser URL"
        );
        return Ok(());
    };
    let url = url.to_str().ok_or("PostgreSQL RLS URL is not UTF-8")?;
    let admin = PgPoolOptions::new().max_connections(3).connect(url).await?;
    let is_superuser: bool =
        query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&admin)
            .await?;
    if !is_superuser {
        admin.close().await;
        if super::support::env_flag("DOVECOTE_POSTGRES_RLS_REQUIRED") {
            return Err("DOVECOTE_POSTGRES_RLS_URL must authenticate as a PostgreSQL superuser to create and grant the proof roles".into());
        }
        eprintln!("skipping PostgreSQL RLS live proof: configured URL is not a superuser URL");
        return Ok(());
    }

    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let schema = format!("dovecote_rls_{}_{}", std::process::id(), suffix);
    let role = format!("dovecote_rls_app_{}_{}", std::process::id(), suffix);
    let password = format!("dovecote_rls_pw_{}_{}", std::process::id(), suffix);
    query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&admin)
        .await?;
    let setup = async {
        query(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'"
        )))
        .execute(&admin)
        .await?;

        let schema_options = PgConnectOptions::from_str(url)?.options([
            ("search_path", format!("\"{schema}\"")),
            ("application_name", format!("dovecote-rls-admin-{schema}")),
        ]);
        let schema_admin = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(schema_options)
            .await?;
        raw_sql(MIGRATIONS[0].sql()).execute(&schema_admin).await?;
        dovecote_sqlx_postgres::check_schema(&schema_admin).await?;
        raw_sql(RLS_PROFILE_SQL).execute(&schema_admin).await?;
        query(sqlx::AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA \"{schema}\" TO \"{role}\""
        )))
        .execute(&admin)
        .await?;
        for table in ["dovecote_events", "dovecote_deliveries"] {
            query(sqlx::AssertSqlSafe(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE \"{schema}\".{table} TO \"{role}\""
            )))
            .execute(&admin)
            .await?;
        }
        query(sqlx::AssertSqlSafe(format!(
            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA \"{schema}\" TO \"{role}\""
        )))
        .execute(&admin)
        .await?;

        let app_options = PgConnectOptions::from_str(url)?
            .username(&role)
            .password(&password)
            .options([
                ("search_path", format!("\"{schema}\"")),
                ("application_name", format!("dovecote-rls-app-{schema}")),
            ]);
        let app = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(app_options)
            .await?;
        Ok::<_, Box<dyn Error>>((schema_admin, app))
    }
    .await;

    let (schema_admin, app) = match setup {
        Ok(pools) => pools,
        Err(error) => {
            let cleanup = cleanup_rls_setup(&admin, &schema, &role).await;
            admin.close().await;
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(format!("{error}; RLS setup cleanup diagnostics: {cleanup}").into())
                }
            };
        }
    };

    let result = async {
        let adapter = PostgresDovecote::new(schema_admin.clone());
        let admin_handle = adapter.admin();
        for (tenant, event_id) in [("alpha", "rls-alpha"), ("beta", "rls-beta")] {
            let mut transaction = schema_admin.begin().await?;
            admin_handle
                .enqueue(
                    &mut transaction,
                    TenantId::new(tenant)?,
                    super::support::event(event_id, "com.example.rls"),
                )
                .await?;
            transaction.commit().await?;
        }

        let unset_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&app)
            .await?;
        assert_eq!(unset_count, 0, "unset tenant setting must see no rows");
        let unset_insert = query(
            "INSERT INTO dovecote_events (tenant_id, stream, specversion, event_id, source, event_type, extensions) VALUES ('alpha', 'audit', '1.0', 'rls-unset', 'https://dovecote.test/rls', 'com.example.rls', '{}')",
        )
        .execute(&app)
        .await;
        assert_eq!(database_code(&unset_insert).as_deref(), Some("42501"));

        let alpha = TenantId::new("alpha")?;
        let mut scoped = app.begin().await?;
        bind_tenant(&mut scoped, &alpha).await?;
        let own_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&mut *scoped)
            .await?;
        assert_eq!(own_count, 1, "alpha setting must see only alpha rows");
        let cross_read: i64 = query_scalar("SELECT count(*) FROM dovecote_events WHERE tenant_id = 'beta'")
            .fetch_one(&mut *scoped)
            .await?;
        assert_eq!(cross_read, 0, "alpha setting must not read beta rows");
        let cross_update = query(
            "UPDATE dovecote_events SET event_type = 'com.example.cross-tenant' WHERE tenant_id = 'beta'",
        )
        .execute(&mut *scoped)
        .await?;
        assert_eq!(cross_update.rows_affected(), 0);
        let bound_insert = query(
            "INSERT INTO dovecote_events (tenant_id, stream, specversion, event_id, source, event_type, extensions) VALUES ('beta', 'audit', '1.0', 'rls-bound', 'https://dovecote.test/rls', 'com.example.rls', '{}')",
        )
        .execute(&mut *scoped)
        .await;
        assert_eq!(database_code(&bound_insert).as_deref(), Some("42501"));
        scoped.commit().await?;

        let reset_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&app)
            .await?;
        assert_eq!(reset_count, 0, "transaction-local tenant setting must reset");
        let admin_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&schema_admin)
            .await?;
        assert_eq!(admin_count, 2, "BYPASSRLS admin must see both tenants");
        let beta_type: String = query_scalar(
            "SELECT event_type FROM dovecote_events WHERE tenant_id = 'beta'",
        )
        .fetch_one(&schema_admin)
        .await?;
        assert_eq!(beta_type, "com.example.rls");
        Ok::<_, Box<dyn Error>>(())
    }
    .await;

    app.close().await;
    schema_admin.close().await;
    let cleanup = cleanup_rls_setup(&admin, &schema, &role).await;
    admin.close().await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(format!("RLS cleanup failed: {cleanup}").into()),
        (Err(error), Err(cleanup)) => Err(format!("{error}; RLS cleanup failed: {cleanup}").into()),
    }
}

async fn cleanup_rls_setup(admin: &sqlx::PgPool, schema: &str, role: &str) -> Result<(), String> {
    // These are independent best-effort operations. Always attempt both, and
    // return all cleanup failures without replacing the setup error.
    let cleanup_schema = query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"
    )))
    .execute(admin)
    .await;
    let cleanup_role = query(sqlx::AssertSqlSafe(format!(
        "DROP ROLE IF EXISTS \"{role}\""
    )))
    .execute(admin)
    .await;

    let mut diagnostics = Vec::new();
    if let Err(error) = cleanup_schema {
        diagnostics.push(format!("drop schema: {error}"));
    }
    if let Err(error) = cleanup_role {
        diagnostics.push(format!("drop role: {error}"));
    }
    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(diagnostics.join("; "))
    }
}

fn database_code<T>(result: &Result<T, sqlx::Error>) -> Option<String> {
    result
        .as_ref()
        .err()
        .and_then(|error| error.as_database_error())
        .and_then(|error| error.code().map(Into::into))
}
