use super::support::*;

#[test]
fn environment_flags_use_explicit_truth_values() {
    for value in ["1", "true", "yes"] {
        assert!(is_truthy(value), "expected {value:?} to be truthy");
    }

    for value in ["0", "false", "no", "", "on", "TRUE", " YES "] {
        assert!(!is_truthy(value), "expected {value:?} to be false");
    }
}

#[tokio::test]
async fn matrix_backend_settings_and_exact_schema() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let info = adapter.backend_info().await?;
    assert!(matches!(
        info.kind,
        dovecote_sqlx_mysql::BackendKind::MySql | dovecote_sqlx_mysql::BackendKind::MariaDb
    ));
    assert!(info.capabilities.skip_locked);
    assert_eq!(
        info.transaction_isolation.to_ascii_uppercase(),
        "REPEATABLE-READ"
    );
    adapter.check_schema().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn schema_check_rejects_altered_marker_column_shape() -> Result<(), Box<dyn std::error::Error>>
{
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    query("ALTER TABLE dovecote_schema MODIFY minimum_crate_minor SMALLINT NULL")
        .execute(&pool)
        .await?;
    assert!(matches!(
        dovecote_sqlx_mysql::check_schema(&pool).await,
        Err(dovecote_sqlx_mysql::SchemaError::MigrationMismatch { .. })
    ));

    // Restore the disposable matrix database so the remaining live tests do
    // not inherit the deliberate corruption.
    query("ALTER TABLE dovecote_schema MODIFY minimum_crate_minor SMALLINT NOT NULL")
        .execute(&pool)
        .await?;
    dovecote_sqlx_mysql::check_schema(&pool).await?;
    pool.close().await;
    Ok(())
}
