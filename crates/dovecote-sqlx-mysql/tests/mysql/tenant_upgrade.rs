use super::support::*;
use dovecote_sqlx_mysql::{V1_TENANT_ACTIVATE_SQL, V1_TENANT_PREPARE_SQL, check_schema};
use sqlx::{query, query_as, query_scalar, raw_sql};

const V1_SQL: &str = include_str!("../../migrations/0001_dovecote.sql");

#[tokio::test]
#[ignore = "requires a dedicated disposable database via DOVECOTE_MYSQL_TENANT_UPGRADE_URL"]
async fn tenant_activation_preflights_then_retries_to_the_exact_v2_schema()
-> Result<(), Box<dyn Error>> {
    let url = std::env::var("DOVECOTE_MYSQL_TENANT_UPGRADE_URL")
        .map_err(|_| "DOVECOTE_MYSQL_TENANT_UPGRADE_URL is required for tenant upgrade")?;
    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;

    reset_to_prepared_v1(&pool).await?;
    query("ALTER TABLE dovecote_events MODIFY tenant_id VARBINARY(1) NOT NULL")
        .execute(&pool)
        .await?;
    assert!(
        raw_sql(V1_TENANT_ACTIVATE_SQL)
            .execute(&pool)
            .await
            .is_err()
    );
    assert_pre_activation_catalog(&pool, 1, "NO").await?;

    reset_to_prepared_v1(&pool).await?;
    query(
        "ALTER TABLE dovecote_events ADD CONSTRAINT dovecote_events_tenant_nonempty CHECK (1 = 1)",
    )
    .execute(&pool)
    .await?;
    assert!(
        raw_sql(V1_TENANT_ACTIVATE_SQL)
            .execute(&pool)
            .await
            .is_err()
    );
    assert_pre_activation_catalog(&pool, 255, "YES").await?;

    reset_to_prepared_v1(&pool).await?;
    let server_version: String = query_scalar("SELECT VERSION()").fetch_one(&pool).await?;
    let drop_state_check = if server_version.to_ascii_lowercase().contains("mariadb") {
        "ALTER TABLE dovecote_deliveries DROP CONSTRAINT dovecote_deliveries_state"
    } else {
        "ALTER TABLE dovecote_deliveries DROP CHECK dovecote_deliveries_state"
    };
    query(drop_state_check).execute(&pool).await?;
    assert!(
        raw_sql(V1_TENANT_ACTIVATE_SQL)
            .execute(&pool)
            .await
            .is_err()
    );
    assert_pre_activation_catalog(&pool, 255, "YES").await?;

    reset_to_prepared_v1(&pool).await?;
    query("CREATE TABLE dovecote_schema (schema_version INT PRIMARY KEY) ENGINE = InnoDB")
        .execute(&pool)
        .await?;
    query("INSERT INTO dovecote_schema VALUES (2)")
        .execute(&pool)
        .await?;
    assert!(
        raw_sql(V1_TENANT_ACTIVATE_SQL)
            .execute(&pool)
            .await
            .is_err()
    );
    let nullable: String = query_scalar(
        "SELECT IS_NULLABLE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND column_name = 'tenant_id'",
    ).fetch_one(&pool).await?;
    assert_eq!(nullable, "YES");

    reset_to_prepared_v1(&pool).await?;
    query("INSERT INTO dovecote_events (stream, specversion, event_id, source, event_type, extensions, tenant_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(b"upgrade".as_slice()).bind(b"1.0".as_slice())
        .bind(b"legacy".as_slice()).bind(b"https://example.test/upgrade".as_slice())
        .bind(b"com.example.upgrade".as_slice()).bind("{}")
        .bind(b"tenant-a".as_slice()).execute(&pool).await?;
    let row_id: i64 = query_scalar("SELECT row_id FROM dovecote_events WHERE event_id = ?")
        .bind(b"legacy".as_slice())
        .fetch_one(&pool)
        .await?;
    query("INSERT INTO dovecote_deliveries (event_row_id, state, tenant_id) VALUES (?, ?, ?)")
        .bind(row_id)
        .bind(b"pending".as_slice())
        .bind(b"tenant-b".as_slice())
        .execute(&pool)
        .await?;

    assert!(
        raw_sql(V1_TENANT_ACTIVATE_SQL)
            .execute(&pool)
            .await
            .is_err()
    );
    assert_pre_activation_catalog(&pool, 255, "YES").await?;
    query("UPDATE dovecote_deliveries SET tenant_id = ? WHERE event_row_id = ?")
        .bind(b"tenant-a".as_slice())
        .bind(row_id)
        .execute(&pool)
        .await?;

    run_activation(&pool, "first valid activation").await?;
    let marker_rows: i64 = query_scalar("SELECT COUNT(*) FROM dovecote_schema")
        .fetch_one(&pool)
        .await?;
    assert_eq!(marker_rows, 1);
    query("DELETE FROM dovecote_schema").execute(&pool).await?;
    let marker_rows: i64 = query_scalar("SELECT COUNT(*) FROM dovecote_schema")
        .fetch_one(&pool)
        .await?;
    assert_eq!(marker_rows, 0);
    run_activation(&pool, "activation after empty marker interruption").await?;
    check_schema(&pool).await?;
    let marker_version: i64 = query_scalar("SELECT schema_version FROM dovecote_schema")
        .fetch_one(&pool)
        .await?;
    assert_eq!(marker_version, 2);
    run_activation(&pool, "completed activation rerun").await?;
    check_schema(&pool).await?;

    let (extra, generation_expression, key_length): (String, String, i64) = query_as(
        "SELECT EXTRA, GENERATION_EXPRESSION, CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND column_name = 'identity_key'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(extra, "STORED GENERATED");
    assert_eq!(key_length, 2310);
    let normalized_generation = generation_expression
        .to_ascii_lowercase()
        .replace(['\\', '`', ' '], "")
        .replace("_binary", "")
        .replace("_utf8mb4", "");
    assert!(matches!(
        normalized_generation.as_str(),
        "concat(lpad(octet_length(tenant_id),3,'0'),tenant_id,lpad(octet_length(source),4,'0'),source,event_id)"
            | "concat(lpad(length(tenant_id),3,'0'),tenant_id,lpad(length(source),4,'0'),source,event_id)"
    ));

    let stored_identity: Vec<u8> = query_scalar(
        "SELECT identity_key FROM dovecote_events WHERE tenant_id = ? AND source = ? AND event_id = ?",
    )
    .bind(b"tenant-a".as_slice())
    .bind(b"https://example.test/upgrade".as_slice())
    .bind(b"legacy".as_slice())
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        stored_identity,
        b"008tenant-a0028https://example.test/upgradelegacy"
    );

    let adapter = MySqlDovecote::new(pool.clone());
    let shared = NewEvent::new(
        StreamName::new("upgrade")?,
        EventId::new("shared")?,
        EventSource::new("https://example.test/shared")?,
        EventType::new("com.example.shared")?,
    )?;
    for name in ["tenant-a", "tenant-b"] {
        let mut transaction = pool.begin().await?;
        let outcome = adapter
            .for_tenant(TenantId::new(name)?)
            .enqueue(&mut transaction, shared.clone())
            .await?;
        assert!(matches!(outcome, EnqueueOutcome::Enqueued { .. }));
        transaction.commit().await?;
    }

    pool.close().await;
    Ok(())
}

async fn reset_to_prepared_v1(pool: &MySqlPool) -> Result<(), Box<dyn Error>> {
    raw_sql(
        "DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_insert;\
         DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_update;\
         DROP TABLE IF EXISTS dovecote_deliveries;\
         DROP TABLE IF EXISTS dovecote_events;\
         DROP TABLE IF EXISTS dovecote_schema;",
    )
    .execute(pool)
    .await?;
    raw_sql(V1_SQL).execute(pool).await?;
    raw_sql(V1_TENANT_PREPARE_SQL).execute(pool).await?;
    Ok(())
}

async fn activation_diagnostics(
    connection: &mut sqlx::MySqlConnection,
) -> Result<String, sqlx::Error> {
    query_scalar(
        "SELECT CONCAT_WS(',', CONCAT('tables=', COALESCE(CAST(@dovecote_tables_valid AS CHAR), 'NULL')), CONCAT('events_columns=', COALESCE(CAST(@dovecote_events_columns_valid AS CHAR), 'NULL')), CONCAT('deliveries_columns=', COALESCE(CAST(@dovecote_deliveries_columns_valid AS CHAR), 'NULL')), CONCAT('identity_column=', COALESCE(CAST(@dovecote_identity_column_valid AS CHAR), 'NULL')), CONCAT('identity_ready=', COALESCE(CAST(@dovecote_identity_ready AS CHAR), 'NULL')), CONCAT('required_checks=', COALESCE(CAST(@dovecote_required_checks_valid AS CHAR), 'NULL')), CONCAT('unexpected_checks=', COALESCE(CAST(@dovecote_unexpected_checks_valid AS CHAR), 'NULL')), CONCAT('check_shapes=', COALESCE(CAST(@dovecote_check_shapes_valid AS CHAR), 'NULL')), CONCAT('checks=', COALESCE(CAST(@dovecote_checks_valid AS CHAR), 'NULL')), CONCAT('triggers=', COALESCE(CAST(@dovecote_triggers_valid AS CHAR), 'NULL')), CONCAT('events_pk=', COALESCE(CAST(@dovecote_events_pk_valid AS CHAR), 'NULL')), CONCAT('deliveries_pk=', COALESCE(CAST(@dovecote_deliveries_pk_valid AS CHAR), 'NULL')), CONCAT('old_identity=', COALESCE(CAST(@dovecote_old_identity_valid AS CHAR), 'NULL')), CONCAT('target_identity=', COALESCE(CAST(@dovecote_target_identity_valid AS CHAR), 'NULL')), CONCAT('tenant_row=', COALESCE(CAST(@dovecote_tenant_row_valid AS CHAR), 'NULL')), CONCAT('fk_old=', COALESCE(CAST(@dovecote_fk_old_valid AS CHAR), 'NULL')), CONCAT('fk_target=', COALESCE(CAST(@dovecote_fk_target_valid AS CHAR), 'NULL')), CONCAT('fk_index=', COALESCE(CAST(@dovecote_fk_index_valid AS CHAR), 'NULL')), CONCAT('claimable=', COALESCE(CAST(@dovecote_claimable_shape_valid AS CHAR), 'NULL')), CONCAT('expired=', COALESCE(CAST(@dovecote_expired_shape_valid AS CHAR), 'NULL')), CONCAT('fk=', COALESCE(CAST(@dovecote_fk_valid AS CHAR), 'NULL')), CONCAT('indexes=', COALESCE(CAST(@dovecote_indexes_valid AS CHAR), 'NULL')), CONCAT('marker_present=', COALESCE(CAST(@dovecote_marker_present AS CHAR), 'NULL')), CONCAT('marker_statistics=', COALESCE(CAST(@dovecote_marker_statistics_valid AS CHAR), 'NULL')), CONCAT('marker_checks=', COALESCE(CAST(@dovecote_marker_checks_valid AS CHAR), 'NULL')), CONCAT('marker_catalog=', COALESCE(CAST(@dovecote_marker_catalog_valid AS CHAR), 'NULL')), CONCAT('marker_data=', COALESCE(CAST(@dovecote_marker_data_valid AS CHAR), 'NULL')), CONCAT('marker_state=', COALESCE(CAST(@dovecote_marker_state_valid AS CHAR), 'NULL')), CONCAT('events_tenant=', COALESCE(CAST(@dovecote_events_tenant_data_valid AS CHAR), 'NULL')), CONCAT('deliveries_tenant=', COALESCE(CAST(@dovecote_deliveries_tenant_data_valid AS CHAR), 'NULL')), CONCAT('delivery_event=', COALESCE(CAST(@dovecote_delivery_event_data_valid AS CHAR), 'NULL')), CONCAT('identity=', COALESCE(CAST(@dovecote_identity_data_valid AS CHAR), 'NULL')), CONCAT('catalog=', COALESCE(CAST(@dovecote_catalog_valid AS CHAR), 'NULL')), CONCAT('backfill=', COALESCE(CAST(@dovecote_backfill_valid AS CHAR), 'NULL')), CONCAT('preflight=', COALESCE(CAST(@dovecote_preflight_valid AS CHAR), 'NULL')))",
    )
    .fetch_one(&mut *connection)
    .await
}

async fn run_activation(pool: &MySqlPool, label: &str) -> Result<(), Box<dyn Error>> {
    let mut connection = pool.acquire().await?;
    if let Err(error) = raw_sql(V1_TENANT_ACTIVATE_SQL)
        .execute(&mut *connection)
        .await
    {
        let diagnostics = activation_diagnostics(&mut connection).await?;
        return Err(std::io::Error::other(format!(
            "{label} failed: {error}; preflight diagnostics: {diagnostics}"
        ))
        .into());
    }
    Ok(())
}

async fn assert_pre_activation_catalog(
    pool: &MySqlPool,
    event_tenant_length: i64,
    event_tenant_nullable: &str,
) -> Result<(), Box<dyn Error>> {
    let event_tenant: (i64, String) = query_as(
        "SELECT CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED), IS_NULLABLE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND column_name = 'tenant_id'",
    ).fetch_one(pool).await?;
    assert_eq!(event_tenant.0, event_tenant_length);
    assert_eq!(event_tenant.1, event_tenant_nullable);
    let delivery_tenant: (i64, String) = query_as(
        "SELECT CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED), IS_NULLABLE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'dovecote_deliveries' AND column_name = 'tenant_id'",
    ).fetch_one(pool).await?;
    assert_eq!(delivery_tenant, (255, "YES".to_owned()));
    let old_identity: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND index_name = 'dovecote_events_source_event_id'",
    ).fetch_one(pool).await?;
    let new_identity: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND index_name = 'dovecote_events_tenant_source_event_id'",
    ).fetch_one(pool).await?;
    assert_eq!(old_identity, 2);
    assert_eq!(new_identity, 0);
    let target_tenant_row: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'dovecote_events' AND index_name = 'dovecote_events_tenant_row_unique'",
    ).fetch_one(pool).await?;
    assert_eq!(target_tenant_row, 0);
    let old_fk: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_deliveries' AND constraint_name = 'dovecote_deliveries_event_fk'",
    ).fetch_one(pool).await?;
    assert_eq!(old_fk, 1);
    let target_fk: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE constraint_schema = DATABASE() AND table_name = 'dovecote_deliveries' AND constraint_name = 'dovecote_deliveries_event_fk' AND column_name = 'tenant_id'",
    ).fetch_one(pool).await?;
    assert_eq!(target_fk, 0);
    let marker: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'dovecote_schema'",
    ).fetch_one(pool).await?;
    assert_eq!(marker, 0);
    Ok(())
}
