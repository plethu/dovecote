use super::test_support::*;

#[tokio::test]
async fn caller_rollback_removes_both_event_and_delivery_rows() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("rolled-back"))
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let delivery_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 0);
    assert_eq!(delivery_count, 0);
}

#[tokio::test]
async fn v1_prepare_backfill_activate_produces_a_verified_v2_schema() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    raw_sql(LEGACY_MIGRATION.sql())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO dovecote_events (stream, specversion, event_id, source, event_type) VALUES ('audit', '1.0', 'event-1', 'https://example.test/source', 'com.example.test')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO dovecote_deliveries (event_row_id, state) VALUES (1, 'pending')")
        .execute(&pool)
        .await
        .unwrap();

    raw_sql(V1_TENANT_PREPARE_SQL).execute(&pool).await.unwrap();
    sqlx::query("UPDATE dovecote_events SET tenant_id = 'tenant-a'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE dovecote_deliveries SET tenant_id = 'tenant-a'")
        .execute(&pool)
        .await
        .unwrap();
    raw_sql(V1_TENANT_ACTIVATE_SQL)
        .execute(&pool)
        .await
        .unwrap();

    check_schema(&pool).await.unwrap();
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM dovecote_events), (SELECT COUNT(*) FROM dovecote_deliveries), (SELECT COUNT(*) FROM dovecote_schema)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1, 1));
    pool.close().await;
}

#[tokio::test]
async fn v1_activation_rejects_orphan_delivery_rows_before_rebuild() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    raw_sql(LEGACY_MIGRATION.sql())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dovecote_deliveries (event_row_id, state) VALUES (999, 'pending')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    raw_sql(V1_TENANT_PREPARE_SQL).execute(&pool).await.unwrap();
    sqlx::query("UPDATE dovecote_deliveries SET tenant_id = 'tenant-a'")
        .execute(&pool)
        .await
        .unwrap();

    let error = raw_sql(V1_TENANT_ACTIVATE_SQL).execute(&pool).await;
    assert!(error.is_err());
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'dovecote_events'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tables, 1);
    pool.close().await;
}

#[tokio::test]
async fn schema_check_rejects_existing_foreign_key_violations() {
    let pool = database().await;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dovecote_deliveries (event_row_id, tenant_id, state) VALUES (999, 'tenant-a', 'pending')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
    pool.close().await;
}

#[tokio::test]
async fn deferred_enqueue_is_rejected_before_adapter_reads() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = pool.begin().await.unwrap();
    let result = adapter.enqueue(&mut transaction, event("deferred")).await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::EnqueueError::WriteTransactionRequired)
    ));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn deferred_transaction_with_prior_application_write_is_supported() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    sqlx::query("CREATE TABLE application_state (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO application_state (id, value) VALUES (1, 'before enqueue')")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("after-application-write"))
        .await
        .unwrap();
    assert!(matches!(outcome, dovecote::EnqueueOutcome::Enqueued { .. }));
    transaction.commit().await.unwrap();
}

#[tokio::test]
async fn claim_counter_overflow_rolls_back_before_returning() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("overflow"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    sqlx::query("UPDATE dovecote_deliveries SET attempts = ?")
        .bind(i64::MAX)
        .execute(&pool)
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("overflow-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::CounterOverflow { .. })
    ));
    tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query("UPDATE dovecote_deliveries SET attempts = 0").execute(&pool),
    )
    .await
    .expect("claim error left a transaction lock held")
    .unwrap();
}

#[tokio::test]
async fn schema_check_rejects_an_incompatible_index() {
    let pool = database().await;
    sqlx::query("DROP INDEX dovecote_events_tenant_source_event_id")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn schema_check_rejects_altered_defaults_and_constraints() {
    let pool = database().await;
    sqlx::query("PRAGMA writable_schema = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE sqlite_master SET sql = REPLACE(sql, 'DEFAULT ''{}''', 'DEFAULT ''[]''') WHERE name = 'dovecote_events'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("PRAGMA writable_schema = OFF")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));

    let pool = database().await;
    sqlx::query("PRAGMA writable_schema = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE sqlite_master SET sql = REPLACE(sql, 'CHECK (attempts >= 0)', 'CHECK (attempts >= 0 OR 1 = 1)') WHERE name = 'dovecote_deliveries'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("PRAGMA writable_schema = OFF")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn schema_check_rejects_altered_schema_marker_definition() {
    let pool = database().await;
    sqlx::query("ALTER TABLE dovecote_schema ADD COLUMN marker_corruption INTEGER")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn database_rejects_invalid_delivery_state_constraints() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("invalid-state"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    assert!(
        sqlx::query("UPDATE dovecote_deliveries SET state = 'invalid' WHERE event_row_id = ?")
            .bind(row_id.get())
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE dovecote_deliveries SET attempts = -1 WHERE event_row_id = ?")
            .bind(row_id.get())
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mutations_classify_missing_and_pending_rows() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let token = dovecote::ClaimToken::from_bytes([3; 16]);
    let missing = dovecote::RowId::new(999).unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        missing,
        &token,
        MutationExpectation::NotFound,
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-delivered"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let delivered = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    let delivered_claim = adapter
        .claim(
            WorkerId::new("classification-delivered").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .ack(delivered, delivered_claim.claim_token())
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        delivered,
        delivered_claim.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Delivered),
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-quarantined"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let quarantined = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    let quarantined_claim = adapter
        .claim(
            WorkerId::new("classification-quarantined").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .quarantine(
            quarantined,
            quarantined_claim.claim_token(),
            &QuarantineReason::new("classification").unwrap(),
        )
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        quarantined,
        quarantined_claim.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Quarantined),
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-pending"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        row_id,
        &token,
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Pending),
    )
    .await;
}

#[tokio::test]
async fn schema_check_rejects_domain_triggers() {
    let pool = database().await;
    sqlx::query(
        "CREATE TRIGGER dovecote_events_audit AFTER INSERT ON dovecote_events BEGIN SELECT 1; END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn schema_check_rejects_temporary_domain_triggers() {
    let pool = database().await;
    sqlx::query(
        "CREATE TEMP TRIGGER temporary_dovecote_events_audit AFTER INSERT ON dovecote_events BEGIN SELECT 1; END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}
