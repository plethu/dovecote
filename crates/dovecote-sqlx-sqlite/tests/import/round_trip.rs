use super::test_support::*;

#[tokio::test]
async fn pending_import_is_atomic_and_exactly_idempotent() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let first = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("pending", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        result
    };
    let row_id = match first {
        ImportOutcome::Imported { row_id } => row_id,
        other => panic!("expected imported outcome, got {other:?}"),
    };
    assert_eq!(counts(&pool).await, (1, 1));

    let mut transaction = adapter.begin_write().await.unwrap();
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("pending", "com.example.import"),
            ImportedDeliveryState::pending(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(replay, ImportOutcome::AlreadyImported { row_id });
    let row: (String, i64, Option<String>, String, String) = sqlx::query_as(
        "SELECT d.state, d.attempts, d.delivered_at, e.enqueued_at, d.available_at FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE d.event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "pending");
    assert_eq!(row.1, 0);
    assert!(row.2.is_none());
    assert_eq!(
        row.3, row.4,
        "pending import uses one database operation time"
    );
}

#[tokio::test]
async fn delivered_import_preserves_authoritative_endpoints_and_is_never_claimable() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let delivered_at = delivered_at_maximum();
    let state = ImportedDeliveryState::delivered(delivered_at).unwrap();
    let row_id = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("delivered", "com.example.import"),
                state,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            other => panic!("expected imported outcome, got {other:?}"),
        }
    };
    let stored: (String, String) = sqlx::query_as(
        "SELECT state, delivered_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        stored,
        (
            "delivered".to_owned(),
            "9999-12-31T23:59:59.999999Z".to_owned()
        )
    );

    let mut transaction = adapter.begin_write().await.unwrap();
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("delivered", "com.example.import"),
            state,
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(replay, ImportOutcome::AlreadyImported { row_id });
    query("UPDATE dovecote_deliveries SET attempts = 1 WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let changed_state = adapter
        .import_for_migration(
            &mut transaction,
            event("delivered", "com.example.import"),
            state,
        )
        .await;
    assert!(matches!(
        changed_state,
        Err(ImportError::ImportConflict { existing_row_id }) if existing_row_id == row_id
    ));
    transaction.rollback().await.unwrap();
    assert!(
        adapter
            .claim(
                dovecote::WorkerId::new("worker").unwrap(),
                dovecote::Lease::new(std::time::Duration::from_secs(5)).unwrap(),
                dovecote::Limit::new(10).unwrap(),
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn delivered_import_preserves_the_lower_timestamp_endpoint() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let row_id = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("delivered-minimum", "com.example.import"),
                ImportedDeliveryState::delivered(OffsetDateTime::UNIX_EPOCH).unwrap(),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            other => panic!("expected imported outcome, got {other:?}"),
        }
    };
    let stored: String =
        query_scalar("SELECT delivered_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, "1970-01-01T00:00:00Z");
}
