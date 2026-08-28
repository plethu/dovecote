use super::test_support::*;

#[tokio::test]
async fn identity_and_imported_state_conflicts_are_distinct_and_non_mutating() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let row_id = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("conflict", "com.example.one"),
                ImportedDeliveryState::Pending,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            other => panic!("expected imported outcome, got {other:?}"),
        }
    };

    let mut transaction = adapter.begin_write().await.unwrap();
    let identity = adapter
        .import_for_migration(
            &mut transaction,
            event("conflict", "com.example.two"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(
        identity,
        Err(ImportError::IdentityConflict { existing_row_id }) if existing_row_id == row_id
    ));
    transaction.rollback().await.unwrap();

    let mut transaction = adapter.begin_write().await.unwrap();
    let state = adapter
        .import_for_migration(
            &mut transaction,
            event("conflict", "com.example.one"),
            ImportedDeliveryState::delivered(OffsetDateTime::UNIX_EPOCH).unwrap(),
        )
        .await;
    assert!(matches!(
        state,
        Err(ImportError::ImportConflict { existing_row_id }) if existing_row_id == row_id
    ));
    transaction.rollback().await.unwrap();
    assert_eq!(counts(&pool).await, (1, 1));
}

#[tokio::test]
async fn changed_canonical_pending_state_is_an_import_conflict() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let row_id = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("retried", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            _ => panic!("expected imported outcome"),
        }
    };
    query("UPDATE dovecote_deliveries SET attempts = 1 WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .import_for_migration(
            &mut transaction,
            event("retried", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(result, Err(ImportError::ImportConflict { .. })));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn changed_import_timestamp_pair_is_an_import_conflict() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let row_id = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("timestamp-pair", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            _ => panic!("expected imported outcome"),
        }
    };
    query("UPDATE dovecote_deliveries SET available_at = ? WHERE event_row_id = ?")
        .bind("1970-01-01T00:00:00.000Z")
        .bind(row_id.get())
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .import_for_migration(
            &mut transaction,
            event("timestamp-pair", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(result, Err(ImportError::ImportConflict { .. })));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn schema_drift_is_rejected_before_event_mutation() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    query("CREATE INDEX dovecote_import_unreviewed ON dovecote_events (event_type)")
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .import_for_migration(
            &mut transaction,
            event("schema-drift", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(result, Err(ImportError::MigrationMismatch { .. })));
    transaction.rollback().await.unwrap();
    assert_eq!(counts(&pool).await, (0, 0));
}

#[tokio::test]
async fn rollback_and_schema_validation_happen_before_event_mutation() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .import_for_migration(
            &mut transaction,
            event("rolled-back", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(counts(&pool).await, (0, 0));

    query("DROP TABLE dovecote_deliveries")
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .import_for_migration(
            &mut transaction,
            event("bad-schema", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(result, Err(ImportError::MigrationMismatch { .. })));
    transaction.rollback().await.unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM dovecote_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn delivered_timestamp_precision_is_rejected_before_mutation() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let invalid = OffsetDateTime::UNIX_EPOCH.replace_nanosecond(1).unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .import_for_migration(
            &mut transaction,
            event("bad-time", "com.example.import"),
            ImportedDeliveryState::Delivered {
                delivered_at: invalid,
            },
        )
        .await;
    match result {
        Err(ImportError::InvalidState { source }) => {
            assert_eq!(source.field(), "delivered_at");
            assert_eq!(source.kind(), ValidationKind::Precision);
            assert_eq!(source.operation(), ValidationOperation::State);
            assert_eq!(source.code(), "precision");
            assert_eq!(source.category_code(), "invalid_state");
        }
        other => panic!("expected structured validation error, got {other:?}"),
    }
    transaction.rollback().await.unwrap();
    assert_eq!(counts(&pool).await, (0, 0));
}

#[tokio::test]
async fn every_stored_immutable_event_field_is_compared_on_replay() {
    let fields = [
        (
            "stream",
            "UPDATE dovecote_events SET stream = ? WHERE event_id = ?",
        ),
        (
            "event_type",
            "UPDATE dovecote_events SET event_type = ? WHERE event_id = ?",
        ),
        (
            "subject",
            "UPDATE dovecote_events SET subject = ? WHERE event_id = ?",
        ),
        (
            "occurred_at",
            "UPDATE dovecote_events SET occurred_at = ? WHERE event_id = ?",
        ),
        (
            "datacontenttype",
            "UPDATE dovecote_events SET datacontenttype = ? WHERE event_id = ?",
        ),
        (
            "dataschema",
            "UPDATE dovecote_events SET dataschema = ? WHERE event_id = ?",
        ),
        (
            "partitionkey",
            "UPDATE dovecote_events SET partitionkey = ? WHERE event_id = ?",
        ),
        (
            "extensions",
            "UPDATE dovecote_events SET extensions = ? WHERE event_id = ?",
        ),
        (
            "data_kind",
            "UPDATE dovecote_events SET data_kind = ? WHERE event_id = ?",
        ),
        (
            "data",
            "UPDATE dovecote_events SET data = ? WHERE event_id = ?",
        ),
    ];

    for (field, update_sql) in fields {
        let pool = database().await;
        let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let event_id = format!("rich-{field}");
        let mut transaction = adapter.begin_write().await.unwrap();
        adapter
            .import_for_migration(
                &mut transaction,
                rich_event(&event_id),
                ImportedDeliveryState::Pending,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        match field {
            "stream" => {
                query(update_sql)
                    .bind("migration-other")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "event_type" => {
                query(update_sql)
                    .bind("com.example.other")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "subject" => {
                query(update_sql)
                    .bind("subject-other")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "occurred_at" => {
                query(update_sql)
                    .bind("1970-01-01T00:00:01Z")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "datacontenttype" => {
                query(update_sql)
                    .bind("application/problem+json")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "dataschema" => {
                query(update_sql)
                    .bind("https://example.test/schema/v2")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "partitionkey" => {
                query(update_sql)
                    .bind("partition-other")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "extensions" => {
                query(update_sql)
                    .bind(r#"{"tenant":{"type":"string","value":"other"}}"#)
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "data_kind" => {
                query(update_sql)
                    .bind("binary")
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "data" => {
                query(update_sql)
                    .bind(br#"{"name":"changed"}"#.as_slice())
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }

        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .import_for_migration(
                &mut transaction,
                rich_event(&event_id),
                ImportedDeliveryState::Pending,
            )
            .await;
        assert!(
            matches!(result, Err(ImportError::IdentityConflict { .. })),
            "stored {field} was not compared"
        );
        transaction.rollback().await.unwrap();
        pool.close().await;
    }
}
