use dovecote::{
    ContentType, EventData, EventId, EventSource, EventSubject, EventType, ExtensionName,
    ExtensionValue, Extensions, ImportOutcome, ImportedDeliveryState, NewEvent, PartitionKey,
    SchemaUri, StreamName, ValidationKind, ValidationOperation,
};
use dovecote_sqlx_sqlite::{ImportError, MIGRATIONS, SqliteDovecote, check_schema};
use sqlx::{SqlitePool, query, query_scalar, raw_sql, sqlite::SqlitePoolOptions};
use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

async fn database() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("SQLite pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("Dovecote migration");
    check_schema(&pool).await.expect("Dovecote schema");
    pool
}

fn event(event_id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("migration-test").unwrap(),
        EventId::new(event_id).unwrap(),
        EventSource::new("https://example.test/migration").unwrap(),
        EventType::new(event_type).unwrap(),
    )
    .unwrap()
}

fn rich_event(event_id: &str) -> NewEvent {
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("tenant").unwrap(),
            ExtensionValue::string("münchen").unwrap(),
        )
        .unwrap();
    NewEvent::builder(
        StreamName::new("migration-test").unwrap(),
        EventId::new(event_id).unwrap(),
        EventSource::new("https://example.test/migration").unwrap(),
        EventType::new("com.example.rich").unwrap(),
    )
    .subject(EventSubject::new("subject-α").unwrap())
    .time(OffsetDateTime::UNIX_EPOCH)
    .datacontenttype(ContentType::new("application/json").unwrap())
    .dataschema(SchemaUri::new("https://example.test/schema/v1").unwrap())
    .partitionkey(PartitionKey::new("partition-α").unwrap())
    .extensions(extensions)
    .data(EventData::json(r#"{ "name": "café" }"#.as_bytes().to_vec()).unwrap())
    .build()
    .unwrap()
}

fn delivered_at_maximum() -> OffsetDateTime {
    OffsetDateTime::new_in_offset(
        Date::from_calendar_date(9999, Month::December, 31).unwrap(),
        Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        UtcOffset::UTC,
    )
}

async fn counts(pool: &SqlitePool) -> (i64, i64) {
    (
        query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(pool)
            .await
            .unwrap(),
        query_scalar("SELECT count(*) FROM dovecote_deliveries")
            .fetch_one(pool)
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn pending_import_is_atomic_and_exactly_idempotent() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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

#[tokio::test]
async fn identity_and_imported_state_conflicts_are_distinct_and_non_mutating() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
    let adapter = SqliteDovecote::new(pool.clone());
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
        let adapter = SqliteDovecote::new(pool.clone());
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
