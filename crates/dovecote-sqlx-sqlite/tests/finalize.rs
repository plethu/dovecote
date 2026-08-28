use dovecote::{
    EventId, EventSource, EventType, FinalizeOutcome, ImportedDeliveryState, NewEvent, RowId,
    StreamName,
};
use dovecote_sqlx_sqlite::{FinalizeError, SqliteDovecote, TenantDovecote};
use sqlx::{query, query_as, query_scalar};
use time::{OffsetDateTime, UtcOffset};

mod support;
use support::database;

fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("migration-finalize").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://example.test/migration").unwrap(),
        EventType::new("com.example.migration.delivered").unwrap(),
    )
    .unwrap()
}

async fn imported_pending(adapter: &TenantDovecote, id: &str) -> RowId {
    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            event(id),
            ImportedDeliveryState::pending(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    match outcome {
        dovecote::ImportOutcome::Imported { row_id } => row_id,
        other => panic!("expected a new import, got {other:?}"),
    }
}

fn delivery_timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds)
        .unwrap()
        .to_offset(UtcOffset::UTC)
}

#[tokio::test]
async fn finalization_is_authoritative_and_idempotent() {
    let pool = database().await;
    let adapter =
        SqliteDovecote::new(pool.clone()).for_tenant(dovecote::TenantId::new("test").unwrap());
    let row_id = imported_pending(&adapter, "finalize").await;
    let delivered_at = delivery_timestamp(123);

    let first = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let outcome = adapter
            .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        outcome
    };
    assert_eq!(first, FinalizeOutcome::Finalized { row_id });

    let stored: (String, Option<String>, i64) = query_as(
        "SELECT state, delivered_at, attempts FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        stored,
        (
            "delivered".to_owned(),
            Some("1970-01-01T00:02:03Z".to_owned()),
            0
        )
    );

    let replay = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let outcome = adapter
            .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        outcome
    };
    assert_eq!(replay, FinalizeOutcome::AlreadyFinalized { row_id });

    let changed = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                row_id,
                delivery_timestamp(124),
            )
            .await;
        transaction.rollback().await.unwrap();
        result
    };
    assert!(matches!(changed, Err(FinalizeError::StateConflict { row_id: id }) if id == row_id));

    let claimed = adapter
        .claim(
            dovecote::WorkerId::new("finalize-test").unwrap(),
            dovecote::Lease::new(std::time::Duration::from_secs(1)).unwrap(),
            dovecote::Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        claimed.is_empty(),
        "delivered migration rows are not publishable"
    );
}

#[tokio::test]
async fn only_canonical_pending_state_can_be_finalized() {
    for (id, mutation) in [
        (
            "claimed",
            "UPDATE dovecote_deliveries SET state = 'claimed', attempts = 1, claim_token = zeroblob(16), claimed_by = 'worker', claim_expires_at = '9999-12-31T23:59:59.999000Z' WHERE event_row_id = ?",
        ),
        (
            "quarantined",
            "UPDATE dovecote_deliveries SET state = 'quarantined', quarantined_at = '1970-01-01T00:00:00.000000Z', quarantine_reason = 'legacy terminal state' WHERE event_row_id = ?",
        ),
        (
            "failed",
            "UPDATE dovecote_deliveries SET attempts = 1, last_failure_code = 'legacy', last_failure_detail = 'failed before cutover' WHERE event_row_id = ?",
        ),
        (
            "delayed",
            "UPDATE dovecote_deliveries SET available_at = '1970-01-01T00:00:01.000000Z' WHERE event_row_id = ?",
        ),
    ] {
        let pool = database().await;
        let adapter =
            SqliteDovecote::new(pool.clone()).for_tenant(dovecote::TenantId::new("test").unwrap());
        let row_id = imported_pending(&adapter, id).await;
        query(mutation)
            .bind(row_id.get())
            .execute(&pool)
            .await
            .unwrap();

        let mut transaction = adapter.begin_write().await.unwrap();
        let result = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                row_id,
                delivery_timestamp(456),
            )
            .await;
        transaction.rollback().await.unwrap();
        assert!(
            matches!(result, Err(FinalizeError::StateConflict { row_id: id }) if id == row_id),
            "{id}: {result:?}"
        );
    }
}

#[tokio::test]
async fn finalization_requires_write_transaction_and_supports_rollback() {
    let pool = database().await;
    let adapter =
        SqliteDovecote::new(pool.clone()).for_tenant(dovecote::TenantId::new("test").unwrap());
    let row_id = imported_pending(&adapter, "rollback").await;

    let mut deferred = pool.begin().await.unwrap();
    let result = adapter
        .finalize_pending_delivery_for_migration(&mut deferred, row_id, delivery_timestamp(789))
        .await;
    assert!(matches!(
        result,
        Err(FinalizeError::WriteTransactionRequired)
    ));
    deferred.rollback().await.unwrap();

    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivery_timestamp(789))
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    let state: String =
        query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "pending");
}

#[tokio::test]
async fn invalid_timestamp_and_schema_mismatch_happen_before_mutation() {
    let pool = database().await;
    let adapter =
        SqliteDovecote::new(pool.clone()).for_tenant(dovecote::TenantId::new("test").unwrap());
    let row_id = imported_pending(&adapter, "invalid").await;
    let invalid = OffsetDateTime::UNIX_EPOCH.replace_nanosecond(1).unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .finalize_pending_delivery_for_migration(&mut transaction, row_id, invalid)
        .await;
    assert!(matches!(
        result,
        Err(FinalizeError::InvalidTimestamp { .. })
    ));
    transaction.rollback().await.unwrap();

    query("DROP INDEX dovecote_deliveries_claimable")
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivery_timestamp(42))
        .await;
    assert!(matches!(
        result,
        Err(FinalizeError::MigrationMismatch { .. })
    ));
    transaction.rollback().await.unwrap();
    let state: String =
        query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "pending");
}

#[tokio::test]
async fn missing_event_is_typed_not_found() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool).for_tenant(dovecote::TenantId::new("test").unwrap());
    let row_id = RowId::new(999).unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let result = adapter
        .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivery_timestamp(1))
        .await;
    assert!(matches!(result, Err(FinalizeError::NotFound)));
    transaction.rollback().await.unwrap();
}
