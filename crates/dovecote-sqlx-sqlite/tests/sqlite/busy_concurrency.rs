use super::test_support::*;

#[tokio::test]
async fn separate_connection_busy_exhaustion_rolls_back_and_then_commits() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let adapter = SqliteDovecote::with_busy_config(
        pool.clone(),
        dovecote_sqlx_sqlite::BusyConfig::new(Duration::ZERO, 0),
    );
    let mut setup = adapter.begin_write().await.unwrap();
    adapter.enqueue(&mut setup, event("busy")).await.unwrap();
    setup.commit().await.unwrap();

    let held = pool
        .begin_with(sqlx::AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("busy-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    held.rollback().await.unwrap();

    let claimed = adapter
        .claim(
            WorkerId::new("after-busy").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts().get(), 1);
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn busy_commit_is_rolled_back_before_the_next_claim() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let busy = dovecote_sqlx_sqlite::BusyConfig::new(Duration::ZERO, 0);
    let adapter = SqliteDovecote::with_busy_config(pool.clone(), busy);
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("busy-commit"))
        .await
        .unwrap();
    setup.commit().await.unwrap();

    // A rollback-journal reader can coexist with BEGIN IMMEDIATE but blocks
    // its COMMIT. The adapter must explicitly await ROLLBACK before returning
    // the busy error so the same pool connection is immediately reusable.
    let mut reader = pool.begin().await.unwrap();
    sqlx::query("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("busy-commit-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    reader.rollback().await.unwrap();
    let claimed = adapter
        .claim(
            WorkerId::new("after-busy-commit").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn busy_timeout_and_retry_budget_are_installed_per_operation() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let busy = dovecote_sqlx_sqlite::BusyConfig::new(Duration::from_millis(10), 2);
    assert_eq!(busy.timeout(), Duration::from_millis(10));
    assert_eq!(busy.retries(), 2);
    let adapter = SqliteDovecote::with_busy_config(pool.clone(), busy);
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("busy-budget"))
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let held = pool
        .begin_with(sqlx::AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let result = adapter
        .claim(
            WorkerId::new("busy-budget-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    let elapsed = started.elapsed();
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    assert!(elapsed < Duration::from_secs(1));
    held.rollback().await.unwrap();
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn invalid_busy_timeout_is_reported_as_configuration_error() {
    let pool = database().await;
    let adapter = SqliteDovecote::with_busy_config(
        pool,
        dovecote_sqlx_sqlite::BusyConfig::new(Duration::from_nanos(1), 0),
    );
    assert!(matches!(
        adapter.begin_write().await,
        Err(dovecote_sqlx_sqlite::EnqueueError::Configuration { .. })
    ));
}

#[tokio::test]
async fn separate_connections_serialize_claims_without_overlap() {
    let (pool, path) = file_database(Duration::from_millis(100)).await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    adapter.enqueue(&mut setup, event("one")).await.unwrap();
    adapter.enqueue(&mut setup, event("two")).await.unwrap();
    setup.commit().await.unwrap();
    let worker_a = WorkerId::new("worker-a").unwrap();
    let worker_b = WorkerId::new("worker-b").unwrap();
    let lease = Lease::new(Duration::from_secs(5)).unwrap();
    let limit = Limit::new(1).unwrap();
    let (first, second) = tokio::join!(
        adapter.claim(worker_a, lease, limit),
        adapter.claim(worker_b, lease, limit)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].row_id(), second[0].row_id());
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn concurrent_same_identity_enqueue_is_idempotent_without_busy_control_flow() {
    let (pool, path) = file_database(Duration::from_secs(1)).await;
    let adapter = Arc::new(SqliteDovecote::new(pool.clone()));
    let barrier = Arc::new(Barrier::new(2));
    let first_adapter = Arc::clone(&adapter);
    let second_adapter = Arc::clone(&adapter);
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first = event("raced");
    let second = event("raced");
    let (first, second) = tokio::join!(
        async move {
            first_barrier.wait().await;
            let mut transaction = first_adapter.begin_enqueue().await.unwrap();
            let outcome = first_adapter
                .enqueue(&mut transaction, first)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            outcome
        },
        async move {
            second_barrier.wait().await;
            let mut transaction = second_adapter.begin_enqueue().await.unwrap();
            let outcome = second_adapter
                .enqueue(&mut transaction, second)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            outcome
        }
    );
    assert!(matches!(
        (first, second),
        (
            dovecote::EnqueueOutcome::Enqueued { .. },
            dovecote::EnqueueOutcome::AlreadyEnqueued { .. }
        ) | (
            dovecote::EnqueueOutcome::AlreadyEnqueued { .. },
            dovecote::EnqueueOutcome::Enqueued { .. }
        )
    ));

    let barrier = Arc::new(Barrier::new(2));
    let first_adapter = Arc::clone(&adapter);
    let second_adapter = Arc::clone(&adapter);
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let changed_a = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("raced").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.changed-a").unwrap(),
    )
    .unwrap();
    let changed_b = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("raced").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.changed-b").unwrap(),
    )
    .unwrap();
    let (conflict_a, conflict_b) = tokio::join!(
        async move {
            first_barrier.wait().await;
            let mut transaction = first_adapter.begin_enqueue().await.unwrap();
            let result = first_adapter.enqueue(&mut transaction, changed_a).await;
            transaction.rollback().await.unwrap();
            result
        },
        async move {
            second_barrier.wait().await;
            let mut transaction = second_adapter.begin_enqueue().await.unwrap();
            let result = second_adapter.enqueue(&mut transaction, changed_b).await;
            transaction.rollback().await.unwrap();
            result
        }
    );
    assert!(matches!(
        conflict_a,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));
    assert!(matches!(
        conflict_b,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));
    pool.close().await;
    let _ = std::fs::remove_file(path);
}
