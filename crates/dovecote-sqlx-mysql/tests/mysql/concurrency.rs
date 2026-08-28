use super::support::*;

#[tokio::test]
async fn matrix_skip_locked_claims_are_non_overlapping() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    assert!(
        sqlx::query("INSERT INTO dovecote_events (row_id, tenant_id, stream, specversion, event_id, source, event_type, extensions) VALUES (-1, ?, ?, ?, ?, ?, ?, ?)")
            .bind(b"test".as_slice())
            .bind(b"mysql-conformance".as_slice())
            .bind(b"1.0".as_slice())
            .bind(b"negative-id".as_slice())
            .bind(b"https://dovecote.test/mysql".as_slice())
            .bind(b"conformance.event".as_slice())
            .bind(b"{}".as_slice())
            .execute(&pool)
            .await
            .is_err()
    );
    let mut tx = pool.begin().await?;
    adapter.enqueue(&mut tx, event("parallel-a")).await?;
    adapter.enqueue(&mut tx, event("parallel-b")).await?;
    tx.commit().await?;
    let left = adapter.clone();
    let right = adapter.clone();
    let (a, b) = tokio::join!(
        left.claim(
            WorkerId::new("parallel-left")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?
        ),
        right.claim(
            WorkerId::new("parallel-right")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?
        )
    );
    let a = a?;
    let b = b?;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_ne!(a[0].row_id(), b[0].row_id());
    pool.close().await;
    Ok(())
}
#[tokio::test]
async fn mysql_snapshot_pages_have_a_fixed_bound_and_release_connections()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let first_id = enqueue_committed(&pool, event("snapshot-first")).await?;
    let second_id = enqueue_committed(&pool, event("snapshot-second")).await?;

    // Allocate a lower row ID in an uncommitted transaction, then commit a
    // later row. This is the commit-inversion race documented by the SPEC.
    let mut earlier_transaction = pool.begin().await?;
    let earlier = adapter
        .enqueue(
            &mut earlier_transaction,
            event("snapshot-inversion-earlier"),
        )
        .await?;
    let earlier_id = match earlier {
        EnqueueOutcome::Enqueued { row_id } => row_id,
        other => return Err(format!("expected fresh inversion insert, got {other:?}").into()),
    };

    let later_id = enqueue_committed(&pool, event("snapshot-inversion-later")).await?;
    assert!(earlier_id < later_id);

    let mut snapshot = adapter.begin_snapshot().await?;
    assert_eq!(snapshot.upper_bound(), Some(later_id));
    let live_before = adapter.page(None, Limit::new(100)?).await?;
    assert_eq!(
        live_before
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![first_id, second_id, later_id]
    );
    earlier_transaction.commit().await?;
    assert!(
        adapter
            .page(Some(later_id), Limit::new(100)?)
            .await?
            .is_empty()
    );

    let first_page = snapshot.next_page(Limit::new(2)?).await?;
    assert_eq!(
        first_page
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![first_id, second_id]
    );
    let second_page = snapshot.next_page(Limit::new(2)?).await?;
    assert_eq!(
        second_page
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![later_id]
    );
    assert!(snapshot.is_exhausted());
    assert!(snapshot.next_page(Limit::new(2)?).await?.is_empty());
    snapshot.finish().await?;

    let url = std::env::var("DOVECOTE_MYSQL_URL")?;
    let single = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect(&url)
        .await?;
    let closable = MySqlDovecote::new(single.clone());
    let closable_snapshot = closable.begin_snapshot().await?;
    assert!(matches!(
        single.acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    closable_snapshot.close().await?;
    query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&single)
        .await?;
    let dropped_snapshot = closable.begin_snapshot().await?;
    assert!(matches!(
        single.acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    drop(dropped_snapshot);
    query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&single)
        .await?;
    single.close().await;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_datetime_common_range_endpoints_round_trip_without_precision_loss()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let minimum = time::OffsetDateTime::UNIX_EPOCH;
    let maximum = time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31)?,
        time::Time::from_hms_micro(23, 59, 59, 999_999)?,
        time::UtcOffset::UTC,
    );
    let timed_event = |id: &str, at: time::OffsetDateTime| {
        NewEvent::builder(
            StreamName::new("mysql-conformance").expect("valid stream"),
            EventId::new(id).expect("valid id"),
            EventSource::new("https://dovecote.test/mysql").expect("valid source"),
            EventType::new("conformance.event").expect("valid type"),
        )
        .time(at)
        .build()
        .expect("valid timestamp")
    };

    let minimum_id = enqueue_committed(&pool, timed_event("datetime-minimum", minimum)).await?;
    let maximum_id = enqueue_committed(&pool, timed_event("datetime-maximum", maximum)).await?;
    let mut replay_transaction = pool.begin().await?;
    let replay = adapter
        .enqueue(
            &mut replay_transaction,
            timed_event("datetime-maximum", maximum),
        )
        .await?;
    assert_eq!(
        replay,
        EnqueueOutcome::AlreadyEnqueued { row_id: maximum_id }
    );
    replay_transaction.commit().await?;
    let rows = adapter.page(None, Limit::new(10)?).await?;
    let minimum_row = rows
        .iter()
        .find(|row| row.row_id() == minimum_id)
        .expect("minimum row");
    let maximum_row = rows
        .iter()
        .find(|row| row.row_id() == maximum_id)
        .expect("maximum row");
    assert_eq!(minimum_row.event().time(), Some(minimum));
    assert_eq!(maximum_row.event().time(), Some(maximum));

    let minimum_claim = adapter
        .claim(
            WorkerId::new("datetime-minimum-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(minimum_claim.row_id(), minimum_id);
    adapter.ack(minimum_id, minimum_claim.claim_token()).await?;
    let maximum_claim = adapter
        .claim(
            WorkerId::new("datetime-maximum-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(maximum_claim.row_id(), maximum_id);
    assert_eq!(maximum_claim.event().time(), Some(maximum));
    adapter.ack(maximum_id, maximum_claim.claim_token()).await?;

    let out_of_range = NewEvent::builder(
        StreamName::new("mysql-conformance")?,
        EventId::new("datetime-out-of-range")?,
        EventSource::new("https://dovecote.test/mysql")?,
        EventType::new("conformance.event")?,
    )
    .time(minimum - time::Duration::microseconds(1))
    .build();
    assert!(
        out_of_range.is_err(),
        "timestamps beyond DATETIME common range must fail before SQL"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_lock_timeout_is_returned_as_a_typed_transient_error() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("lock-timeout")).await?;
    let claim = adapter
        .claim(
            WorkerId::new("lock-timeout-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);

    let mut locker = pool.begin().await?;
    query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = ? FOR UPDATE")
        .bind(row_id.get())
        .fetch_one(&mut *locker)
        .await?;

    let url = std::env::var("DOVECOTE_MYSQL_URL")?;
    let timeout_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&url)
        .await?;
    query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&timeout_pool)
        .await?;
    let timeout_adapter = MySqlDovecote::new(timeout_pool.clone());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        timeout_adapter.ack(row_id, claim.claim_token()),
    )
    .await?;
    match result {
        Err(MutationError::Transient {
            kind: TransientKind::StatementOrLockTimeout,
            source,
            ..
        }) => {
            let number = source.as_database_error().and_then(|error| {
                error
                    .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                    .map(|error| error.number())
            });
            assert_eq!(number, Some(1205));
        }
        other => return Err(format!("expected typed lock timeout, got {other:?}").into()),
    }
    locker.rollback().await?;
    timeout_pool.close().await;
    pool.close().await;
    Ok(())
}
