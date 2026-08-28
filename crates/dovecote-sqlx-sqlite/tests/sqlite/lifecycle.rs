use super::test_support::*;

#[tokio::test]
async fn stale_token_is_fenced_and_terminal_state_is_illegal() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("one"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let claimed = adapter
        .claim(
            WorkerId::new("worker-a").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let wrong = dovecote::ClaimToken::from_bytes([7; 16]);
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        claimed.row_id(),
        &wrong,
        MutationExpectation::LostClaim,
    )
    .await;
    adapter
        .quarantine(
            claimed.row_id(),
            claimed.claim_token(),
            &QuarantineReason::new("operator decision").unwrap(),
        )
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        claimed.row_id(),
        claimed.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Quarantined),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_mutations_persist_their_exact_fields_and_database_times() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("fields"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let first = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let old_expiry = first.claim_expires_at();
    adapter
        .renew(
            first.row_id(),
            first.claim_token(),
            Lease::new(Duration::from_secs(10)).unwrap(),
        )
        .await
        .unwrap();
    let renewed: String = sqlx::query_scalar(
        "SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(first.row_id().get())
    .fetch_one(&pool)
    .await
    .unwrap();
    let renewed =
        time::OffsetDateTime::parse(&renewed, &time::format_description::well_known::Rfc3339)
            .unwrap();
    assert!(renewed > old_expiry);
    assert_eq!(renewed.microsecond() % 1_000, 0);

    let failure = Failure::new("temporary", "retry detail").unwrap();
    adapter
        .retry(
            first.row_id(),
            first.claim_token(),
            &failure,
            Delay::new(Duration::ZERO).unwrap(),
        )
        .await
        .unwrap();
    let retried = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        retried[0].delivery(),
        dovecote::DeliverySnapshot::Pending {
            last_failure: Some(stored), ..
        } if stored == &failure
    ));

    let second = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .release(
            second.row_id(),
            second.claim_token(),
            Delay::new(Duration::ZERO).unwrap(),
        )
        .await
        .unwrap();
    let released = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        released[0].delivery(),
        dovecote::DeliverySnapshot::Pending {
            last_failure: Some(stored), ..
        } if stored == &failure
    ));

    let third = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .quarantine(
            third.row_id(),
            third.claim_token(),
            &QuarantineReason::new("manual quarantine").unwrap(),
        )
        .await
        .unwrap();
    let quarantined = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        quarantined[0].delivery(),
        dovecote::DeliverySnapshot::Quarantined { reason, .. }
            if reason.as_str() == "manual quarantine"
    ));
}

#[tokio::test]
async fn crash_after_claim_commit_reclaims_and_fences_the_expired_token() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("reclaim"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let first = adapter
        .claim(
            WorkerId::new("reclaim-a").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    // Returning from claim proves that its short BEGIN IMMEDIATE transaction
    // committed. A worker crash now leaves this durable claim for recovery.
    let expired_token = first.claim_token().clone();
    sqlx::query("UPDATE dovecote_deliveries SET claim_expires_at = '1970-01-01T00:00:00.000000Z'")
        .execute(&pool)
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        first.row_id(),
        &expired_token,
        MutationExpectation::LostClaim,
    )
    .await;
    let second = adapter
        .claim(
            WorkerId::new("reclaim-b").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.attempts().get(), 2);
    assert_ne!(&expired_token, second.claim_token());
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        second.row_id(),
        &expired_token,
        MutationExpectation::LostClaim,
    )
    .await;
    adapter
        .ack(second.row_id(), second.claim_token())
        .await
        .unwrap();
}

#[tokio::test]
async fn common_occurrence_time_endpoints_round_trip_and_reject_outside_before_sql() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let minimum = time::OffsetDateTime::UNIX_EPOCH;
    let maximum = time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    );
    let timed_event = |id: &str, at: time::OffsetDateTime| {
        NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new(id).unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.time").unwrap(),
        )
        .time(at)
        .build()
    };

    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(
            &mut transaction,
            timed_event("time-minimum", minimum).unwrap(),
        )
        .await
        .unwrap();
    let maximum_outcome = adapter
        .enqueue(
            &mut transaction,
            timed_event("time-maximum", maximum).unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let maximum_id = match maximum_outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };

    let mut replay_transaction = adapter.begin_write().await.unwrap();
    let replay = adapter
        .enqueue(
            &mut replay_transaction,
            timed_event("time-maximum", maximum).unwrap(),
        )
        .await
        .unwrap();
    replay_transaction.commit().await.unwrap();
    assert_eq!(
        replay,
        dovecote::EnqueueOutcome::AlreadyEnqueued { row_id: maximum_id }
    );

    let rows = adapter.page(None, Limit::new(10).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].event().time(), Some(minimum));
    assert_eq!(rows[1].event().time(), Some(maximum));

    // NewEvent validates the common portable range and precision before an
    // adapter transaction is opened, so neither invalid value can reach SQL.
    assert!(
        timed_event(
            "time-before-minimum",
            minimum - time::Duration::microseconds(1)
        )
        .is_err()
    );
    assert!(
        timed_event(
            "time-after-maximum",
            maximum + time::Duration::nanoseconds(1)
        )
        .is_err()
    );
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 2);
}

#[tokio::test]
async fn crash_before_claim_commit_exposes_no_claim() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut setup, event("crash-before-claim"))
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };

    // An uncommitted claim is rolled back when the worker process crashes. An
    // explicit rollback gives that crash boundary a deterministic test shape.
    let mut uncommitted = pool
        .begin_with(AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE dovecote_deliveries SET state = 'claimed', attempts = 1, claim_token = ?, claimed_by = ?, claim_expires_at = ? WHERE event_row_id = ?",
    )
    .bind([0x42_u8; 16].as_slice())
    .bind("crashed-before-commit")
    .bind("9999-12-31T23:59:59.999000Z")
    .bind(row_id.get())
    .execute(&mut *uncommitted)
    .await
    .unwrap();
    uncommitted.rollback().await.unwrap();

    let stored: (String, i64, Option<Vec<u8>>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?",
        )
        .bind(row_id.get())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, ("pending".to_owned(), 0, None, None, None));

    let recovered = adapter
        .claim(
            WorkerId::new("after-crash").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovered.row_id(), row_id);
    assert_eq!(recovered.attempts().get(), 1);
    adapter.ack(row_id, recovered.claim_token()).await.unwrap();
}

#[tokio::test]
async fn transport_success_before_ack_can_produce_a_reclaimed_duplicate() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("transport-success-before-ack"))
        .await
        .unwrap();
    setup.commit().await.unwrap();

    let claimed = adapter
        .claim(
            WorkerId::new("transport-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let original_row_id = claimed.row_id();
    let original_token = claimed.claim_token().clone();
    let original_event_id = claimed.event().id().clone();

    // The fake transport accepts outside the database transaction, then the
    // worker crashes before ack. Transport success is deliberately not durable.
    let mut transport = FakeTransport::default();
    transport.accept(&claimed);
    drop(claimed);
    let stored: (String, Option<String>, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT state, delivered_at, claim_token FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(original_row_id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored.0, "claimed");
    assert!(stored.1.is_none());
    assert_eq!(stored.2, Some(original_token.as_bytes().to_vec()));

    // Recovery sees the expired claim and returns the same durable event with
    // a fresh token: this is the expected possible duplicate consequence.
    sqlx::query(
        "UPDATE dovecote_deliveries SET claim_expires_at = '1970-01-01T00:00:00.000000Z' WHERE event_row_id = ?",
    )
    .bind(original_row_id.get())
    .execute(&pool)
    .await
    .unwrap();
    let reclaimed = adapter
        .claim(
            WorkerId::new("transport-recovery").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reclaimed.row_id(), original_row_id);
    assert_eq!(reclaimed.event().id(), &original_event_id);
    assert_eq!(reclaimed.attempts().get(), 2);
    assert_ne!(reclaimed.claim_token(), &original_token);
    transport.accept(&reclaimed);
    assert_eq!(
        transport.accepted,
        vec![
            (
                "https://example.test/source".to_owned(),
                "transport-success-before-ack".to_owned()
            ),
            (
                "https://example.test/source".to_owned(),
                "transport-success-before-ack".to_owned()
            )
        ]
    );
    assert_eq!(transport.accepted[0], transport.accepted[1]);
    let still_claimed: (String, Option<String>) = sqlx::query_as(
        "SELECT state, delivered_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(reclaimed.row_id().get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(still_claimed, ("claimed".to_owned(), None));
    adapter
        .ack(reclaimed.row_id(), reclaimed.claim_token())
        .await
        .unwrap();
}
