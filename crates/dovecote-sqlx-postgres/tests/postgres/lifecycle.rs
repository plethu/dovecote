use super::support::*;

#[tokio::test]
async fn lifecycle_mutations_fence_and_preserve_delivery_state_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "lifecycle").await?;
        let quarantine_id = enqueue_committed(&database, "quarantine").await?;
        let worker = WorkerId::new("worker-a")?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;
        let claims = adapter.claim(worker.clone(), lease, Limit::new(1)?).await?;
        assert_eq!(claims.len(), 1);
        let claim = &claims[0];
        assert_eq!(claim.row_id(), row_id);
        assert_eq!(claim.attempts().get(), 1);
        let token = claim.claim_token().clone();

        adapter.renew(row_id, &token, lease).await?;
        let renewed_expiry: time::OffsetDateTime = query_scalar(
            "SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        let renewed_now: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
        .fetch_one(&database.pool)
        .await?;
        assert!(renewed_expiry > renewed_now);
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() + INTERVAL '1 second' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        adapter.renew(row_id, &token, lease).await?;
        let renewed_from_database_time: bool = query_scalar(
            "SELECT claim_expires_at > clock_timestamp() + INTERVAL '4 seconds' AND claim_expires_at < clock_timestamp() + INTERVAL '6 seconds' FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert!(renewed_from_database_time);

        let failure = Failure::new("transport_unavailable", "temporary")?;
        let retry_started: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&database.pool)
            .await?;
        adapter
            .retry(
                row_id,
                &token,
                &failure,
                Delay::new(std::time::Duration::from_millis(100))?,
            )
            .await?;
        let retry_snapshot = query_as::<_, (String, time::OffsetDateTime, Option<Vec<u8>>, Option<String>, Option<String>)>(
            "SELECT state, available_at, claim_token, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(retry_snapshot.0, "pending");
        assert!(retry_snapshot.1 > retry_started);
        assert!(retry_snapshot.2.is_none());
        assert_eq!(retry_snapshot.3.as_deref(), Some("transport_unavailable"));
        assert_eq!(retry_snapshot.4.as_deref(), Some("temporary"));
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(matches!(
            adapter.ack(row_id, &token).await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Pending
            })
        ));

        let reclaimed = adapter
            .claim(WorkerId::new("worker-b")?, lease, Limit::new(1)?)
            .await?;
        assert_eq!(reclaimed[0].row_id(), row_id);
        assert_eq!(reclaimed[0].attempts().get(), 2);
        assert_ne!(reclaimed[0].claim_token(), &token);
        assert!(matches!(
            adapter.ack(row_id, &token).await,
            Err(MutationError::LostClaim)
        ));
        let second_token = reclaimed[0].claim_token().clone();
        adapter
            .release(
                row_id,
                &second_token,
                Delay::new(std::time::Duration::from_millis(100))?,
            )
            .await?;
        let release_snapshot = query_as::<_, (String, Option<String>, Option<String>, Option<Vec<u8>>)>(
            "SELECT state, last_failure_code, last_failure_detail, claim_token FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(release_snapshot.0, "pending");
        assert_eq!(release_snapshot.1.as_deref(), Some("transport_unavailable"));
        assert_eq!(release_snapshot.2.as_deref(), Some("temporary"));
        assert!(release_snapshot.3.is_none());
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        let released = adapter
            .claim(WorkerId::new("worker-c")?, lease, Limit::new(1)?)
            .await?;
        let released_token = released[0].claim_token().clone();
        adapter.ack(row_id, &released_token).await?;
        assert!(matches!(
            adapter.ack(row_id, &released_token).await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Delivered
            })
        ));

        let quarantined = adapter
            .claim(WorkerId::new("worker-quarantine")?, lease, Limit::new(1)?)
            .await?
            .remove(0);
        assert_eq!(quarantined.row_id(), quarantine_id);
        let quarantine_token = quarantined.claim_token().clone();
        let reason = dovecote::QuarantineReason::new("operator_review")?;
        adapter
            .quarantine(quarantine_id, &quarantine_token, &reason)
            .await?;
        assert!(matches!(
            adapter
                .release(
                    quarantine_id,
                    &quarantine_token,
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Quarantined
            })
        ));
        let stored_reason: String = query_scalar(
            "SELECT quarantine_reason FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(quarantine_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored_reason, "operator_review");

        let attempts: i64 =
            query_scalar("SELECT attempts FROM dovecote_deliveries WHERE event_row_id = $1")
                .bind(row_id.get())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(attempts, 3);
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn expired_claims_reclaim_and_counter_overflow_rolls_back_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let expired_id = enqueue_committed(&database, "expired").await?;
        let claim = adapter
            .claim(
                WorkerId::new("worker-a")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let wrong_token = ClaimToken::from_bytes([7; dovecote::CLAIM_TOKEN_BYTES]);
        assert!(matches!(
            adapter.ack(expired_id, &wrong_token).await,
            Err(MutationError::LostClaim)
        ));
        query("UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE event_row_id = $1")
            .bind(expired_id.get())
            .execute(&database.pool)
            .await?;
        assert!(matches!(
            adapter.ack(expired_id, claim.claim_token()).await,
            Err(MutationError::LostClaim)
        ));
        let reclaimed = adapter
            .claim(
                WorkerId::new("worker-b")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), claim.claim_token());
        let stale_failure = Failure::new("stale", "must not mutate")?;
        assert!(matches!(
            adapter
                .renew(
                    expired_id,
                    claim.claim_token(),
                    Lease::new(std::time::Duration::from_secs(5))?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter.ack(expired_id, claim.claim_token()).await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .retry(
                    expired_id,
                    claim.claim_token(),
                    &stale_failure,
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .release(
                    expired_id,
                    claim.claim_token(),
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .quarantine(
                    expired_id,
                    claim.claim_token(),
                    &dovecote::QuarantineReason::new("stale")?
                )
                .await,
            Err(MutationError::LostClaim)
        ));

        let valid_before_overflow = enqueue_committed(&database, "valid-before-overflow").await?;
        let overflow_id = enqueue_committed(&database, "overflow").await?;
        query("UPDATE dovecote_deliveries SET attempts = $1 WHERE event_row_id = $2")
            .bind(i64::MAX)
            .bind(overflow_id.get())
            .execute(&database.pool)
            .await?;
        let overflow = adapter
            .claim(
                WorkerId::new("worker-overflow")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(2)?,
            )
            .await;
        assert!(matches!(
            overflow,
            Err(ClaimError::CounterOverflow { row_id }) if row_id == overflow_id
        ));
        let valid_snapshot: (String, i64, Option<Vec<u8>>, Option<String>) = query_as(
            "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(valid_before_overflow.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            valid_snapshot,
            ("pending".to_owned(), 0, None, None)
        );
        let state: String = query_scalar(
            "SELECT state FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(overflow_id.get())
        .fetch_one(&database.pool)
        .await?;
        let attempts: i64 = query_scalar(
            "SELECT attempts FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(overflow_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!((state, attempts), ("pending".to_owned(), i64::MAX));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn common_occurrence_time_endpoints_round_trip_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let minimum = time::OffsetDateTime::UNIX_EPOCH;
        let maximum = time::OffsetDateTime::new_in_offset(
            time::Date::from_calendar_date(9999, time::Month::December, 31)?,
            time::Time::from_hms_micro(23, 59, 59, 999_999)?,
            time::UtcOffset::UTC,
        );
        let minimum_id =
            enqueue_event_committed(&database, event_with_time("time-minimum", minimum)).await?;
        let maximum_id =
            enqueue_event_committed(&database, event_with_time("time-maximum", maximum)).await?;

        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let rows = adapter.page(None, Limit::new(2)?).await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row_id(), minimum_id);
        assert_eq!(rows[0].event().time(), Some(minimum));
        assert_eq!(rows[1].row_id(), maximum_id);
        assert_eq!(rows[1].event().time(), Some(maximum));

        // Values outside the shared portable range are rejected by the event
        // constructor before an adapter transaction is even opened.
        assert!(
            NewEvent::builder(
                StreamName::new("audit")?,
                EventId::new("time-before-minimum")?,
                EventSource::new("https://example.test/source")?,
                EventType::new("com.example.time")?,
            )
            .time(minimum - time::Duration::microseconds(1))
            .build()
            .is_err()
        );
        assert!(
            NewEvent::builder(
                StreamName::new("audit")?,
                EventId::new("time-after-maximum")?,
                EventSource::new("https://example.test/source")?,
                EventType::new("com.example.time")?,
            )
            // The time crate's representable maximum is only nanoseconds
            // beyond the shared microsecond-precision upper endpoint.
            .time(maximum + time::Duration::nanoseconds(999))
            .build()
            .is_err()
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn crash_after_claim_commit_leaves_a_reclaimable_claim_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "crash-after-claim").await?;
        let first = adapter
            .claim(
                WorkerId::new("crashed-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let token = first.claim_token().clone();

        // Returning from claim proves its transaction committed. Dropping the
        // worker result models a process crash before any transport ack.
        let stored: (String, i64, Option<Vec<u8>>, Option<String>) = query_as(
            "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, "claimed");
        assert_eq!(stored.1, 1);
        assert_eq!(stored.2, Some(token.as_bytes().to_vec()));
        assert_eq!(stored.3.as_deref(), Some("crashed-worker"));
        drop(first);

        // Move database time past the lease as the recovery worker would
        // observe it, without making the test depend on wall-clock sleeps.
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() - INTERVAL '1 millisecond' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        let reclaimed = adapter
            .claim(
                WorkerId::new("recovery-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.row_id(), row_id);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), &token);
        adapter.ack(row_id, reclaimed.claim_token()).await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn transport_success_before_crash_can_produce_a_reclaimed_duplicate_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "transport-success-before-crash").await?;
        let claimed = adapter
            .claim(
                WorkerId::new("transport-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let original_token = claimed.claim_token().clone();
        let original_event_id = claimed.event().id().clone();

        // The fake transport accepts the event, then the worker crashes before
        // ack. This is deliberately outside any database transaction.
        let transport_accepted = true;
        assert!(transport_accepted);
        drop(claimed);
        let stored: (String, Option<time::OffsetDateTime>, Option<Vec<u8>>) = query_as(
            "SELECT state, delivered_at, claim_token FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, "claimed");
        assert!(stored.1.is_none(), "transport success is not an ack");
        assert_eq!(stored.2, Some(original_token.as_bytes().to_vec()));

        // Recovery sees the expired lease and receives the same durable event
        // with a new token, making the possible duplicate explicit.
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() - INTERVAL '1 millisecond' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        let reclaimed = adapter
            .claim(
                WorkerId::new("transport-recovery")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.row_id(), row_id);
        assert_eq!(reclaimed.event().id(), &original_event_id);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), &original_token);
        adapter.ack(row_id, reclaimed.claim_token()).await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
