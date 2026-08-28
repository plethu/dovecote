use super::support::*;

#[tokio::test]
async fn matrix_wrong_token_is_fenced_for_every_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter.enqueue(&mut transaction, event("fencing")).await?;
    transaction.commit().await?;
    let row_id = match outcome {
        EnqueueOutcome::Enqueued { row_id } | EnqueueOutcome::AlreadyEnqueued { row_id } => row_id,
        _ => return Err("unknown enqueue outcome".into()),
    };

    let claimed = adapter
        .claim(
            WorkerId::new("fencing-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?;
    let good = claimed.first().ok_or("claim returned no row")?;
    let bad = ClaimToken::from_bytes([0xA5; dovecote::CLAIM_TOKEN_BYTES]);
    let delay = Delay::new(std::time::Duration::from_secs(1))?;
    let failure = Failure::new("test.failure", "fencing")?;
    let reason = QuarantineReason::new("fencing")?;
    assert!(matches!(
        adapter
            .renew(row_id, &bad, Lease::new(std::time::Duration::from_secs(1))?)
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.ack(row_id, &bad).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.retry(row_id, &bad, &failure, delay).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.release(row_id, &bad, delay).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.quarantine(row_id, &bad, &reason).await,
        Err(MutationError::LostClaim)
    ));
    adapter.ack(row_id, good.claim_token()).await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_expired_reclaim_rotates_token_and_classifies_stale_calls()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("reclaim")).await?;
    let first = adapter
        .claim(
            WorkerId::new("reclaim-first")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    let first_token = first.claim_token().clone();
    query("UPDATE dovecote_deliveries SET claim_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;

    assert!(matches!(
        adapter.ack(row_id, &first_token).await,
        Err(MutationError::LostClaim)
    ));
    let reclaimed = adapter
        .claim(
            WorkerId::new("reclaim-second")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(reclaimed.row_id(), row_id);
    assert_eq!(reclaimed.attempts().get(), 2);
    assert_ne!(reclaimed.claim_token(), &first_token);

    let failure = Failure::new("stale", "must not mutate")?;
    let reason = QuarantineReason::new("stale")?;
    assert!(matches!(
        adapter
            .renew(
                row_id,
                &first_token,
                Lease::new(std::time::Duration::from_secs(1))?
            )
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter
            .retry(
                row_id,
                &first_token,
                &failure,
                Delay::new(std::time::Duration::ZERO)?
            )
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter
            .release(row_id, &first_token, Delay::new(std::time::Duration::ZERO)?)
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.quarantine(row_id, &first_token, &reason).await,
        Err(MutationError::LostClaim)
    ));
    adapter.ack(row_id, reclaimed.claim_token()).await?;
    assert!(matches!(
        adapter.ack(row_id, reclaimed.claim_token()).await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Delivered
        })
    ));
    for mutation in ["renew", "retry", "release", "quarantine"] {
        let result = match mutation {
            "renew" => {
                adapter
                    .renew(
                        row_id,
                        reclaimed.claim_token(),
                        Lease::new(std::time::Duration::from_secs(1))?,
                    )
                    .await
            }
            "retry" => {
                adapter
                    .retry(
                        row_id,
                        reclaimed.claim_token(),
                        &failure,
                        Delay::new(std::time::Duration::ZERO)?,
                    )
                    .await
            }
            "release" => {
                adapter
                    .release(
                        row_id,
                        reclaimed.claim_token(),
                        Delay::new(std::time::Duration::ZERO)?,
                    )
                    .await
            }
            "quarantine" => {
                adapter
                    .quarantine(row_id, reclaimed.claim_token(), &reason)
                    .await
            }
            _ => unreachable!(),
        };
        assert!(
            matches!(
                result,
                Err(MutationError::IllegalTransition {
                    state: DeliveryState::Delivered
                })
            ),
            "{mutation} did not classify delivered row"
        );
    }

    assert!(matches!(
        adapter
            .ack(dovecote::RowId::new(i64::MAX)?, reclaimed.claim_token())
            .await,
        Err(MutationError::NotFound)
    ));
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_lifecycle_mutations_use_database_time_and_preserve_fields()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("lifecycle")).await?;
    let ack_id = enqueue_committed(&pool, event("lifecycle-ack")).await?;
    let lease = Lease::new(std::time::Duration::from_secs(5))?;
    let claim = adapter
        .claim(WorkerId::new("lifecycle-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let token = claim.claim_token().clone();

    // Make the original expiry close to the database clock, then prove renew
    // is computed from the operation clock rather than from the old expiry.
    query("UPDATE dovecote_deliveries SET claim_expires_at = UTC_TIMESTAMP(6) + INTERVAL 1 SECOND WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    adapter.renew(row_id, &token, lease).await?;
    let renewed_expiry: time::OffsetDateTime =
        query_scalar("SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await?;
    let now: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    assert!(renewed_expiry > now + time::Duration::seconds(4));

    let failure = Failure::new("transport_unavailable", "temporary")?;
    let before_retry: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    adapter
        .retry(
            row_id,
            &token,
            &failure,
            Delay::new(std::time::Duration::from_secs(5))?,
        )
        .await?;
    let retry_row: RetryRow = query_as(
        "SELECT state, available_at, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(retry_row.state, b"pending");
    assert!(retry_row.available_at > before_retry);
    assert!(
        retry_row.claim_token.is_none()
            && retry_row.claimed_by.is_none()
            && retry_row.claim_expires_at.is_none()
    );
    assert_eq!(
        retry_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        retry_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    // Advance the fixture with database time so no wall-clock sleep is part
    // of the conformance test.
    query("UPDATE dovecote_deliveries SET available_at = UTC_TIMESTAMP(6) WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    let reclaimed = adapter
        .claim(WorkerId::new("release-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let release_token = reclaimed.claim_token().clone();
    let before_release: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    adapter
        .release(
            row_id,
            &release_token,
            Delay::new(std::time::Duration::from_secs(5))?,
        )
        .await?;
    let release_row: ReleaseRow = query_as(
        "SELECT state, available_at, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(release_row.state, b"pending");
    assert!(release_row.available_at > before_release);
    assert!(
        release_row.claim_token.is_none()
            && release_row.claimed_by.is_none()
            && release_row.claim_expires_at.is_none()
    );
    assert_eq!(
        release_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        release_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    query("UPDATE dovecote_deliveries SET available_at = UTC_TIMESTAMP(6) WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    let final_claim = adapter
        .claim(WorkerId::new("quarantine-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let reason = QuarantineReason::new("operator_review")?;
    adapter
        .quarantine(row_id, final_claim.claim_token(), &reason)
        .await?;
    let quarantine_row: QuarantineRow = query_as(
        "SELECT state, claim_token, claimed_by, claim_expires_at, quarantined_at, quarantine_reason, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(quarantine_row.state, b"quarantined");
    assert!(quarantine_row.claim_token.is_none());
    assert!(quarantine_row.claimed_by.is_none() && quarantine_row.claim_expires_at.is_none());
    assert!(quarantine_row.quarantined_at.is_some());
    assert_eq!(
        quarantine_row.quarantine_reason.as_deref(),
        Some(b"operator_review".as_slice())
    );
    assert_eq!(
        quarantine_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        quarantine_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    let ack_claim = adapter
        .claim(WorkerId::new("ack-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    assert_eq!(ack_claim.row_id(), ack_id);
    adapter.ack(ack_id, ack_claim.claim_token()).await?;
    let ack_row: AckRow = query_as(
        "SELECT state, delivered_at, claim_token, claimed_by, claim_expires_at, quarantined_at, quarantine_reason FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(ack_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(ack_row.state, b"delivered");
    assert!(ack_row.delivered_at.is_some());
    assert!(
        ack_row.claim_token.is_none()
            && ack_row.claimed_by.is_none()
            && ack_row.claim_expires_at.is_none()
            && ack_row.quarantined_at.is_none()
            && ack_row.quarantine_reason.is_none()
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_mutation_categories_are_exact_for_pending_terminal_and_missing_rows()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let pending_id = enqueue_committed(&pool, event("categories")).await?;
    let token = ClaimToken::from_bytes([0xC3; dovecote::CLAIM_TOKEN_BYTES]);
    let delay = Delay::new(std::time::Duration::ZERO)?;
    let failure = Failure::new("temporary", "retry")?;
    let reason = QuarantineReason::new("terminal")?;

    assert!(matches!(
        adapter.ack(pending_id, &token).await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Pending
        })
    ));
    assert!(matches!(
        adapter.ack(dovecote::RowId::new(i64::MAX)?, &token).await,
        Err(MutationError::NotFound)
    ));

    let first_claim = adapter
        .claim(
            WorkerId::new("category-first")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    adapter
        .retry(pending_id, first_claim.claim_token(), &failure, delay)
        .await?;
    assert!(matches!(
        adapter
            .release(pending_id, first_claim.claim_token(), delay)
            .await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Pending
        })
    ));

    let second_claim = adapter
        .claim(
            WorkerId::new("category-second")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    adapter
        .quarantine(pending_id, second_claim.claim_token(), &reason)
        .await?;
    for mutation in ["ack", "renew", "retry", "release", "quarantine"] {
        let result = match mutation {
            "ack" => adapter.ack(pending_id, second_claim.claim_token()).await,
            "renew" => {
                adapter
                    .renew(
                        pending_id,
                        second_claim.claim_token(),
                        Lease::new(std::time::Duration::from_secs(1))?,
                    )
                    .await
            }
            "retry" => {
                adapter
                    .retry(pending_id, second_claim.claim_token(), &failure, delay)
                    .await
            }
            "release" => {
                adapter
                    .release(pending_id, second_claim.claim_token(), delay)
                    .await
            }
            "quarantine" => {
                adapter
                    .quarantine(pending_id, second_claim.claim_token(), &reason)
                    .await
            }
            _ => unreachable!(),
        };
        assert!(
            matches!(
                result,
                Err(MutationError::IllegalTransition {
                    state: DeliveryState::Quarantined
                })
            ),
            "{mutation} did not classify quarantined row"
        );
    }
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_attempt_overflow_rolls_back_the_entire_claim_batch() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let first_id = enqueue_committed(&pool, event("overflow-first")).await?;
    let overflow_id = enqueue_committed(&pool, event("overflow-second")).await?;
    query("UPDATE dovecote_deliveries SET attempts = ? WHERE event_row_id = ?")
        .bind(i64::MAX)
        .bind(overflow_id.get())
        .execute(&pool)
        .await?;

    let result = adapter
        .claim(
            WorkerId::new("overflow-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(2)?,
        )
        .await;
    assert!(matches!(
        result,
        Err(ClaimError::CounterOverflow { row_id }) if row_id == overflow_id
    ));

    let first: DeliveryStateRow = query_as(
        "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(first_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        first,
        DeliveryStateRow {
            state: b"pending".to_vec(),
            attempts: 0,
            claim_token: None,
            claimed_by: None,
        }
    );
    let overflow: DeliveryStateRow = query_as(
        "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(overflow_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        overflow,
        DeliveryStateRow {
            state: b"pending".to_vec(),
            attempts: i64::MAX,
            claim_token: None,
            claimed_by: None,
        }
    );
    pool.close().await;
    Ok(())
}
