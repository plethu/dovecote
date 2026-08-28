use super::support::*;

#[derive(Clone, Copy, Debug)]
enum ExpectedMutationClassification {
    NotFound,
    IllegalTransition(DeliveryState),
    LostClaim,
}

fn assert_mutation_classification(
    operation: &str,
    result: Result<(), MutationError>,
    expected: ExpectedMutationClassification,
) -> Result<(), Box<dyn Error>> {
    match result {
        Err(MutationError::NotFound)
            if matches!(expected, ExpectedMutationClassification::NotFound) =>
        {
            Ok(())
        }
        Err(MutationError::LostClaim)
            if matches!(expected, ExpectedMutationClassification::LostClaim) =>
        {
            Ok(())
        }
        Err(MutationError::IllegalTransition { state }) if matches!(expected, ExpectedMutationClassification::IllegalTransition(expected_state) if expected_state == state) => {
            Ok(())
        }
        other => Err(format!("{operation} returned {other:?}, expected {expected:?}").into()),
    }
}

async fn assert_all_mutation_classifications(
    adapter: &TenantDovecote,
    row_id: RowId,
    token: &ClaimToken,
    expected: ExpectedMutationClassification,
) -> Result<(), Box<dyn Error>> {
    let failure = Failure::new("classification", "classification detail")?;
    let reason = QuarantineReason::new("classification reason")?;
    let lease = Lease::new(std::time::Duration::from_secs(5))?;
    let delay = Delay::new(std::time::Duration::ZERO)?;
    assert_mutation_classification("renew", adapter.renew(row_id, token, lease).await, expected)?;
    assert_mutation_classification("ack", adapter.ack(row_id, token).await, expected)?;
    assert_mutation_classification(
        "retry",
        adapter.retry(row_id, token, &failure, delay).await,
        expected,
    )?;
    assert_mutation_classification(
        "release",
        adapter.release(row_id, token, delay).await,
        expected,
    )?;
    assert_mutation_classification(
        "quarantine",
        adapter.quarantine(row_id, token, &reason).await,
        expected,
    )?;
    Ok(())
}

#[tokio::test]
async fn every_mutation_classifies_missing_and_non_claimed_states_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let token = ClaimToken::from_bytes([0x5a; dovecote::CLAIM_TOKEN_BYTES]);
        let delivered = enqueue_committed(&database, "classification-delivered").await?;
        let delivered_claim = adapter
            .claim(
                WorkerId::new("classification-delivered")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        adapter
            .ack(delivered, delivered_claim.claim_token())
            .await?;
        assert_all_mutation_classifications(
            &adapter,
            delivered,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Delivered),
        )
        .await?;

        // Keep this row unclaimed while the other fixtures are staged; the
        // claim API intentionally takes the lowest eligible row ID.
        let claimed = enqueue_committed(&database, "classification-claimed").await?;
        let claimed_event = adapter
            .claim(
                WorkerId::new("classification-claimed")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_all_mutation_classifications(
            &adapter,
            claimed,
            &token,
            ExpectedMutationClassification::LostClaim,
        )
        .await?;
        assert!(
            adapter
                .ack(claimed, claimed_event.claim_token())
                .await
                .is_ok()
        );

        let quarantined = enqueue_committed(&database, "classification-quarantined").await?;
        let quarantined_claim = adapter
            .claim(
                WorkerId::new("classification-quarantined")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        adapter
            .quarantine(
                quarantined,
                quarantined_claim.claim_token(),
                &QuarantineReason::new("classification")?,
            )
            .await?;
        assert_all_mutation_classifications(
            &adapter,
            quarantined,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Quarantined),
        )
        .await?;

        let pending = enqueue_committed(&database, "classification-pending").await?;
        assert_all_mutation_classifications(
            &adapter,
            pending,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Pending),
        )
        .await?;

        let missing = RowId::new(i64::MAX)?;
        assert_all_mutation_classifications(
            &adapter,
            missing,
            &token,
            ExpectedMutationClassification::NotFound,
        )
        .await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn crash_before_claim_commit_exposes_no_claim_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "crash-before-claim").await?;
        let key = advisory_key(&database.schema);
        let mut barrier = database.admin.begin().await?;
        query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *barrier)
            .await?;

        install_trigger(
            &database,
            "dovecote_test_pause_claim",
            "dovecote_test_pause_claim",
            &format!(
                "IF NEW.state = 'claimed' AND OLD.state <> 'claimed' THEN PERFORM pg_advisory_xact_lock({key}); END IF; RETURN NEW;"
            ),
        )
        .await?;

        let claim_task = tokio::spawn(async move {
            adapter
                .claim(
                    WorkerId::new("crashed-before-commit").expect("valid worker"),
                    Lease::new(std::time::Duration::from_secs(5)).expect("valid lease"),
                    Limit::new(1).expect("valid limit"),
                )
                .await
        });
        let pid = match wait_for_active_query(
            &database.admin,
            &application_name(&database.schema),
        )
        .await
        {
            Ok(pid) => pid,
            Err(error) => {
                barrier.rollback().await?;
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), claim_task).await;
                remove_trigger(
                    &database,
                    "dovecote_test_pause_claim",
                    "dovecote_test_pause_claim",
                )
                .await?;
                return Err(error);
            }
        };
        assert!(query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&database.admin)
            .await?);
        let claim_result = tokio::time::timeout(std::time::Duration::from_secs(2), claim_task)
            .await??;
        assert!(claim_result.is_err(), "terminated claim unexpectedly succeeded");
        barrier.rollback().await?;
        remove_trigger(
            &database,
            "dovecote_test_pause_claim",
            "dovecote_test_pause_claim",
        )
        .await?;

        let stored: (String, i64, Option<Vec<u8>>, Option<String>, Option<time::OffsetDateTime>) =
            query_as(
                "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
            )
            .bind(row_id.get())
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(stored, ("pending".to_owned(), 0, None, None, None));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
