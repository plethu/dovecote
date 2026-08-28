use super::support::*;

#[tokio::test]
async fn concurrent_claims_do_not_overlap_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter_a =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let adapter_b =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "concurrent").await?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;
        let (a, b) = tokio::join!(
            adapter_a.claim(WorkerId::new("worker-a")?, lease, Limit::new(1)?),
            adapter_b.claim(WorkerId::new("worker-b")?, lease, Limit::new(1)?),
        );
        let a = a?;
        let b = b?;
        assert_eq!(
            a.iter().filter(|claim| claim.row_id() == row_id).count()
                + b.iter().filter(|claim| claim.row_id() == row_id).count(),
            1
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn skip_locked_claims_later_rows_and_releases_on_rollback_or_commit_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_id = enqueue_committed(&database, "locked-first").await?;
        let second_id = enqueue_committed(&database, "locked-second").await?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;

        let mut locker = database.pool.begin().await?;
        query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = $1 FOR UPDATE")
            .bind(first_id.get())
            .fetch_one(&mut *locker)
            .await?;
        let started = std::time::Instant::now();
        let later = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            adapter.claim(WorkerId::new("skip-locked")?, lease, Limit::new(1)?),
        )
        .await??;
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].row_id(), second_id);
        locker.rollback().await?;

        // The rolled-back lock did not mutate the first row; it becomes the
        // next claimable row immediately.
        let first_claim = adapter
            .claim(WorkerId::new("after-rollback")?, lease, Limit::new(1)?)
            .await?
            .remove(0);
        assert_eq!(first_claim.row_id(), first_id);
        adapter
            .release(
                first_id,
                first_claim.claim_token(),
                Delay::new(std::time::Duration::ZERO)?,
            )
            .await?;

        let mut committed_locker = database.pool.begin().await?;
        query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = $1 FOR UPDATE")
            .bind(first_id.get())
            .fetch_one(&mut *committed_locker)
            .await?;
        committed_locker.commit().await?;
        let after_commit = adapter
            .claim(WorkerId::new("after-commit")?, lease, Limit::new(1)?)
            .await?
            .remove(0);
        assert_eq!(after_commit.row_id(), first_id);
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn blocked_ack_and_renew_cannot_revive_an_expired_claim_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "blocked-expiry").await?;
        let short_lease = Lease::new(std::time::Duration::from_millis(60))?;
        let initial_claim = adapter
            .claim(
                WorkerId::new("blocked-worker")?,
                short_lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let initial_token = initial_claim.claim_token().clone();

        // Hold the row lock while the claim's short lease expires. The ack
        // must take its database time only after this lock is released.
        let mut locker = database.pool.begin().await?;
        query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = $1 FOR UPDATE")
            .bind(row_id.get())
            .fetch_one(&mut *locker)
            .await?;
        let ack_adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let ack_token = initial_token.clone();
        let ack_task = tokio::spawn(async move { ack_adapter.ack(row_id, &ack_token).await });
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        locker.rollback().await?;
        assert!(matches!(ack_task.await?, Err(MutationError::LostClaim)));

        let after_ack: (String, Option<Vec<u8>>, Option<String>, Option<time::OffsetDateTime>) =
            query_as(
                "SELECT state, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
            )
            .bind(row_id.get())
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(after_ack.0, "claimed");
        assert_eq!(after_ack.1, Some(initial_token.as_bytes().to_vec()));
        assert_eq!(after_ack.2.as_deref(), Some("blocked-worker"));
        assert!(after_ack.3.expect("claimed expiry") <= query_scalar::<_, time::OffsetDateTime>(
            "SELECT clock_timestamp()",
        )
        .fetch_one(&database.pool)
        .await?);

        // Reclaim the expired row, then repeat the lock/expiry race for renew.
        let reclaimed = adapter
            .claim(
                WorkerId::new("renew-blocked-worker")?,
                short_lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let reclaimed_token = reclaimed.claim_token().clone();
        assert_ne!(reclaimed_token, initial_token);
        let mut renew_locker = database.pool.begin().await?;
        query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = $1 FOR UPDATE")
            .bind(row_id.get())
            .fetch_one(&mut *renew_locker)
            .await?;
        let renew_adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let renew_token = reclaimed_token.clone();
        let renew_task = tokio::spawn(async move {
            renew_adapter
                .renew(row_id, &renew_token, short_lease)
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        renew_locker.rollback().await?;
        assert!(matches!(renew_task.await?, Err(MutationError::LostClaim)));

        let after_renew: (String, Option<Vec<u8>>, Option<String>, Option<time::OffsetDateTime>) =
            query_as(
                "SELECT state, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
            )
            .bind(row_id.get())
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(after_renew.0, "claimed");
        assert_eq!(after_renew.1, Some(reclaimed_token.as_bytes().to_vec()));
        assert_eq!(after_renew.2.as_deref(), Some("renew-blocked-worker"));
        assert!(after_renew.3.expect("reclaimed expiry") <= query_scalar::<_, time::OffsetDateTime>(
            "SELECT clock_timestamp()",
        )
        .fetch_one(&database.pool)
        .await?);
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
