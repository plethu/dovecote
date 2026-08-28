use super::support::*;

#[tokio::test]
async fn tenant_handles_isolate_page_claim_import_finalize_and_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let root = MySqlDovecote::new(pool.clone());
    let tenant_a = root.for_tenant(TenantId::new("tenant-a")?);
    let tenant_b = root.for_tenant(TenantId::new("tenant-b")?);

    let event_a = event("tenant-a-isolation");
    let event_b = event_a.clone();
    let row_a = {
        let mut tx = pool.begin().await?;
        let outcome = tenant_a.enqueue(&mut tx, event_a.clone()).await?;
        tx.commit().await?;
        match outcome {
            EnqueueOutcome::Enqueued { row_id } => row_id,
            other => return Err(format!("expected tenant-a insert, got {other:?}").into()),
        }
    };
    let row_b = {
        let mut tx = pool.begin().await?;
        let outcome = tenant_b.enqueue(&mut tx, event_b).await?;
        tx.commit().await?;
        match outcome {
            EnqueueOutcome::Enqueued { row_id } => row_id,
            other => return Err(format!("expected tenant-b insert, got {other:?}").into()),
        }
    };
    assert_ne!(row_a, row_b);

    assert_eq!(tenant_a.page(None, Limit::new(10)?).await?.len(), 1);
    assert_eq!(tenant_b.page(None, Limit::new(10)?).await?.len(), 1);
    assert_eq!(root.admin().page(None, Limit::new(10)?).await?.len(), 2);

    let claim = tenant_a
        .claim(
            WorkerId::new("tenant-a-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .pop()
        .ok_or("tenant-a claim missing")?;
    assert_eq!(claim.tenant_id(), &TenantId::new("tenant-a")?);
    assert!(tenant_b.ack(row_a, claim.claim_token()).await.is_err());
    tenant_a.ack(row_a, claim.claim_token()).await?;

    let mut tx = pool.begin().await?;
    let import = tenant_b
        .import_for_migration(&mut tx, event_a, ImportedDeliveryState::pending())
        .await;
    assert_eq!(import?, ImportOutcome::AlreadyImported { row_id: row_b });
    tx.commit().await?;

    let mut tx = pool.begin().await?;
    let finalize = tenant_b
        .finalize_pending_delivery_for_migration(&mut tx, row_a, time::OffsetDateTime::UNIX_EPOCH)
        .await;
    assert!(matches!(
        finalize,
        Err(dovecote_sqlx_mysql::FinalizeError::NotFound)
    ));
    tx.rollback().await?;
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    Ok(())
}
