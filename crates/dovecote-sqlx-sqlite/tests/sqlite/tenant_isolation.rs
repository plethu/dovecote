use super::test_support::*;

#[tokio::test]
async fn tenant_handles_isolate_page_claim_import_finalize_and_mutation() {
    let pool = database().await;
    let root = SqliteDovecote::new(pool.clone());
    let tenant_a = root.for_tenant(TenantId::new("tenant-a").unwrap());
    let tenant_b = root.for_tenant(TenantId::new("tenant-b").unwrap());

    let event_a = event("tenant-a-event");
    let event_b = event_a.clone();
    let row_a = {
        let mut tx = tenant_a.begin_write().await.unwrap();
        let outcome = tenant_a.enqueue(&mut tx, event_a.clone()).await.unwrap();
        tx.commit().await.unwrap();
        match outcome {
            EnqueueOutcome::Enqueued { row_id } => row_id,
            other => panic!("expected tenant-a insert, got {other:?}"),
        }
    };
    let row_b = {
        let mut tx = tenant_b.begin_write().await.unwrap();
        let outcome = tenant_b.enqueue(&mut tx, event_b.clone()).await.unwrap();
        tx.commit().await.unwrap();
        match outcome {
            EnqueueOutcome::Enqueued { row_id } => row_id,
            other => panic!("expected tenant-b insert, got {other:?}"),
        }
    };
    assert_ne!(row_a, row_b);

    assert_eq!(
        tenant_a
            .page(None, Limit::new(10).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        tenant_b
            .page(None, Limit::new(10).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        root.admin()
            .page(None, Limit::new(10).unwrap())
            .await
            .unwrap()
            .len(),
        2
    );

    let claim = tenant_a
        .claim(
            WorkerId::new("tenant-a-worker").unwrap(),
            Lease::new(Duration::from_secs(30)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .expect("tenant-a claim");
    assert_eq!(*claim.tenant_id(), TenantId::new("tenant-a").unwrap());
    assert!(tenant_b.ack(row_a, claim.claim_token()).await.is_err());
    tenant_a.ack(row_a, claim.claim_token()).await.unwrap();

    let mut tx = tenant_b.begin_write().await.unwrap();
    let import = tenant_b
        .import_for_migration(&mut tx, event_a, ImportedDeliveryState::pending())
        .await;
    assert_eq!(
        import.unwrap(),
        ImportOutcome::AlreadyImported { row_id: row_b }
    );
    tx.commit().await.unwrap();

    let mut tx = tenant_b.begin_write().await.unwrap();
    let finalize = tenant_b
        .finalize_pending_delivery_for_migration(&mut tx, row_a, time::OffsetDateTime::UNIX_EPOCH)
        .await;
    assert!(matches!(
        finalize,
        Err(dovecote_sqlx_sqlite::FinalizeError::NotFound)
    ));
    tx.rollback().await.unwrap();
}
