use super::support::*;

#[tokio::test]
async fn enqueue_is_transactional_and_idempotent_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = exercise(&database).await;
    database.cleanup().await?;
    result
}

async fn enqueue_committed(
    database: &IsolatedDatabase,
    event_id: &str,
) -> Result<dovecote::RowId, Box<dyn Error>> {
    enqueue_event_committed(database, event(event_id, "com.example.lifecycle")).await
}

async fn enqueue_event_committed(
    database: &IsolatedDatabase,
    event: NewEvent,
) -> Result<dovecote::RowId, Box<dyn Error>> {
    let mut transaction = database.pool.begin().await?;
    let outcome = enqueue_for_test_tenant(database, &mut transaction, event).await?;
    transaction.commit().await?;
    match outcome {
        EnqueueOutcome::Enqueued { row_id } => Ok(row_id),
        EnqueueOutcome::AlreadyEnqueued { row_id } => Ok(row_id),
        _ => Err("unexpected enqueue outcome".into()),
    }
}

#[tokio::test]
async fn paging_is_ordered_bounded_and_includes_every_delivery_state_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let delivered_id = enqueue_committed(&database, "page-delivered").await?;
        let delivered = adapter
            .claim(
                WorkerId::new("page-delivered-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        adapter.ack(delivered_id, delivered.claim_token()).await?;

        // A rolled-back insert consumes a sequence value and proves that page
        // cursors preserve gaps rather than treating row IDs as dense indexes.
        let skipped_id = {
            let mut transaction = database.pool.begin().await?;
            let outcome = enqueue_for_test_tenant(
                &database,
                &mut transaction,
                event("page-skipped", "com.example.page"),
            )
            .await?;
            let row_id = match outcome {
                EnqueueOutcome::Enqueued { row_id } => row_id,
                other => return Err(format!("expected skipped insert, got {other:?}").into()),
            };
            transaction.rollback().await?;
            row_id
        };

        let claimed_id = enqueue_committed(&database, "page-claimed").await?;
        let claimed = adapter
            .claim(
                WorkerId::new("page-claimed-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(claimed.row_id(), claimed_id);

        let quarantined_id = enqueue_committed(&database, "page-quarantined").await?;
        let quarantined = adapter
            .claim(
                WorkerId::new("page-quarantine-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(quarantined.row_id(), quarantined_id);
        adapter
            .quarantine(
                quarantined_id,
                quarantined.claim_token(),
                &dovecote::QuarantineReason::new("page-test")?,
            )
            .await?;

        let pending_id = enqueue_committed(&database, "page-pending").await?;
        let limit = Limit::new(2)?;
        let first = adapter.page(None, limit).await?;
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].row_id(), delivered_id);
        assert_eq!(first[1].row_id(), claimed_id);
        assert_eq!(first[0].delivery().state(), DeliveryState::Delivered);
        assert_eq!(first[1].delivery().state(), DeliveryState::Claimed);

        let second = adapter
            .page(first.last().map(|row| row.row_id()), limit)
            .await?;
        assert_eq!(second.len(), 2);
        assert_eq!(second[0].row_id(), quarantined_id);
        assert_eq!(second[1].row_id(), pending_id);
        assert_eq!(second[0].delivery().state(), DeliveryState::Quarantined);
        assert_eq!(second[1].delivery().state(), DeliveryState::Pending);

        let repeated = adapter.page(None, Limit::new(100)?).await?;
        assert_eq!(
            repeated.iter().map(|row| row.row_id()).collect::<Vec<_>>(),
            vec![delivered_id, claimed_id, quarantined_id, pending_id]
        );
        assert!(!repeated.iter().any(|row| row.row_id() == skipped_id));
        assert_eq!(adapter.page(Some(pending_id), limit).await?, Vec::new());
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn paging_surfaces_an_event_without_a_delivery_in_live_and_snapshot_reads_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "page-orphan").await?;
        query("DELETE FROM dovecote_deliveries WHERE event_row_id = $1")
            .bind(row_id.get())
            .execute(&database.pool)
            .await?;

        let live = adapter.page(None, Limit::new(10)?).await;
        assert!(matches!(
            live,
            Err(PageError::Serialization { detail })
                if detail == format!("event row {} has no required delivery row", row_id.get())
        ));

        let mut snapshot = adapter.begin_snapshot().await?;
        let snapshot_page = snapshot.next_page(Limit::new(10)?).await;
        assert!(matches!(
            snapshot_page,
            Err(PageError::Serialization { detail })
                if detail == format!("event row {} has no required delivery row", row_id.get())
        ));
        snapshot.rollback().await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn schema_check_rejects_an_extra_not_null_column_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        query("ALTER TABLE dovecote_events ADD COLUMN schema_probe TEXT NOT NULL")
            .execute(&database.pool)
            .await?;
        let error = check_schema(&database.pool).await;
        assert!(matches!(
            error,
            Err(SchemaError::MigrationMismatch { detail })
                if detail == "unexpected column dovecote_events.schema_probe"
        ));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn schema_check_rejects_an_extra_constraint_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        query(
            "ALTER TABLE dovecote_events ADD CONSTRAINT dovecote_events_schema_probe CHECK (row_id > 0)",
        )
        .execute(&database.pool)
        .await?;
        let error = check_schema(&database.pool).await;
        assert!(matches!(
            error,
            Err(SchemaError::MigrationMismatch { detail })
                if detail == "unexpected constraint dovecote_events_schema_probe"
        ));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn schema_check_rejects_an_extra_index_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        query("CREATE INDEX dovecote_events_schema_probe ON dovecote_events (occurred_at)")
            .execute(&database.pool)
            .await?;
        let error = check_schema(&database.pool).await;
        assert!(matches!(
            error,
            Err(SchemaError::MigrationMismatch { detail })
                if detail == "unexpected index dovecote_events_schema_probe"
        ));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn snapshot_paging_is_stable_and_releases_its_transaction_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_id = enqueue_committed(&database, "snapshot-first").await?;
        let second_id = enqueue_committed(&database, "snapshot-second").await?;

        let mut pager = adapter.begin_snapshot().await?;
        assert_eq!(pager.upper_bound(), Some(second_id));
        assert_eq!(pager.cursor(), None);
        let first_page = pager.next_page(Limit::new(1)?).await?;
        assert_eq!(first_page.len(), 1);
        assert_eq!(first_page[0].row_id(), first_id);
        assert_eq!(pager.cursor(), Some(first_id));
        assert!(!pager.is_exhausted());

        let outside_snapshot = enqueue_committed(&database, "snapshot-after-start").await?;
        let second_page = pager.next_page(Limit::new(1)?).await?;
        assert_eq!(second_page.len(), 1);
        assert_eq!(second_page[0].row_id(), second_id);
        assert!(pager.is_exhausted());
        assert_eq!(pager.next_page(Limit::new(1)?).await?, Vec::new());
        pager.finish().await?;

        let live = adapter.page(None, Limit::new(100)?).await?;
        assert!(live.iter().any(|row| row.row_id() == outside_snapshot));

        let mut rollback_pager = adapter.begin_snapshot().await?;
        assert!(rollback_pager.next_page(Limit::new(100)?).await?.len() >= 3);
        rollback_pager.close().await?;
        // A released pager must not strand its pool connection or transaction.
        assert_eq!(adapter.page(None, Limit::new(1)?).await?.len(), 1);
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn snapshot_pager_release_paths_free_a_single_pool_connection_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        enqueue_committed(&database, "snapshot-release").await?;
        let single = single_connection_pool(&database).await?;
        let adapter =
            PostgresDovecote::new(single.clone()).for_tenant(TenantId::new("test").unwrap());

        let finished = adapter.begin_snapshot().await?;
        assert!(single.try_acquire().is_none());
        finished.finish().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let rolled_back = adapter.begin_snapshot().await?;
        assert!(single.try_acquire().is_none());
        rolled_back.rollback().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let closable = adapter.begin_snapshot().await?;
        assert!(single.try_acquire().is_none());
        closable.close().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let dropped = adapter.begin_snapshot().await?;
        assert!(single.try_acquire().is_none());
        drop(dropped);
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        single.close().await;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn paging_corruption_is_a_typed_serialization_error_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = enqueue_committed(&database, "page-corrupt").await?;
        query("UPDATE dovecote_events SET extensions = '{\"bad\": 1}' WHERE row_id = $1")
            .bind(row_id.get())
            .execute(&database.pool)
            .await?;
        assert!(matches!(
            adapter.page(None, Limit::new(1)?).await,
            Err(dovecote_sqlx_postgres::PageError::Serialization { .. })
        ));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn commit_inversion_exposes_live_limitation_and_snapshot_boundary_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let visible_first = enqueue_committed(&database, "inversion-visible-first").await?;
        let visible_second = enqueue_committed(&database, "inversion-visible-second").await?;
        let visible_third = enqueue_committed(&database, "inversion-visible-third").await?;

        // Hold the lower sequence value uncommitted while the later value is
        // committed. This is the barrier controlling the commit inversion.
        let mut earlier_transaction = database.pool.begin().await?;
        let earlier = enqueue_for_test_tenant(
            &database,
            &mut earlier_transaction,
            event("inversion-earlier", "com.example.page"),
        )
        .await?;
        let earlier_id = match earlier {
            EnqueueOutcome::Enqueued { row_id } => row_id,
            other => return Err(format!("expected earlier insert, got {other:?}").into()),
        };

        let later_id = enqueue_committed(&database, "inversion-later").await?;
        assert!(earlier_id < later_id);

        // Establish both observations before releasing the earlier commit.
        let mut snapshot = adapter.begin_snapshot().await?;
        assert_eq!(snapshot.upper_bound(), Some(later_id));
        assert_eq!(snapshot.cursor(), None);
        let live_before = adapter.page(None, Limit::new(100)?).await?;
        assert_eq!(
            live_before
                .iter()
                .map(|row| row.row_id())
                .collect::<Vec<_>>(),
            vec![visible_first, visible_second, visible_third, later_id]
        );

        earlier_transaction.commit().await?;

        // Advancing the live cursor past the later row misses the row that
        // committed later despite its lower allocated row ID.
        assert_eq!(
            adapter.page(Some(later_id), Limit::new(100)?).await?,
            Vec::new()
        );

        // The snapshot sees exactly the rows visible when it began and does
        // not gain the earlier row after its commit. Use multiple pages so
        // cursor advancement and the fixed upper bound are both exercised.
        let first_snapshot_page = snapshot.next_page(Limit::new(2)?).await?;
        assert_eq!(
            first_snapshot_page
                .iter()
                .map(|row| row.row_id())
                .collect::<Vec<_>>(),
            vec![visible_first, visible_second]
        );
        assert_eq!(snapshot.cursor(), Some(visible_second));
        assert_eq!(snapshot.upper_bound(), Some(later_id));
        assert!(!snapshot.is_exhausted());

        let second_snapshot_page = snapshot.next_page(Limit::new(2)?).await?;
        assert_eq!(
            second_snapshot_page
                .iter()
                .map(|row| row.row_id())
                .collect::<Vec<_>>(),
            vec![visible_third, later_id]
        );
        assert_eq!(snapshot.cursor(), Some(later_id));
        assert!(snapshot.is_exhausted());
        assert_eq!(snapshot.next_page(Limit::new(2)?).await?, Vec::new());
        assert_eq!(snapshot.cursor(), Some(later_id));
        snapshot.rollback().await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
