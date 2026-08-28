use super::support::*;

#[tokio::test]
async fn paging_surfaces_an_event_without_a_delivery_in_live_and_snapshot_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("page-orphan")).await?;
    query("DELETE FROM dovecote_deliveries WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;

    let live = adapter.page(None, dovecote::Limit::new(10)?).await;
    assert!(matches!(
        live,
        Err(PageError::Serialization { detail })
            if detail == format!("event row {} has no required delivery row", row_id.get())
    ));

    let mut snapshot = adapter.begin_snapshot().await?;
    let snapshot_page = snapshot.next_page(dovecote::Limit::new(10)?).await;
    assert!(matches!(
        snapshot_page,
        Err(PageError::Serialization { detail })
            if detail == format!("event row {} has no required delivery row", row_id.get())
    ));
    snapshot.rollback().await?;
    query("DELETE FROM dovecote_events WHERE row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn matrix_enqueue_claim_ack_and_snapshot_commit() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter
        .enqueue(&mut transaction, event("claim-ack"))
        .await?;
    transaction.commit().await?;
    let row_id = match outcome {
        EnqueueOutcome::Enqueued { row_id } => row_id,
        EnqueueOutcome::AlreadyEnqueued { row_id } => row_id,
        _ => return Err("unknown enqueue outcome".into()),
    };
    // Exercise the row-id trigger without a delivery FK participating in the
    // UPDATE: this row is a complete, valid event intentionally lacking a
    // companion delivery.
    // Keep the valid probe adjacent to the allocated row.  A very large
    // explicit AUTO_INCREMENT value changes the server's next generated ID
    // and can contaminate later isolated conformance cases, especially on
    // MariaDB.  The trigger contract only requires a positive immutable row.
    let direct_id = row_id.get() + 1;
    sqlx::query("INSERT INTO dovecote_events (row_id, tenant_id, stream, specversion, event_id, source, event_type, extensions) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(direct_id).bind(&b"test"[..]).bind(b"mysql-conformance".as_slice()).bind(b"1.0".as_slice())
        .bind(&b"direct-immutable"[..]).bind(&b"https://dovecote.test/mysql"[..]).bind(&b"conformance.event"[..]).bind(&b"{}"[..]).execute(&pool).await?;
    assert!(
        sqlx::query("UPDATE dovecote_events SET row_id = ? WHERE row_id = ?")
            .bind(direct_id + 1)
            .bind(direct_id)
            .execute(&pool)
            .await
            .is_err()
    );
    let unchanged: i64 = sqlx::query_scalar("SELECT row_id FROM dovecote_events WHERE row_id = ?")
        .bind(direct_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(unchanged, direct_id);
    assert!(
        sqlx::query("UPDATE dovecote_events SET row_id = row_id + 1000000 WHERE row_id = ?")
            .bind(row_id.get())
            .execute(&pool)
            .await
            .is_err()
    );
    let unchanged: i64 = sqlx::query_scalar("SELECT row_id FROM dovecote_events WHERE row_id = ?")
        .bind(row_id.get())
        .fetch_one(&pool)
        .await?;
    assert_eq!(unchanged, row_id.get());
    query("DELETE FROM dovecote_events WHERE row_id = ?")
        .bind(direct_id)
        .execute(&pool)
        .await?;
    let worker = WorkerId::new("mysql-worker")?;
    let claimed = adapter
        .claim(
            worker,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?;
    assert_eq!(claimed.len(), 1);
    adapter.ack(row_id, claimed[0].claim_token()).await?;
    let mut snapshot = adapter.begin_snapshot().await?;
    let page = snapshot.next_page(Limit::new(10)?).await?;
    assert!(page.iter().any(|event| event.row_id() == row_id));
    snapshot.finish().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_transaction_rollback_and_idempotency_are_atomic() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());

    let mut transaction = pool.begin().await?;
    adapter.enqueue(&mut transaction, event("rollback")).await?;
    transaction.rollback().await?;
    let count: i64 = query_scalar("SELECT COUNT(*) FROM dovecote_events WHERE stream = ?")
        .bind(b"mysql-conformance".as_slice())
        .fetch_one(&pool)
        .await?;
    let delivery_count: i64 = query_scalar(
        "SELECT COUNT(*) FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.stream = ?",
    )
    .bind(b"mysql-conformance".as_slice())
    .fetch_one(&pool)
    .await?;
    assert_eq!((count, delivery_count), (0, 0));

    let row_id = enqueue_committed(&pool, event("idempotent")).await?;
    let mut replay_transaction = pool.begin().await?;
    let replay = adapter
        .enqueue(&mut replay_transaction, event("idempotent"))
        .await?;
    assert_eq!(replay, EnqueueOutcome::AlreadyEnqueued { row_id });
    replay_transaction.commit().await?;

    let mut conflict_transaction = pool.begin().await?;
    let conflict = adapter
        .enqueue(
            &mut conflict_transaction,
            event_with_type("idempotent", "conformance.other"),
        )
        .await;
    assert!(matches!(
        conflict,
        Err(EnqueueError::IdempotencyConflict { existing_row_id }) if existing_row_id == row_id
    ));
    conflict_transaction.rollback().await?;
    let count: i64 = query_scalar("SELECT COUNT(*) FROM dovecote_events WHERE stream = ?")
        .bind(b"mysql-conformance".as_slice())
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);
    pool.close().await;
    Ok(())
}
