use super::support::*;
use std::time::Duration;
use tokio::{sync::oneshot, time::timeout};

#[derive(sqlx::FromRow)]
struct CanonicalImportedDelivery {
    state: Vec<u8>,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<time::PrimitiveDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
    delivered_at: Option<time::PrimitiveDateTime>,
    quarantined_at: Option<time::PrimitiveDateTime>,
    quarantine_reason: Option<Vec<u8>>,
    available_at: time::PrimitiveDateTime,
    enqueued_at: time::PrimitiveDateTime,
}

#[tokio::test]
async fn migration_import_competing_transactions_have_one_canonical_winner()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let mut first_transaction = pool.begin().await?;
    let first = adapter
        .import_for_migration(
            &mut first_transaction,
            event("migration-import-race"),
            ImportedDeliveryState::Pending,
        )
        .await?;
    let row_id = match first {
        ImportOutcome::Imported { row_id } => row_id,
        other => return Err(format!("expected first import, got {other:?}").into()),
    };

    let (before_import, before_import_received) = oneshot::channel();
    let second_adapter = adapter.clone();
    let second_pool = pool.clone();
    let mut second = tokio::spawn(async move {
        let mut transaction = second_pool
            .begin()
            .await
            .map_err(|error| error.to_string())?;
        before_import
            .send(())
            .map_err(|_| "race test receiver dropped".to_owned())?;
        let outcome = second_adapter
            .import_for_migration(
                &mut transaction,
                event("migration-import-race"),
                ImportedDeliveryState::Pending,
            )
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(outcome)
    });
    timeout(Duration::from_secs(2), before_import_received).await??;
    tokio::task::yield_now().await;
    assert!(
        timeout(Duration::from_secs(1), &mut second).await.is_err(),
        "competing importer completed while the first transaction held the unique-key lock"
    );

    // The second importer is blocked in its duplicate-key INSERT. Committing
    // the first transaction releases the row lock; its current reads then
    // resolve the event and delivery rows and return AlreadyImported.
    first_transaction.commit().await?;
    let second = timeout(Duration::from_secs(5), second)
        .await??
        .map_err(|error| format!("competing importer failed: {error}"))?;
    assert_eq!(second, ImportOutcome::AlreadyImported { row_id });

    let counts: (i64, i64) = query_as(
        "SELECT (SELECT count(*) FROM dovecote_events WHERE stream = ?), (SELECT count(*) FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.stream = ?)",
    )
    .bind(b"mysql-conformance".as_slice())
    .bind(b"mysql-conformance".as_slice())
    .fetch_one(&pool)
    .await?;
    assert_eq!(counts, (1, 1));
    let stored: CanonicalImportedDelivery = query_as(
        "SELECT d.state, d.attempts, d.claim_token, d.claimed_by, d.claim_expires_at, d.last_failure_code, d.last_failure_detail, d.delivered_at, d.quarantined_at, d.quarantine_reason, d.available_at, e.enqueued_at FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE d.event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(stored.state, b"pending");
    assert_eq!(stored.attempts, 0);
    assert!(stored.claim_token.is_none());
    assert!(stored.claimed_by.is_none());
    assert!(stored.claim_expires_at.is_none());
    assert!(stored.last_failure_code.is_none());
    assert!(stored.last_failure_detail.is_none());
    assert!(stored.delivered_at.is_none());
    assert!(stored.quarantined_at.is_none());
    assert!(stored.quarantine_reason.is_none());
    assert_eq!(stored.available_at, stored.enqueued_at);
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_import_is_idempotent_and_state_fenced() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let imported = {
        let mut transaction = pool.begin().await?;
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-import"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        transaction.commit().await?;
        result
    };

    let row_id = match imported {
        ImportOutcome::Imported { row_id } => row_id,
        other => return Err(format!("expected imported outcome, got {other:?}").into()),
    };

    let mut transaction = pool.begin().await?;
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-import"),
            ImportedDeliveryState::Pending,
        )
        .await?;
    transaction.commit().await?;
    assert_eq!(replay, ImportOutcome::AlreadyImported { row_id });

    let delivered_at = time::OffsetDateTime::UNIX_EPOCH;
    let delivered_row_id = {
        let mut transaction = pool.begin().await?;
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-delivered"),
                ImportedDeliveryState::Delivered { delivered_at },
            )
            .await?;
        transaction.commit().await?;
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected imported outcome, got {other:?}").into()),
        }
    };

    let stored_delivered_at: time::PrimitiveDateTime =
        query_scalar("SELECT delivered_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(delivered_row_id.get())
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        stored_delivered_at,
        time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(1970, time::Month::January, 1)?,
            time::Time::MIDNIGHT,
        )
    );
    let mut transaction = pool.begin().await?;
    let state_conflict = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-delivered"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(
        state_conflict,
        Err(dovecote_sqlx_mysql::ImportError::ImportConflict { existing_row_id })
            if existing_row_id == delivered_row_id
    ));
    transaction.rollback().await?;

    let mut transaction = pool.begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            event_with_type("migration-import", "com.example.changed"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(
        matches!(conflict, Err(dovecote_sqlx_mysql::ImportError::IdentityConflict { existing_row_id }) if existing_row_id == row_id)
    );
    transaction.rollback().await?;
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_import_rejects_schema_drift_before_event_mutation() -> Result<(), Box<dyn Error>>
{
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let result = async {
        query("CREATE INDEX dovecote_import_unreviewed ON dovecote_events (event_type)")
            .execute(&pool)
            .await?;
        let adapter = MySqlDovecote::new(pool.clone());
        let mut transaction = pool.begin().await?;
        let import = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-schema-drift"),
                ImportedDeliveryState::Pending,
            )
            .await;
        assert!(matches!(
            import,
            Err(dovecote_sqlx_mysql::ImportError::MigrationMismatch { .. })
        ));
        transaction.rollback().await?;
        let event_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&pool)
            .await?;
        let delivery_count: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
            .fetch_one(&pool)
            .await?;
        assert_eq!((event_count, delivery_count), (0, 0));
        query("DROP INDEX dovecote_import_unreviewed ON dovecote_events")
            .execute(&pool)
            .await?;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    result
}

#[tokio::test]
async fn migration_import_rollback_removes_event_and_delivery_together()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-rollback"),
            ImportedDeliveryState::Pending,
        )
        .await?;
    assert!(matches!(outcome, ImportOutcome::Imported { .. }));
    transaction.rollback().await?;
    let event_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await?;
    let delivery_count: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
        .fetch_one(&pool)
        .await?;
    assert_eq!((event_count, delivery_count), (0, 0));
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_import_rejects_changed_available_at_on_replay() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = {
        let mut transaction = pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-available-at"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        transaction.commit().await?;
        match outcome {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected imported outcome, got {other:?}").into()),
        }
    };
    query("UPDATE dovecote_deliveries SET available_at = ? WHERE event_row_id = ?")
        .bind(time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(1970, time::Month::January, 1)?,
            time::Time::MIDNIGHT,
        ))
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-available-at"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(
        replay,
        Err(dovecote_sqlx_mysql::ImportError::ImportConflict { existing_row_id })
            if existing_row_id == row_id
    ));
    transaction.rollback().await?;
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_import_preserves_maximum_delivered_time_and_never_claims_it()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let delivered_at = maximum_timestamp();
    let row_id = {
        let mut transaction = pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-delivered-max"),
                ImportedDeliveryState::Delivered { delivered_at },
            )
            .await?;
        transaction.commit().await?;
        match outcome {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected imported outcome, got {other:?}").into()),
        }
    };

    let stored: time::PrimitiveDateTime =
        query_scalar("SELECT delivered_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        stored,
        time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(9999, time::Month::December, 31)?,
            time::Time::from_hms_micro(23, 59, 59, 999_999)?,
        )
    );
    let mut transaction = pool.begin().await?;
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-delivered-max"),
            ImportedDeliveryState::Delivered { delivered_at },
        )
        .await?;
    transaction.commit().await?;
    assert_eq!(replay, ImportOutcome::AlreadyImported { row_id });
    assert!(
        adapter
            .claim(
                WorkerId::new("migration-claim")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(10)?,
            )
            .await?
            .is_empty()
    );
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_finalization_is_idempotent_fenced_and_transactional()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let result = async {
        let adapter = MySqlDovecote::new(pool.clone());
        let row_id = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };

        let delivered_at = time::OffsetDateTime::UNIX_EPOCH;
        let first = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
                .await?;
            transaction.commit().await?;
            outcome
        };
        assert_eq!(first, FinalizeOutcome::Finalized { row_id });
        let replay = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
                .await?;
            transaction.commit().await?;
            outcome
        };
        assert_eq!(replay, FinalizeOutcome::AlreadyFinalized { row_id });
        let changed = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .finalize_pending_delivery_for_migration(
                    &mut transaction,
                    row_id,
                    delivered_at + time::Duration::seconds(1),
                )
                .await;
            transaction.rollback().await?;
            outcome
        };
        assert!(matches!(
            changed,
            Err(dovecote_sqlx_mysql::FinalizeError::StateConflict { row_id: id })
                if id == row_id
        ));

        let rollback_id = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-rollback"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };

        let mut transaction = pool.begin().await?;
        adapter
            .finalize_pending_delivery_for_migration(&mut transaction, rollback_id, delivered_at)
            .await?;
        transaction.rollback().await?;
        let state: Vec<u8> =
            query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = ?")
                .bind(rollback_id.get())
                .fetch_one(&pool)
                .await?;
        assert_eq!(state, b"pending");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    clear_conformance_rows(&pool).await?;
    pool.close().await;
    result
}

#[tokio::test]
async fn migration_finalization_rejects_noncanonical_rows_and_preflights_schema()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let mut claimable_index_dropped = false;
    let result = async {
        let adapter = MySqlDovecote::new(pool.clone());
        let changed_availability = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-delayed"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };
        query("UPDATE dovecote_deliveries SET available_at = ? WHERE event_row_id = ?")
            .bind(time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(1970, time::Month::January, 1)?,
                time::Time::MIDNIGHT,
            ))
            .bind(changed_availability.get())
            .execute(&pool)
            .await?;
        let mut transaction = pool.begin().await?;
        let conflict = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                changed_availability,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(
            conflict,
            Err(dovecote_sqlx_mysql::FinalizeError::StateConflict { row_id })
                if row_id == changed_availability
        ));
        transaction.rollback().await?;

        let invalid_timestamp = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-invalid-time"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };

        let invalid = time::OffsetDateTime::UNIX_EPOCH
            .replace_nanosecond(1)
            .expect("valid nanosecond");
        let mut transaction = pool.begin().await?;
        let invalid_result = adapter
            .finalize_pending_delivery_for_migration(&mut transaction, invalid_timestamp, invalid)
            .await;
        assert!(matches!(
            invalid_result,
            Err(dovecote_sqlx_mysql::FinalizeError::InvalidTimestamp { .. })
        ));
        transaction.rollback().await?;

        let mut transaction = pool.begin().await?;
        let missing = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                RowId::new(i64::MAX)?,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(
            missing,
            Err(dovecote_sqlx_mysql::FinalizeError::NotFound)
        ));
        transaction.rollback().await?;

        let schema_row = {
            let mut transaction = pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-schema"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };
        query("DROP INDEX dovecote_deliveries_claimable ON dovecote_deliveries")
            .execute(&pool)
            .await?;
        claimable_index_dropped = true;
        let mut transaction = pool.begin().await?;
        let schema_result = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                schema_row,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        if !matches!(
            schema_result,
            Err(dovecote_sqlx_mysql::FinalizeError::MigrationMismatch { .. })
        ) {
            return Err(format!(
                "schema drift finalization returned unexpected result: {schema_result:?}"
            )
            .into());
        }
        transaction.rollback().await?;
        let state: Vec<u8> =
            query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = ?")
                .bind(schema_row.get())
                .fetch_one(&pool)
                .await?;
        if state != b"pending" {
            return Err(
                format!("schema drift finalization changed delivery state to {state:?}").into(),
            );
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    // The live MySQL/MariaDB database is shared by the environment-gated
    // tests. Restore the exact migration index even when an assertion or
    // query above fails, so schema drift cannot poison later tests.
    let restore_index = if claimable_index_dropped {
        query("CREATE INDEX dovecote_deliveries_claimable ON dovecote_deliveries (tenant_id, state, available_at, event_row_id)")
            .execute(&pool)
            .await
            .map(|_| ())
    } else {
        Ok(())
    };

    let clear_rows = clear_conformance_rows(&pool).await;
    pool.close().await;
    restore_index?;
    clear_rows?;
    result
}

#[test]
fn migration_import_state_rejects_submicrosecond_delivery_time() {
    let invalid = time::OffsetDateTime::UNIX_EPOCH
        .replace_nanosecond(1)
        .expect("valid nanosecond");
    assert!(dovecote::ImportedDeliveryState::delivered(invalid).is_err());
}
