use super::support::*;

#[tokio::test]
async fn migration_import_is_idempotent_and_state_fenced() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let adapter =
        PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let imported = {
        let mut transaction = database.pool.begin().await?;
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-import", "com.example.import"),
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

    let mut transaction = database.pool.begin().await?;
    let replay = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-import", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await?;
    transaction.commit().await?;
    assert_eq!(replay, ImportOutcome::AlreadyImported { row_id });

    let delivered_at = time::OffsetDateTime::UNIX_EPOCH;
    let delivered_row_id = {
        let mut transaction = database.pool.begin().await?;
        let result = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-delivered", "com.example.import"),
                ImportedDeliveryState::Delivered { delivered_at },
            )
            .await?;
        transaction.commit().await?;
        match result {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected imported outcome, got {other:?}").into()),
        }
    };

    let stored_delivered_at: time::OffsetDateTime =
        query_scalar("SELECT delivered_at FROM dovecote_deliveries WHERE event_row_id = $1")
            .bind(delivered_row_id.get())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(stored_delivered_at, delivered_at);
    let mut transaction = database.pool.begin().await?;
    let state_conflict = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-delivered", "com.example.import"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(matches!(
        state_conflict,
        Err(dovecote_sqlx_postgres::ImportError::ImportConflict { existing_row_id })
            if existing_row_id == delivered_row_id
    ));
    transaction.rollback().await?;

    let mut transaction = database.pool.begin().await?;
    let conflict = adapter
        .import_for_migration(
            &mut transaction,
            event("migration-import", "com.example.changed"),
            ImportedDeliveryState::Pending,
        )
        .await;
    assert!(
        matches!(conflict, Err(dovecote_sqlx_postgres::ImportError::IdentityConflict { existing_row_id }) if existing_row_id == row_id)
    );
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn migration_import_rejects_schema_drift_before_event_mutation() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        query("ALTER TABLE dovecote_events ALTER COLUMN event_type TYPE VARCHAR(1023)")
            .execute(&database.pool)
            .await?;
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let mut transaction = database.pool.begin().await?;
        let import = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-schema-drift", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await;
        assert!(matches!(
            import,
            Err(dovecote_sqlx_postgres::ImportError::MigrationMismatch { .. })
        ));
        transaction.rollback().await?;
        let event_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&database.pool)
            .await?;
        let delivery_count: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!((event_count, delivery_count), (0, 0));
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn migration_import_uses_statement_database_time_not_transaction_start()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let mut transaction = database.pool.begin().await?;
        let transaction_time: time::OffsetDateTime =
            query_scalar("SELECT CURRENT_TIMESTAMP")
                .fetch_one(&mut *transaction)
                .await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let before_import: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-statement-time", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        let after_import: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let row_id = match outcome {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected imported outcome, got {other:?}").into()),
        };

        let (enqueued_at, available_at): (time::OffsetDateTime, time::OffsetDateTime) =
            query_as(
                "SELECT e.enqueued_at, d.available_at FROM dovecote_events e JOIN dovecote_deliveries d ON d.event_row_id = e.row_id WHERE e.row_id = $1",
            )
            .bind(row_id.get())
            .fetch_one(&mut *transaction)
            .await?;
        assert_eq!(enqueued_at, available_at);
        assert!(
            enqueued_at > transaction_time + time::Duration::milliseconds(50),
            "import used transaction-start time: {enqueued_at:?} <= {transaction_time:?}"
        );
        assert!(enqueued_at >= before_import && enqueued_at <= after_import);
        transaction.rollback().await?;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn migration_import_rollback_removes_event_and_delivery_together()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let mut transaction = database.pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-rollback", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        assert!(matches!(outcome, ImportOutcome::Imported { .. }));
        transaction.rollback().await?;
        let event_count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(&database.pool)
            .await?;
        let delivery_count: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!((event_count, delivery_count), (0, 0));
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn migration_import_rejects_changed_available_at_on_replay() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-available-at", "com.example.import"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };
        query("UPDATE dovecote_deliveries SET available_at = $1 WHERE event_row_id = $2")
            .bind(time::OffsetDateTime::UNIX_EPOCH)
            .bind(row_id.get())
            .execute(&database.pool)
            .await?;
        let mut transaction = database.pool.begin().await?;
        let replay = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-available-at", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await;
        assert!(matches!(
            replay,
            Err(dovecote_sqlx_postgres::ImportError::ImportConflict { existing_row_id })
                if existing_row_id == row_id
        ));
        transaction.rollback().await?;
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn migration_import_preserves_maximum_delivered_time_and_never_claims_it()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let delivered_at = maximum_timestamp();
        let row_id = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-delivered-max", "com.example.import"),
                    ImportedDeliveryState::Delivered { delivered_at },
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };

        let stored: time::OffsetDateTime =
            query_scalar("SELECT delivered_at FROM dovecote_deliveries WHERE event_row_id = $1")
                .bind(row_id.get())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(stored, delivered_at);
        let mut transaction = database.pool.begin().await?;
        let replay = adapter
            .import_for_migration(
                &mut transaction,
                event("migration-delivered-max", "com.example.import"),
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
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn migration_finalization_is_idempotent_fenced_and_transactional()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let row_id = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize", "com.example.import"),
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
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
                .await?;
            transaction.commit().await?;
            outcome
        };
        assert_eq!(first, FinalizeOutcome::Finalized { row_id });
        let replay = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .finalize_pending_delivery_for_migration(&mut transaction, row_id, delivered_at)
                .await?;
            transaction.commit().await?;
            outcome
        };
        assert_eq!(replay, FinalizeOutcome::AlreadyFinalized { row_id });
        let changed = {
            let mut transaction = database.pool.begin().await?;
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
            Err(dovecote_sqlx_postgres::FinalizeError::StateConflict { row_id: id })
                if id == row_id
        ));

        let rollback_id = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-rollback", "com.example.import"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };

        let mut transaction = database.pool.begin().await?;
        adapter
            .finalize_pending_delivery_for_migration(&mut transaction, rollback_id, delivered_at)
            .await?;
        transaction.rollback().await?;
        let state: String =
            query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = $1")
                .bind(rollback_id.get())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(state, "pending");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
