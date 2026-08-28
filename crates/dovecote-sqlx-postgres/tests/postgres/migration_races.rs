use super::support::*;

#[tokio::test]
async fn concurrent_exact_imports_have_one_insert_and_one_idempotent_replay()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let event_id = "migration-concurrent-exact";
    let left_marker = "dovecote-import-left-exact";
    let right_marker = "dovecote-import-right-exact";
    let left_pool = match concurrent_import_pool(&database, left_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            database.cleanup().await?;
            return Err(error);
        }
    };

    let right_pool = match concurrent_import_pool(&database, right_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            left_pool.close().await;
            database.cleanup().await?;
            return Err(error);
        }
    };

    let mut blocker = match database.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            left_pool.close().await;
            right_pool.close().await;
            database.cleanup().await?;
            return Err(error.into());
        }
    };

    if let Err(error) = query("LOCK TABLE dovecote_events IN SHARE MODE")
        .execute(&mut *blocker)
        .await
    {
        let _ = blocker.rollback().await;
        left_pool.close().await;
        right_pool.close().await;
        database.cleanup().await?;
        return Err(error.into());
    }

    let left_work_pool = left_pool.clone();
    let left = async move {
        let adapter = PostgresDovecote::new(left_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = left_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.concurrent"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        transaction.commit().await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(outcome)
    };

    let right_work_pool = right_pool.clone();
    let right = async move {
        let adapter = PostgresDovecote::new(right_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = right_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.concurrent"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        transaction.commit().await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(outcome)
    };

    let left = tokio::spawn(left);
    let right = tokio::spawn(right);
    let waiters = wait_for_import_lock_waiters(&database, left_marker, right_marker).await;
    let release = blocker.rollback().await;
    let left: Result<ImportOutcome, Box<dyn Error>> = match left.await {
        Ok(result) => result.map_err(|error| format!("left import failed: {error}").into()),
        Err(error) => Err(format!("left import task failed: {error}").into()),
    };

    let right: Result<ImportOutcome, Box<dyn Error>> = match right.await {
        Ok(result) => result.map_err(|error| format!("right import failed: {error}").into()),
        Err(error) => Err(format!("right import task failed: {error}").into()),
    };

    let check = match (release, waiters, left, right) {
        (Err(error), _, _, _) => {
            Err(format!("failed to release PostgreSQL blocker: {error}").into())
        }
        (_, Err(error), _, _) => Err(error),
        (_, _, Err(error), _) | (_, _, _, Err(error)) => Err(error),
        (Ok(()), Ok(()), Ok(left), Ok(right)) => match (left, right) {
            (
                ImportOutcome::Imported { row_id: left },
                ImportOutcome::AlreadyImported { row_id: right },
            )
            | (
                ImportOutcome::AlreadyImported { row_id: left },
                ImportOutcome::Imported { row_id: right },
            ) => {
                if left == right {
                    Ok(())
                } else {
                    Err("concurrent exact imports returned different row IDs".into())
                }
            }
            other => Err(format!("expected one insert and one replay, got {other:?}").into()),
        },
    };
    finish_concurrent_import_test(database, left_pool, right_pool, check).await
}

#[tokio::test]
async fn concurrent_changed_event_content_returns_one_identity_conflict()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let event_id = "migration-concurrent-content";
    let left_marker = "dovecote-import-left-content";
    let right_marker = "dovecote-import-right-content";
    let left_pool = match concurrent_import_pool(&database, left_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            database.cleanup().await?;
            return Err(error);
        }
    };

    let right_pool = match concurrent_import_pool(&database, right_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            left_pool.close().await;
            database.cleanup().await?;
            return Err(error);
        }
    };

    let mut blocker = match database.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            left_pool.close().await;
            right_pool.close().await;
            database.cleanup().await?;
            return Err(error.into());
        }
    };

    if let Err(error) = query("LOCK TABLE dovecote_events IN SHARE MODE")
        .execute(&mut *blocker)
        .await
    {
        let _ = blocker.rollback().await;
        left_pool.close().await;
        right_pool.close().await;
        database.cleanup().await?;
        return Err(error.into());
    }

    let left_work_pool = left_pool.clone();
    let left = async move {
        let adapter = PostgresDovecote::new(left_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = left_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.first"),
                ImportedDeliveryState::Pending,
            )
            .await;
        match outcome {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Ok(outcome))
            }
            Err(error) => {
                transaction.rollback().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Err(error))
            }
        }
    };

    let right_work_pool = right_pool.clone();
    let right = async move {
        let adapter = PostgresDovecote::new(right_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = right_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.second"),
                ImportedDeliveryState::Pending,
            )
            .await;
        match outcome {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Ok(outcome))
            }
            Err(error) => {
                transaction.rollback().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Err(error))
            }
        }
    };

    let left = tokio::spawn(left);
    let right = tokio::spawn(right);
    let waiters = wait_for_import_lock_waiters(&database, left_marker, right_marker).await;
    let release = blocker.rollback().await;
    let left = match left.await {
        Ok(result) => result,
        Err(error) => Err(format!("left import task failed: {error}").into()),
    };

    let right = match right.await {
        Ok(result) => result,
        Err(error) => Err(format!("right import task failed: {error}").into()),
    };

    let outcomes = [left, right];
    let check = if release.is_ok()
        && waiters.is_ok()
        && outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(Ok(_))))
            .count()
            == 1
        && outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome,
                    Ok(Err(
                        dovecote_sqlx_postgres::ImportError::IdentityConflict { .. }
                    ))
                )
            })
            .count()
            == 1
    {
        Ok(())
    } else {
        Err(
            "concurrent content import did not produce one success and one identity conflict"
                .into(),
        )
    };
    finish_concurrent_import_test(database, left_pool, right_pool, check).await
}

#[tokio::test]
async fn concurrent_changed_imported_state_returns_one_state_conflict() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let event_id = "migration-concurrent-state";
    let left_marker = "dovecote-import-left-state";
    let right_marker = "dovecote-import-right-state";
    let left_pool = match concurrent_import_pool(&database, left_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            database.cleanup().await?;
            return Err(error);
        }
    };

    let right_pool = match concurrent_import_pool(&database, right_marker).await {
        Ok(pool) => pool,
        Err(error) => {
            left_pool.close().await;
            database.cleanup().await?;
            return Err(error);
        }
    };

    let mut blocker = match database.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            left_pool.close().await;
            right_pool.close().await;
            database.cleanup().await?;
            return Err(error.into());
        }
    };

    if let Err(error) = query("LOCK TABLE dovecote_events IN SHARE MODE")
        .execute(&mut *blocker)
        .await
    {
        let _ = blocker.rollback().await;
        left_pool.close().await;
        right_pool.close().await;
        database.cleanup().await?;
        return Err(error.into());
    }

    let left_work_pool = left_pool.clone();
    let left = async move {
        let adapter = PostgresDovecote::new(left_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = left_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.concurrent-state"),
                ImportedDeliveryState::Pending,
            )
            .await;
        match outcome {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Ok(outcome))
            }
            Err(error) => {
                transaction.rollback().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Err(error))
            }
        }
    };

    let right_work_pool = right_pool.clone();
    let right = async move {
        let adapter = PostgresDovecote::new(right_work_pool.clone())
            .for_tenant(TenantId::new("test").unwrap());
        let mut transaction = right_work_pool.begin().await?;
        let outcome = adapter
            .import_for_migration(
                &mut transaction,
                event(event_id, "com.example.concurrent-state"),
                ImportedDeliveryState::Delivered {
                    delivered_at: time::OffsetDateTime::UNIX_EPOCH,
                },
            )
            .await;
        match outcome {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Ok(outcome))
            }
            Err(error) => {
                transaction.rollback().await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(Err(error))
            }
        }
    };

    let left = tokio::spawn(left);
    let right = tokio::spawn(right);
    let waiters = wait_for_import_lock_waiters(&database, left_marker, right_marker).await;
    let release = blocker.rollback().await;
    let left = match left.await {
        Ok(result) => result,
        Err(error) => Err(format!("left import task failed: {error}").into()),
    };

    let right = match right.await {
        Ok(result) => result,
        Err(error) => Err(format!("right import task failed: {error}").into()),
    };

    let outcomes = [left, right];
    let check = if release.is_ok()
        && waiters.is_ok()
        && outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(Ok(_))))
            .count()
            == 1
        && outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome,
                    Ok(Err(
                        dovecote_sqlx_postgres::ImportError::ImportConflict { .. }
                    ))
                )
            })
            .count()
            == 1
    {
        Ok(())
    } else {
        Err("concurrent state import did not produce one success and one state conflict".into())
    };
    finish_concurrent_import_test(database, left_pool, right_pool, check).await
}
