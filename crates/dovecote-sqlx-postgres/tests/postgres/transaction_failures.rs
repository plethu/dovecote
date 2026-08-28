use super::support::*;

#[tokio::test]
async fn lock_timeout_is_a_typed_transient_mutation_error_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let row_id = enqueue_committed(&database, "lock-timeout").await?;
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let claim = adapter
            .claim(
                WorkerId::new("lock-timeout-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let timeout_url = std::env::var("DOVECOTE_POSTGRES_URL")?;
        let timeout_options = PgConnectOptions::from_str(&timeout_url)?.options([
            ("search_path", format!("\"{}\"", database.schema)),
            ("lock_timeout", "50ms".to_owned()),
        ]);
        let timeout_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(timeout_options)
            .await?;
        let mut locker = database.pool.begin().await?;
        query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = $1 FOR UPDATE")
            .bind(row_id.get())
            .fetch_one(&mut *locker)
            .await?;
        let timeout_adapter =
            PostgresDovecote::new(timeout_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let error = timeout_adapter.ack(row_id, claim.claim_token()).await;
        match error {
            Err(MutationError::Transient {
                kind: TransientKind::StatementOrLockTimeout,
                source,
                ..
            }) => assert_eq!(
                source
                    .as_database_error()
                    .and_then(|db| db.code().map(|code| code.into_owned())),
                Some("55P03".to_owned())
            ),
            other => return Err(format!("expected typed lock timeout, got {other:?}").into()),
        }
        locker.rollback().await?;
        timeout_pool.close().await;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn statement_timeout_rolls_back_and_is_typed_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let row_id = enqueue_committed(&database, "statement-timeout").await?;
        let adapter = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let claim = adapter
            .claim(
                WorkerId::new("statement-timeout-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        install_trigger(
            &database,
            "dovecote_test_sleep_mutation",
            "dovecote_test_sleep_mutation",
            "PERFORM pg_sleep(1); RETURN NEW;",
        )
        .await?;

        let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
        let options = PgConnectOptions::from_str(&url)?.options([
            ("search_path", format!("\"{}\"", database.schema)),
            ("statement_timeout", "50ms".to_owned()),
            ("lock_timeout", "1s".to_owned()),
        ]);
        let timeout_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let timeout_adapter = PostgresDovecote::new(timeout_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let error = timeout_adapter.ack(row_id, claim.claim_token()).await;
        timeout_pool.close().await;
        match error {
            Err(MutationError::Transient {
                kind: TransientKind::StatementOrLockTimeout,
                source,
                ..
            }) => assert_eq!(
                source
                    .as_database_error()
                    .and_then(|db| db.code().map(|code| code.into_owned())),
                Some("57014".to_owned())
            ),
            other => return Err(format!("expected typed statement timeout, got {other:?}").into()),
        }
        remove_trigger(
            &database,
            "dovecote_test_sleep_mutation",
            "dovecote_test_sleep_mutation",
        )
        .await?;

        let stored: (String, i64, Option<Vec<u8>>, Option<String>) = query_as(
            "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, "claimed");
        assert_eq!(stored.1, 1);
        assert_eq!(stored.2, Some(claim.claim_token().as_bytes().to_vec()));
        assert_eq!(stored.3.as_deref(), Some("statement-timeout-worker"));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn repeatable_read_write_conflict_is_a_typed_serialization_failure_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let setup = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_id = enqueue_committed(&database, "serialization-first").await?;
        let second_id = enqueue_committed(&database, "serialization-second").await?;
        let shared_id = enqueue_committed(&database, "serialization-shared").await?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;
        let first = setup
            .claim(
                WorkerId::new("serialization-first")?,
                lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let second = setup
            .claim(
                WorkerId::new("serialization-second")?,
                lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);

        install_trigger(
            &database,
            "dovecote_test_serialization_conflict",
            "dovecote_test_serialization_conflict",
            &format!(
                "IF NEW.state = 'delivered' AND NEW.event_row_id IN ({}, {}) THEN PERFORM pg_sleep(0.25); UPDATE dovecote_deliveries SET available_at = available_at WHERE event_row_id = {}; END IF; RETURN NEW;",
                first_id.get(),
                second_id.get(),
                shared_id.get(),
            ),
        )
        .await?;

        let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
        let options = |schema: &str| {
            PgConnectOptions::from_str(&url)
                .expect("valid PostgreSQL URL")
                .options([
                    ("search_path", format!("\"{schema}\"")),
                    ("application_name", application_name(schema)),
                    ("default_transaction_isolation", "repeatable read".to_owned()),
                    ("statement_timeout", "2s".to_owned()),
                ])
        };

        let first_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options(&database.schema))
            .await?;
        let second_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options(&database.schema))
            .await?;
        let first_adapter = PostgresDovecote::new(first_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let second_adapter = PostgresDovecote::new(second_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_token = first.claim_token().clone();
        let second_token = second.claim_token().clone();
        let first_task = tokio::spawn(async move {
            first_adapter.ack(first_id, &first_token).await
        });
        // Wait until the first transaction has taken its row lock and entered
        // the trigger. This makes the second repeatable-read snapshot precede
        // the first commit rather than relying on scheduler luck.
        wait_for_active_query(&database.admin, &application_name(&database.schema)).await?;
        let second_result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            second_adapter.ack(second_id, &second_token),
        )
        .await?;
        let first_result = tokio::time::timeout(std::time::Duration::from_secs(3), first_task)
            .await??;
        first_pool.close().await;
        second_pool.close().await;
        remove_trigger(
            &database,
            "dovecote_test_serialization_conflict",
            "dovecote_test_serialization_conflict",
        )
        .await?;

        assert_single_transient_failure(
            &first_result,
            &second_result,
            TransientKind::SerializationFailure,
            "40001",
        )?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn conflicting_row_locks_are_a_typed_deadlock_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let setup = PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_id = enqueue_committed(&database, "deadlock-first").await?;
        let second_id = enqueue_committed(&database, "deadlock-second").await?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;
        let first = setup
            .claim(
                WorkerId::new("deadlock-first")?,
                lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let second = setup
            .claim(
                WorkerId::new("deadlock-second")?,
                lease,
                Limit::new(1)?,
            )
            .await?
            .remove(0);

        install_trigger(
            &database,
            "dovecote_test_deadlock",
            "dovecote_test_deadlock",
            &format!(
                "IF NEW.state = 'delivered' AND NEW.event_row_id = {} THEN PERFORM pg_sleep(0.25); PERFORM 1 FROM dovecote_deliveries WHERE event_row_id = {} FOR UPDATE; ELSIF NEW.state = 'delivered' AND NEW.event_row_id = {} THEN PERFORM pg_sleep(0.25); PERFORM 1 FROM dovecote_deliveries WHERE event_row_id = {} FOR UPDATE; END IF; RETURN NEW;",
                first_id.get(),
                second_id.get(),
                second_id.get(),
                first_id.get(),
            ),
        )
        .await?;

        let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
        let options = PgConnectOptions::from_str(&url)?.options([
            ("search_path", format!("\"{}\"", database.schema)),
            (
                "application_name",
                application_name(&database.schema),
            ),
            ("statement_timeout", "2s".to_owned()),
            ("deadlock_timeout", "50ms".to_owned()),
        ]);
        let first_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        let second_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let first_adapter = PostgresDovecote::new(first_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let second_adapter = PostgresDovecote::new(second_pool.clone()).for_tenant(TenantId::new("test").unwrap());
        let first_token = first.claim_token().clone();
        let second_token = second.claim_token().clone();
        let first_task = tokio::spawn(async move {
            first_adapter.ack(first_id, &first_token).await
        });
        wait_for_active_query(&database.admin, &application_name(&database.schema)).await?;
        let second_result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            second_adapter.ack(second_id, &second_token),
        )
        .await?;
        let first_result = tokio::time::timeout(std::time::Duration::from_secs(3), first_task)
            .await??;
        first_pool.close().await;
        second_pool.close().await;
        remove_trigger(
            &database,
            "dovecote_test_deadlock",
            "dovecote_test_deadlock",
        )
        .await?;

        assert_single_transient_failure(
            &first_result,
            &second_result,
            TransientKind::DeadlockDetected,
            "40P01",
        )?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}
