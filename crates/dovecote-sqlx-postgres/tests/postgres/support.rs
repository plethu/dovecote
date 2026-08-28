pub(crate) use dovecote::{
    ClaimToken, Delay, DeliveryState, EnqueueOutcome, EventId, EventSource, EventType, Failure,
    FinalizeOutcome, ImportOutcome, ImportedDeliveryState, Lease, Limit, NewEvent,
    QuarantineReason, RowId, StreamName, TenantId, WorkerId,
};
pub(crate) use dovecote_sqlx_postgres::{
    ClaimError, EnqueueError, MIGRATIONS, MutationError, PageError, PostgresDovecote, SchemaError,
    TenantDovecote, TransientKind, check_schema,
};
pub(crate) use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_as, query_scalar, raw_sql,
};

pub(crate) async fn enqueue_for_test_tenant<'c>(
    database: &IsolatedDatabase,
    tx: &mut sqlx::Transaction<'c, sqlx::Postgres>,
    event: NewEvent,
) -> Result<EnqueueOutcome, EnqueueError> {
    PostgresDovecote::new(database.pool.clone())
        .for_tenant(TenantId::new("test").unwrap())
        .enqueue(tx, event)
        .await
}
pub(crate) use std::{
    error::Error,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) static NEXT_SCHEMA_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn isolated_schema_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = NEXT_SCHEMA_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}_{}", std::process::id(), timestamp, sequence)
}

#[test]
pub(crate) fn isolated_schema_names_are_unique_and_identifier_safe() {
    let first = isolated_schema_name("dovecote_test");
    let second = isolated_schema_name("dovecote_test");
    assert_ne!(first, second);
    assert!(
        first
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    assert!(
        second
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
}

pub(crate) struct IsolatedDatabase {
    pub(crate) admin: PgPool,
    pub(crate) pool: PgPool,
    pub(crate) schema: String,
}

impl IsolatedDatabase {
    pub(crate) async fn cleanup(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        let statement = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
        query(sqlx::AssertSqlSafe(statement))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

pub(crate) fn event(event_id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").expect("valid stream"),
        EventId::new(event_id).expect("valid id"),
        EventSource::new("https://example.test/source").expect("valid source"),
        EventType::new(event_type).expect("valid type"),
    )
    .expect("valid event")
}

pub(crate) fn event_with_time(event_id: &str, occurred_at: time::OffsetDateTime) -> NewEvent {
    NewEvent::builder(
        StreamName::new("audit").expect("valid stream"),
        EventId::new(event_id).expect("valid id"),
        EventSource::new("https://example.test/source").expect("valid source"),
        EventType::new("com.example.time").expect("valid type"),
    )
    .time(occurred_at)
    .build()
    .expect("valid event")
}

pub(crate) fn maximum_timestamp() -> time::OffsetDateTime {
    time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    )
}

pub(crate) fn advisory_key(schema: &str) -> i64 {
    // Advisory locks are cluster-wide. Derive a positive, test-local key from
    // the isolated schema so parallel live tests cannot share a barrier.
    let hash = schema.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(byte))
    });
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

pub(crate) fn application_name(schema: &str) -> String {
    format!("dovecote-test-{schema}")
}

pub(crate) async fn wait_for_active_query(
    admin: &PgPool,
    application_name: &str,
) -> Result<i32, Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let pid = query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1 AND state = 'active' AND query LIKE '%UPDATE dovecote_deliveries%' LIMIT 1",
            )
            .bind(application_name)
            .fetch_optional(admin)
            .await?;
            if let Some(pid) = pid {
                return Ok(pid);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await?
}

pub(crate) async fn install_trigger(
    database: &IsolatedDatabase,
    function_name: &str,
    trigger_name: &str,
    body: &str,
) -> Result<(), Box<dyn Error>> {
    let function = format!(
        r#"CREATE FUNCTION "{}"."{}"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    {}
END
$$"#,
        database.schema, function_name, body
    );
    query(sqlx::AssertSqlSafe(function))
        .execute(&database.admin)
        .await?;
    let trigger = format!(
        r#"CREATE TRIGGER "{}" BEFORE UPDATE OF state ON "{}"."dovecote_deliveries"
           FOR EACH ROW EXECUTE FUNCTION "{}"."{}"()"#,
        trigger_name, database.schema, database.schema, function_name
    );
    query(sqlx::AssertSqlSafe(trigger))
        .execute(&database.admin)
        .await?;
    Ok(())
}

pub(crate) async fn remove_trigger(
    database: &IsolatedDatabase,
    function_name: &str,
    trigger_name: &str,
) -> Result<(), Box<dyn Error>> {
    let trigger = format!(
        "DROP TRIGGER IF EXISTS \"{trigger_name}\" ON \"{}\".\"dovecote_deliveries\"",
        database.schema
    );
    query(sqlx::AssertSqlSafe(trigger))
        .execute(&database.admin)
        .await?;
    let function = format!(
        "DROP FUNCTION IF EXISTS \"{}\".\"{function_name}\"()",
        database.schema
    );
    query(sqlx::AssertSqlSafe(function))
        .execute(&database.admin)
        .await?;
    Ok(())
}

pub(crate) async fn concurrent_import_pool(
    database: &IsolatedDatabase,
    marker: &str,
) -> Result<PgPool, Box<dyn Error>> {
    let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
    let options = PgConnectOptions::from_str(&url)?.options([
        ("search_path", format!("\"{}\"", database.schema)),
        ("application_name", marker.to_owned()),
    ]);
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?)
}

pub(crate) async fn wait_for_import_lock_waiters(
    database: &IsolatedDatabase,
    left_marker: &str,
    right_marker: &str,
) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiters: i64 = query_scalar(
                "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_stat_activity a ON a.pid = l.pid WHERE n.nspname = $1 AND c.relname = 'dovecote_events' AND l.mode = 'RowExclusiveLock' AND NOT l.granted AND a.application_name IN ($2, $3)",
            )
            .bind(&database.schema)
            .bind(left_marker)
            .bind(right_marker)
            .fetch_one(&database.admin)
            .await?;
            if waiters == 2 {
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

pub(crate) async fn finish_concurrent_import_test(
    database: IsolatedDatabase,
    left_pool: PgPool,
    right_pool: PgPool,
    body: Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    left_pool.close().await;
    right_pool.close().await;
    let cleanup = database.cleanup().await;
    match (body, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(body), Ok(())) => Err(body),
        (Ok(()), Err(cleanup)) => Err(cleanup.into()),
        (Err(body), Err(cleanup)) => {
            Err(format!("{body}; PostgreSQL cleanup failed: {cleanup}").into())
        }
    }
}

pub(crate) fn assert_single_transient_failure(
    first: &Result<(), MutationError>,
    second: &Result<(), MutationError>,
    expected_kind: TransientKind,
    expected_sqlstate: &str,
) -> Result<(), Box<dyn Error>> {
    let (kind, source) = match (first, second) {
        (Err(MutationError::Transient { kind, source, .. }), Ok(()))
        | (Ok(()), Err(MutationError::Transient { kind, source, .. })) => (*kind, source),
        other => {
            return Err(
                format!("expected one transient failure and one Ok(()), got {other:?}").into(),
            );
        }
    };
    assert_eq!(kind, expected_kind);
    assert_eq!(
        source
            .as_database_error()
            .and_then(|database| database.code().map(|code| code.into_owned()))
            .as_deref(),
        Some(expected_sqlstate)
    );
    Ok(())
}

pub(crate) fn required() -> bool {
    env_flag("DOVECOTE_POSTGRES_REQUIRED")
        || env_flag("DOVECOTE_RELEASE_MODE")
        || (env_flag("CI") && !env_flag("DOVECOTE_POSTGRES_OPTIONAL"))
}

pub(crate) fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref().is_some_and(is_truthy)
}

pub(crate) fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "yes")
}

#[test]
pub(crate) fn environment_flags_use_explicit_truth_values() {
    for value in ["1", "true", "yes"] {
        assert!(is_truthy(value), "expected {value:?} to be truthy");
    }

    for value in ["0", "false", "no", "", "on", "TRUE", " YES "] {
        assert!(!is_truthy(value), "expected {value:?} to be false");
    }
}

pub(crate) async fn isolated_database() -> Result<Option<IsolatedDatabase>, Box<dyn Error>> {
    let Ok(url) = std::env::var("DOVECOTE_POSTGRES_URL") else {
        if required() {
            return Err(
                "DOVECOTE_POSTGRES_URL is required by CI/release PostgreSQL conformance".into(),
            );
        }
        eprintln!("skipping PostgreSQL integration test: DOVECOTE_POSTGRES_URL is unset");
        return Ok(None);
    };

    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    let schema = isolated_schema_name("dovecote_test");
    query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&admin)
        .await?;

    let result = async {
        let options = PgConnectOptions::from_str(&url)?.options([
            ("search_path", format!("\"{schema}\"")),
            ("application_name", application_name(&schema)),
        ]);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        raw_sql(MIGRATIONS[0].sql()).execute(&pool).await?;
        check_schema(&pool).await?;
        Ok::<_, Box<dyn Error>>(IsolatedDatabase {
            admin: admin.clone(),
            pool,
            schema: schema.clone(),
        })
    }
    .await;
    match result {
        Ok(database) => Ok(Some(database)),
        Err(error) => {
            query(sqlx::AssertSqlSafe(format!(
                "DROP SCHEMA \"{schema}\" CASCADE"
            )))
            .execute(&admin)
            .await?;
            admin.close().await;
            Err(error)
        }
    }
}

pub(crate) async fn single_connection_pool(
    database: &IsolatedDatabase,
) -> Result<PgPool, Box<dyn Error>> {
    let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
    let options = PgConnectOptions::from_str(&url)?
        .options([("search_path", format!("\"{}\"", database.schema))]);
    Ok(PgPoolOptions::new()
        .max_connections(1)
        // Connection establishment in a disposable PostgreSQL container can
        // exceed a scheduling-sized test timeout. The held-connection checks
        // use `try_acquire` below, so this timeout only covers pool startup.
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_with(options)
        .await?)
}

pub(crate) async fn exercise(database: &IsolatedDatabase) -> Result<(), Box<dyn Error>> {
    let mut transaction = database.pool.begin().await?;
    let first = enqueue_for_test_tenant(
        database,
        &mut transaction,
        event("one", "com.example.audit"),
    )
    .await?;
    let first_row_id = match first {
        EnqueueOutcome::Enqueued { row_id } => row_id,
        other => return Err(format!("expected first insertion, got {other:?}").into()),
    };
    transaction.commit().await?;

    let count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
        .fetch_one(&database.pool)
        .await?;
    let deliveries: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!((count, deliveries), (1, 1));

    let mut transaction = database.pool.begin().await?;
    let replay = enqueue_for_test_tenant(
        database,
        &mut transaction,
        event("one", "com.example.audit"),
    )
    .await?;
    assert_eq!(
        replay,
        EnqueueOutcome::AlreadyEnqueued {
            row_id: first_row_id
        }
    );
    transaction.commit().await?;

    query("UPDATE dovecote_events SET extensions = '{\"bad\": 1}' WHERE row_id = $1")
        .bind(first_row_id.get())
        .execute(&database.pool)
        .await?;
    let mut transaction = database.pool.begin().await?;
    let corrupted = enqueue_for_test_tenant(
        database,
        &mut transaction,
        event("one", "com.example.audit"),
    )
    .await;
    if !matches!(corrupted, Err(EnqueueError::Serialization { .. })) {
        return Err(format!(
            "expected corrupted durable data to fail serialization, got {corrupted:?}"
        )
        .into());
    }
    transaction.rollback().await?;
    query("UPDATE dovecote_events SET extensions = '{}' WHERE row_id = $1")
        .bind(first_row_id.get())
        .execute(&database.pool)
        .await?;

    let mut transaction = database.pool.begin().await?;
    let conflict = enqueue_for_test_tenant(
        database,
        &mut transaction,
        event("one", "com.example.other"),
    )
    .await;
    match conflict {
        Err(EnqueueError::IdempotencyConflict { existing_row_id })
            if existing_row_id == first_row_id => {}
        other => return Err(format!("expected idempotency conflict, got {other:?}").into()),
    }
    transaction.rollback().await?;

    let mut transaction = database.pool.begin().await?;
    enqueue_for_test_tenant(
        database,
        &mut transaction,
        event("rolled-back", "com.example.audit"),
    )
    .await?;
    transaction.rollback().await?;
    let count: i64 = query_scalar("SELECT count(*) FROM dovecote_events")
        .fetch_one(&database.pool)
        .await?;
    let deliveries: i64 = query_scalar("SELECT count(*) FROM dovecote_deliveries")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!((count, deliveries), (1, 1));

    let shared_time: bool = query_scalar(
        "SELECT e.enqueued_at = d.available_at FROM dovecote_events e JOIN dovecote_deliveries d ON d.event_row_id = e.row_id",
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(shared_time);
    Ok(())
}

pub(crate) async fn enqueue_committed(
    database: &IsolatedDatabase,
    event_id: &str,
) -> Result<dovecote::RowId, Box<dyn Error>> {
    enqueue_event_committed(database, event(event_id, "com.example.lifecycle")).await
}

pub(crate) async fn enqueue_event_committed(
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
