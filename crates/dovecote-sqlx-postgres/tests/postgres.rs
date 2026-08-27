use dovecote::{
    ClaimToken, Delay, DeliveryState, EnqueueOutcome, EventId, EventSource, EventType, Failure,
    FinalizeOutcome, ImportOutcome, ImportedDeliveryState, Lease, Limit, NewEvent,
    QuarantineReason, RowId, StreamName, WorkerId,
};
use dovecote_sqlx_postgres::{
    ClaimError, EnqueueError, MIGRATIONS, MutationError, PostgresDovecote, TransientKind,
    check_schema, enqueue,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    query, query_as, query_scalar, raw_sql,
};
use std::{
    error::Error,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_SCHEMA_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn isolated_schema_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = NEXT_SCHEMA_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}_{}", std::process::id(), timestamp, sequence)
}

#[test]
fn isolated_schema_names_are_unique_and_identifier_safe() {
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

struct IsolatedDatabase {
    admin: PgPool,
    pool: PgPool,
    schema: String,
}

impl IsolatedDatabase {
    async fn cleanup(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        let statement = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
        query(sqlx::AssertSqlSafe(statement))
            .execute(&self.admin)
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

fn event(event_id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").expect("valid stream"),
        EventId::new(event_id).expect("valid id"),
        EventSource::new("https://example.test/source").expect("valid source"),
        EventType::new(event_type).expect("valid type"),
    )
    .expect("valid event")
}

fn event_with_time(event_id: &str, occurred_at: time::OffsetDateTime) -> NewEvent {
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

fn maximum_timestamp() -> time::OffsetDateTime {
    time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    )
}

fn advisory_key(schema: &str) -> i64 {
    // Advisory locks are cluster-wide. Derive a positive, test-local key from
    // the isolated schema so parallel live tests cannot share a barrier.
    let hash = schema.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(byte))
    });
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

fn application_name(schema: &str) -> String {
    format!("dovecote-test-{schema}")
}

async fn wait_for_active_query(
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

async fn install_trigger(
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

async fn remove_trigger(
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

async fn concurrent_import_pool(
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

async fn wait_for_import_lock_waiters(
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

async fn finish_concurrent_import_test(
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

fn assert_single_transient_failure(
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

fn required() -> bool {
    env_flag("DOVECOTE_POSTGRES_REQUIRED")
        || env_flag("DOVECOTE_RELEASE_MODE")
        || (env_flag("CI") && !env_flag("DOVECOTE_POSTGRES_OPTIONAL"))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref().is_some_and(is_truthy)
}

fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "yes")
}

#[test]
fn environment_flags_use_explicit_truth_values() {
    for value in ["1", "true", "yes"] {
        assert!(is_truthy(value), "expected {value:?} to be truthy");
    }

    for value in ["0", "false", "no", "", "on", "TRUE", " YES "] {
        assert!(!is_truthy(value), "expected {value:?} to be false");
    }
}

async fn isolated_database() -> Result<Option<IsolatedDatabase>, Box<dyn Error>> {
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

async fn single_connection_pool(database: &IsolatedDatabase) -> Result<PgPool, Box<dyn Error>> {
    let url = std::env::var("DOVECOTE_POSTGRES_URL")?;
    let options = PgConnectOptions::from_str(&url)?
        .options([("search_path", format!("\"{}\"", database.schema))]);
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(120))
        .connect_with(options)
        .await?)
}

async fn exercise(database: &IsolatedDatabase) -> Result<(), Box<dyn Error>> {
    let mut transaction = database.pool.begin().await?;
    let first = enqueue(&mut transaction, event("one", "com.example.audit")).await?;
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
    let replay = enqueue(&mut transaction, event("one", "com.example.audit")).await?;
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
    let corrupted = enqueue(&mut transaction, event("one", "com.example.audit")).await;
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
    let conflict = enqueue(&mut transaction, event("one", "com.example.other")).await;
    match conflict {
        Err(EnqueueError::IdempotencyConflict { existing_row_id })
            if existing_row_id == first_row_id => {}
        other => return Err(format!("expected idempotency conflict, got {other:?}").into()),
    }
    transaction.rollback().await?;

    let mut transaction = database.pool.begin().await?;
    enqueue(&mut transaction, event("rolled-back", "com.example.audit")).await?;
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
    let outcome = enqueue(&mut transaction, event).await?;
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
            let outcome =
                enqueue(&mut transaction, event("page-skipped", "com.example.page")).await?;
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
async fn snapshot_paging_is_stable_and_releases_its_transaction_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(single.clone());

        let finished = adapter.begin_snapshot().await?;
        assert!(matches!(
            single.acquire().await,
            Err(sqlx::Error::PoolTimedOut)
        ));
        finished.finish().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let rolled_back = adapter.begin_snapshot().await?;
        assert!(matches!(
            single.acquire().await,
            Err(sqlx::Error::PoolTimedOut)
        ));
        rolled_back.rollback().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let closable = adapter.begin_snapshot().await?;
        assert!(matches!(
            single.acquire().await,
            Err(sqlx::Error::PoolTimedOut)
        ));
        closable.close().await?;
        query_scalar::<_, i64>("SELECT 1::BIGINT")
            .fetch_one(&single)
            .await?;

        let dropped = adapter.begin_snapshot().await?;
        assert!(matches!(
            single.acquire().await,
            Err(sqlx::Error::PoolTimedOut)
        ));
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
        let visible_first = enqueue_committed(&database, "inversion-visible-first").await?;
        let visible_second = enqueue_committed(&database, "inversion-visible-second").await?;
        let visible_third = enqueue_committed(&database, "inversion-visible-third").await?;

        // Hold the lower sequence value uncommitted while the later value is
        // committed. This is the barrier controlling the commit inversion.
        let mut earlier_transaction = database.pool.begin().await?;
        let earlier = enqueue(
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

#[tokio::test]
async fn lifecycle_mutations_fence_and_preserve_delivery_state_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let row_id = enqueue_committed(&database, "lifecycle").await?;
        let quarantine_id = enqueue_committed(&database, "quarantine").await?;
        let worker = WorkerId::new("worker-a")?;
        let lease = Lease::new(std::time::Duration::from_secs(5))?;
        let claims = adapter.claim(worker.clone(), lease, Limit::new(1)?).await?;
        assert_eq!(claims.len(), 1);
        let claim = &claims[0];
        assert_eq!(claim.row_id(), row_id);
        assert_eq!(claim.attempts().get(), 1);
        let token = claim.claim_token().clone();

        adapter.renew(row_id, &token, lease).await?;
        let renewed_expiry: time::OffsetDateTime = query_scalar(
            "SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        let renewed_now: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
        .fetch_one(&database.pool)
        .await?;
        assert!(renewed_expiry > renewed_now);
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() + INTERVAL '1 second' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        adapter.renew(row_id, &token, lease).await?;
        let renewed_from_database_time: bool = query_scalar(
            "SELECT claim_expires_at > clock_timestamp() + INTERVAL '4 seconds' AND claim_expires_at < clock_timestamp() + INTERVAL '6 seconds' FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert!(renewed_from_database_time);

        let failure = Failure::new("transport_unavailable", "temporary")?;
        let retry_started: time::OffsetDateTime = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&database.pool)
            .await?;
        adapter
            .retry(
                row_id,
                &token,
                &failure,
                Delay::new(std::time::Duration::from_millis(100))?,
            )
            .await?;
        let retry_snapshot = query_as::<_, (String, time::OffsetDateTime, Option<Vec<u8>>, Option<String>, Option<String>)>(
            "SELECT state, available_at, claim_token, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(retry_snapshot.0, "pending");
        assert!(retry_snapshot.1 > retry_started);
        assert!(retry_snapshot.2.is_none());
        assert_eq!(retry_snapshot.3.as_deref(), Some("transport_unavailable"));
        assert_eq!(retry_snapshot.4.as_deref(), Some("temporary"));
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(matches!(
            adapter.ack(row_id, &token).await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Pending
            })
        ));

        let reclaimed = adapter
            .claim(WorkerId::new("worker-b")?, lease, Limit::new(1)?)
            .await?;
        assert_eq!(reclaimed[0].row_id(), row_id);
        assert_eq!(reclaimed[0].attempts().get(), 2);
        assert_ne!(reclaimed[0].claim_token(), &token);
        assert!(matches!(
            adapter.ack(row_id, &token).await,
            Err(MutationError::LostClaim)
        ));
        let second_token = reclaimed[0].claim_token().clone();
        adapter
            .release(
                row_id,
                &second_token,
                Delay::new(std::time::Duration::from_millis(100))?,
            )
            .await?;
        let release_snapshot = query_as::<_, (String, Option<String>, Option<String>, Option<Vec<u8>>)>(
            "SELECT state, last_failure_code, last_failure_detail, claim_token FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(release_snapshot.0, "pending");
        assert_eq!(release_snapshot.1.as_deref(), Some("transport_unavailable"));
        assert_eq!(release_snapshot.2.as_deref(), Some("temporary"));
        assert!(release_snapshot.3.is_none());
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        let released = adapter
            .claim(WorkerId::new("worker-c")?, lease, Limit::new(1)?)
            .await?;
        let released_token = released[0].claim_token().clone();
        adapter.ack(row_id, &released_token).await?;
        assert!(matches!(
            adapter.ack(row_id, &released_token).await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Delivered
            })
        ));

        let quarantined = adapter
            .claim(WorkerId::new("worker-quarantine")?, lease, Limit::new(1)?)
            .await?
            .remove(0);
        assert_eq!(quarantined.row_id(), quarantine_id);
        let quarantine_token = quarantined.claim_token().clone();
        let reason = dovecote::QuarantineReason::new("operator_review")?;
        adapter
            .quarantine(quarantine_id, &quarantine_token, &reason)
            .await?;
        assert!(matches!(
            adapter
                .release(
                    quarantine_id,
                    &quarantine_token,
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::IllegalTransition {
                state: DeliveryState::Quarantined
            })
        ));
        let stored_reason: String = query_scalar(
            "SELECT quarantine_reason FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(quarantine_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored_reason, "operator_review");

        let attempts: i64 =
            query_scalar("SELECT attempts FROM dovecote_deliveries WHERE event_row_id = $1")
                .bind(row_id.get())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(attempts, 3);
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn expired_claims_reclaim_and_counter_overflow_rolls_back_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let expired_id = enqueue_committed(&database, "expired").await?;
        let claim = adapter
            .claim(
                WorkerId::new("worker-a")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let wrong_token = ClaimToken::from_bytes([7; dovecote::CLAIM_TOKEN_BYTES]);
        assert!(matches!(
            adapter.ack(expired_id, &wrong_token).await,
            Err(MutationError::LostClaim)
        ));
        query("UPDATE dovecote_deliveries SET claim_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE event_row_id = $1")
            .bind(expired_id.get())
            .execute(&database.pool)
            .await?;
        assert!(matches!(
            adapter.ack(expired_id, claim.claim_token()).await,
            Err(MutationError::LostClaim)
        ));
        let reclaimed = adapter
            .claim(
                WorkerId::new("worker-b")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), claim.claim_token());
        let stale_failure = Failure::new("stale", "must not mutate")?;
        assert!(matches!(
            adapter
                .renew(
                    expired_id,
                    claim.claim_token(),
                    Lease::new(std::time::Duration::from_secs(5))?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter.ack(expired_id, claim.claim_token()).await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .retry(
                    expired_id,
                    claim.claim_token(),
                    &stale_failure,
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .release(
                    expired_id,
                    claim.claim_token(),
                    Delay::new(std::time::Duration::ZERO)?
                )
                .await,
            Err(MutationError::LostClaim)
        ));
        assert!(matches!(
            adapter
                .quarantine(
                    expired_id,
                    claim.claim_token(),
                    &dovecote::QuarantineReason::new("stale")?
                )
                .await,
            Err(MutationError::LostClaim)
        ));

        let valid_before_overflow = enqueue_committed(&database, "valid-before-overflow").await?;
        let overflow_id = enqueue_committed(&database, "overflow").await?;
        query("UPDATE dovecote_deliveries SET attempts = $1 WHERE event_row_id = $2")
            .bind(i64::MAX)
            .bind(overflow_id.get())
            .execute(&database.pool)
            .await?;
        let overflow = adapter
            .claim(
                WorkerId::new("worker-overflow")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(2)?,
            )
            .await;
        assert!(matches!(
            overflow,
            Err(ClaimError::CounterOverflow { row_id }) if row_id == overflow_id
        ));
        let valid_snapshot: (String, i64, Option<Vec<u8>>, Option<String>) = query_as(
            "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(valid_before_overflow.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            valid_snapshot,
            ("pending".to_owned(), 0, None, None)
        );
        let state: String = query_scalar(
            "SELECT state FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(overflow_id.get())
        .fetch_one(&database.pool)
        .await?;
        let attempts: i64 = query_scalar(
            "SELECT attempts FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(overflow_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!((state, attempts), ("pending".to_owned(), i64::MAX));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn concurrent_claims_do_not_overlap_when_configured() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter_a = PostgresDovecote::new(database.pool.clone());
        let adapter_b = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
async fn lock_timeout_is_a_typed_transient_mutation_error_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let row_id = enqueue_committed(&database, "lock-timeout").await?;
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let timeout_adapter = PostgresDovecote::new(timeout_pool.clone());
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
async fn blocked_ack_and_renew_cannot_revive_an_expired_claim_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let ack_adapter = PostgresDovecote::new(database.pool.clone());
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
        let renew_adapter = PostgresDovecote::new(database.pool.clone());
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

#[tokio::test]
async fn common_occurrence_time_endpoints_round_trip_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let minimum = time::OffsetDateTime::UNIX_EPOCH;
        let maximum = time::OffsetDateTime::new_in_offset(
            time::Date::from_calendar_date(9999, time::Month::December, 31)?,
            time::Time::from_hms_micro(23, 59, 59, 999_999)?,
            time::UtcOffset::UTC,
        );
        let minimum_id =
            enqueue_event_committed(&database, event_with_time("time-minimum", minimum)).await?;
        let maximum_id =
            enqueue_event_committed(&database, event_with_time("time-maximum", maximum)).await?;

        let adapter = PostgresDovecote::new(database.pool.clone());
        let rows = adapter.page(None, Limit::new(2)?).await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row_id(), minimum_id);
        assert_eq!(rows[0].event().time(), Some(minimum));
        assert_eq!(rows[1].row_id(), maximum_id);
        assert_eq!(rows[1].event().time(), Some(maximum));

        // Values outside the shared portable range are rejected by the event
        // constructor before an adapter transaction is even opened.
        assert!(
            NewEvent::builder(
                StreamName::new("audit")?,
                EventId::new("time-before-minimum")?,
                EventSource::new("https://example.test/source")?,
                EventType::new("com.example.time")?,
            )
            .time(minimum - time::Duration::microseconds(1))
            .build()
            .is_err()
        );
        assert!(
            NewEvent::builder(
                StreamName::new("audit")?,
                EventId::new("time-after-maximum")?,
                EventSource::new("https://example.test/source")?,
                EventType::new("com.example.time")?,
            )
            // The time crate's representable maximum is only nanoseconds
            // beyond the shared microsecond-precision upper endpoint.
            .time(maximum + time::Duration::nanoseconds(999))
            .build()
            .is_err()
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn crash_after_claim_commit_leaves_a_reclaimable_claim_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let row_id = enqueue_committed(&database, "crash-after-claim").await?;
        let first = adapter
            .claim(
                WorkerId::new("crashed-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let token = first.claim_token().clone();

        // Returning from claim proves its transaction committed. Dropping the
        // worker result models a process crash before any transport ack.
        let stored: (String, i64, Option<Vec<u8>>, Option<String>) = query_as(
            "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, "claimed");
        assert_eq!(stored.1, 1);
        assert_eq!(stored.2, Some(token.as_bytes().to_vec()));
        assert_eq!(stored.3.as_deref(), Some("crashed-worker"));
        drop(first);

        // Move database time past the lease as the recovery worker would
        // observe it, without making the test depend on wall-clock sleeps.
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() - INTERVAL '1 millisecond' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        let reclaimed = adapter
            .claim(
                WorkerId::new("recovery-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.row_id(), row_id);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), &token);
        adapter.ack(row_id, reclaimed.claim_token()).await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn transport_success_before_crash_can_produce_a_reclaimed_duplicate_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let row_id = enqueue_committed(&database, "transport-success-before-crash").await?;
        let claimed = adapter
            .claim(
                WorkerId::new("transport-worker")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        let original_token = claimed.claim_token().clone();
        let original_event_id = claimed.event().id().clone();

        // The fake transport accepts the event, then the worker crashes before
        // ack. This is deliberately outside any database transaction.
        let transport_accepted = true;
        assert!(transport_accepted);
        drop(claimed);
        let stored: (String, Option<time::OffsetDateTime>, Option<Vec<u8>>) = query_as(
            "SELECT state, delivered_at, claim_token FROM dovecote_deliveries WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0, "claimed");
        assert!(stored.1.is_none(), "transport success is not an ack");
        assert_eq!(stored.2, Some(original_token.as_bytes().to_vec()));

        // Recovery sees the expired lease and receives the same durable event
        // with a new token, making the possible duplicate explicit.
        query(
            "UPDATE dovecote_deliveries SET claim_expires_at = clock_timestamp() - INTERVAL '1 millisecond' WHERE event_row_id = $1",
        )
        .bind(row_id.get())
        .execute(&database.pool)
        .await?;
        let reclaimed = adapter
            .claim(
                WorkerId::new("transport-recovery")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_eq!(reclaimed.row_id(), row_id);
        assert_eq!(reclaimed.event().id(), &original_event_id);
        assert_eq!(reclaimed.attempts().get(), 2);
        assert_ne!(reclaimed.claim_token(), &original_token);
        adapter.ack(row_id, reclaimed.claim_token()).await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[derive(Clone, Copy, Debug)]
enum ExpectedMutationClassification {
    NotFound,
    IllegalTransition(DeliveryState),
    LostClaim,
}

fn assert_mutation_classification(
    operation: &str,
    result: Result<(), MutationError>,
    expected: ExpectedMutationClassification,
) -> Result<(), Box<dyn Error>> {
    match result {
        Err(MutationError::NotFound)
            if matches!(expected, ExpectedMutationClassification::NotFound) =>
        {
            Ok(())
        }
        Err(MutationError::LostClaim)
            if matches!(expected, ExpectedMutationClassification::LostClaim) =>
        {
            Ok(())
        }
        Err(MutationError::IllegalTransition { state }) if matches!(expected, ExpectedMutationClassification::IllegalTransition(expected_state) if expected_state == state) => {
            Ok(())
        }
        other => Err(format!("{operation} returned {other:?}, expected {expected:?}").into()),
    }
}

async fn assert_all_mutation_classifications(
    adapter: &PostgresDovecote,
    row_id: RowId,
    token: &ClaimToken,
    expected: ExpectedMutationClassification,
) -> Result<(), Box<dyn Error>> {
    let failure = Failure::new("classification", "classification detail")?;
    let reason = QuarantineReason::new("classification reason")?;
    let lease = Lease::new(std::time::Duration::from_secs(5))?;
    let delay = Delay::new(std::time::Duration::ZERO)?;
    assert_mutation_classification("renew", adapter.renew(row_id, token, lease).await, expected)?;
    assert_mutation_classification("ack", adapter.ack(row_id, token).await, expected)?;
    assert_mutation_classification(
        "retry",
        adapter.retry(row_id, token, &failure, delay).await,
        expected,
    )?;
    assert_mutation_classification(
        "release",
        adapter.release(row_id, token, delay).await,
        expected,
    )?;
    assert_mutation_classification(
        "quarantine",
        adapter.quarantine(row_id, token, &reason).await,
        expected,
    )?;
    Ok(())
}

#[tokio::test]
async fn every_mutation_classifies_missing_and_non_claimed_states_when_configured()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let token = ClaimToken::from_bytes([0x5a; dovecote::CLAIM_TOKEN_BYTES]);
        let delivered = enqueue_committed(&database, "classification-delivered").await?;
        let delivered_claim = adapter
            .claim(
                WorkerId::new("classification-delivered")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        adapter
            .ack(delivered, delivered_claim.claim_token())
            .await?;
        assert_all_mutation_classifications(
            &adapter,
            delivered,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Delivered),
        )
        .await?;

        // Keep this row unclaimed while the other fixtures are staged; the
        // claim API intentionally takes the lowest eligible row ID.
        let claimed = enqueue_committed(&database, "classification-claimed").await?;
        let claimed_event = adapter
            .claim(
                WorkerId::new("classification-claimed")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        assert_all_mutation_classifications(
            &adapter,
            claimed,
            &token,
            ExpectedMutationClassification::LostClaim,
        )
        .await?;
        assert!(
            adapter
                .ack(claimed, claimed_event.claim_token())
                .await
                .is_ok()
        );

        let quarantined = enqueue_committed(&database, "classification-quarantined").await?;
        let quarantined_claim = adapter
            .claim(
                WorkerId::new("classification-quarantined")?,
                Lease::new(std::time::Duration::from_secs(5))?,
                Limit::new(1)?,
            )
            .await?
            .remove(0);
        adapter
            .quarantine(
                quarantined,
                quarantined_claim.claim_token(),
                &QuarantineReason::new("classification")?,
            )
            .await?;
        assert_all_mutation_classifications(
            &adapter,
            quarantined,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Quarantined),
        )
        .await?;

        let pending = enqueue_committed(&database, "classification-pending").await?;
        assert_all_mutation_classifications(
            &adapter,
            pending,
            &token,
            ExpectedMutationClassification::IllegalTransition(DeliveryState::Pending),
        )
        .await?;

        let missing = RowId::new(i64::MAX)?;
        assert_all_mutation_classifications(
            &adapter,
            missing,
            &token,
            ExpectedMutationClassification::NotFound,
        )
        .await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn crash_before_claim_commit_exposes_no_claim_when_configured() -> Result<(), Box<dyn Error>>
{
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let row_id = enqueue_committed(&database, "crash-before-claim").await?;
        let key = advisory_key(&database.schema);
        let mut barrier = database.admin.begin().await?;
        query("SELECT pg_advisory_xact_lock($1)")
            .bind(key)
            .execute(&mut *barrier)
            .await?;

        install_trigger(
            &database,
            "dovecote_test_pause_claim",
            "dovecote_test_pause_claim",
            &format!(
                "IF NEW.state = 'claimed' AND OLD.state <> 'claimed' THEN PERFORM pg_advisory_xact_lock({key}); END IF; RETURN NEW;"
            ),
        )
        .await?;

        let claim_task = tokio::spawn(async move {
            adapter
                .claim(
                    WorkerId::new("crashed-before-commit").expect("valid worker"),
                    Lease::new(std::time::Duration::from_secs(5)).expect("valid lease"),
                    Limit::new(1).expect("valid limit"),
                )
                .await
        });
        let pid = match wait_for_active_query(
            &database.admin,
            &application_name(&database.schema),
        )
        .await
        {
            Ok(pid) => pid,
            Err(error) => {
                barrier.rollback().await?;
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), claim_task).await;
                remove_trigger(
                    &database,
                    "dovecote_test_pause_claim",
                    "dovecote_test_pause_claim",
                )
                .await?;
                return Err(error);
            }
        };
        assert!(query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&database.admin)
            .await?);
        let claim_result = tokio::time::timeout(std::time::Duration::from_secs(2), claim_task)
            .await??;
        assert!(claim_result.is_err(), "terminated claim unexpectedly succeeded");
        barrier.rollback().await?;
        remove_trigger(
            &database,
            "dovecote_test_pause_claim",
            "dovecote_test_pause_claim",
        )
        .await?;

        let stored: (String, i64, Option<Vec<u8>>, Option<String>, Option<time::OffsetDateTime>) =
            query_as(
                "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = $1",
            )
            .bind(row_id.get())
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(stored, ("pending".to_owned(), 0, None, None, None));
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let timeout_adapter = PostgresDovecote::new(timeout_pool.clone());
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
        let setup = PostgresDovecote::new(database.pool.clone());
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
        let first_adapter = PostgresDovecote::new(first_pool.clone());
        let second_adapter = PostgresDovecote::new(second_pool.clone());
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
        let setup = PostgresDovecote::new(database.pool.clone());
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
        let first_adapter = PostgresDovecote::new(first_pool.clone());
        let second_adapter = PostgresDovecote::new(second_pool.clone());
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

#[tokio::test]
async fn migration_import_is_idempotent_and_state_fenced() -> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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
        let adapter = PostgresDovecote::new(database.pool.clone());
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

#[tokio::test]
async fn migration_finalization_rejects_noncanonical_rows_and_preflights_schema()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let changed_availability = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-delayed", "com.example.import"),
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
            .bind(changed_availability.get())
            .execute(&database.pool)
            .await?;
        let mut transaction = database.pool.begin().await?;
        let conflict = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                changed_availability,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(
            conflict,
            Err(dovecote_sqlx_postgres::FinalizeError::StateConflict { row_id })
                if row_id == changed_availability
        ));
        transaction.rollback().await?;

        let invalid_timestamp = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-invalid-time", "com.example.import"),
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
        let mut transaction = database.pool.begin().await?;
        let invalid_result = adapter
            .finalize_pending_delivery_for_migration(&mut transaction, invalid_timestamp, invalid)
            .await;
        assert!(matches!(
            invalid_result,
            Err(dovecote_sqlx_postgres::FinalizeError::InvalidTimestamp { .. })
        ));
        transaction.rollback().await?;

        let mut transaction = database.pool.begin().await?;
        let missing = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                RowId::new(i64::MAX)?,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(
            missing,
            Err(dovecote_sqlx_postgres::FinalizeError::NotFound)
        ));
        transaction.rollback().await?;

        let schema_row = {
            let mut transaction = database.pool.begin().await?;
            let outcome = adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-finalize-schema", "com.example.import"),
                    ImportedDeliveryState::Pending,
                )
                .await?;
            transaction.commit().await?;
            match outcome {
                ImportOutcome::Imported { row_id } => row_id,
                other => return Err(format!("expected imported outcome, got {other:?}").into()),
            }
        };
        query("DROP INDEX dovecote_deliveries_claimable")
            .execute(&database.pool)
            .await?;
        let mut transaction = database.pool.begin().await?;
        let schema_result = adapter
            .finalize_pending_delivery_for_migration(
                &mut transaction,
                schema_row,
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .await;
        assert!(matches!(
            schema_result,
            Err(dovecote_sqlx_postgres::FinalizeError::MigrationMismatch { .. })
        ));
        transaction.rollback().await?;
        let state: String =
            query_scalar("SELECT state FROM dovecote_deliveries WHERE event_row_id = $1")
                .bind(schema_row.get())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(state, "pending");
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

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
        let adapter = PostgresDovecote::new(left_work_pool.clone());
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
        let adapter = PostgresDovecote::new(right_work_pool.clone());
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
        let adapter = PostgresDovecote::new(left_work_pool.clone());
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
        let adapter = PostgresDovecote::new(right_work_pool.clone());
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
        let adapter = PostgresDovecote::new(left_work_pool.clone());
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
        let adapter = PostgresDovecote::new(right_work_pool.clone());
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

#[test]
fn migration_import_state_rejects_submicrosecond_delivery_time() {
    let invalid = time::OffsetDateTime::UNIX_EPOCH
        .replace_nanosecond(1)
        .expect("valid nanosecond");
    assert!(dovecote::ImportedDeliveryState::delivered(invalid).is_err());
}
