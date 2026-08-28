//! Live MySQL/MariaDB conformance gates.
//!
//! The harness intentionally never drops tables: point `DOVECOTE_MYSQL_URL` at
//! a disposable database (the matrix creates one per server run).  In
//! required mode an omitted URL is a failure rather than an accidental skip.
//!
//! Concurrent same-identity importer races are not asserted here.  A portable
//! LOCK TABLES READ blocker plus observable insert waiters differs between
//! MySQL and MariaDB releases; a preflight barrier alone would not establish
//! that both sessions competed at the identity insert.  The PostgreSQL suite
//! has the equivalent live evidence through pg_locks; MySQL/MariaDB coverage
//! remains an explicit live evidence gap until a server-matrix coordination
//! fixture can observe this boundary without timing assumptions.

use dovecote::{
    ClaimToken, Delay, DeliveryState, EnqueueOutcome, EventId, EventSource, EventType, Failure,
    FinalizeOutcome, ImportOutcome, ImportedDeliveryState, Lease, Limit, NewEvent,
    QuarantineReason, RowId, StreamName, WorkerId,
};
use dovecote_sqlx_mysql::{
    ClaimError, EnqueueError, MIGRATIONS, MutationError, MySqlDovecote, PageError, TransientKind,
};
use sqlx::{MySqlPool, mysql::MySqlPoolOptions, query, query_as, query_scalar};
use std::error::Error;
use std::sync::OnceLock;

static CONFORMANCE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static INSTALL_DONE: OnceLock<()> = OnceLock::new();

async fn serialize_live_tests() -> tokio::sync::MutexGuard<'static, ()> {
    CONFORMANCE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).ok().as_deref().is_some_and(is_truthy)
}

fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "yes")
}

fn required() -> bool {
    env_flag("DOVECOTE_MYSQL_REQUIRED")
        || env_flag("DOVECOTE_RELEASE_MODE")
        || (env_flag("CI") && !env_flag("DOVECOTE_MYSQL_OPTIONAL"))
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

#[tokio::test]
async fn matrix_skip_locked_claims_are_non_overlapping() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    assert!(
        sqlx::query("INSERT INTO dovecote_events (row_id, stream, specversion, event_id, source, event_type, extensions) VALUES (-1, ?, ?, ?, ?, ?, ?)")
            .bind(b"mysql-conformance".as_slice())
            .bind(b"1.0".as_slice())
            .bind(b"negative-id".as_slice())
            .bind(b"https://dovecote.test/mysql".as_slice())
            .bind(b"conformance.event".as_slice())
            .bind(b"{}".as_slice())
            .execute(&pool)
            .await
            .is_err()
    );
    let mut tx = pool.begin().await?;
    adapter.enqueue(&mut tx, event("parallel-a")).await?;
    adapter.enqueue(&mut tx, event("parallel-b")).await?;
    tx.commit().await?;
    let left = adapter.clone();
    let right = adapter.clone();
    let (a, b) = tokio::join!(
        left.claim(
            WorkerId::new("parallel-left")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?
        ),
        right.claim(
            WorkerId::new("parallel-right")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?
        )
    );
    let a = a?;
    let b = b?;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_ne!(a[0].row_id(), b[0].row_id());
    pool.close().await;
    Ok(())
}

async fn live_pool() -> Result<Option<MySqlPool>, Box<dyn std::error::Error>> {
    let Some(url) = std::env::var_os("DOVECOTE_MYSQL_URL") else {
        if required() {
            return Err("DOVECOTE_MYSQL_URL is required for MySQL/MariaDB conformance".into());
        }
        eprintln!("skipping MySQL/MariaDB conformance: DOVECOTE_MYSQL_URL is unset");
        return Ok(None);
    };
    Ok(Some(
        MySqlPoolOptions::new()
            .max_connections(4)
            .connect(url.to_str().ok_or("database URL is not UTF-8")?)
            .await?,
    ))
}

async fn install(pool: &MySqlPool) -> Result<(), Box<dyn std::error::Error>> {
    if INSTALL_DONE.get().is_some() {
        return Ok(());
    }
    // The release artifact has no procedural delimiter; each statement is
    // executed separately so this works with both SQLx and MariaDB clients.
    let mut trigger = false;
    let mut buffered = String::new();
    for fragment in MIGRATIONS[0].sql().split(';') {
        let fragment = fragment.trim();
        if fragment.is_empty() {
            continue;
        }

        if fragment.to_ascii_uppercase().starts_with("CREATE TRIGGER") || trigger {
            if !buffered.is_empty() {
                buffered.push(';');
            }
            buffered.push_str(fragment);
            trigger = !fragment.to_ascii_uppercase().ends_with("END");
            if trigger {
                continue;
            }

            let statement = buffered.trim();
            let statement: &'static str = Box::leak(statement.to_owned().into_boxed_str());
            sqlx::raw_sql(statement).execute(pool).await?;
            buffered.clear();
            continue;
        }

        let statement = fragment;
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }

    let _ = INSTALL_DONE.set(());
    Ok(())
}

async fn clear_conformance_rows(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE d FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.stream = ?")
        .bind(b"mysql-conformance".as_slice()).execute(pool).await?;
    sqlx::query("DELETE FROM dovecote_events WHERE stream = ?")
        .bind(b"mysql-conformance".as_slice())
        .execute(pool)
        .await?;
    Ok(())
}

fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("mysql-conformance").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://dovecote.test/mysql").unwrap(),
        EventType::new("conformance.event").unwrap(),
    )
    .unwrap()
}

fn event_with_type(id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("mysql-conformance").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://dovecote.test/mysql").unwrap(),
        EventType::new(event_type).unwrap(),
    )
    .unwrap()
}

fn maximum_timestamp() -> time::OffsetDateTime {
    time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    )
}

#[derive(Debug, sqlx::FromRow)]
struct RetryRow {
    state: Vec<u8>,
    available_at: time::OffsetDateTime,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<time::OffsetDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
struct ReleaseRow {
    state: Vec<u8>,
    available_at: time::OffsetDateTime,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<time::OffsetDateTime>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
struct QuarantineRow {
    state: Vec<u8>,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<time::OffsetDateTime>,
    quarantined_at: Option<time::OffsetDateTime>,
    quarantine_reason: Option<Vec<u8>>,
    last_failure_code: Option<Vec<u8>>,
    last_failure_detail: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow)]
struct AckRow {
    state: Vec<u8>,
    delivered_at: Option<time::OffsetDateTime>,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
    claim_expires_at: Option<time::OffsetDateTime>,
    quarantined_at: Option<time::OffsetDateTime>,
    quarantine_reason: Option<Vec<u8>>,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
struct DeliveryStateRow {
    state: Vec<u8>,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<Vec<u8>>,
}

#[tokio::test]
async fn matrix_backend_settings_and_exact_schema() -> Result<(), Box<dyn std::error::Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let info = adapter.backend_info().await?;
    assert!(matches!(
        info.kind,
        dovecote_sqlx_mysql::BackendKind::MySql | dovecote_sqlx_mysql::BackendKind::MariaDb
    ));
    assert!(info.capabilities.skip_locked);
    assert_eq!(
        info.transaction_isolation.to_ascii_uppercase(),
        "REPEATABLE-READ"
    );
    adapter.check_schema().await?;
    pool.close().await;
    Ok(())
}

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
    sqlx::query("INSERT INTO dovecote_events (row_id, stream, specversion, event_id, source, event_type, extensions) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(direct_id).bind(b"mysql-conformance".as_slice()).bind(b"1.0".as_slice())
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
async fn matrix_wrong_token_is_fenced_for_every_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter.enqueue(&mut transaction, event("fencing")).await?;
    transaction.commit().await?;
    let row_id = match outcome {
        EnqueueOutcome::Enqueued { row_id } | EnqueueOutcome::AlreadyEnqueued { row_id } => row_id,
        _ => return Err("unknown enqueue outcome".into()),
    };

    let claimed = adapter
        .claim(
            WorkerId::new("fencing-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?;
    let good = claimed.first().ok_or("claim returned no row")?;
    let bad = ClaimToken::from_bytes([0xA5; dovecote::CLAIM_TOKEN_BYTES]);
    let delay = Delay::new(std::time::Duration::from_secs(1))?;
    let failure = Failure::new("test.failure", "fencing")?;
    let reason = QuarantineReason::new("fencing")?;
    assert!(matches!(
        adapter
            .renew(row_id, &bad, Lease::new(std::time::Duration::from_secs(1))?)
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.ack(row_id, &bad).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.retry(row_id, &bad, &failure, delay).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.release(row_id, &bad, delay).await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.quarantine(row_id, &bad, &reason).await,
        Err(MutationError::LostClaim)
    ));
    adapter.ack(row_id, good.claim_token()).await?;
    pool.close().await;
    Ok(())
}

async fn enqueue_committed(
    pool: &MySqlPool,
    event: NewEvent,
) -> Result<dovecote::RowId, Box<dyn Error>> {
    let adapter = MySqlDovecote::new(pool.clone());
    let mut transaction = pool.begin().await?;
    let outcome = adapter.enqueue(&mut transaction, event).await?;
    transaction.commit().await?;
    match outcome {
        EnqueueOutcome::Enqueued { row_id } | EnqueueOutcome::AlreadyEnqueued { row_id } => {
            Ok(row_id)
        }
        _ => Err("unknown enqueue outcome".into()),
    }
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

#[tokio::test]
async fn mysql_expired_reclaim_rotates_token_and_classifies_stale_calls()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("reclaim")).await?;
    let first = adapter
        .claim(
            WorkerId::new("reclaim-first")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    let first_token = first.claim_token().clone();
    query("UPDATE dovecote_deliveries SET claim_expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;

    assert!(matches!(
        adapter.ack(row_id, &first_token).await,
        Err(MutationError::LostClaim)
    ));
    let reclaimed = adapter
        .claim(
            WorkerId::new("reclaim-second")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(reclaimed.row_id(), row_id);
    assert_eq!(reclaimed.attempts().get(), 2);
    assert_ne!(reclaimed.claim_token(), &first_token);

    let failure = Failure::new("stale", "must not mutate")?;
    let reason = QuarantineReason::new("stale")?;
    assert!(matches!(
        adapter
            .renew(
                row_id,
                &first_token,
                Lease::new(std::time::Duration::from_secs(1))?
            )
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter
            .retry(
                row_id,
                &first_token,
                &failure,
                Delay::new(std::time::Duration::ZERO)?
            )
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter
            .release(row_id, &first_token, Delay::new(std::time::Duration::ZERO)?)
            .await,
        Err(MutationError::LostClaim)
    ));
    assert!(matches!(
        adapter.quarantine(row_id, &first_token, &reason).await,
        Err(MutationError::LostClaim)
    ));
    adapter.ack(row_id, reclaimed.claim_token()).await?;
    assert!(matches!(
        adapter.ack(row_id, reclaimed.claim_token()).await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Delivered
        })
    ));
    for mutation in ["renew", "retry", "release", "quarantine"] {
        let result = match mutation {
            "renew" => {
                adapter
                    .renew(
                        row_id,
                        reclaimed.claim_token(),
                        Lease::new(std::time::Duration::from_secs(1))?,
                    )
                    .await
            }
            "retry" => {
                adapter
                    .retry(
                        row_id,
                        reclaimed.claim_token(),
                        &failure,
                        Delay::new(std::time::Duration::ZERO)?,
                    )
                    .await
            }
            "release" => {
                adapter
                    .release(
                        row_id,
                        reclaimed.claim_token(),
                        Delay::new(std::time::Duration::ZERO)?,
                    )
                    .await
            }
            "quarantine" => {
                adapter
                    .quarantine(row_id, reclaimed.claim_token(), &reason)
                    .await
            }
            _ => unreachable!(),
        };
        assert!(
            matches!(
                result,
                Err(MutationError::IllegalTransition {
                    state: DeliveryState::Delivered
                })
            ),
            "{mutation} did not classify delivered row"
        );
    }

    assert!(matches!(
        adapter
            .ack(dovecote::RowId::new(i64::MAX)?, reclaimed.claim_token())
            .await,
        Err(MutationError::NotFound)
    ));
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_lifecycle_mutations_use_database_time_and_preserve_fields()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("lifecycle")).await?;
    let ack_id = enqueue_committed(&pool, event("lifecycle-ack")).await?;
    let lease = Lease::new(std::time::Duration::from_secs(5))?;
    let claim = adapter
        .claim(WorkerId::new("lifecycle-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let token = claim.claim_token().clone();

    // Make the original expiry close to the database clock, then prove renew
    // is computed from the operation clock rather than from the old expiry.
    query("UPDATE dovecote_deliveries SET claim_expires_at = UTC_TIMESTAMP(6) + INTERVAL 1 SECOND WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    adapter.renew(row_id, &token, lease).await?;
    let renewed_expiry: time::OffsetDateTime =
        query_scalar("SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?")
            .bind(row_id.get())
            .fetch_one(&pool)
            .await?;
    let now: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    assert!(renewed_expiry > now + time::Duration::seconds(4));

    let failure = Failure::new("transport_unavailable", "temporary")?;
    let before_retry: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    adapter
        .retry(
            row_id,
            &token,
            &failure,
            Delay::new(std::time::Duration::from_secs(5))?,
        )
        .await?;
    let retry_row: RetryRow = query_as(
        "SELECT state, available_at, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(retry_row.state, b"pending");
    assert!(retry_row.available_at > before_retry);
    assert!(
        retry_row.claim_token.is_none()
            && retry_row.claimed_by.is_none()
            && retry_row.claim_expires_at.is_none()
    );
    assert_eq!(
        retry_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        retry_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    // Advance the fixture with database time so no wall-clock sleep is part
    // of the conformance test.
    query("UPDATE dovecote_deliveries SET available_at = UTC_TIMESTAMP(6) WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    let reclaimed = adapter
        .claim(WorkerId::new("release-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let release_token = reclaimed.claim_token().clone();
    let before_release: time::OffsetDateTime = query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&pool)
        .await?;
    adapter
        .release(
            row_id,
            &release_token,
            Delay::new(std::time::Duration::from_secs(5))?,
        )
        .await?;
    let release_row: ReleaseRow = query_as(
        "SELECT state, available_at, claim_token, claimed_by, claim_expires_at, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(release_row.state, b"pending");
    assert!(release_row.available_at > before_release);
    assert!(
        release_row.claim_token.is_none()
            && release_row.claimed_by.is_none()
            && release_row.claim_expires_at.is_none()
    );
    assert_eq!(
        release_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        release_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    query("UPDATE dovecote_deliveries SET available_at = UTC_TIMESTAMP(6) WHERE event_row_id = ?")
        .bind(row_id.get())
        .execute(&pool)
        .await?;
    let final_claim = adapter
        .claim(WorkerId::new("quarantine-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    let reason = QuarantineReason::new("operator_review")?;
    adapter
        .quarantine(row_id, final_claim.claim_token(), &reason)
        .await?;
    let quarantine_row: QuarantineRow = query_as(
        "SELECT state, claim_token, claimed_by, claim_expires_at, quarantined_at, quarantine_reason, last_failure_code, last_failure_detail FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(quarantine_row.state, b"quarantined");
    assert!(quarantine_row.claim_token.is_none());
    assert!(quarantine_row.claimed_by.is_none() && quarantine_row.claim_expires_at.is_none());
    assert!(quarantine_row.quarantined_at.is_some());
    assert_eq!(
        quarantine_row.quarantine_reason.as_deref(),
        Some(b"operator_review".as_slice())
    );
    assert_eq!(
        quarantine_row.last_failure_code.as_deref(),
        Some(b"transport_unavailable".as_slice())
    );
    assert_eq!(
        quarantine_row.last_failure_detail.as_deref(),
        Some(b"temporary".as_slice())
    );

    let ack_claim = adapter
        .claim(WorkerId::new("ack-worker")?, lease, Limit::new(1)?)
        .await?
        .remove(0);
    assert_eq!(ack_claim.row_id(), ack_id);
    adapter.ack(ack_id, ack_claim.claim_token()).await?;
    let ack_row: AckRow = query_as(
        "SELECT state, delivered_at, claim_token, claimed_by, claim_expires_at, quarantined_at, quarantine_reason FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(ack_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(ack_row.state, b"delivered");
    assert!(ack_row.delivered_at.is_some());
    assert!(
        ack_row.claim_token.is_none()
            && ack_row.claimed_by.is_none()
            && ack_row.claim_expires_at.is_none()
            && ack_row.quarantined_at.is_none()
            && ack_row.quarantine_reason.is_none()
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_mutation_categories_are_exact_for_pending_terminal_and_missing_rows()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let pending_id = enqueue_committed(&pool, event("categories")).await?;
    let token = ClaimToken::from_bytes([0xC3; dovecote::CLAIM_TOKEN_BYTES]);
    let delay = Delay::new(std::time::Duration::ZERO)?;
    let failure = Failure::new("temporary", "retry")?;
    let reason = QuarantineReason::new("terminal")?;

    assert!(matches!(
        adapter.ack(pending_id, &token).await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Pending
        })
    ));
    assert!(matches!(
        adapter.ack(dovecote::RowId::new(i64::MAX)?, &token).await,
        Err(MutationError::NotFound)
    ));

    let first_claim = adapter
        .claim(
            WorkerId::new("category-first")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    adapter
        .retry(pending_id, first_claim.claim_token(), &failure, delay)
        .await?;
    assert!(matches!(
        adapter
            .release(pending_id, first_claim.claim_token(), delay)
            .await,
        Err(MutationError::IllegalTransition {
            state: DeliveryState::Pending
        })
    ));

    let second_claim = adapter
        .claim(
            WorkerId::new("category-second")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    adapter
        .quarantine(pending_id, second_claim.claim_token(), &reason)
        .await?;
    for mutation in ["ack", "renew", "retry", "release", "quarantine"] {
        let result = match mutation {
            "ack" => adapter.ack(pending_id, second_claim.claim_token()).await,
            "renew" => {
                adapter
                    .renew(
                        pending_id,
                        second_claim.claim_token(),
                        Lease::new(std::time::Duration::from_secs(1))?,
                    )
                    .await
            }
            "retry" => {
                adapter
                    .retry(pending_id, second_claim.claim_token(), &failure, delay)
                    .await
            }
            "release" => {
                adapter
                    .release(pending_id, second_claim.claim_token(), delay)
                    .await
            }
            "quarantine" => {
                adapter
                    .quarantine(pending_id, second_claim.claim_token(), &reason)
                    .await
            }
            _ => unreachable!(),
        };
        assert!(
            matches!(
                result,
                Err(MutationError::IllegalTransition {
                    state: DeliveryState::Quarantined
                })
            ),
            "{mutation} did not classify quarantined row"
        );
    }
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_attempt_overflow_rolls_back_the_entire_claim_batch() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let first_id = enqueue_committed(&pool, event("overflow-first")).await?;
    let overflow_id = enqueue_committed(&pool, event("overflow-second")).await?;
    query("UPDATE dovecote_deliveries SET attempts = ? WHERE event_row_id = ?")
        .bind(i64::MAX)
        .bind(overflow_id.get())
        .execute(&pool)
        .await?;

    let result = adapter
        .claim(
            WorkerId::new("overflow-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(2)?,
        )
        .await;
    assert!(matches!(
        result,
        Err(ClaimError::CounterOverflow { row_id }) if row_id == overflow_id
    ));

    let first: DeliveryStateRow = query_as(
        "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(first_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        first,
        DeliveryStateRow {
            state: b"pending".to_vec(),
            attempts: 0,
            claim_token: None,
            claimed_by: None,
        }
    );
    let overflow: DeliveryStateRow = query_as(
        "SELECT state, attempts, claim_token, claimed_by FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(overflow_id.get())
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        overflow,
        DeliveryStateRow {
            state: b"pending".to_vec(),
            attempts: i64::MAX,
            claim_token: None,
            claimed_by: None,
        }
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_snapshot_pages_have_a_fixed_bound_and_release_connections()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let first_id = enqueue_committed(&pool, event("snapshot-first")).await?;
    let second_id = enqueue_committed(&pool, event("snapshot-second")).await?;

    // Allocate a lower row ID in an uncommitted transaction, then commit a
    // later row. This is the commit-inversion race documented by the SPEC.
    let mut earlier_transaction = pool.begin().await?;
    let earlier = adapter
        .enqueue(
            &mut earlier_transaction,
            event("snapshot-inversion-earlier"),
        )
        .await?;
    let earlier_id = match earlier {
        EnqueueOutcome::Enqueued { row_id } => row_id,
        other => return Err(format!("expected fresh inversion insert, got {other:?}").into()),
    };

    let later_id = enqueue_committed(&pool, event("snapshot-inversion-later")).await?;
    assert!(earlier_id < later_id);

    let mut snapshot = adapter.begin_snapshot().await?;
    assert_eq!(snapshot.upper_bound(), Some(later_id));
    let live_before = adapter.page(None, Limit::new(100)?).await?;
    assert_eq!(
        live_before
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![first_id, second_id, later_id]
    );
    earlier_transaction.commit().await?;
    assert!(
        adapter
            .page(Some(later_id), Limit::new(100)?)
            .await?
            .is_empty()
    );

    let first_page = snapshot.next_page(Limit::new(2)?).await?;
    assert_eq!(
        first_page
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![first_id, second_id]
    );
    let second_page = snapshot.next_page(Limit::new(2)?).await?;
    assert_eq!(
        second_page
            .iter()
            .map(|row| row.row_id())
            .collect::<Vec<_>>(),
        vec![later_id]
    );
    assert!(snapshot.is_exhausted());
    assert!(snapshot.next_page(Limit::new(2)?).await?.is_empty());
    snapshot.finish().await?;

    let url = std::env::var("DOVECOTE_MYSQL_URL")?;
    let single = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect(&url)
        .await?;
    let closable = MySqlDovecote::new(single.clone());
    let closable_snapshot = closable.begin_snapshot().await?;
    assert!(matches!(
        single.acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    closable_snapshot.close().await?;
    query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&single)
        .await?;
    let dropped_snapshot = closable.begin_snapshot().await?;
    assert!(matches!(
        single.acquire().await,
        Err(sqlx::Error::PoolTimedOut)
    ));
    drop(dropped_snapshot);
    query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&single)
        .await?;
    single.close().await;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_datetime_common_range_endpoints_round_trip_without_precision_loss()
-> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };

    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let minimum = time::OffsetDateTime::UNIX_EPOCH;
    let maximum = time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31)?,
        time::Time::from_hms_micro(23, 59, 59, 999_999)?,
        time::UtcOffset::UTC,
    );
    let timed_event = |id: &str, at: time::OffsetDateTime| {
        NewEvent::builder(
            StreamName::new("mysql-conformance").expect("valid stream"),
            EventId::new(id).expect("valid id"),
            EventSource::new("https://dovecote.test/mysql").expect("valid source"),
            EventType::new("conformance.event").expect("valid type"),
        )
        .time(at)
        .build()
        .expect("valid timestamp")
    };

    let minimum_id = enqueue_committed(&pool, timed_event("datetime-minimum", minimum)).await?;
    let maximum_id = enqueue_committed(&pool, timed_event("datetime-maximum", maximum)).await?;
    let mut replay_transaction = pool.begin().await?;
    let replay = adapter
        .enqueue(
            &mut replay_transaction,
            timed_event("datetime-maximum", maximum),
        )
        .await?;
    assert_eq!(
        replay,
        EnqueueOutcome::AlreadyEnqueued { row_id: maximum_id }
    );
    replay_transaction.commit().await?;
    let rows = adapter.page(None, Limit::new(10)?).await?;
    let minimum_row = rows
        .iter()
        .find(|row| row.row_id() == minimum_id)
        .expect("minimum row");
    let maximum_row = rows
        .iter()
        .find(|row| row.row_id() == maximum_id)
        .expect("maximum row");
    assert_eq!(minimum_row.event().time(), Some(minimum));
    assert_eq!(maximum_row.event().time(), Some(maximum));

    let minimum_claim = adapter
        .claim(
            WorkerId::new("datetime-minimum-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(minimum_claim.row_id(), minimum_id);
    adapter.ack(minimum_id, minimum_claim.claim_token()).await?;
    let maximum_claim = adapter
        .claim(
            WorkerId::new("datetime-maximum-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);
    assert_eq!(maximum_claim.row_id(), maximum_id);
    assert_eq!(maximum_claim.event().time(), Some(maximum));
    adapter.ack(maximum_id, maximum_claim.claim_token()).await?;

    let out_of_range = NewEvent::builder(
        StreamName::new("mysql-conformance")?,
        EventId::new("datetime-out-of-range")?,
        EventSource::new("https://dovecote.test/mysql")?,
        EventType::new("conformance.event")?,
    )
    .time(minimum - time::Duration::microseconds(1))
    .build();
    assert!(
        out_of_range.is_err(),
        "timestamps beyond DATETIME common range must fail before SQL"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn mysql_lock_timeout_is_returned_as_a_typed_transient_error() -> Result<(), Box<dyn Error>> {
    let _serial = serialize_live_tests().await;
    let Some(pool) = live_pool().await? else {
        return Ok(());
    };
    install(&pool).await?;
    clear_conformance_rows(&pool).await?;
    let adapter = MySqlDovecote::new(pool.clone());
    let row_id = enqueue_committed(&pool, event("lock-timeout")).await?;
    let claim = adapter
        .claim(
            WorkerId::new("lock-timeout-worker")?,
            Lease::new(std::time::Duration::from_secs(30))?,
            Limit::new(1)?,
        )
        .await?
        .remove(0);

    let mut locker = pool.begin().await?;
    query("SELECT event_row_id FROM dovecote_deliveries WHERE event_row_id = ? FOR UPDATE")
        .bind(row_id.get())
        .fetch_one(&mut *locker)
        .await?;

    let url = std::env::var("DOVECOTE_MYSQL_URL")?;
    let timeout_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&url)
        .await?;
    query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&timeout_pool)
        .await?;
    let timeout_adapter = MySqlDovecote::new(timeout_pool.clone());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        timeout_adapter.ack(row_id, claim.claim_token()),
    )
    .await?;
    match result {
        Err(MutationError::Transient {
            kind: TransientKind::StatementOrLockTimeout,
            source,
            ..
        }) => {
            let number = source.as_database_error().and_then(|error| {
                error
                    .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                    .map(|error| error.number())
            });
            assert_eq!(number, Some(1205));
        }
        other => return Err(format!("expected typed lock timeout, got {other:?}").into()),
    }
    locker.rollback().await?;
    timeout_pool.close().await;
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
        query("CREATE INDEX dovecote_deliveries_claimable ON dovecote_deliveries (state, available_at, event_row_id)")
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
