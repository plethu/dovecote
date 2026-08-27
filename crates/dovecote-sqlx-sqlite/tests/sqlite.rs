use dovecote::{ContentType, EventSubject, ExtensionName, ExtensionValue, PartitionKey, SchemaUri};
use dovecote::{
    Delay, EventData, EventId, EventSource, EventType, Extensions, Failure, Lease, Limit, NewEvent,
    QuarantineReason, StreamName, WorkerId,
};
use dovecote_sqlx_sqlite::{MIGRATIONS, SqliteDovecote, check_schema};
use sqlx::{AssertSqlSafe, SqlitePool, raw_sql, sqlite::SqlitePoolOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

async fn database() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("SQLite pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("migration");
    check_schema(&pool).await.expect("schema");
    let version: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&pool)
        .await
        .expect("SQLite runtime version");
    assert!(!version.is_empty());
    eprintln!("SQLite linked runtime version: {version}");
    pool
}

async fn file_database(busy_timeout: Duration) -> (SqlitePool, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "dovecote-sqlite-{}-{}.db",
        std::process::id(),
        unique_suffix()
    ));
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .busy_timeout(busy_timeout);
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("SQLite file pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("migration");
    check_schema(&pool).await.expect("schema");
    (pool, path)
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn event(id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new(id).unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum MutationExpectation {
    NotFound,
    LostClaim,
    IllegalTransition(dovecote::DeliveryState),
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct DurableDeliveryState {
    state: String,
    attempts: i64,
    claim_token: Option<Vec<u8>>,
    claimed_by: Option<String>,
    claim_expires_at: Option<String>,
    available_at: String,
    last_failure_code: Option<String>,
    last_failure_detail: Option<String>,
    delivered_at: Option<String>,
    quarantined_at: Option<String>,
    quarantine_reason: Option<String>,
}

async fn durable_delivery_state(
    pool: &SqlitePool,
    row_id: dovecote::RowId,
) -> Option<DurableDeliveryState> {
    sqlx::query_as(
        "SELECT state, attempts, claim_token, claimed_by, claim_expires_at, available_at, last_failure_code, last_failure_detail, delivered_at, quarantined_at, quarantine_reason FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(row_id.get())
    .fetch_optional(pool)
    .await
    .unwrap()
}

#[derive(Default)]
struct FakeTransport {
    accepted: Vec<(String, String)>,
}

impl FakeTransport {
    fn accept(&mut self, event: &dovecote::ClaimedEvent) {
        self.accepted.push((
            event.event().source().as_str().to_owned(),
            event.event().id().as_str().to_owned(),
        ));
    }
}

fn assert_mutation_classification(
    operation: &str,
    result: Result<(), dovecote_sqlx_sqlite::MutationError>,
    expected: MutationExpectation,
) {
    match (result, expected) {
        (Err(dovecote_sqlx_sqlite::MutationError::NotFound), MutationExpectation::NotFound)
        | (Err(dovecote_sqlx_sqlite::MutationError::LostClaim), MutationExpectation::LostClaim) => {
        }
        (
            Err(dovecote_sqlx_sqlite::MutationError::IllegalTransition { state }),
            MutationExpectation::IllegalTransition(expected),
        ) => assert_eq!(state, expected, "{operation} returned the wrong state"),
        (result, expected) => panic!("{operation} returned {result:?}, expected {expected:?}"),
    }
}

async fn assert_all_mutation_classifications(
    adapter: &SqliteDovecote,
    pool: &SqlitePool,
    row_id: dovecote::RowId,
    token: &dovecote::ClaimToken,
    expected: MutationExpectation,
) {
    let before = if matches!(expected, MutationExpectation::NotFound) {
        None
    } else {
        Some(
            durable_delivery_state(pool, row_id)
                .await
                .expect("delivery row"),
        )
    };
    let failure = Failure::new("classification", "classification detail").unwrap();
    let reason = QuarantineReason::new("classification reason").unwrap();
    let lease = Lease::new(Duration::from_secs(5)).unwrap();
    let delay = Delay::new(Duration::ZERO).unwrap();
    assert_mutation_classification("renew", adapter.renew(row_id, token, lease).await, expected);
    assert_mutation_classification("ack", adapter.ack(row_id, token).await, expected);
    assert_mutation_classification(
        "retry",
        adapter.retry(row_id, token, &failure, delay).await,
        expected,
    );
    assert_mutation_classification(
        "release",
        adapter.release(row_id, token, delay).await,
        expected,
    );
    assert_mutation_classification(
        "quarantine",
        adapter.quarantine(row_id, token, &reason).await,
        expected,
    );
    if let Some(before) = before {
        assert_eq!(
            durable_delivery_state(pool, row_id).await,
            Some(before),
            "failed mutation group changed durable delivery state",
        );
    }
}

#[test]
fn identity_boundary_accepts_2048_bytes_and_rejects_2049() {
    let id = EventId::new("i".repeat(1_024)).unwrap();
    let source = EventSource::new("s".repeat(1_024)).unwrap();
    assert!(
        NewEvent::new(
            StreamName::new("audit").unwrap(),
            id,
            source,
            EventType::new("com.example.boundary").unwrap(),
        )
        .is_ok()
    );
    assert!(
        NewEvent::new(
            StreamName::new("audit").unwrap(),
            EventId::new("i".repeat(1_024)).unwrap(),
            EventSource::new("s".repeat(1_025)).unwrap(),
            EventType::new("com.example.boundary").unwrap(),
        )
        .is_err()
    );
}

#[tokio::test]
async fn database_identity_boundary_inserts_and_deduplicates_2048_bytes() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool);
    let source = EventSource::new("s".repeat(1_024)).unwrap();
    let id = EventId::new("i".repeat(1_024)).unwrap();
    let event = NewEvent::new(
        StreamName::new("audit").unwrap(),
        id,
        source,
        EventType::new("com.example.boundary").unwrap(),
    )
    .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let first = adapter
        .enqueue(&mut transaction, event.clone())
        .await
        .unwrap();
    let second = adapter.enqueue(&mut transaction, event).await.unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(first, dovecote::EnqueueOutcome::Enqueued { .. }));
    assert!(matches!(
        second,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. }
    ));
}

#[tokio::test]
async fn enqueue_claim_mutate_and_page_round_trip() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    let first = adapter
        .enqueue(&mut transaction, event("one"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let replay = {
        let mut transaction = adapter.begin_write().await.unwrap();
        let replay = adapter
            .enqueue(&mut transaction, event("one"))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        replay
    };
    assert!(matches!(first, dovecote::EnqueueOutcome::Enqueued { .. }));
    assert!(matches!(
        replay,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. }
    ));
    let conflict_event = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("one").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.changed").unwrap(),
    )
    .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    let conflict = adapter.enqueue(&mut transaction, conflict_event).await;
    transaction.rollback().await.unwrap();
    assert!(matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));

    let worker = WorkerId::new("worker-a").unwrap();
    let claim = adapter
        .claim(
            worker,
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claim.len(), 1);
    let row_id = claim[0].row_id();
    let token = claim[0].claim_token().clone();
    adapter
        .renew(row_id, &token, Lease::new(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    adapter
        .retry(
            row_id,
            &token,
            &Failure::new("temporary", "try again").unwrap(),
            Delay::new(Duration::ZERO).unwrap(),
        )
        .await
        .unwrap();
    let claim = adapter
        .claim(
            WorkerId::new("worker-b").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claim[0].attempts().get(), 2);
    adapter
        .ack(claim[0].row_id(), claim[0].claim_token())
        .await
        .unwrap();
    let rows = adapter.page(None, Limit::new(10).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].delivery().state(),
        dovecote::DeliveryState::Delivered
    );
}

#[tokio::test]
async fn round_trip_hydrates_all_event_content_and_delivery_fields() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("attemptkind").unwrap(),
            ExtensionValue::string("full").unwrap(),
        )
        .unwrap();
    let event = NewEvent::builder(
        StreamName::new("audit").unwrap(),
        EventId::new("full").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.full").unwrap(),
    )
    .subject(EventSubject::new("subject").unwrap())
    .time(time::OffsetDateTime::UNIX_EPOCH)
    .datacontenttype(ContentType::new("application/json").unwrap())
    .dataschema(SchemaUri::new("https://example.test/schema").unwrap())
    .partitionkey(PartitionKey::new("partition").unwrap())
    .extensions(extensions)
    .data(EventData::json(br#"{"value": 1}"#.to_vec()).unwrap())
    .build()
    .unwrap();
    let expected_extensions = event.extensions().canonical_json();
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event.clone())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let rows = adapter.page(None, Limit::new(10).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 1);
    let restored = rows[0].event();
    assert_eq!(restored.stream(), event.stream());
    assert_eq!(restored.id(), event.id());
    assert_eq!(restored.source(), event.source());
    assert_eq!(restored.event_type(), event.event_type());
    assert_eq!(restored.subject(), event.subject());
    assert_eq!(restored.time(), event.time());
    assert_eq!(restored.datacontenttype(), event.datacontenttype());
    assert_eq!(restored.dataschema(), event.dataschema());
    assert_eq!(restored.partitionkey(), event.partitionkey());
    assert_eq!(restored.extensions().canonical_json(), expected_extensions);
    assert_eq!(restored.data(), event.data());
    assert!(matches!(
        rows[0].delivery(),
        dovecote::DeliverySnapshot::Pending { attempts, .. } if attempts.get() == 0
    ));
}

#[tokio::test]
async fn data_variants_and_all_tagged_extension_types_round_trip() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("bool").unwrap(),
            ExtensionValue::Boolean(true),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("integer").unwrap(),
            ExtensionValue::Integer(-7),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("string").unwrap(),
            ExtensionValue::string("value").unwrap(),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("binary").unwrap(),
            ExtensionValue::Binary(vec![1, 2, 3]),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("uri").unwrap(),
            ExtensionValue::uri("https://example.test/u").unwrap(),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("reference").unwrap(),
            ExtensionValue::uri_reference("/resource").unwrap(),
        )
        .unwrap();
    extensions
        .insert(
            ExtensionName::new("timestamp").unwrap(),
            ExtensionValue::timestamp(time::OffsetDateTime::UNIX_EPOCH).unwrap(),
        )
        .unwrap();
    let make = |id: &str, data: Option<EventData>, content_type: Option<&str>| {
        let mut builder = NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new(id).unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.data").unwrap(),
        )
        .extensions(extensions.clone());
        // These optional event fields are independent; declaration order is
        // not policy.
        // ast-grep-ignore: rust-if-let-policy-cascade
        if let Some(content_type) = content_type {
            builder = builder.datacontenttype(ContentType::new(content_type).unwrap());
        }

        if let Some(data) = data {
            builder = builder.data(data);
        }
        builder.build().unwrap()
    };

    let events = vec![
        make("absent", None, None),
        make("empty", Some(EventData::binary(Vec::new())), None),
        make(
            "json",
            Some(EventData::json(br#"{"ok":true}"#.to_vec()).unwrap()),
            Some("application/json"),
        ),
        make(
            "binary",
            Some(EventData::binary(vec![0, 255])),
            Some("application/octet-stream"),
        ),
    ];
    let mut transaction = adapter.begin_write().await.unwrap();
    for event in events {
        adapter.enqueue(&mut transaction, event).await.unwrap();
    }
    transaction.commit().await.unwrap();
    let rows = adapter.page(None, Limit::new(10).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter()
            .all(|row| row.event().extensions().iter().count() == 7)
    );
    assert!(rows[0].event().data().is_none());
    assert_eq!(rows[1].event().data().unwrap().as_bytes(), &[] as &[u8]);
    assert!(rows[2].event().data().unwrap().is_json());
    assert_eq!(rows[3].event().data().unwrap().as_bytes(), &[0, 255]);
}

#[tokio::test]
async fn page_rejects_corrupt_durable_event_encoding() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("corrupt"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    sqlx::query("UPDATE dovecote_events SET extensions = '[]'")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        adapter.page(None, Limit::new(1).unwrap()).await,
        Err(dovecote_sqlx_sqlite::PageError::Serialization { .. })
    ));
}

#[tokio::test]
async fn paging_surfaces_orphan_events_live_and_in_a_snapshot() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("orphan"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    sqlx::query("DELETE FROM dovecote_deliveries")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        adapter.page(None, Limit::new(1).unwrap()).await,
        Err(dovecote_sqlx_sqlite::PageError::Serialization { .. })
    ));
    let mut pager = adapter.begin_snapshot().await.unwrap();
    assert!(matches!(
        pager.next_page(Limit::new(1).unwrap()).await,
        Err(dovecote_sqlx_sqlite::PageError::Serialization { .. })
    ));
    pager.rollback().await.unwrap();
}

#[tokio::test]
async fn snapshot_is_finite_and_preserves_database_millisecond_shape() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("one"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let mut pager = adapter.begin_snapshot().await.unwrap();
    let page = pager.next_page(Limit::new(1).unwrap()).await.unwrap();
    assert_eq!(page.len(), 1);
    assert!(page[0].enqueued_at().microsecond() % 1_000 == 0);
    assert!(pager.is_exhausted());
    assert!(
        pager
            .next_page(Limit::new(1).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    pager.finish().await.unwrap();
}

#[tokio::test]
async fn multi_page_snapshot_has_a_finite_upper_bound() {
    let (pool, path) = file_database(Duration::from_secs(1)).await;
    let adapter = SqliteDovecote::new(pool.clone());
    sqlx::query("PRAGMA journal_mode = WAL")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut transaction = adapter.begin_write().await.unwrap();
    for id in ["first", "second", "third"] {
        adapter.enqueue(&mut transaction, event(id)).await.unwrap();
    }
    transaction.commit().await.unwrap();
    let mut pager = adapter.begin_snapshot().await.unwrap();
    let first = pager.next_page(Limit::new(1).unwrap()).await.unwrap();
    assert_eq!(first.len(), 1);

    let mut later = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut later, event("after-snapshot"))
        .await
        .unwrap();
    later.commit().await.unwrap();

    let second = pager.next_page(Limit::new(1).unwrap()).await.unwrap();
    let third = pager.next_page(Limit::new(1).unwrap()).await.unwrap();
    let fourth = pager.next_page(Limit::new(1).unwrap()).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(third.len(), 1);
    assert!(fourth.is_empty());
    assert!(pager.is_exhausted());
    pager.finish().await.unwrap();
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn dropping_snapshot_releases_a_single_pool_connection() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let pager = adapter.begin_snapshot().await.unwrap();
    drop(pager);
    let acquired = tokio::time::timeout(Duration::from_secs(1), pool.acquire()).await;
    assert!(
        acquired.is_ok(),
        "snapshot drop retained the only pool connection"
    );
}

#[tokio::test]
async fn stale_token_is_fenced_and_terminal_state_is_illegal() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("one"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let claimed = adapter
        .claim(
            WorkerId::new("worker-a").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let wrong = dovecote::ClaimToken::from_bytes([7; 16]);
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        claimed.row_id(),
        &wrong,
        MutationExpectation::LostClaim,
    )
    .await;
    adapter
        .quarantine(
            claimed.row_id(),
            claimed.claim_token(),
            &QuarantineReason::new("operator decision").unwrap(),
        )
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        claimed.row_id(),
        claimed.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Quarantined),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_mutations_persist_their_exact_fields_and_database_times() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("fields"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let first = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let old_expiry = first.claim_expires_at();
    adapter
        .renew(
            first.row_id(),
            first.claim_token(),
            Lease::new(Duration::from_secs(10)).unwrap(),
        )
        .await
        .unwrap();
    let renewed: String = sqlx::query_scalar(
        "SELECT claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(first.row_id().get())
    .fetch_one(&pool)
    .await
    .unwrap();
    let renewed =
        time::OffsetDateTime::parse(&renewed, &time::format_description::well_known::Rfc3339)
            .unwrap();
    assert!(renewed > old_expiry);
    assert_eq!(renewed.microsecond() % 1_000, 0);

    let failure = Failure::new("temporary", "retry detail").unwrap();
    adapter
        .retry(
            first.row_id(),
            first.claim_token(),
            &failure,
            Delay::new(Duration::ZERO).unwrap(),
        )
        .await
        .unwrap();
    let retried = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        retried[0].delivery(),
        dovecote::DeliverySnapshot::Pending {
            last_failure: Some(stored), ..
        } if stored == &failure
    ));

    let second = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .release(
            second.row_id(),
            second.claim_token(),
            Delay::new(Duration::ZERO).unwrap(),
        )
        .await
        .unwrap();
    let released = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        released[0].delivery(),
        dovecote::DeliverySnapshot::Pending {
            last_failure: Some(stored), ..
        } if stored == &failure
    ));

    let third = adapter
        .claim(
            WorkerId::new("fields-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .quarantine(
            third.row_id(),
            third.claim_token(),
            &QuarantineReason::new("manual quarantine").unwrap(),
        )
        .await
        .unwrap();
    let quarantined = adapter.page(None, Limit::new(1).unwrap()).await.unwrap();
    assert!(matches!(
        quarantined[0].delivery(),
        dovecote::DeliverySnapshot::Quarantined { reason, .. }
            if reason.as_str() == "manual quarantine"
    ));
}

#[tokio::test]
async fn crash_after_claim_commit_reclaims_and_fences_the_expired_token() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("reclaim"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let first = adapter
        .claim(
            WorkerId::new("reclaim-a").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    // Returning from claim proves that its short BEGIN IMMEDIATE transaction
    // committed. A worker crash now leaves this durable claim for recovery.
    let expired_token = first.claim_token().clone();
    sqlx::query("UPDATE dovecote_deliveries SET claim_expires_at = '1970-01-01T00:00:00.000000Z'")
        .execute(&pool)
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        first.row_id(),
        &expired_token,
        MutationExpectation::LostClaim,
    )
    .await;
    let second = adapter
        .claim(
            WorkerId::new("reclaim-b").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.attempts().get(), 2);
    assert_ne!(&expired_token, second.claim_token());
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        second.row_id(),
        &expired_token,
        MutationExpectation::LostClaim,
    )
    .await;
    adapter
        .ack(second.row_id(), second.claim_token())
        .await
        .unwrap();
}

#[tokio::test]
async fn common_occurrence_time_endpoints_round_trip_and_reject_outside_before_sql() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let minimum = time::OffsetDateTime::UNIX_EPOCH;
    let maximum = time::OffsetDateTime::new_in_offset(
        time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
        time::Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        time::UtcOffset::UTC,
    );
    let timed_event = |id: &str, at: time::OffsetDateTime| {
        NewEvent::builder(
            StreamName::new("audit").unwrap(),
            EventId::new(id).unwrap(),
            EventSource::new("https://example.test/source").unwrap(),
            EventType::new("com.example.time").unwrap(),
        )
        .time(at)
        .build()
    };

    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(
            &mut transaction,
            timed_event("time-minimum", minimum).unwrap(),
        )
        .await
        .unwrap();
    let maximum_outcome = adapter
        .enqueue(
            &mut transaction,
            timed_event("time-maximum", maximum).unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let maximum_id = match maximum_outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };

    let mut replay_transaction = adapter.begin_write().await.unwrap();
    let replay = adapter
        .enqueue(
            &mut replay_transaction,
            timed_event("time-maximum", maximum).unwrap(),
        )
        .await
        .unwrap();
    replay_transaction.commit().await.unwrap();
    assert_eq!(
        replay,
        dovecote::EnqueueOutcome::AlreadyEnqueued { row_id: maximum_id }
    );

    let rows = adapter.page(None, Limit::new(10).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].event().time(), Some(minimum));
    assert_eq!(rows[1].event().time(), Some(maximum));

    // NewEvent validates the common portable range and precision before an
    // adapter transaction is opened, so neither invalid value can reach SQL.
    assert!(
        timed_event(
            "time-before-minimum",
            minimum - time::Duration::microseconds(1)
        )
        .is_err()
    );
    assert!(
        timed_event(
            "time-after-maximum",
            maximum + time::Duration::nanoseconds(1)
        )
        .is_err()
    );
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 2);
}

#[tokio::test]
async fn crash_before_claim_commit_exposes_no_claim() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut setup, event("crash-before-claim"))
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };

    // An uncommitted claim is rolled back when the worker process crashes. An
    // explicit rollback gives that crash boundary a deterministic test shape.
    let mut uncommitted = pool
        .begin_with(AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE dovecote_deliveries SET state = 'claimed', attempts = 1, claim_token = ?, claimed_by = ?, claim_expires_at = ? WHERE event_row_id = ?",
    )
    .bind([0x42_u8; 16].as_slice())
    .bind("crashed-before-commit")
    .bind("9999-12-31T23:59:59.999000Z")
    .bind(row_id.get())
    .execute(&mut *uncommitted)
    .await
    .unwrap();
    uncommitted.rollback().await.unwrap();

    let stored: (String, i64, Option<Vec<u8>>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT state, attempts, claim_token, claimed_by, claim_expires_at FROM dovecote_deliveries WHERE event_row_id = ?",
        )
        .bind(row_id.get())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored, ("pending".to_owned(), 0, None, None, None));

    let recovered = adapter
        .claim(
            WorkerId::new("after-crash").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovered.row_id(), row_id);
    assert_eq!(recovered.attempts().get(), 1);
    adapter.ack(row_id, recovered.claim_token()).await.unwrap();
}

#[tokio::test]
async fn transport_success_before_ack_can_produce_a_reclaimed_duplicate() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("transport-success-before-ack"))
        .await
        .unwrap();
    setup.commit().await.unwrap();

    let claimed = adapter
        .claim(
            WorkerId::new("transport-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let original_row_id = claimed.row_id();
    let original_token = claimed.claim_token().clone();
    let original_event_id = claimed.event().id().clone();

    // The fake transport accepts outside the database transaction, then the
    // worker crashes before ack. Transport success is deliberately not durable.
    let mut transport = FakeTransport::default();
    transport.accept(&claimed);
    drop(claimed);
    let stored: (String, Option<String>, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT state, delivered_at, claim_token FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(original_row_id.get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored.0, "claimed");
    assert!(stored.1.is_none());
    assert_eq!(stored.2, Some(original_token.as_bytes().to_vec()));

    // Recovery sees the expired claim and returns the same durable event with
    // a fresh token: this is the expected possible duplicate consequence.
    sqlx::query(
        "UPDATE dovecote_deliveries SET claim_expires_at = '1970-01-01T00:00:00.000000Z' WHERE event_row_id = ?",
    )
    .bind(original_row_id.get())
    .execute(&pool)
    .await
    .unwrap();
    let reclaimed = adapter
        .claim(
            WorkerId::new("transport-recovery").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reclaimed.row_id(), original_row_id);
    assert_eq!(reclaimed.event().id(), &original_event_id);
    assert_eq!(reclaimed.attempts().get(), 2);
    assert_ne!(reclaimed.claim_token(), &original_token);
    transport.accept(&reclaimed);
    assert_eq!(
        transport.accepted,
        vec![
            (
                "https://example.test/source".to_owned(),
                "transport-success-before-ack".to_owned()
            ),
            (
                "https://example.test/source".to_owned(),
                "transport-success-before-ack".to_owned()
            )
        ]
    );
    assert_eq!(transport.accepted[0], transport.accepted[1]);
    let still_claimed: (String, Option<String>) = sqlx::query_as(
        "SELECT state, delivered_at FROM dovecote_deliveries WHERE event_row_id = ?",
    )
    .bind(reclaimed.row_id().get())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(still_claimed, ("claimed".to_owned(), None));
    adapter
        .ack(reclaimed.row_id(), reclaimed.claim_token())
        .await
        .unwrap();
}

#[tokio::test]
async fn caller_rollback_removes_both_event_and_delivery_rows() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("rolled-back"))
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let delivery_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 0);
    assert_eq!(delivery_count, 0);
}

#[tokio::test]
async fn deferred_enqueue_is_rejected_before_adapter_reads() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = pool.begin().await.unwrap();
    let result = adapter.enqueue(&mut transaction, event("deferred")).await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::EnqueueError::WriteTransactionRequired)
    ));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn deferred_transaction_with_prior_application_write_is_supported() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    sqlx::query("CREATE TABLE application_state (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO application_state (id, value) VALUES (1, 'before enqueue')")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("after-application-write"))
        .await
        .unwrap();
    assert!(matches!(outcome, dovecote::EnqueueOutcome::Enqueued { .. }));
    transaction.commit().await.unwrap();
}

#[tokio::test]
async fn claim_counter_overflow_rolls_back_before_returning() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut transaction, event("overflow"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    sqlx::query("UPDATE dovecote_deliveries SET attempts = ?")
        .bind(i64::MAX)
        .execute(&pool)
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("overflow-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::CounterOverflow { .. })
    ));
    tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query("UPDATE dovecote_deliveries SET attempts = 0").execute(&pool),
    )
    .await
    .expect("claim error left a transaction lock held")
    .unwrap();
}

#[tokio::test]
async fn schema_check_rejects_an_incompatible_index() {
    let pool = database().await;
    sqlx::query("DROP INDEX dovecote_events_source_event_id")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn schema_check_rejects_altered_defaults_and_constraints() {
    let pool = database().await;
    sqlx::query("PRAGMA writable_schema = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE sqlite_master SET sql = REPLACE(sql, 'DEFAULT ''{}''', 'DEFAULT ''[]''') WHERE name = 'dovecote_events'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("PRAGMA writable_schema = OFF")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));

    let pool = database().await;
    sqlx::query("PRAGMA writable_schema = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE sqlite_master SET sql = REPLACE(sql, 'CHECK (attempts >= 0)', 'CHECK (attempts >= 0 OR 1 = 1)') WHERE name = 'dovecote_deliveries'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("PRAGMA writable_schema = OFF")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn database_rejects_invalid_delivery_state_constraints() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("invalid-state"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    assert!(
        sqlx::query("UPDATE dovecote_deliveries SET state = 'invalid' WHERE event_row_id = ?")
            .bind(row_id.get())
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE dovecote_deliveries SET attempts = -1 WHERE event_row_id = ?")
            .bind(row_id.get())
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mutations_classify_missing_and_pending_rows() {
    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let token = dovecote::ClaimToken::from_bytes([3; 16]);
    let missing = dovecote::RowId::new(999).unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        missing,
        &token,
        MutationExpectation::NotFound,
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-delivered"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let delivered = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    let delivered_claim = adapter
        .claim(
            WorkerId::new("classification-delivered").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .ack(delivered, delivered_claim.claim_token())
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        delivered,
        delivered_claim.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Delivered),
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-quarantined"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let quarantined = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    let quarantined_claim = adapter
        .claim(
            WorkerId::new("classification-quarantined").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    adapter
        .quarantine(
            quarantined,
            quarantined_claim.claim_token(),
            &QuarantineReason::new("classification").unwrap(),
        )
        .await
        .unwrap();
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        quarantined,
        quarantined_claim.claim_token(),
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Quarantined),
    )
    .await;

    let mut transaction = adapter.begin_write().await.unwrap();
    let outcome = adapter
        .enqueue(&mut transaction, event("classification-pending"))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let row_id = match outcome {
        dovecote::EnqueueOutcome::Enqueued { row_id } => row_id,
        dovecote::EnqueueOutcome::AlreadyEnqueued { .. } => unreachable!(),
        _ => unreachable!(),
    };
    assert_all_mutation_classifications(
        &adapter,
        &pool,
        row_id,
        &token,
        MutationExpectation::IllegalTransition(dovecote::DeliveryState::Pending),
    )
    .await;
}

#[tokio::test]
async fn schema_check_rejects_domain_triggers() {
    let pool = database().await;
    sqlx::query(
        "CREATE TRIGGER dovecote_events_audit AFTER INSERT ON dovecote_events BEGIN SELECT 1; END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn schema_check_rejects_temporary_domain_triggers() {
    let pool = database().await;
    sqlx::query(
        "CREATE TEMP TRIGGER temporary_dovecote_events_audit AFTER INSERT ON dovecote_events BEGIN SELECT 1; END",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        check_schema(&pool).await,
        Err(dovecote_sqlx_sqlite::SchemaError::MigrationMismatch { .. })
    ));
}

#[tokio::test]
async fn separate_connection_busy_exhaustion_rolls_back_and_then_commits() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let adapter = SqliteDovecote::with_busy_config(
        pool.clone(),
        dovecote_sqlx_sqlite::BusyConfig::new(Duration::ZERO, 0),
    );
    let mut setup = adapter.begin_write().await.unwrap();
    adapter.enqueue(&mut setup, event("busy")).await.unwrap();
    setup.commit().await.unwrap();

    let held = pool
        .begin_with(sqlx::AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("busy-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    held.rollback().await.unwrap();

    let claimed = adapter
        .claim(
            WorkerId::new("after-busy").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempts().get(), 1);
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn busy_commit_is_rolled_back_before_the_next_claim() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let busy = dovecote_sqlx_sqlite::BusyConfig::new(Duration::ZERO, 0);
    let adapter = SqliteDovecote::with_busy_config(pool.clone(), busy);
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("busy-commit"))
        .await
        .unwrap();
    setup.commit().await.unwrap();

    // A rollback-journal reader can coexist with BEGIN IMMEDIATE but blocks
    // its COMMIT. The adapter must explicitly await ROLLBACK before returning
    // the busy error so the same pool connection is immediately reusable.
    let mut reader = pool.begin().await.unwrap();
    sqlx::query("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    let result = adapter
        .claim(
            WorkerId::new("busy-commit-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    reader.rollback().await.unwrap();
    let claimed = adapter
        .claim(
            WorkerId::new("after-busy-commit").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn busy_timeout_and_retry_budget_are_installed_per_operation() {
    let (pool, path) = file_database(Duration::ZERO).await;
    let busy = dovecote_sqlx_sqlite::BusyConfig::new(Duration::from_millis(10), 2);
    assert_eq!(busy.timeout(), Duration::from_millis(10));
    assert_eq!(busy.retries(), 2);
    let adapter = SqliteDovecote::with_busy_config(pool.clone(), busy);
    let mut setup = adapter.begin_write().await.unwrap();
    adapter
        .enqueue(&mut setup, event("busy-budget"))
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let held = pool
        .begin_with(sqlx::AssertSqlSafe("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let result = adapter
        .claim(
            WorkerId::new("busy-budget-worker").unwrap(),
            Lease::new(Duration::from_secs(5)).unwrap(),
            Limit::new(1).unwrap(),
        )
        .await;
    let elapsed = started.elapsed();
    assert!(matches!(
        result,
        Err(dovecote_sqlx_sqlite::ClaimError::BusyExhausted { .. })
    ));
    assert!(elapsed < Duration::from_secs(1));
    held.rollback().await.unwrap();
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn invalid_busy_timeout_is_reported_as_configuration_error() {
    let pool = database().await;
    let adapter = SqliteDovecote::with_busy_config(
        pool,
        dovecote_sqlx_sqlite::BusyConfig::new(Duration::from_nanos(1), 0),
    );
    assert!(matches!(
        adapter.begin_write().await,
        Err(dovecote_sqlx_sqlite::EnqueueError::Configuration { .. })
    ));
}

#[tokio::test]
async fn separate_connections_serialize_claims_without_overlap() {
    let (pool, path) = file_database(Duration::from_millis(100)).await;
    let adapter = SqliteDovecote::new(pool.clone());
    let mut setup = adapter.begin_write().await.unwrap();
    adapter.enqueue(&mut setup, event("one")).await.unwrap();
    adapter.enqueue(&mut setup, event("two")).await.unwrap();
    setup.commit().await.unwrap();
    let worker_a = WorkerId::new("worker-a").unwrap();
    let worker_b = WorkerId::new("worker-b").unwrap();
    let lease = Lease::new(Duration::from_secs(5)).unwrap();
    let limit = Limit::new(1).unwrap();
    let (first, second) = tokio::join!(
        adapter.claim(worker_a, lease, limit),
        adapter.claim(worker_b, lease, limit)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].row_id(), second[0].row_id());
    pool.close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn concurrent_same_identity_enqueue_is_idempotent_without_busy_control_flow() {
    let (pool, path) = file_database(Duration::from_secs(1)).await;
    let adapter = Arc::new(SqliteDovecote::new(pool.clone()));
    let barrier = Arc::new(Barrier::new(2));
    let first_adapter = Arc::clone(&adapter);
    let second_adapter = Arc::clone(&adapter);
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first = event("raced");
    let second = event("raced");
    let (first, second) = tokio::join!(
        async move {
            first_barrier.wait().await;
            let mut transaction = first_adapter.begin_enqueue().await.unwrap();
            let outcome = first_adapter
                .enqueue(&mut transaction, first)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            outcome
        },
        async move {
            second_barrier.wait().await;
            let mut transaction = second_adapter.begin_enqueue().await.unwrap();
            let outcome = second_adapter
                .enqueue(&mut transaction, second)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            outcome
        }
    );
    assert!(matches!(
        (first, second),
        (
            dovecote::EnqueueOutcome::Enqueued { .. },
            dovecote::EnqueueOutcome::AlreadyEnqueued { .. }
        ) | (
            dovecote::EnqueueOutcome::AlreadyEnqueued { .. },
            dovecote::EnqueueOutcome::Enqueued { .. }
        )
    ));

    let barrier = Arc::new(Barrier::new(2));
    let first_adapter = Arc::clone(&adapter);
    let second_adapter = Arc::clone(&adapter);
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let changed_a = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("raced").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.changed-a").unwrap(),
    )
    .unwrap();
    let changed_b = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("raced").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.changed-b").unwrap(),
    )
    .unwrap();
    let (conflict_a, conflict_b) = tokio::join!(
        async move {
            first_barrier.wait().await;
            let mut transaction = first_adapter.begin_enqueue().await.unwrap();
            let result = first_adapter.enqueue(&mut transaction, changed_a).await;
            transaction.rollback().await.unwrap();
            result
        },
        async move {
            second_barrier.wait().await;
            let mut transaction = second_adapter.begin_enqueue().await.unwrap();
            let result = second_adapter.enqueue(&mut transaction, changed_b).await;
            transaction.rollback().await.unwrap();
            result
        }
    );
    assert!(matches!(
        conflict_a,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));
    assert!(matches!(
        conflict_b,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));
    pool.close().await;
    let _ = std::fs::remove_file(path);
}
