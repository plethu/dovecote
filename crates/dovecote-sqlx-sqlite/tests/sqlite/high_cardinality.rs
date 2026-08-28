//! Opt-in deterministic evidence for a bounded, high-cardinality tenant set.
//!
//! This is deliberately ignored: it creates 10,000 tenants and is intended
//! for an explicit local evidence run rather than the ordinary test suite.

use super::test_support::*;

const TENANT_COUNT: usize = 10_000;
const HOT_TENANT_INDEX: usize = TENANT_COUNT / 2;
const HOT_EVENT_COUNT: usize = 64;

#[tokio::test]
#[ignore = "opt-in high-cardinality evidence; set DOVECOTE_HIGH_CARDINALITY=1 and run --ignored"]
async fn ten_thousand_tenants_keep_identity_and_scoped_reads_isolated() {
    if !std::env::var("DOVECOTE_HIGH_CARDINALITY")
        .ok()
        .as_deref()
        .is_some_and(is_truthy)
    {
        eprintln!("skipping SQLite high-cardinality evidence: DOVECOTE_HIGH_CARDINALITY is unset");
        return;
    }

    let pool = database().await;
    let adapter = SqliteDovecote::new(pool.clone());
    let admin = adapter.admin();
    let shared = event("high-cardinality-shared");

    // One explicit write transaction keeps setup bounded while every row
    // still crosses the public tenant-aware enqueue operation.
    let mut transaction = adapter.begin_write().await.unwrap();
    for index in 0..TENANT_COUNT {
        let outcome = admin
            .enqueue(&mut transaction, tenant(index), shared.clone())
            .await
            .unwrap();
        assert!(
            matches!(outcome, EnqueueOutcome::Enqueued { .. }),
            "tenant {index} did not independently enqueue the shared identity: {outcome:?}"
        );
    }
    transaction.commit().await.unwrap();

    let base_counts: (i64, i64) =
        sqlx::query_as("SELECT COUNT(*), COUNT(DISTINCT tenant_id) FROM dovecote_events")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        base_counts,
        (TENANT_COUNT as i64, TENANT_COUNT as i64),
        "the shared CloudEvents source/id must have one row per tenant",
    );

    // A small skewed tenant keeps the fixture useful for selective and hot
    // tenant reads without turning it into a throughput benchmark.
    let hot_tenant = tenant(HOT_TENANT_INDEX);
    let mut transaction = adapter.begin_write().await.unwrap();
    for index in 0..HOT_EVENT_COUNT {
        let outcome = admin
            .enqueue(
                &mut transaction,
                hot_tenant.clone(),
                event(&format!("high-cardinality-hot-{index:03}")),
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, EnqueueOutcome::Enqueued { .. }),
            "hot tenant event {index} was not enqueued: {outcome:?}"
        );
    }
    transaction.commit().await.unwrap();

    let replay_tenant = tenant(HOT_TENANT_INDEX);
    let mut transaction = adapter.begin_write().await.unwrap();
    let replay = admin
        .enqueue(&mut transaction, replay_tenant.clone(), shared.clone())
        .await
        .unwrap();
    assert!(matches!(replay, EnqueueOutcome::AlreadyEnqueued { .. }));
    transaction.commit().await.unwrap();

    let mut transaction = adapter.begin_write().await.unwrap();
    let conflict = admin
        .enqueue(
            &mut transaction,
            replay_tenant,
            NewEvent::new(
                StreamName::new("audit").unwrap(),
                EventId::new("high-cardinality-shared").unwrap(),
                EventSource::new("https://example.test/source").unwrap(),
                EventType::new("com.example.high-cardinality.conflict").unwrap(),
            )
            .unwrap(),
        )
        .await;
    assert!(matches!(
        conflict,
        Err(dovecote_sqlx_sqlite::EnqueueError::IdempotencyConflict { .. })
    ));
    transaction.rollback().await.unwrap();

    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dovecote_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        total_count,
        (TENANT_COUNT + HOT_EVENT_COUNT) as i64,
        "replay and conflict must not add rows",
    );

    for index in [0, HOT_TENANT_INDEX, TENANT_COUNT - 1] {
        let tenant = tenant(index);
        let scoped = adapter.for_tenant(tenant.clone());
        let page = scoped.page(None, Limit::new(2).unwrap()).await.unwrap();
        let expected_page_len = if index == HOT_TENANT_INDEX { 2 } else { 1 };
        assert_eq!(page.len(), expected_page_len);
        assert!(page.iter().all(|row| row.tenant_id() == &tenant));

        let claims = scoped
            .claim(
                WorkerId::new(format!("high-cardinality-worker-{index}")).unwrap(),
                Lease::new(std::time::Duration::from_secs(30)).unwrap(),
                Limit::new(1).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].tenant_id(), &tenant);
        assert_eq!(
            claims[0].event().source().as_str(),
            shared.source().as_str()
        );
    }

    // SQLite's query-plan detail is stable enough to assert both tenant-leading
    // indexes. This records index shape, not a latency SLO.
    let identity_plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN SELECT row_id FROM dovecote_events WHERE tenant_id = ? AND source = ? AND event_id = ?",
    )
    .bind(format!("tenant-{HOT_TENANT_INDEX:05}"))
    .bind(shared.source().as_str())
    .bind(shared.id().as_str())
    .fetch_all(&pool)
    .await
    .unwrap();
    let identity_detail = identity_plan
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    assert!(
        identity_detail.contains("USING INDEX dovecote_events_tenant_source_event_id")
            || identity_detail
                .contains("USING COVERING INDEX dovecote_events_tenant_source_event_id"),
        "identity lookup plan did not use tenant-leading identity index: {identity_detail}"
    );

    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN SELECT row_id FROM dovecote_events WHERE tenant_id = ? AND row_id > ? ORDER BY row_id ASC LIMIT ?",
    )
    .bind(format!("tenant-{HOT_TENANT_INDEX:05}"))
    .bind(0_i64)
    .bind(2_i64)
    .fetch_all(&pool)
    .await
    .unwrap();
    let detail = plan
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    assert!(
        detail.contains("USING INDEX dovecote_events_tenant_row")
            || detail.contains("USING COVERING INDEX dovecote_events_tenant_row"),
        "scoped cursor plan did not use tenant-leading index: {detail}"
    );

    pool.close().await;
}

fn tenant(index: usize) -> TenantId {
    TenantId::new(format!("tenant-{index:05}")).unwrap()
}

fn is_truthy(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "yes")
}
