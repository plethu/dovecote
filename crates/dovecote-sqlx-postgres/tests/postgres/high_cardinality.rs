//! Opt-in evidence for a bounded, high-cardinality tenant population.
//!
//! This is deliberately ignored: it creates 10,000 tenants and is intended for
//! a disposable PostgreSQL instance, not the ordinary adapter test suite.

use super::support::*;

const TENANT_COUNT: usize = 10_000;
const HOT_TENANT_INDEX: usize = TENANT_COUNT / 2;
const HOT_EVENT_COUNT: usize = 64;

#[tokio::test]
#[ignore = "opt-in high-cardinality evidence; set DOVECOTE_HIGH_CARDINALITY=1 and run --ignored"]
async fn ten_thousand_tenants_keep_identity_and_scoped_reads_isolated() -> Result<(), Box<dyn Error>>
{
    if !env_flag("DOVECOTE_HIGH_CARDINALITY") {
        eprintln!(
            "skipping PostgreSQL high-cardinality evidence: DOVECOTE_HIGH_CARDINALITY is unset"
        );
        return Ok(());
    }

    let Some(database) = isolated_database().await? else {
        return Err(
            "DOVECOTE_HIGH_CARDINALITY=1 requires DOVECOTE_POSTGRES_URL and an available disposable PostgreSQL database"
                .into(),
        );
    };

    let result = async {
        let adapter = PostgresDovecote::new(database.pool.clone());
        let admin = adapter.admin();
        let shared = event("high-cardinality-shared", "com.example.high-cardinality");

        // One caller transaction keeps fixture setup bounded while every row
        // still crosses the public tenant-aware enqueue operation.
        let mut transaction = database.pool.begin().await?;
        for index in 0..TENANT_COUNT {
            let tenant = tenant(index)?;
            let outcome = admin
                .enqueue(&mut transaction, tenant, shared.clone())
                .await?;
            require_enqueued(
                outcome,
                format!(
                    "tenant {index} did not independently enqueue the shared identity"
                ),
            )?;
        }
        transaction.commit().await?;

        let base_counts: (i64, i64) = query_as(
            "SELECT COUNT(*)::bigint, COUNT(DISTINCT tenant_id)::bigint FROM dovecote_events",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            base_counts,
            (TENANT_COUNT as i64, TENANT_COUNT as i64),
            "the shared CloudEvents source/id must have one row per tenant",
        );

        // Keep one intentionally skewed tenant in the same bounded fixture so
        // sampled reads cover both a sparse tenant and a hot tenant.
        let hot_tenant = tenant(HOT_TENANT_INDEX)?;
        let mut transaction = database.pool.begin().await?;
        for index in 0..HOT_EVENT_COUNT {
            let outcome = admin
                .enqueue(
                    &mut transaction,
                    hot_tenant.clone(),
                    event(
                        &format!("high-cardinality-hot-{index:03}"),
                        "com.example.high-cardinality.hot",
                    ),
                )
                .await?;
            require_enqueued(
                outcome,
                format!("hot tenant event {index} was not enqueued"),
            )?;
        }
        transaction.commit().await?;

        let replay_tenant = tenant(HOT_TENANT_INDEX)?;
        let mut transaction = database.pool.begin().await?;
        let replay = admin
            .enqueue(&mut transaction, replay_tenant.clone(), shared.clone())
            .await?;
        assert!(matches!(replay, EnqueueOutcome::AlreadyEnqueued { .. }));
        transaction.commit().await?;

        let mut transaction = database.pool.begin().await?;
        let conflict = admin
            .enqueue(
                &mut transaction,
                replay_tenant,
                event("high-cardinality-shared", "com.example.high-cardinality.conflict"),
            )
            .await;
        assert!(matches!(
            conflict,
            Err(EnqueueError::IdempotencyConflict { .. })
        ));
        transaction.rollback().await?;

        let total_count: i64 = query_scalar("SELECT COUNT(*) FROM dovecote_events")
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(
            total_count,
            (TENANT_COUNT + HOT_EVENT_COUNT) as i64,
            "replay and conflict must not add rows",
        );

        for index in [0, HOT_TENANT_INDEX, TENANT_COUNT - 1] {
            let tenant = tenant(index)?;
            let scoped = adapter.for_tenant(tenant.clone());
            let page = scoped.page(None, Limit::new(2)?).await?;
            let expected_page_len = 1 + usize::from(index == HOT_TENANT_INDEX);
            assert_eq!(page.len(), expected_page_len);
            assert!(page.iter().all(|row| row.tenant_id() == &tenant));

            let claims = scoped
                .claim(
                    WorkerId::new(format!("high-cardinality-worker-{index}"))?,
                    Lease::new(std::time::Duration::from_secs(30))?,
                    Limit::new(1)?,
                )
                .await?;
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].tenant_id(), &tenant);
            assert_eq!(claims[0].event().source().as_str(), shared.source().as_str());
        }

        // The plan assertion is intentionally limited to the stable property:
        // a tenant-leading btree index is available for the scoped cursor.
        // It makes no latency claim; PostgreSQL cost choices are workload- and
        // version-dependent.
        query("ANALYZE dovecote_events")
            .execute(&database.pool)
            .await?;
        let indexes: Vec<(String, String)> = query_as(
            "SELECT index_name, columns FROM (SELECT i.relname AS index_name, array_to_string(ARRAY(SELECT a.attname FROM unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ordinal) JOIN pg_attribute AS a ON a.attrelid = ix.indrelid AND a.attnum = key.attnum WHERE key.attnum > 0 ORDER BY key.ordinal), ',') AS columns FROM pg_index AS ix JOIN pg_class AS i ON i.oid = ix.indexrelid WHERE ix.indrelid = 'dovecote_events'::regclass) AS indexes",
        )
        .fetch_all(&database.pool)
        .await?;
        let identity_index = indexes
            .iter()
            .find(|(_, columns)| columns == "tenant_id,source,event_id")
            .map(|(name, _)| name.clone())
            .ok_or("PostgreSQL schema must expose a tenant-leading identity index")?;
        let tenant_row_index = indexes
            .iter()
            .find(|(_, columns)| columns == "tenant_id,row_id")
            .map(|(name, _)| name.clone())
            .ok_or(
                "PostgreSQL schema must expose a tenant-leading (tenant_id, row_id) event index",
            )?;
        let identity_plan: Vec<String> = query_scalar(
            "EXPLAIN (COSTS OFF) SELECT row_id FROM dovecote_events WHERE tenant_id = $1 AND source = $2 AND event_id = $3",
        )
        .bind(format!("tenant-{HOT_TENANT_INDEX:05}"))
        .bind(shared.source().as_str())
        .bind(shared.id().as_str())
        .fetch_all(&database.pool)
        .await?;
        let identity_plan = identity_plan.join("\n");
        assert!(
            identity_plan.contains(&format!(" using {identity_index}")),
            "identity lookup plan did not use tenant-leading index {identity_index}: {identity_plan}"
        );
        let plan: Vec<String> = query_scalar(
            "EXPLAIN (COSTS OFF) SELECT row_id FROM dovecote_events WHERE tenant_id = $1 AND row_id > $2 ORDER BY row_id ASC LIMIT $3",
        )
        .bind(format!("tenant-{HOT_TENANT_INDEX:05}"))
        .bind(0_i64)
        .bind(2_i64)
        .fetch_all(&database.pool)
        .await?;
        let plan = plan.join("\n");
        assert!(
            plan.contains(&format!(" using {tenant_row_index}")),
            "scoped cursor plan did not use tenant-leading index {tenant_row_index}: {plan}"
        );

        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn tenant(index: usize) -> Result<TenantId, Box<dyn Error>> {
    Ok(TenantId::new(format!("tenant-{index:05}"))?)
}

fn require_enqueued(outcome: EnqueueOutcome, context: String) -> Result<(), Box<dyn Error>> {
    if !matches!(outcome, EnqueueOutcome::Enqueued { .. }) {
        return Err(format!("{context}: {outcome:?}").into());
    }
    Ok(())
}
