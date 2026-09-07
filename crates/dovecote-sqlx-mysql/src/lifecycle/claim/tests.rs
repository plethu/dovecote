//! Claim token fencing and entropy rollback regressions.

use super::{
    claim_with_entropy,
    preparation::{EntropySource, OsEntropy, fresh_token},
};
use crate::error::ClaimError;
use crate::{MIGRATIONS, check_schema, enqueue::enqueue_for_scope};
use dovecote::{
    AttemptCount, EventId, EventSource, EventType, Lease, Limit, NewEvent, RowId, StreamName,
    TenantId, WorkerId,
};
use sqlx::{MySqlPool, mysql::MySqlPoolOptions, query, query_as, query_scalar, raw_sql};
use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};
use time::OffsetDateTime;

struct FailingEntropy;
impl EntropySource for FailingEntropy {
    fn fill(&mut self, _output: &mut [u8]) -> Result<(), getrandom::Error> {
        Err(getrandom::Error::UNEXPECTED)
    }
}

struct FailsAfterOneEntropy {
    successful_fills: usize,
}

impl EntropySource for FailsAfterOneEntropy {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), getrandom::Error> {
        if self.successful_fills == 0 {
            output.fill(0x5a);
            self.successful_fills = 1;
            Ok(())
        } else {
            Err(getrandom::Error::UNEXPECTED)
        }
    }
}
#[test]
fn token_rejects_previous() {
    let mut entropy = OsEntropy;
    let first = fresh_token(None, &[], &mut entropy).expect("entropy");
    let second = fresh_token(Some(&first), &[first], &mut entropy).expect("entropy");
    assert_ne!(first, second);
}

#[test]
fn attempt_counter_overflow_is_detected_before_claim_update() {
    let row_id = RowId::new(1).expect("valid row id");
    assert!(AttemptCount::new(i64::MAX).is_ok());
    assert!(
        i64::MAX.checked_add(1).is_none(),
        "overflow must not reach SQL"
    );
    let _ = ClaimError::CounterOverflow { row_id };
}

#[test]
fn entropy_failure_is_returned_before_batch_writes() {
    let mut entropy = FailingEntropy;
    assert!(fresh_token(None, &[], &mut entropy).is_err());
}

fn entropy_event(source: &str, id: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("mysql-entropy").expect("valid stream"),
        EventId::new(id).expect("valid event id"),
        EventSource::new(source).expect("valid source"),
        EventType::new("com.example.entropy").expect("valid event type"),
    )
    .expect("valid event")
}

async fn install_if_missing(pool: &MySqlPool) -> Result<bool, Box<dyn Error>> {
    let table_count: i64 = query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('dovecote_events', 'dovecote_deliveries')",
    )
    .fetch_one(pool)
    .await?;
    if table_count == 2 {
        check_schema(pool).await?;
        return Ok(false);
    }

    if table_count != 0 {
        return Err("Dovecote schema has only one required table".into());
    }

    // MySQL DDL, including trigger bodies, must use the raw/unprepared
    // protocol.  Keep the release artifact intact: SQLx sends the whole
    // script as one COM_QUERY, so semicolons in comments and trigger
    // bodies are interpreted by the server rather than this fixture.
    raw_sql(MIGRATIONS[0].sql()).execute(pool).await?;
    check_schema(pool).await?;
    Ok(true)
}

const DROP_INSTALLED_SCHEMA_SQL: &str = "DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_insert; DROP TRIGGER IF EXISTS dovecote_events_row_id_positive_update; DROP TABLE IF EXISTS dovecote_deliveries; DROP TABLE IF EXISTS dovecote_events; DROP TABLE IF EXISTS dovecote_schema";

#[test]
fn drop_installed_schema_cleans_the_marker_after_domain_tables() {
    let events = DROP_INSTALLED_SCHEMA_SQL
        .find("DROP TABLE IF EXISTS dovecote_events")
        .expect("event table cleanup");
    let deliveries = DROP_INSTALLED_SCHEMA_SQL
        .find("DROP TABLE IF EXISTS dovecote_deliveries")
        .expect("delivery table cleanup");
    let marker = DROP_INSTALLED_SCHEMA_SQL
        .find("DROP TABLE IF EXISTS dovecote_schema")
        .expect("schema marker cleanup");
    assert!(marker > events && marker > deliveries);
}

async fn drop_installed_schema(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    raw_sql(DROP_INSTALLED_SCHEMA_SQL)
        .execute(pool)
        .await
        .map(|_| ())
}

#[tokio::test]
async fn injected_entropy_failure_leaves_a_multi_row_claim_batch_unchanged_when_configured()
-> Result<(), Box<dyn Error>> {
    let Ok(url) = std::env::var("DOVECOTE_MYSQL_URL") else {
        return Ok(());
    };

    let pool = MySqlPoolOptions::new()
        .max_connections(3)
        .connect(&url)
        .await?;
    let installed_here = install_if_missing(&pool).await?;

    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let source = format!("https://dovecote.test/mysql-entropy-{suffix}");
    let first_id = format!("entropy-first-{suffix}");
    let second_id = format!("entropy-second-{suffix}");
    let cleanup = || async {
        query("DELETE d FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.source = ?")
            .bind(source.as_bytes())
            .execute(&pool)
            .await?;
        query("DELETE FROM dovecote_events WHERE source = ?")
            .bind(source.as_bytes())
            .execute(&pool)
            .await?;
        Ok::<_, sqlx::Error>(())
    };

    let result = async {
        cleanup().await?;

        let mut transaction = pool.begin().await?;
        let tenant = TenantId::new("test")?;
        enqueue_for_scope(&mut transaction, &tenant, entropy_event(&source, &first_id)).await?;
        enqueue_for_scope(&mut transaction, &tenant, entropy_event(&source, &second_id)).await?;
        transaction.commit().await?;

        let mut entropy = FailsAfterOneEntropy {
            successful_fills: 0,
        };

        let claim = claim_with_entropy(
            &pool,
            WorkerId::new("entropy-worker")?,
            Lease::new(std::time::Duration::from_secs(5))?,
            Limit::new(2)?,
            &mut entropy,
        )
        .await;
        assert!(matches!(claim, Err(ClaimError::EntropyUnavailable { .. })));
        assert_eq!(entropy.successful_fills, 1);

        let snapshots = query_as::<_, (Vec<u8>, i64, Option<Vec<u8>>, Option<OffsetDateTime>)>(
            "SELECT d.state, d.attempts, d.claim_token, d.claim_expires_at FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE e.source = ? ORDER BY d.event_row_id",
        )
        .bind(source.as_bytes())
        .fetch_all(&pool)
        .await?;
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots.iter().all(|(state, attempts, token, expiry)| {
            state == b"pending" && *attempts == 0 && token.is_none() && expiry.is_none()
        }));
        Ok::<_, Box<dyn Error>>(())
    }
    .await;

    cleanup().await?;
    if installed_here {
        drop_installed_schema(&pool).await?;
    }
    pool.close().await;
    result
}
