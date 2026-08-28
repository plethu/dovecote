use super::test_support::*;
use std::time::Duration;
use tokio::{sync::oneshot, time::timeout};

#[tokio::test]
async fn competing_imports_have_one_canonical_winner() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, path) = file_database().await;
    let adapter = SqliteDovecote::new(pool.clone()).for_tenant(TenantId::new("test").unwrap());
    let result = async {
        let mut first_transaction = adapter.begin_write().await?;
        let first = adapter
            .import_for_migration(
                &mut first_transaction,
                event("migration-import-race", "com.example.import"),
                ImportedDeliveryState::Pending,
            )
            .await?;
        let row_id = match first {
            ImportOutcome::Imported { row_id } => row_id,
            other => return Err(format!("expected first import, got {other:?}").into()),
        };

        let (before_begin, before_begin_received) = oneshot::channel();
        let second_adapter = adapter.clone();
        let mut second = tokio::spawn(async move {
            before_begin
                .send(())
                .map_err(|_| "race test receiver dropped".to_owned())?;
            let mut transaction = second_adapter
                .begin_write()
                .await
                .map_err(|error| error.to_string())?;
            let outcome = second_adapter
                .import_for_migration(
                    &mut transaction,
                    event("migration-import-race", "com.example.import"),
                    ImportedDeliveryState::Pending,
                )
                .await
                .map_err(|error| error.to_string())?;
            transaction.commit().await.map_err(|error| error.to_string())?;
            Ok::<_, String>(outcome)
        });
        timeout(Duration::from_secs(2), before_begin_received).await??;
        tokio::task::yield_now().await;
        assert!(
            timeout(Duration::from_secs(1), &mut second).await.is_err(),
            "competing writer completed while the first transaction held BEGIN IMMEDIATE"
        );

        // SQLite has one writer at a time. Releasing the first transaction's
        // lock lets the independent writer begin, observe the committed row,
        // and return the canonical idempotent outcome without a sleep.
        first_transaction.commit().await?;
        let second = timeout(Duration::from_secs(5), second).await??
            .map_err(|error| format!("competing importer failed: {error}"))?;
        assert_eq!(second, ImportOutcome::AlreadyImported { row_id });
        assert_eq!(counts(&pool).await, (1, 1));
        let stored: (String, i64, Option<Vec<u8>>, String, String) = query_as(
            "SELECT d.state, d.attempts, d.claim_token, d.available_at, e.enqueued_at FROM dovecote_deliveries d JOIN dovecote_events e ON e.row_id = d.event_row_id WHERE d.event_row_id = ?",
        )
        .bind(row_id.get())
        .fetch_one(&pool)
        .await?;
        assert_eq!(stored.0, "pending");
        assert_eq!(stored.1, 0);
        assert!(stored.2.is_none());
        assert_eq!(stored.3, stored.4);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    pool.close().await;
    let _ = std::fs::remove_file(path);
    result
}
