use super::support::*;

#[tokio::test]
async fn migration_finalization_rejects_noncanonical_rows_and_preflights_schema()
-> Result<(), Box<dyn Error>> {
    let Some(database) = isolated_database().await? else {
        return Ok(());
    };

    let result = async {
        let adapter =
            PostgresDovecote::new(database.pool.clone()).for_tenant(TenantId::new("test").unwrap());
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

#[test]
fn migration_import_state_rejects_submicrosecond_delivery_time() {
    let invalid = time::OffsetDateTime::UNIX_EPOCH
        .replace_nanosecond(1)
        .expect("valid nanosecond");
    assert!(dovecote::ImportedDeliveryState::delivered(invalid).is_err());
}
