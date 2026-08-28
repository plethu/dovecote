//! Helpers shared by the SQLite migration-import concerns.

pub(crate) use super::support::database;
pub(crate) use dovecote::{
    ContentType, EnqueueOutcome, EventData, EventId, EventSource, EventSubject, EventType,
    ExtensionName, ExtensionValue, Extensions, FinalizeOutcome, ImportOutcome,
    ImportedDeliveryState, NewEvent, PartitionKey, RowId, SchemaUri, StreamName, TenantId,
    ValidationKind, ValidationOperation,
};
pub(crate) use dovecote_sqlx_sqlite::{ImportError, MIGRATIONS, SqliteDovecote, check_schema};
pub(crate) use sqlx::{
    SqlitePool, query, query_as, query_scalar, raw_sql, sqlite::SqlitePoolOptions,
};
pub(crate) use std::path::PathBuf;
pub(crate) use std::time::Duration;
pub(crate) use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

#[allow(dead_code)]
pub(crate) trait TestTenantOps {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_sqlite::EnqueueError>;
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_sqlite::ImportError>;
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        row_id: RowId,
        at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_sqlite::FinalizeError>;
}
impl TestTenantOps for SqliteDovecote {
    async fn enqueue<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
    ) -> Result<EnqueueOutcome, dovecote_sqlx_sqlite::EnqueueError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .enqueue(tx, event)
            .await
    }
    async fn import_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        event: NewEvent,
        state: ImportedDeliveryState,
    ) -> Result<ImportOutcome, dovecote_sqlx_sqlite::ImportError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .import_for_migration(tx, event, state)
            .await
    }
    async fn finalize_pending_delivery_for_migration<'c>(
        &self,
        tx: &mut sqlx::Transaction<'c, sqlx::Sqlite>,
        row_id: RowId,
        at: OffsetDateTime,
    ) -> Result<FinalizeOutcome, dovecote_sqlx_sqlite::FinalizeError> {
        self.for_tenant(TenantId::new("test").unwrap())
            .finalize_pending_delivery_for_migration(tx, row_id, at)
            .await
    }
}

pub(crate) fn event(event_id: &str, event_type: &str) -> NewEvent {
    NewEvent::new(
        StreamName::new("migration-test").unwrap(),
        EventId::new(event_id).unwrap(),
        EventSource::new("https://example.test/migration").unwrap(),
        EventType::new(event_type).unwrap(),
    )
    .unwrap()
}

pub(crate) fn rich_event(event_id: &str) -> NewEvent {
    let mut extensions = Extensions::new();
    extensions
        .insert(
            ExtensionName::new("tenant").unwrap(),
            ExtensionValue::string("münchen").unwrap(),
        )
        .unwrap();
    NewEvent::builder(
        StreamName::new("migration-test").unwrap(),
        EventId::new(event_id).unwrap(),
        EventSource::new("https://example.test/migration").unwrap(),
        EventType::new("com.example.rich").unwrap(),
    )
    .subject(EventSubject::new("subject-α").unwrap())
    .time(OffsetDateTime::UNIX_EPOCH)
    .datacontenttype(ContentType::new("application/json").unwrap())
    .dataschema(SchemaUri::new("https://example.test/schema/v1").unwrap())
    .partitionkey(PartitionKey::new("partition-α").unwrap())
    .extensions(extensions)
    .data(EventData::json(r#"{ "name": "café" }"#.as_bytes().to_vec()).unwrap())
    .build()
    .unwrap()
}

pub(crate) fn delivered_at_maximum() -> OffsetDateTime {
    OffsetDateTime::new_in_offset(
        Date::from_calendar_date(9999, Month::December, 31).unwrap(),
        Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
        UtcOffset::UTC,
    )
}

pub(crate) async fn counts(pool: &SqlitePool) -> (i64, i64) {
    (
        query_scalar("SELECT count(*) FROM dovecote_events")
            .fetch_one(pool)
            .await
            .unwrap(),
        query_scalar("SELECT count(*) FROM dovecote_deliveries")
            .fetch_one(pool)
            .await
            .unwrap(),
    )
}

pub(crate) async fn file_database() -> (SqlitePool, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "dovecote-sqlite-import-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("SQLite import-race pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("Dovecote migration");
    check_schema(&pool).await.expect("Dovecote schema");
    (pool, path)
}
