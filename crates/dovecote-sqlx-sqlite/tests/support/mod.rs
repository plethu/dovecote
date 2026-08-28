use dovecote_sqlx_sqlite::{MIGRATIONS, check_schema};
use sqlx::{SqlitePool, raw_sql, sqlite::SqlitePoolOptions};

pub(crate) async fn database() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("SQLite pool");
    raw_sql(MIGRATIONS[0].sql())
        .execute(&pool)
        .await
        .expect("Dovecote migration");
    check_schema(&pool).await.expect("Dovecote schema");
    let version: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&pool)
        .await
        .expect("SQLite runtime version");
    assert!(!version.is_empty());
    eprintln!("SQLite linked runtime version: {version}");
    pool
}
