use super::test_support::*;

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
