use super::test_support::*;

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
