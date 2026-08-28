use std::time::Duration;

use dovecote::{
    AttemptCount, ClaimToken, ClaimedEvent, Delay, DeliverySnapshot, DeliveryState, EventId,
    EventSource, EventType, ImportedDeliveryState, Lease, Limit, NewEvent, PagedEvent, RowId,
    StreamName, TenantId, ValidationKind, WorkerId,
};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

#[test]
fn public_bounds_are_checked_before_adapter_work() {
    assert!(RowId::new(0).is_err());
    assert!(AttemptCount::new(-1).is_err());
    assert!(Limit::new(0).is_err());
    assert!(Limit::new(1_001).is_err());
    assert!(Lease::new(Duration::ZERO).is_err());
    assert!(Lease::new(Duration::from_nanos(1)).is_err());
    assert!(Delay::new(Duration::from_nanos(1)).is_err());
    assert!(WorkerId::new("").is_err());
    assert!(TenantId::new("").is_err());
    assert!(TenantId::new("tenant\n1").is_err());
    assert!(TenantId::new("x".repeat(dovecote::MAX_TENANT_ID_BYTES + 1)).is_err());
}

#[test]
fn state_is_exclusive_and_claim_tokens_are_fixed_width() {
    let token = ClaimToken::from_bytes([7; 16]);
    assert_eq!(token.as_bytes(), &[7; 16]);

    let snapshot = DeliverySnapshot::pending(
        time::OffsetDateTime::UNIX_EPOCH,
        AttemptCount::new(0).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(snapshot.state(), DeliveryState::Pending);
}

#[test]
fn validation_errors_have_typed_categories_and_english_projection() {
    let error = Limit::new(0).unwrap_err();
    assert_eq!(error.kind(), ValidationKind::Range);
    assert_eq!(error.code(), "range");
    assert_eq!(error.category_code(), "invalid_limit");
    assert_eq!(error.to_english(), "limit: is outside the supported range");
}

#[test]
fn state_constructor_timestamps_are_canonical_utc() {
    let local = OffsetDateTime::parse("2026-01-01T01:00:00+01:00", &Rfc3339).unwrap();
    let utc = local.to_offset(UtcOffset::UTC);
    let imported = ImportedDeliveryState::delivered(local).unwrap();
    assert_eq!(imported.delivered_at(), Some(utc));

    let event = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("event-state-time").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .unwrap()
    .into_stored()
    .unwrap();
    let token = ClaimToken::from_bytes([3; 16]);
    let tenant = TenantId::new("tenant-a").unwrap();
    let worker = WorkerId::new("worker").unwrap();
    let snapshot = DeliverySnapshot::claimed(
        local,
        worker.clone(),
        local,
        AttemptCount::new(0).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(snapshot.available_at(), Some(utc));
    assert_eq!(snapshot.claim_expires_at(), Some(utc));

    let claimed = ClaimedEvent::new(
        tenant.clone(),
        RowId::new(1).unwrap(),
        event.clone(),
        AttemptCount::new(0).unwrap(),
        token,
        worker,
        local,
    )
    .unwrap();
    assert_eq!(claimed.claim_expires_at(), utc);

    let paged = PagedEvent::new(
        tenant.clone(),
        RowId::new(1).unwrap(),
        event,
        local,
        snapshot,
    )
    .unwrap();
    assert_eq!(paged.enqueued_at(), utc);
}

#[test]
fn tenant_metadata_is_preserved_on_claimed_and_paged_values() {
    let tenant = TenantId::new("tenant-a").unwrap();
    let event = NewEvent::new(
        StreamName::new("audit").unwrap(),
        EventId::new("tenant-event").unwrap(),
        EventSource::new("https://example.test/source").unwrap(),
        EventType::new("com.example.audit").unwrap(),
    )
    .unwrap()
    .into_stored()
    .unwrap();
    let token = ClaimToken::from_bytes([9; 16]);
    let worker = WorkerId::new("worker").unwrap();
    let claimed = ClaimedEvent::new(
        tenant.clone(),
        RowId::new(1).unwrap(),
        event.clone(),
        AttemptCount::new(1).unwrap(),
        token,
        worker,
        OffsetDateTime::UNIX_EPOCH,
    )
    .unwrap();
    assert_eq!(claimed.tenant_id(), &tenant);

    let delivery = DeliverySnapshot::pending(
        OffsetDateTime::UNIX_EPOCH,
        AttemptCount::new(0).unwrap(),
        None,
    )
    .unwrap();
    let paged = PagedEvent::new(
        tenant.clone(),
        RowId::new(1).unwrap(),
        event,
        OffsetDateTime::UNIX_EPOCH,
        delivery,
    )
    .unwrap();
    assert_eq!(paged.tenant_id(), &tenant);
}

#[test]
fn empty_validation_is_distinct_from_oversized_values() {
    assert_eq!(
        StreamName::new("").unwrap_err().kind(),
        ValidationKind::Empty
    );
    assert_eq!(WorkerId::new("").unwrap_err().kind(), ValidationKind::Empty);
    assert_eq!(
        dovecote::ExtensionName::new("").unwrap_err().kind(),
        ValidationKind::Empty
    );
    assert_eq!(
        StreamName::new("a".repeat(dovecote::MAX_STREAM_BYTES + 1))
            .unwrap_err()
            .kind(),
        ValidationKind::Length
    );
}
