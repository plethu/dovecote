use std::time::Duration;

use dovecote::{
    AttemptCount, ClaimToken, Delay, DeliverySnapshot, DeliveryState, Lease, Limit, RowId,
    ValidationKind, WorkerId,
};

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
