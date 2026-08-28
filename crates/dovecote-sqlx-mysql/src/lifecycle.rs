//! MySQL/MariaDB delivery lifecycle operations.
//!
//! Claim selection and claim-token-fenced mutations live in separate private
//! modules so their transaction and state-transition responsibilities remain
//! easy to inspect without changing the public adapter API.

mod claim;
pub(super) mod mutation;

pub(crate) use claim::claim_for_scope;

pub(super) fn duration_micros(value: std::time::Duration) -> Result<i64, String> {
    i64::try_from(value.as_micros())
        .map_err(|_| "duration does not fit MySQL microseconds".to_owned())
}

pub(super) async fn database_time(
    transaction: &mut sqlx::Transaction<'_, sqlx::MySql>,
) -> Result<time::OffsetDateTime, sqlx::Error> {
    sqlx::query_scalar("SELECT UTC_TIMESTAMP(6)")
        .fetch_one(&mut **transaction)
        .await
}
