alter table keepsakes
  add constraint keepsakes_expiry_policy_projection
  check (
    coalesce(
      expiry_policy->>'type' in ('manual_only', 'at', 'when_fulfilled')
      and (
        (
          expiry_policy->>'type' = 'at'
          and expires_at is not null
          and (expiry_policy->>'timestamp')::timestamptz = expires_at
        )
        or (
          expiry_policy->>'type' in ('manual_only', 'when_fulfilled')
          and expires_at is null
        )
      ),
      false
    )
  );

alter table keepsakes
  add constraint keepsakes_lifecycle_timestamps
  check (
    coalesce(
      expiry_policy->>'type' in ('manual_only', 'at', 'when_fulfilled')
      and (
        (
          state = 'applied'
          and revoked_at is null
          and fulfilled_at is null
        )
        or (
          state = 'revoked'
          and revoked_at is not null
          and fulfilled_at is null
        )
        or (
          state = 'expired'
          and revoked_at is null
          and (
            (
              expiry_policy->>'type' = 'at'
              and expires_at is not null
              and fulfilled_at is null
            )
            or (
              expiry_policy->>'type' = 'when_fulfilled'
              and fulfilled_at is not null
              and expires_at is null
            )
          )
        )
      ),
      false
    )
  );
