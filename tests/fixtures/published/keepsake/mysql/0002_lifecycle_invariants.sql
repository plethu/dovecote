alter table keepsakes
  add constraint keepsakes_expiry_policy_projection
  check (
    (
      json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'at', 'when_fulfilled')
      and (
        (
          json_unquote(json_extract(expiry_policy, '$.type')) = 'at'
          and expires_at is not null
          and cast(replace(replace(json_unquote(json_extract(expiry_policy, '$.timestamp')), 'T', ' '), 'Z', '') as datetime(6)) = expires_at
        )
        or (
          json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'when_fulfilled')
          and expires_at is null
        )
      )
    ) is true
  );

alter table keepsakes
  add constraint keepsakes_lifecycle_timestamps
  check (
    (
      json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'at', 'when_fulfilled')
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
              json_unquote(json_extract(expiry_policy, '$.type')) = 'at'
              and expires_at is not null
              and fulfilled_at is null
            )
            or (
              json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled'
              and fulfilled_at is not null
              and expires_at is null
            )
          )
        )
      )
    ) is true
  );
