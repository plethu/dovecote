create trigger keepsakes_expiry_policy_projection_insert
before insert on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (
      json_extract(new.expiry_policy, '$.type') = 'at'
      and new.expires_at is not null
      and (
        case
          when instr(json_extract(new.expiry_policy, '$.timestamp'), '.') = 0
          then replace(json_extract(new.expiry_policy, '$.timestamp'), 'Z', '.000000Z')
          else
            substr(
              json_extract(new.expiry_policy, '$.timestamp'),
              1,
              instr(json_extract(new.expiry_policy, '$.timestamp'), '.')
            )
            || substr(
              substr(
                json_extract(new.expiry_policy, '$.timestamp'),
                instr(json_extract(new.expiry_policy, '$.timestamp'), '.') + 1,
                instr(json_extract(new.expiry_policy, '$.timestamp'), 'Z')
                  - instr(json_extract(new.expiry_policy, '$.timestamp'), '.')
                  - 1
              ) || '000000',
              1,
              6
            )
            || 'Z'
        end
      ) = new.expires_at
    )
    or (
      json_extract(new.expiry_policy, '$.type') in ('manual_only', 'when_fulfilled')
      and new.expires_at is null
    )
  )
), 1)
begin
  select raise(abort, 'keepsakes_expiry_policy_projection');
end;

create trigger keepsakes_expiry_policy_projection_update
before update on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (
      json_extract(new.expiry_policy, '$.type') = 'at'
      and new.expires_at is not null
      and (
        case
          when instr(json_extract(new.expiry_policy, '$.timestamp'), '.') = 0
          then replace(json_extract(new.expiry_policy, '$.timestamp'), 'Z', '.000000Z')
          else
            substr(
              json_extract(new.expiry_policy, '$.timestamp'),
              1,
              instr(json_extract(new.expiry_policy, '$.timestamp'), '.')
            )
            || substr(
              substr(
                json_extract(new.expiry_policy, '$.timestamp'),
                instr(json_extract(new.expiry_policy, '$.timestamp'), '.') + 1,
                instr(json_extract(new.expiry_policy, '$.timestamp'), 'Z')
                  - instr(json_extract(new.expiry_policy, '$.timestamp'), '.')
                  - 1
              ) || '000000',
              1,
              6
            )
            || 'Z'
        end
      ) = new.expires_at
    )
    or (
      json_extract(new.expiry_policy, '$.type') in ('manual_only', 'when_fulfilled')
      and new.expires_at is null
    )
  )
), 1)
begin
  select raise(abort, 'keepsakes_expiry_policy_projection');
end;

create trigger keepsakes_lifecycle_timestamps_insert
before insert on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (
      new.state = 'applied'
      and new.revoked_at is null
      and new.fulfilled_at is null
    )
    or (
      new.state = 'revoked'
      and new.revoked_at is not null
      and new.fulfilled_at is null
    )
    or (
      new.state = 'expired'
      and new.revoked_at is null
      and (
        (
          json_extract(new.expiry_policy, '$.type') = 'at'
          and new.expires_at is not null
          and new.fulfilled_at is null
        )
        or (
          json_extract(new.expiry_policy, '$.type') = 'when_fulfilled'
          and new.fulfilled_at is not null
          and new.expires_at is null
        )
      )
    )
  )
), 1)
begin
  select raise(abort, 'keepsakes_lifecycle_timestamps');
end;

create trigger keepsakes_lifecycle_timestamps_update
before update on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (
      new.state = 'applied'
      and new.revoked_at is null
      and new.fulfilled_at is null
    )
    or (
      new.state = 'revoked'
      and new.revoked_at is not null
      and new.fulfilled_at is null
    )
    or (
      new.state = 'expired'
      and new.revoked_at is null
      and (
        (
          json_extract(new.expiry_policy, '$.type') = 'at'
          and new.expires_at is not null
          and new.fulfilled_at is null
        )
        or (
          json_extract(new.expiry_policy, '$.type') = 'when_fulfilled'
          and new.fulfilled_at is not null
          and new.expires_at is null
        )
      )
    )
  )
), 1)
begin
  select raise(abort, 'keepsakes_lifecycle_timestamps');
end;
