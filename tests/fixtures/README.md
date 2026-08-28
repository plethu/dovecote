# Migration fixture reference data

`reconstructed-payload-golden-v1.json` is the independent golden manifest for
the four normalized legacy rows that have no outbox payload. Its output digests
are reproduced from detached bridge-era sources by the checked helper
`check-reconstructed-payload-golden.sh`, using these exact commits:

- Keepsake: `b5d1c1fdebb19164c0c569c75f3a2e21c1c667fc`,
  `crates/keepsake-sqlx/src/repository/dovecote_bridge.rs`.
- Gatekeep: `d7450f2c02e2510da38c5e66e5e55954c3005bd6`,
  `crates/gatekeep-sqlx/src/audit/bridge.rs`.

The helper exports those commits, adds the bridge-era Dovecote 0.1.0 source,
feeds the exact four fixture inputs through the retired public encoders, and
prints the resulting payload SHA-256 values. It never reads the expected output
digests while generating them:

```sh
tests/fixtures/check-reconstructed-payload-golden.sh
```

It also verifies the source symbols before compiling. To perform only the
non-mutating provenance checks against existing checkouts:

```sh
git -C ../keepsake-rs cat-file -e b5d1c1fdebb19164c0c569c75f3a2e21c1c667fc^{commit}
git -C ../keepsake-rs grep -n encode_reconstructed_audit_v1 \
  b5d1c1fdebb19164c0c569c75f3a2e21c1c667fc -- \
  crates/keepsake-sqlx/src/repository/dovecote_bridge.rs
git -C ../gatekeep-rs cat-file -e d7450f2c02e2510da38c5e66e5e55954c3005bd6^{commit}
git -C ../gatekeep-rs grep -n encode_reconstructed_audit_v1 \
  d7450f2c02e2510da38c5e66e5e55954c3005bd6 -- \
  crates/gatekeep-sqlx/src/audit/bridge.rs
```

The complete-history runner independently checks the normalized source-value
digests and the manifest output digests on every backend. The Keepsake source
digest is the SHA-256 of UTF-8 JSON serialized from the explicitly ordered
`LegacyAuditEventV1` input fields: `event_type`, `occurred_at`, actor
`kind`/`id`, `keepsake_id`, subject `kind`/`id`, `relation_id`, `decision`, and
`context`. `occurred_at` is normalized with the RFC 3339 formatter and
`context` is decoded as a sorted string map before serialization. Gatekeep
hashes the UTF-8 JSON serialization of its normalized entry value.
The current 3.0 crates intentionally do not provide the retired codec API.

For manifest-scoped Cargo commands in the local fallback layout, Cargo also
needs the lexical sibling name expected by Gatekeep. The migration harness
creates `tests/sibling-worktrees/keepsake-rs` as an ignored alias while it
runs, and removes an alias it created on exit. To run the same check manually:

```sh
ln -s keepsake tests/sibling-worktrees/keepsake-rs
cargo fmt --manifest-path tests/fixture-runner/Cargo.toml --all -- --check
rm tests/sibling-worktrees/keepsake-rs
```

Refuse this setup if `tests/sibling-worktrees/keepsake-rs` already exists; use
the existing checkout only after confirming it is the intended Keepsake
source. CI continues to use its reviewed sibling checkouts and commit-SHA
guards.
