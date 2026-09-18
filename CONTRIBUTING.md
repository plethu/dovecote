# Contributing to Dovecote

Dovecote is a pre-1.0 transactional outbox. Start with the
[SQLite example](crates/dovecote-sqlx-sqlite/examples/basic.rs) to follow an event
from enqueue through retry and acknowledgement.

## Setup and checks

The repository uses the pinned tools in `.mise.toml`:

```sh
mise install
mise run fmt
mise run check
```

`mise run check` runs the canonical local gate defined in
[`scripts/check-project-gates.sh`](scripts/check-project-gates.sh). For a focused
test, use `mise exec -- just test <filter> -- --nocapture`; use
`mise exec -- just clippy` for Rust checks.

## Finding your way around

- `crates/dovecote` owns validated events, delivery states, and projections.
- The three `crates/dovecote-sqlx-*` adapters own their database's transactions,
  locking, and schema checks.
- [Architecture](docs/architecture.md) explains those boundaries;
  [SPEC.md](SPEC.md) defines the detailed contracts.
- `tests/fixture-runner` exercises historical Keepsake and Gatekeep migrations.

## Migration fixtures

The standalone migration runner has its own workspace. To check it against this
checkout, first prepare its ignored sibling path from the repository root:

```sh
mkdir -p tests/sibling-worktrees
ln -s ../.. tests/sibling-worktrees/carrier
mise exec -- just check-migration-runner
```

Use the symlink command only when that path is absent; preserve an existing
reviewed checkout. CI supplies the same path explicitly. `just check` runs this
lane when the fixture dependency is available and visibly skips it otherwise.
The complete-history harness also prepares the documented local sibling layout
and validates historical migration hashes before touching a database.

## Database tests

Live database tests are required when a backend claim or release is being
reviewed. Set the URL and the matching required flag explicitly:

```sh
DOVECOTE_POSTGRES_URL=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  DOVECOTE_POSTGRES_REQUIRED=1 \
  cargo test --workspace --all-features

DOVECOTE_MYSQL_URL=mysql://root:password@127.0.0.1:3306/dovecote_test \
  DOVECOTE_MYSQL_REQUIRED=1 \
  cargo test --workspace --all-features
```

The MySQL adapter detects MySQL versus MariaDB from the server. Test those
servers separately; a MySQL result is not MariaDB evidence. The required
server versions and session settings are in the
[support matrix](docs/support-matrix.md). PostgreSQL uses `DOVECOTE_POSTGRES_URL`;
the MySQL/MariaDB adapter uses `DOVECOTE_MYSQL_URL`. `*_REQUIRED=1` makes a
missing URL an error. In CI or release mode, an unset URL is also an error
unless the backend's matching `*_OPTIONAL=1` flag is deliberately set for a
non-target job. SQLite uses its linked SQLx runtime and does not use a URL.

The destructive v1-to-v2 MySQL/MariaDB upgrade fixture is ignored during the
ordinary suite because it drops and recreates its tables. Run it only against a
dedicated disposable database:

```sh
DOVECOTE_MYSQL_TENANT_UPGRADE_URL=mysql://root:password@127.0.0.1:3306/dovecote_upgrade_test \
  cargo test -p dovecote-sqlx-mysql --test mysql tenant_upgrade -- --ignored
```

## Migrations and durable bytes

Never edit a historical published migration in place. Before and after a
change, record byte-level SHA-256 checksums for every historical migration in
the source release line. A new schema change gets a new forward-only migration
and an explicit compatibility and rollback plan.

Migration importers must use the real Keepsake and Gatekeep source schemas,
copy complete history, preserve source identity and event type, and compare
payload length and SHA-256 digests when original bytes exist. JSON values that
were not stored as bytes must use the declared versioned deterministic codec;
do not describe those reconstructed bytes as preserved. Import delivered state
with its authoritative delivery timestamp, never recreate a live legacy claim,
and leave legacy source rows available for rollback and reconciliation.

## Security and review

Do not report an unpatched vulnerability, credential, exploit, or personal data
in a public issue. Use the private GitHub Security Advisory route described in
[SECURITY.md](SECURITY.md); there is no public-issue fallback. Availability of
that route is verified again as a release gate.

Changes to durable state, publication identity, migration semantics, or public
API need focused tests and an independent read-only review. Keep review
findings concrete and tied to files, lines, observable behaviour, or missing
evidence. A green local SQLite run does not stand in for the required server
backend or migration fixture.

Focused fixes, documentation changes, and questions are welcome. Discuss a
change to the durable schema, event identity, delivery state, or publication
ownership before implementation: each one changes the migration and consumer
contract.

AI tools may assist with bounded exploration, implementation, and test
execution. They do not replace a maintainer's direction, authorship, review,
or responsibility for the resulting contract. Check generated changes against
the repository and remove unsupported claims, invented citations, and generic
promotional prose. Do not add agent attribution trailers.

## Publishing

Follow the [release procedure](docs/releases.md) for backend verification,
package checks, and publication order.

## API compatibility

Run `mise exec -- just check-public-api` before changing public APIs. CI runs
this separately from the local `check` gate because it builds published baselines
from crates.io. The pinned versions live in `scripts/check-public-api.sh`;
default, no-default, and all-feature surfaces are compared. Update those
baselines after publication. Use `just check-public-api major` only for an
intentional breaking release; major mode permits breaks and is not a compatibility
check. Database and wire compatibility remain covered by their own tests.
