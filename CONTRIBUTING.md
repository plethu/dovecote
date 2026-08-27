# Contributing to Dovecote

Dovecote is an unreleased 0.1 project. The accepted contract is
[SPEC.md](SPEC.md); the public API, SQL schema, migration rules, and release
evidence are part of the same review surface.

## Before changing the repository

Read the relevant contract and inspect the current consumers before editing a
public type, durable value, migration, or projection. Keep the core crate
runtime-free and SQLx-free. Keep backend-specific transaction and locking
behaviour in its concrete adapter. Do not add a common repository trait just to
make the adapters look alike.

The repository uses the pinned tools in `.mise.toml`:

```sh
mise install
mise run fmt
mise run check
```

The check task includes formatting, structural checks, all-target/all-feature
Clippy, workspace tests, TOML and spelling checks, the SQLite migration smoke
test, the Debezium reference fixture, package archive construction, and a
whitespace check. Run focused tests as well when changing a backend or public
contract. Examples and rustdoc are part of the public API and must compile
under the repository's documented gates.

## Database evidence

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
servers separately; a MySQL result is not MariaDB evidence. The exact required
images, session settings, and current evidence are in the
[support matrix](docs/support-matrix.md). PostgreSQL uses `DOVECOTE_POSTGRES_URL`;
the MySQL/MariaDB adapter uses `DOVECOTE_MYSQL_URL`. `*_REQUIRED=1` makes a
missing URL an error. In CI or release mode, an unset URL is also an error
unless the backend's matching `*_OPTIONAL=1` flag is deliberately set for a
non-target job. SQLite uses its linked SQLx runtime and does not use a URL.

These flags select whether a test may skip; they do not advertise a backend.
Database advertisement additionally requires the exact CI image, conformance
and migration evidence, package verification, and independent review. CDC has
a separate release decision; the checked-in Debezium properties file is a
reference fixture and not live connector evidence.

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

Publish `dovecote` first and wait for crates.io to serve the new version. Then
run `cargo package --package <adapter> --locked` without `--no-verify` for each
SQLx adapter. Publish an adapter only after that check passes.
