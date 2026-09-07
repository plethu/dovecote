# Publishing a Dovecote release

The current candidate is 0.2.2 for `dovecote` and its PostgreSQL, MySQL/MariaDB
and SQLite adapters. It retains the 0.2 public API and durable schema version 2;
no database migration is required from 0.2.1. Historical SQL artifacts remain
unchanged. A dated changelog entry describes the candidate, not evidence that
packages have been published.

## Evidence before publication

Review the final revision and record its canonical CI run. All ten jobs in
`.github/workflows/check.yml` must pass: `postgres`, `mysql-84`,
`mysql-innovation`, `mariadb` and `sqlite`, each on Rust 1.94.0 and stable.
The stable jobs additionally run the complete-history fixtures. MariaDB uses
the documented maintenance-window transition from 10.3.17 to 11.8.6.
The [support matrix](support-matrix.md) records the exact images and settings;
its historical release results do not establish a new candidate's CI status.

Stable jobs require two repository variables containing reviewed, reachable
40-hex commit SHAs:

- `DOVECOTE_KEEPSAKE_BRIDGE_REF`, checked out from `plethu/keepsake`;
- `DOVECOTE_GATEKEEP_BRIDGE_REF`, checked out from `plethu/gatekeep`.

These identify the migration fixture's sibling sources. They are separate from
crates.io versions and from a decision to release either sibling project. The
harness validates historical migration hashes before touching its database;
a source checkout cannot replace the published-artifact compatibility proof.
Keep existing reviewed pins unless the fixture needs a different revision.
Changes to repository variables require their own authorization.

Record the independent review and verify the private reporting route as required
by [SECURITY.md](../SECURITY.md). Inspect the package archives and preserved
migration bytes. Local database, source-dependency and archive results remain
separate from CI and registry evidence. CDC remains unadvertised without its
separate live-connector release evidence.

## Publication order

After the final revision has green CI and the release evidence is complete:

1. Publish `dovecote` and wait until crates.io serves the new version.
2. Run `DOVECOTE_VERIFY_PUBLISHED_ADAPTERS=1 mise run check` with the documented
   backend configuration. This verifies each adapter archive normally against
   the registry core; the ordinary pre-publication gate only constructs those
   adapter archives with `--no-verify`.
3. Publish `dovecote-sqlx-postgres`, `dovecote-sqlx-mysql` and
   `dovecote-sqlx-sqlite`. They depend on the core and may be published in any
   order after their verified packages pass.
4. Confirm that all four versions resolve from the registry, and record the
   final revision, CI run and package results with the release tag and notes.

Keepsake and Gatekeep publication follows their own dependency order. A local
path dependency on Dovecote is not proof that their normalized packages resolve
from crates.io. Do not recreate a package or tag that already exists; inspect
its identity and publication outcome before recovering an interrupted release.
